#!/usr/bin/env python3

import threading
import time
from unittest.mock import Mock, patch

import pytest

from ytmusicfs.thread_manager import ThreadManager


@pytest.fixture
def manager():
    tm = ThreadManager(logger=Mock())
    yield tm
    tm.shutdown(wait=True)


class TestThreadManager:
    def test_init_creates_default_pools(self, manager):
        assert set(manager._pools) == {"io", "api", "extraction", "processing"}
        assert manager.get_pool("io")._max_workers == 8
        assert manager.get_pool("processing")._max_workers == 2

    def test_get_pool_returns_same_pool_for_same_name(self, manager):
        assert manager.get_pool("api") is manager.get_pool("api")

    def test_get_pool_creates_unknown_pool_with_generic_defaults(self, manager):
        pool = manager.get_pool("custom")

        assert pool._max_workers == 4
        assert pool._thread_name_prefix == "custom_pool"

    def test_get_pool_honours_explicit_worker_count_and_prefix(self, manager):
        pool = manager.get_pool("custom", max_workers=1, thread_name_prefix="mine")

        assert pool._max_workers == 1
        assert pool._thread_name_prefix == "mine"

    def test_get_pool_replaces_pool_that_was_shut_down(self, manager):
        old_pool = manager.get_pool("api")
        old_pool.shutdown(wait=True)

        new_pool = manager.get_pool("api")

        assert new_pool is not old_pool
        assert new_pool.submit(lambda: 5).result(timeout=1) == 5

    def test_submit_task_runs_function_and_tracks_completion(self, manager):
        future = manager.submit_task("io", lambda a, b=0: a + b, 2, b=3)

        assert future.result(timeout=1) == 5
        deadline = time.monotonic() + 1
        while manager.get_active_tasks() and time.monotonic() < deadline:
            time.sleep(0.001)
        assert manager.get_active_tasks() == 0

    def test_submit_task_logs_task_exceptions(self, manager):
        def fail():
            raise ValueError("task broke")

        future = manager.submit_task("io", fail)

        with pytest.raises(ValueError, match="task broke"):
            future.result(timeout=1)
        deadline = time.monotonic() + 1
        while manager.get_active_tasks() and time.monotonic() < deadline:
            time.sleep(0.001)
        manager.logger.error.assert_called_once()
        assert "task broke" in manager.logger.error.call_args.args[0]

    def test_create_lock_returns_reentrant_lock(self, manager):
        lock = manager.create_lock()

        with lock, lock:
            pass
        assert lock is not manager.create_lock()

    def test_shutdown_is_clean_and_idempotent(self, manager):
        assert manager.is_shutdown() is False

        assert manager.shutdown(wait=True) is True
        assert manager.is_shutdown() is True
        assert manager.shutdown(wait=True) is True

    def test_shutdown_waits_for_active_tasks_until_timeout(self, manager):
        release = threading.Event()
        manager.submit_task("io", release.wait, 1)

        real_sleep = time.sleep

        def fake_sleep(_seconds):
            release.set()
            real_sleep(0.005)

        with patch("ytmusicfs.thread_manager.time.sleep", side_effect=fake_sleep):
            clean = manager.shutdown(wait=True, timeout=1.0)

        assert clean is True
        assert manager.get_active_tasks() == 0

    def test_shutdown_without_wait_reports_active_tasks(self, manager):
        release = threading.Event()
        started = threading.Event()

        def block():
            started.set()
            release.wait(1)

        future = manager.submit_task("io", block)
        started.wait(1)

        try:
            assert manager.shutdown(wait=False) is False
            manager.logger.warning.assert_called_once()
        finally:
            release.set()
            future.result(timeout=1)

    def test_shutdown_logs_pool_shutdown_errors_and_continues(self, manager):
        broken_pool = Mock(_shutdown=False)
        broken_pool.shutdown.side_effect = RuntimeError("stuck")
        manager._pools["broken"] = broken_pool

        assert manager.shutdown(wait=True) is True
        assert all(
            pool._shutdown for name, pool in manager._pools.items() if name != "broken"
        )
        assert any(
            "broken" in call.args[0] for call in manager.logger.error.call_args_list
        )

    def test_del_shuts_down_pools_when_not_shut_down(self):
        tm = ThreadManager(logger=Mock())
        pools = list(tm._pools.values())

        tm.__del__()

        assert tm.is_shutdown() is True
        assert all(pool._shutdown for pool in pools)

    def test_del_is_noop_after_shutdown(self, manager):
        manager.shutdown(wait=True)
        manager.logger.reset_mock()

        manager.__del__()

        manager.logger.info.assert_not_called()
