"""Concurrent access to FileHandler.futures (the in-flight stream extractions).

FUSE reads run on several threads, so two reads of one track can both ask for
its stream info at once. These tests force the unlucky interleavings with a
barrier instead of hoping the scheduler produces them.
"""

import contextlib
import errno
import threading
import unittest
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from unittest.mock import patch

from tests.test_file_handler import _RealFileHandlerMixin

# A rendezvous only trips when both threads reach it. Code that serialises the
# two threads never trips it, so the wait is short and the timeout is ignored.
RENDEZVOUS_TIMEOUT = 0.3


def _rendezvous(barrier: threading.Barrier) -> None:
    with contextlib.suppress(threading.BrokenBarrierError):
        barrier.wait(timeout=RENDEZVOUS_TIMEOUT)


class _RendezvousDict(dict):
    """A dict whose membership test pauses until the other thread asks too."""

    def __init__(self, barrier: threading.Barrier) -> None:
        super().__init__()
        self._barrier = barrier

    def __contains__(self, key: object) -> bool:
        found = super().__contains__(key)
        _rendezvous(self._barrier)
        return found


class TestStreamInfoFuturesAreThreadSafe(_RealFileHandlerMixin, unittest.TestCase):
    def _call_twice_at_once(self) -> tuple[list[dict], list[BaseException]]:
        results: list[dict] = []
        errors: list[BaseException] = []

        def call() -> None:
            try:
                results.append(self.handler._get_stream_info("vid"))
            except BaseException as error:
                errors.append(error)

        threads = [threading.Thread(target=call) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
            self.assertFalse(thread.is_alive())
        return results, errors

    def test_two_reads_of_one_track_start_a_single_extraction(self):
        stat_barrier = threading.Barrier(2)
        self.record_stat.side_effect = lambda _name: _rendezvous(stat_barrier)
        self.yt_dlp.extract_stream_url_async.side_effect = (
            lambda _video_id, _browser: self._stream_result()
        )

        results, errors = self._call_twice_at_once()

        self.assertEqual(errors, [])
        self.assertEqual(len(results), 2)
        self.assertEqual(self.yt_dlp.extract_stream_url_async.call_count, 1)
        self.record_stat.assert_called_once_with("stream_extractions")
        self.assertNotIn("vid", self.handler.futures)

    def test_two_waiters_on_one_extraction_both_get_the_result(self):
        self.handler.futures = _RendezvousDict(threading.Barrier(2))
        in_flight = self._stream_result()
        self.handler.futures["vid"] = in_flight
        both_waiting = threading.Barrier(2)
        wait_for_result = in_flight.result

        def result_once_both_are_waiting(timeout: float) -> dict:
            both_waiting.wait(timeout=10)
            return wait_for_result(timeout=timeout)

        in_flight.result = result_once_both_are_waiting  # type: ignore[method-assign]

        results, errors = self._call_twice_at_once()

        self.assertEqual(errors, [])
        self.assertEqual([r["format_id"] for r in results], ["141", "141"])
        self.yt_dlp.extract_stream_url_async.assert_not_called()
        self.assertNotIn("vid", self.handler.futures)

    def test_a_finished_wait_does_not_drop_a_newer_extraction(self):
        newer: Future = Future()
        older = self._stream_result()
        self.handler.futures["vid"] = older

        def replace_while_waiting(timeout: float) -> dict:
            self.handler.futures["vid"] = newer
            return self._stream_result().result()

        older.result = replace_while_waiting  # type: ignore[method-assign]

        self.handler._get_stream_info("vid")

        self.assertIs(self.handler.futures.get("vid"), newer)


class TestFuturesAreClearedWhenAnExtractionIsGivenUp(
    _RealFileHandlerMixin, unittest.TestCase
):
    def _recover(self, error: Exception) -> None:
        file_info = {"video_id": "vid", "status": "ready"}
        with self.assertRaises(OSError):
            self.handler._recover_stream_failure("/a.m4a", file_info, "vid", error)

    def test_a_failed_extraction_is_forgotten_so_the_next_read_retries(self):
        self.handler.futures["vid"] = Future()
        self.handler.futures["other"] = Future()

        self._recover(OSError(errno.EIO, "boom"))

        self.assertEqual(list(self.handler.futures), ["other"])

    def test_a_slow_extraction_is_kept_for_the_next_read_to_wait_on(self):
        slow: Future = Future()
        self.handler.futures["vid"] = slow

        self._recover(FutureTimeoutError())

        self.assertIs(self.handler.futures["vid"], slow)

    def test_auto_repair_forgets_the_old_track_but_not_other_tracks(self):
        self.handler.on_stream_unavailable = lambda _old, _path: "new"
        self.handler.futures["vid"] = Future()
        self.handler.futures["other"] = Future()
        file_info = {"video_id": "vid"}

        with patch.object(self.handler, "_extract_and_apply_stream") as extract:
            repaired = self.handler._attempt_auto_repair("/a.m4a", file_info, "vid")

        self.assertTrue(repaired)
        extract.assert_called_once_with("new", file_info)
        self.assertEqual(list(self.handler.futures), ["other"])
