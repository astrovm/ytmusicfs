#!/usr/bin/env python3

from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional, Callable
import logging
import threading
import time


class ThreadManager:
    """
    Centralized manager for thread pools and thread synchronization.

    Provides shared thread pools for different types of operations and
    ensures proper shutdown of all thread resources.
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize the ThreadManager.

        Args:
            logger: Logger instance for logging thread-related operations.
        """
        self.logger = logger or logging.getLogger("ThreadManager")
        self.logger.info("Initializing ThreadManager")

        # Thread pools for different operation types
        self._pools: Dict[str, ThreadPoolExecutor] = {}

        # Default pool configurations
        self._default_configs = {
            "io": {"max_workers": 8, "thread_name_prefix": "io_pool"},
            "api": {"max_workers": 4, "thread_name_prefix": "api_pool"},
            "extraction": {"max_workers": 4, "thread_name_prefix": "extract_pool"},
            "processing": {"max_workers": 2, "thread_name_prefix": "process_pool"},
        }

        # Lock for thread pool access
        self._pools_lock = threading.RLock()

        # Shutdown flag
        self._shutdown = False

        # Active tasks tracking
        self._active_tasks = 0
        self._active_tasks_lock = threading.RLock()

        # Create default pools
        for pool_name, config in self._default_configs.items():
            self.get_pool(pool_name, **config)

        self.logger.debug("ThreadManager initialized with default pools")

    def get_pool(self, pool_name: str, **kwargs) -> ThreadPoolExecutor:
        """
        Get or create a thread pool with the specified name.

        Args:
            pool_name: Name of the thread pool.
            **kwargs: Additional arguments for ThreadPoolExecutor.

        Returns:
            The requested ThreadPoolExecutor instance.
        """
        with self._pools_lock:
            if pool_name in self._pools and not self._pools[pool_name]._shutdown:
                return self._pools[pool_name]

            # Create new pool with provided kwargs or defaults
            config = self._default_configs.get(pool_name, {}).copy()
            config.update(kwargs)

            max_workers = config.pop("max_workers", 4)
            thread_name_prefix = config.pop("thread_name_prefix", f"{pool_name}_pool")

            self.logger.debug(
                f"Creating thread pool '{pool_name}' with {max_workers} workers"
            )
            pool = ThreadPoolExecutor(
                max_workers=max_workers, thread_name_prefix=thread_name_prefix, **config
            )
            self._pools[pool_name] = pool
            return pool

    def submit_task(self, pool_name: str, fn: Callable, *args, **kwargs):
        """
        Submit a task to the specified thread pool and track it.

        Args:
            pool_name: Name of the thread pool to use.
            fn: Function to execute.
            *args: Arguments to pass to the function.
            **kwargs: Keyword arguments to pass to the function.

        Returns:
            Future object representing the submitted task.
        """
        pool = self.get_pool(pool_name)

        with self._active_tasks_lock:
            self._active_tasks += 1

        future = pool.submit(fn, *args, **kwargs)

        # Add callback to track task completion
        future.add_done_callback(self._task_done_callback)

        return future

    def _task_done_callback(self, future):
        """
        Callback for when a task is completed.

        Args:
            future: The completed Future object.
        """
        with self._active_tasks_lock:
            self._active_tasks -= 1

        # Check for and log exceptions
        if future.exception():
            self.logger.error(f"Task failed with exception: {future.exception()}")

    def create_lock(self) -> threading.RLock:
        """
        Create a reentrant lock for thread synchronization.

        Returns:
            A new RLock instance.
        """
        return threading.RLock()

    def shutdown(self, wait: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Shutdown all thread pools managed by this ThreadManager.

        Args:
            wait: Whether to wait for tasks to complete before shutdown.
            timeout: Maximum time to wait for tasks to complete (in seconds).
                     None means wait indefinitely.

        Returns:
            True if shutdown was clean, False if there were still active tasks.
        """
        if self._shutdown:
            return True

        self._shutdown = True
        self.logger.info(
            f"Shutting down ThreadManager (wait={wait}, timeout={timeout})"
        )

        if wait and timeout is not None:
            # Wait for active tasks to complete
            start_time = time.time()
            while self._active_tasks > 0 and (time.time() - start_time) < timeout:
                time.sleep(0.1)

        # Shutdown all pools
        with self._pools_lock:
            for pool_name, pool in self._pools.items():
                self.logger.debug(f"Shutting down '{pool_name}' thread pool")
                try:
                    pool.shutdown(wait=wait)
                except Exception as e:
                    self.logger.error(f"Error shutting down '{pool_name}' pool: {e}")

        clean_shutdown = self._active_tasks == 0
        if not clean_shutdown:
            self.logger.warning(
                f"Shutdown with {self._active_tasks} tasks still active"
            )
        else:
            self.logger.info("All thread pools shut down cleanly")

        return clean_shutdown

    def get_active_tasks(self) -> int:
        """
        Get the number of active tasks across all pools.

        Returns:
            The number of active tasks.
        """
        with self._active_tasks_lock:
            return self._active_tasks

    def __del__(self):
        """
        Ensure thread pools are shut down when the manager is deleted.
        """
        if not self._shutdown:
            try:
                self.shutdown(wait=False)
            except Exception:
                pass  # Don't raise exceptions in __del__
