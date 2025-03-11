#!/usr/bin/env python3

from cachetools import LRUCache
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Optional, Callable
import json
import logging
import threading
import time


class CacheManager:
    """Manager for handling cache operations."""

    def __init__(
        self,
        cache_dir: Optional[str] = None,
        cache_timeout: int = 2592000,
        maxsize: int = 1000,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize the cache manager.

        Args:
            cache_dir: Directory for persistent cache (default: ~/.cache/ytmusicfs)
            cache_timeout: Time in seconds before cached data expires (default: 30 days)
            maxsize: Maximum number of items to keep in memory cache (default: 1000)
            logger: Logger instance to use
        """
        # Set up logger
        self.logger = logger or logging.getLogger("CacheManager")

        # Set up cache directory
        if cache_dir:
            self.cache_dir = Path(cache_dir)
        else:
            self.cache_dir = Path.home() / ".cache" / "ytmusicfs"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Using cache directory: {self.cache_dir}")

        self.cache = LRUCache(
            maxsize=maxsize
        )  # In-memory cache with LRU eviction strategy
        self.cache_timeout = cache_timeout
        self.cache_lock = threading.RLock()  # Lock for cache access

    def get(self, path: str) -> Optional[Any]:
        """Get data from cache if it's still valid.

        Args:
            path: The path to retrieve from cache

        Returns:
            The cached data if valid, None otherwise
        """
        # First check memory cache
        with self.cache_lock:
            if (
                path in self.cache
                and time.time() - self.cache[path]["time"] < self.cache_timeout
            ):
                self.logger.debug(f"Cache hit (memory) for {path}")
                return self.cache[path]["data"]

        # Then check disk cache
        cache_file = self.cache_dir / f"{path.replace('/', '_')}.json"
        if cache_file.exists():
            try:
                with open(cache_file, "r") as f:
                    cache_data = json.load(f)

                if time.time() - cache_data["time"] < self.cache_timeout:
                    self.logger.debug(f"Cache hit (disk) for {path}")
                    # Also update memory cache
                    with self.cache_lock:
                        self.cache[path] = cache_data
                    return cache_data["data"]
            except Exception as e:
                self.logger.debug(f"Failed to read disk cache for {path}: {e}")

        return None

    def set(self, path: str, data: Any) -> None:
        """Set data in both memory and disk cache.

        Args:
            path: The path to cache
            data: The data to cache
        """
        cache_entry = {"data": data, "time": time.time()}

        # Update memory cache (with lock)
        with self.cache_lock:
            self.cache[path] = cache_entry

        # Update disk cache
        try:
            cache_file = self.cache_dir / f"{path.replace('/', '_')}.json"
            with open(cache_file, "w") as f:
                json.dump(cache_entry, f)
        except Exception as e:
            self.logger.warning(f"Failed to write disk cache for {path}: {e}")

    def delete(self, path: str) -> None:
        """Delete data from cache.

        Args:
            path: The path to delete from cache
        """
        with self.cache_lock:
            if path in self.cache:
                del self.cache[path]

        # Also try to delete from disk cache
        try:
            cache_file = self.cache_dir / f"{path.replace('/', '_')}.json"
            if cache_file.exists():
                cache_file.unlink()
        except Exception as e:
            self.logger.warning(f"Failed to delete disk cache for {path}: {e}")

    def get_metadata(self, cache_key: str, metadata_key: str) -> Any:
        """Get metadata associated with a cache key.

        Args:
            cache_key: The cache key
            metadata_key: The metadata key

        Returns:
            The metadata value or None if not found
        """
        metadata_cache_key = f"{cache_key}_metadata"
        metadata = self.get(metadata_cache_key) or {}
        return metadata.get(metadata_key)

    def set_metadata(self, cache_key: str, metadata_key: str, value: Any) -> None:
        """Set metadata associated with a cache key.

        Args:
            cache_key: The cache key
            metadata_key: The metadata key
            value: The metadata value to set
        """
        metadata_cache_key = f"{cache_key}_metadata"
        metadata = self.get(metadata_cache_key) or {}
        metadata[metadata_key] = value
        self.set(metadata_cache_key, metadata)

    @contextmanager
    def cached_data(self, cache_key: str, fetch_func: Callable, *args, **kwargs):
        """Manage cache fetching and updating.

        Args:
            cache_key: The cache key to use.
            fetch_func: Function to fetch data if not cached.
            *args, **kwargs: Arguments for fetch_func.

        Yields:
            The cached or freshly fetched data.
        """
        data = self.get(cache_key)
        if data is None:
            self.logger.debug(f"Cache miss for {cache_key}, fetching data")
            data = fetch_func(*args, **kwargs)
            self.set(cache_key, data)
        yield data
