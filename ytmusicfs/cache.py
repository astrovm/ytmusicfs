#!/usr/bin/env python3

from cachetools import LRUCache
from pathlib import Path
from typing import Any, Optional, Callable, Dict, List
import hashlib
import json
import logging
import os
import re
import sqlite3
import threading
import time


class CacheManager:
    """Manager for handling cache operations."""

    def __init__(
        self,
        cache_dir: Optional[str] = None,
        cache_timeout: int = 2592000,
        maxsize: int = 10000,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize the cache manager.

        Args:
            cache_dir: Directory for persistent cache (default: ~/.cache/ytmusicfs)
            cache_timeout: Time in seconds before cached data expires (default: 30 days)
            maxsize: Maximum number of items to keep in memory cache (default: 10000)
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

        # Initialize a single lock for most operations
        self.lock = threading.RLock()

        # Initialize SQLite database
        self.db_path = self.cache_dir / "cache.db"
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL;")

        # Create a single cursor for all operations
        with self.lock:
            self.cursor = self.conn.cursor()

            # Check if we need to migrate the schema
            self.cursor.execute("PRAGMA table_info(cache_entries)")
            columns = [column[1] for column in self.cursor.fetchall()]

            if "cache_entries" not in columns:
                # Create the table if it doesn't exist
                self.cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS cache_entries (
                        key TEXT PRIMARY KEY,
                        entry TEXT,
                        entry_type TEXT CHECK(entry_type IN ('file', 'directory')) DEFAULT 'directory'
                    )
                """
                )
            elif "entry_type" not in columns:
                # Alter the table to add the entry_type column if needed
                self.logger.info("Migrating database schema to add entry_type column")
                self.cursor.execute(
                    """
                    ALTER TABLE cache_entries
                    ADD COLUMN entry_type TEXT CHECK(entry_type IN ('file', 'directory')) DEFAULT 'directory'
                    """
                )

            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS hash_mappings (
                    hashed_key TEXT PRIMARY KEY,
                    original_path TEXT
                )
            """
            )
            self.conn.commit()

        self.cache = LRUCache(
            maxsize=maxsize
        )  # In-memory cache with LRU eviction strategy
        self.cache_timeout = cache_timeout

        # Add a set to track valid paths in memory for faster lookup
        self.valid_paths = set()
        self._load_valid_paths()  # Load valid paths from SQLite at startup

    def _load_valid_paths(self) -> None:
        """Load valid paths from SQLite into memory."""
        self.logger.debug("Loading valid paths from SQLite into memory...")
        valid_paths_count = 0

        # Initialize a dictionary to store path types
        if not hasattr(self, "path_types"):
            self.path_types = {}

        with self.lock:
            try:
                # Load paths from valid_dir: entries
                self.cursor.execute(
                    "SELECT key, entry_type FROM cache_entries WHERE key LIKE 'valid_dir:%'"
                )
                for row in self.cursor.fetchall():
                    path = self.key_to_path(row[0].replace("valid_dir:", ""))
                    self.valid_paths.add(path)
                    if row[1]:  # If entry_type exists
                        self.path_types[path] = row[1]
                    valid_paths_count += 1

                # Also load paths from exact_path: entries
                self.cursor.execute(
                    "SELECT key, entry_type FROM cache_entries WHERE key LIKE 'exact_path:%'"
                )
                for row in self.cursor.fetchall():
                    path = self.key_to_path(row[0].replace("exact_path:", ""))
                    self.valid_paths.add(path)
                    if row[1]:  # If entry_type exists
                        self.path_types[path] = row[1]
                    valid_paths_count += 1

                self.logger.info(f"Loaded {valid_paths_count} valid paths into memory")
            except sqlite3.Error as e:
                self.logger.warning(
                    f"Failed to load valid paths: {e.__class__.__name__}: {e}"
                )

    def add_valid_path(self, path: str, is_directory: bool = None) -> None:
        """Add a path to the valid paths set and persist it.

        Args:
            path: The path to mark as valid
            is_directory: If specified, whether this path represents a directory
        """
        # Add to in-memory set with minimal locking
        add_to_cache = False
        with self.lock:
            if path not in self.valid_paths:
                self.valid_paths.add(path)
                add_to_cache = True

        # Only persist to database if needed
        if add_to_cache:
            # Use appropriate key prefix based on directory type
            key_prefix = "valid_dir:" if is_directory else "exact_path:"
            self.set(f"{key_prefix}{path}", True, is_directory=is_directory)

    def remove_valid_path(self, path: str) -> None:
        """Remove a path from the valid paths set.

        Args:
            path: The path to remove from valid paths
        """
        # Remove from in-memory set
        with self.lock:
            if path in self.valid_paths:
                self.valid_paths.remove(path)

        # Also remove from database
        self.delete(f"valid_dir:{path}")
        self.delete(f"exact_path:{path}")

    def is_valid_path(self, path: str) -> bool:
        """Check if a path is in the valid paths set.

        Args:
            path: The path to check

        Returns:
            True if the path is valid, False otherwise
        """
        # Fast path for common static paths - avoid cache lookup
        if path == "/":
            return True

        # Common top-level paths that are always valid
        if path in ["/playlists", "/liked_songs", "/albums"]:
            return True

        # Check in the precomputed valid paths set with minimal lock time
        with self.lock:
            return path in self.valid_paths

    def add_valid_dir(self, dir_path: str) -> None:
        """Add a directory to valid paths and mark it in the cache.

        This is a convenience method that updates both the in-memory set
        and the cached valid_dir marker.

        Args:
            dir_path: The directory path to mark as valid
        """
        self.add_valid_path(dir_path, is_directory=True)

    def add_valid_file(self, file_path: str) -> None:
        """Add a file to valid paths and mark it in the cache.

        This is a convenience method that updates both the in-memory set
        and the cached exact_path marker.

        Args:
            file_path: The file path to mark as valid
        """
        self.add_valid_path(file_path, is_directory=False)
        self.set(f"exact_path:{file_path}", True, is_directory=False)

    def path_to_key(self, path: str) -> str:
        """Convert a filesystem path to a cache key.

        Args:
            path: The filesystem path

        Returns:
            Sanitized cache key suitable for database storage
        """
        # Simple sanitization: replace slashes and special characters
        key = path.replace("/", "_").replace("'", "''").replace(" ", "_")

        # Only hash if the key is too long (> 200 chars)
        MAX_KEY_LENGTH = 200
        if len(key) > MAX_KEY_LENGTH:
            # Get the first 30 chars for readability
            prefix = key[:30]
            # Hash the full path for uniqueness
            path_hash = hashlib.md5(path.encode("utf-8")).hexdigest()
            # Create a new key with prefix and hash
            key = f"{prefix}_{path_hash}"

            # Store the mapping for this hash for potential recovery
            self._store_hash_mapping(key, path)

        return key

    def key_to_path(self, key: str) -> str:
        """Convert a cache key back to a filesystem path.

        Args:
            key: The cache key

        Returns:
            Original filesystem path
        """
        # Fast path: most keys aren't hashed
        if (
            "_" in key
            and len(key) > 30
            and key[30:31] == "_"
            and len(key) - key.rfind("_") == 33
        ):
            # This is a hashed key, try to look up the original path
            original_path = self.get_original_path(key)
            if original_path:
                return original_path
            self.logger.warning(f"Cannot convert hashed key back to path: {key}")
            return key  # Return as is if we can't reconstruct the original

        # Regular key - just replace underscores with slashes and restore quotes
        path = key.replace("_", "/")
        path = path.replace("''", "'")
        return path

    def get(self, path: str, include_metadata: bool = False) -> Optional[Any]:
        """Get data from cache if it's still valid.

        Args:
            path: The path to retrieve from cache
            include_metadata: If True, return (data, metadata) tuple

        Returns:
            The cached data if valid, or (data, metadata) tuple if include_metadata is True, None otherwise
        """
        # First check memory cache with minimal lock time
        with self.lock:
            if path in self.cache:
                cache_entry = self.cache[path]
                if time.time() - cache_entry["time"] < self.cache_timeout:
                    self.logger.debug(f"Cache hit (memory) for {path}")
                    if include_metadata:
                        metadata = self.get(f"{path}_search_metadata")
                        return cache_entry["data"], metadata
                    return cache_entry["data"]

        # Then check SQLite cache (outside the lock)
        key = self.path_to_key(path)
        cache_data = None

        with self.lock:
            try:
                self.cursor.execute(
                    "SELECT entry FROM cache_entries WHERE key = ?", (key,)
                )
                row = self.cursor.fetchone()
                if row:
                    cache_data = json.loads(row[0])
            except sqlite3.Error as e:
                self.logger.debug(
                    f"Failed to read database cache for {path}: {e.__class__.__name__}: {e}"
                )

        # Process the SQLite result outside both locks
        if cache_data and time.time() - cache_data["time"] < self.cache_timeout:
            self.logger.debug(f"Cache hit (disk) for {path}")
            # Update memory cache with minimal lock time
            with self.lock:
                self.cache[path] = cache_data
            if include_metadata:
                metadata = self.get(f"{path}_search_metadata")
                return cache_data["data"], metadata
            return cache_data["data"]

        return None

    def set(self, path: str, data: Any, is_directory: bool = None) -> None:
        """Set data in both memory and SQLite cache.

        Args:
            path: The path to cache
            data: The data to cache
            is_directory: If specified, whether this path represents a directory
        """
        # Prepare cache entry outside of lock
        cache_entry = {"data": data, "time": time.time()}

        # Update memory cache with minimal lock time
        with self.lock:
            self.cache[path] = cache_entry

        # Prepare SQLite data outside of lock
        key = self.path_to_key(path)
        entry_str = json.dumps(cache_entry)

        # Determine entry_type if provided
        entry_type = None
        if is_directory is not None:
            entry_type = "directory" if is_directory else "file"

        # Update SQLite cache with a dedicated lock
        with self.lock:
            try:
                if entry_type is not None:
                    self.cursor.execute(
                        """
                        INSERT OR REPLACE INTO cache_entries (key, entry, entry_type)
                        VALUES (?, ?, ?)
                        """,
                        (key, entry_str, entry_type),
                    )
                else:
                    self.cursor.execute(
                        """
                        INSERT OR REPLACE INTO cache_entries (key, entry)
                        VALUES (?, ?)
                        """,
                        (key, entry_str),
                    )
                self.conn.commit()
            except sqlite3.Error as e:
                self.logger.warning(
                    f"Failed to write database cache for {path}: {e.__class__.__name__}: {e}"
                )

    def delete(self, path: str) -> None:
        """Delete data from cache.

        Args:
            path: The path to delete from cache
        """
        with self.lock:
            if path in self.cache:
                del self.cache[path]

        # Also delete from SQLite cache with a dedicated lock
        key = self.path_to_key(path)
        with self.lock:
            try:
                self.cursor.execute("DELETE FROM cache_entries WHERE key = ?", (key,))
                self.conn.commit()
            except sqlite3.Error as e:
                self.logger.warning(
                    f"Failed to delete from database cache for {path}: {e.__class__.__name__}: {e}"
                )

    def get_keys_by_pattern(self, pattern: str) -> List[str]:
        """Get all cache keys matching a pattern.

        Args:
            pattern: The pattern to match, e.g. "valid_dir:/search/*"

        Returns:
            List of matching cache keys
        """
        matching_keys = []
        pattern_re = pattern.replace("*", ".*")

        # First check memory cache with minimal lock time
        memory_keys = []
        with self.lock:
            memory_keys = list(self.cache.keys())

        # Process keys outside the lock
        for key in memory_keys:
            if re.match(f"^{pattern_re}$", key):
                matching_keys.append(key)

        # Also check SQLite database for keys not in memory
        db_keys = []
        with self.lock:
            try:
                # Fetch all keys from the database
                self.cursor.execute("SELECT key FROM cache_entries")
                db_keys = [row[0] for row in self.cursor.fetchall()]
            except sqlite3.Error as e:
                self.logger.warning(
                    f"Failed to get keys matching pattern {pattern}: {e.__class__.__name__}: {e}"
                )

        # Process database keys outside the lock
        for key in db_keys:
            # For hashed keys, get the original path and check against pattern
            is_hashed = (
                "_" in key
                and len(key) > 30
                and key[30:31] == "_"
                and len(key) - key.rfind("_") == 33
            )
            if is_hashed:
                original_path = self.get_original_path(key)
                if original_path and re.match(f"^{pattern_re}$", original_path):
                    if original_path not in matching_keys:
                        matching_keys.append(original_path)
            else:
                # For regular keys, convert back to path and check against pattern
                path = self.key_to_path(key)
                if re.match(f"^{pattern_re}$", path) and path not in matching_keys:
                    matching_keys.append(path)

        self.logger.debug(
            f"Found {len(matching_keys)} keys matching pattern: {pattern}"
        )
        return matching_keys

    def delete_pattern(self, pattern: str) -> int:
        """Delete all cache keys matching a pattern.

        Args:
            pattern: The pattern to match, e.g. "/search/*"

        Returns:
            Number of cache keys deleted
        """
        count = 0
        pattern_re = pattern.replace("*", ".*")

        # First identify keys to delete from memory cache (minimal lock time)
        keys_to_delete = []
        with self.lock:
            # Find all matching keys in memory cache
            keys_to_delete = [
                k for k in self.cache.keys() if re.match(f"^{pattern_re}$", k)
            ]

        # Then delete them with minimal lock time
        if keys_to_delete:
            with self.lock:
                for key in keys_to_delete:
                    if key in self.cache:  # Check again in case it was removed
                        del self.cache[key]
                        count += 1

        # Get all matching paths - leveraging our updated get_keys_by_pattern method
        # This is done outside of locks
        matching_paths = self.get_keys_by_pattern(pattern)

        # Delete each matching path from the database within a single transaction
        if matching_paths:
            with self.lock:
                try:
                    for path in matching_paths:
                        key = self.path_to_key(path)
                        self.cursor.execute(
                            "DELETE FROM cache_entries WHERE key = ?", (key,)
                        )
                        count += 1
                    self.conn.commit()
                except sqlite3.Error as e:
                    self.logger.warning(
                        f"Failed to delete pattern {pattern} from database: {e.__class__.__name__}: {e}"
                    )

        self.logger.debug(f"Deleted {count} cache entries matching pattern: {pattern}")
        return count

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

    def _store_hash_mapping(self, hashed_key: str, original_path: str) -> None:
        """Store a mapping between a hashed key and its original path.

        Args:
            hashed_key: The hashed key created for a long path
            original_path: The original path that was hashed
        """
        try:
            # Store in memory for quick lookups
            with self.lock:
                if not hasattr(self, "hash_to_path"):
                    self.hash_to_path = {}
                self.hash_to_path[hashed_key] = original_path

            # Store in SQLite database with a dedicated lock
            with self.lock:
                self.cursor.execute(
                    """
                    INSERT OR REPLACE INTO hash_mappings (hashed_key, original_path)
                    VALUES (?, ?)
                    """,
                    (hashed_key, original_path),
                )
                self.conn.commit()

        except sqlite3.Error as e:
            self.logger.warning(
                f"Failed to store hash mapping: {e.__class__.__name__}: {e}"
            )

    def get_original_path(self, hashed_key: str) -> Optional[str]:
        """Retrieve the original path for a hashed key.

        Args:
            hashed_key: The hashed key to look up

        Returns:
            The original path if found, None otherwise
        """
        # First check in-memory cache
        with self.lock:
            if hasattr(self, "hash_to_path") and hashed_key in self.hash_to_path:
                return self.hash_to_path[hashed_key]

        # Then check in SQLite database with a dedicated lock
        with self.lock:
            try:
                self.cursor.execute(
                    "SELECT original_path FROM hash_mappings WHERE hashed_key = ?",
                    (hashed_key,),
                )
                row = self.cursor.fetchone()
                if row:
                    original_path = row[0]
                    # Also update in-memory cache for next time
                    with self.lock:
                        if not hasattr(self, "hash_to_path"):
                            self.hash_to_path = {}
                        self.hash_to_path[hashed_key] = original_path
                    return original_path
            except sqlite3.Error as e:
                self.logger.warning(
                    f"Failed to retrieve hash mapping: {e.__class__.__name__}: {e}"
                )

        return None

    def auto_refresh_cache(
        self, cache_key: str, refresh_method: Callable, refresh_interval: int = 600
    ) -> bool:
        """Automatically refresh the cache if the last refresh time exceeds the interval.

        Args:
            cache_key: The cache key to check.
            refresh_method: Method to call to refresh the cache.
            refresh_interval: Time in seconds before triggering a refresh (default: 600).

        Returns:
            True if cache was refreshed, False otherwise
        """
        # Check refresh time outside of any locks
        last_refresh_time = self.get_metadata(cache_key, "last_refresh_time")
        current_time = time.time()

        # Determine if refresh is needed outside of locks
        refresh_needed = (
            last_refresh_time is None
            or (current_time - last_refresh_time) > refresh_interval
        )

        if refresh_needed:
            self.logger.info(f"Auto-refreshing cache for {cache_key}")
            # Call refresh method outside of locks
            refresh_method()

            # Only update metadata after refresh completes
            self.set_metadata(cache_key, "last_refresh_time", current_time)
            return True

        return False

    def refresh_cache(
        self, cache_key: str, fetch_func: Callable, limit: int = 10000
    ) -> bool:
        """Refresh cache data for a given key using the provided fetch function.

        Args:
            cache_key: The cache key to refresh (e.g., '/playlists', '/albums')
            fetch_func: Function to fetch new data
            limit: Maximum number of items to fetch (default: 10000)

        Returns:
            True if refresh succeeded, False otherwise
        """
        self.logger.info(f"Refreshing cache for {cache_key}")
        try:
            data = fetch_func(limit=limit)
            if data:
                self.set(cache_key, data)
                self.logger.info(f"Refreshed {cache_key} with {len(data)} items")
                return True
            else:
                self.logger.warning(f"No data returned for {cache_key}")
                return False
        except Exception as e:
            self.logger.error(f"Failed to refresh {cache_key}: {e}")
            return False

    def clean_path_metadata(self, path: str) -> None:
        """Clean up all cache metadata entries related to a specific path.

        This is particularly useful when forcing recreation of a path that might
        have stale cache entries preventing it from being recognized as new.

        Args:
            path: The path to clean from the cache
        """
        self.logger.debug(f"Cleaning all path metadata for: {path}")

        # List of all cache keys that might affect path existence checks
        key_patterns = [
            f"valid_dir:{path}",
            f"exact_path:{path}",
            f"valid_path:{path}",
            f"{path}_metadata",
            f"{path}_processed",
        ]

        # Clean up parent directory metadata that might reference this path
        parent_dir = os.path.dirname(path)
        if parent_dir:
            filename = os.path.basename(path)
            key_patterns.extend(
                [
                    f"valid_files:{parent_dir}",
                    f"valid_base_names:{parent_dir}",
                    f"{parent_dir}_listing_with_attrs",  # Add directory listing with attributes
                ]
            )

            # Also clean up the file from the directory listing with attributes if it exists
            dir_listing = self.get_directory_listing_with_attrs(parent_dir)
            if dir_listing and filename in dir_listing:
                self.logger.debug(
                    f"Removing {filename} from {parent_dir} directory listing with attributes"
                )
                del dir_listing[filename]
                self.set_directory_listing_with_attrs(parent_dir, dir_listing)

        # Delete each key
        for key_pattern in key_patterns:
            # Delete exact match first
            self.delete(key_pattern)

            # Then delete any pattern match (for wildcards)
            if "*" in key_pattern:
                self.delete_pattern(key_pattern)

        # Remove from in-memory valid paths set
        with self.lock:
            if path in self.valid_paths:
                self.valid_paths.remove(path)

        self.logger.debug(f"Finished cleaning path metadata for: {path}")

    def get_directory_listing_with_attrs(
        self, path: str
    ) -> Optional[Dict[str, Dict[str, Any]]]:
        """Get directory listing with file attributes.

        Args:
            path: Directory path

        Returns:
            Dictionary mapping filenames to their attributes, or None if not cached
        """
        # Use the main lock for thread safety
        with self.lock:
            return self.get(f"{path}_listing_with_attrs")

    def set_directory_listing_with_attrs(
        self, path: str, listing_with_attrs: Dict[str, Dict[str, Any]]
    ) -> None:
        """Set directory listing with file attributes.

        Args:
            path: Directory path
            listing_with_attrs: Dictionary mapping filenames to their attributes
        """
        # Use the main lock for thread safety
        with self.lock:
            self.set(f"{path}_listing_with_attrs", listing_with_attrs)

    def get_file_attrs_from_parent_dir(self, path: str) -> Optional[Dict[str, Any]]:
        """Try to get file attributes from the parent directory's cached listing.

        Args:
            path: File path

        Returns:
            File attributes if found in parent directory cache, None otherwise
        """
        parent_dir = os.path.dirname(path)
        filename = os.path.basename(path)

        # Get the cached directory listing with attributes
        dir_listing = self.get_directory_listing_with_attrs(parent_dir)

        # Return the file's attributes if found
        if dir_listing and filename in dir_listing:
            self.logger.debug(f"Found attributes for {filename} in {parent_dir} cache")
            return dir_listing[filename]

        return None

    def update_file_attrs_in_parent_dir(self, path: str, attrs: Dict[str, Any]) -> None:
        """Update file attributes in the parent directory's cached listing.

        Args:
            path: File path
            attrs: File attributes to update
        """
        parent_dir = os.path.dirname(path)
        filename = os.path.basename(path)

        # Get the cached directory listing with attributes
        dir_listing = self.get_directory_listing_with_attrs(parent_dir)

        # Update the listing if it exists
        if dir_listing is not None:
            dir_listing[filename] = attrs
            self.set_directory_listing_with_attrs(parent_dir, dir_listing)
            self.logger.debug(
                f"Updated attributes for {filename} in {parent_dir} cache"
            )

    def get_last_refresh(self, cache_key: str) -> Optional[float]:
        """Get the last refresh time for a cache key.

        Args:
            cache_key: The cache key to check

        Returns:
            The timestamp of the last refresh, or None if never refreshed
        """
        return self.get_metadata(cache_key, "last_refresh_time")

    def set_last_refresh(self, cache_key: str, timestamp: float) -> None:
        """Set the last refresh time for a cache key.

        Args:
            cache_key: The cache key to update
            timestamp: The timestamp of the refresh
        """
        self.set_metadata(cache_key, "last_refresh_time", timestamp)

    def store_search_metadata(
        self, path: str, query: str, scope: Optional[str], filter_type: Optional[str]
    ) -> None:
        """Store metadata for a search query.

        Args:
            path: The search path (e.g., '/search/library/songs/query')
            query: The search query string
            scope: 'library' or None for catalog
            filter_type: Type of results (e.g., 'songs', 'albums')
        """
        metadata = {
            "query": query,
            "scope": scope,
            "filter_type": filter_type,
            "last_refresh": time.time(),
        }
        self.set(f"{path}_search_metadata", metadata)
        self.logger.debug(f"Stored search metadata for {path}: {metadata}")

    def set_duration(self, video_id: str, duration_seconds: int) -> None:
        """Store duration for a video ID.

        Args:
            video_id: YouTube video ID
            duration_seconds: Duration in seconds
        """
        self.set(f"duration:{video_id}", duration_seconds)
        self.logger.debug(f"Cached duration for {video_id}: {duration_seconds}s")

    def get_duration(self, video_id: str) -> Optional[int]:
        """Retrieve cached duration for a video ID.

        Args:
            video_id: YouTube video ID

        Returns:
            Duration in seconds if cached, None otherwise
        """
        duration = self.get(f"duration:{video_id}")
        if duration is not None:
            self.logger.debug(f"Retrieved cached duration for {video_id}: {duration}s")
        return duration

    def set_durations_batch(self, durations: Dict[str, int]) -> None:
        """Store multiple durations in a batch operation.

        Args:
            durations: Dictionary mapping video IDs to durations in seconds
        """
        if not durations:
            return

        # Update memory cache outside of lock preparation
        memory_updates = {}
        for video_id, duration in durations.items():
            cache_key = f"duration:{video_id}"
            memory_updates[cache_key] = {"data": duration, "time": time.time()}

        # Update memory cache with minimal lock time
        with self.lock:
            for key, value in memory_updates.items():
                self.cache[key] = value

        # Prepare SQLite operation batch outside of lock
        values = []
        for video_id, duration in durations.items():
            key = self.path_to_key(f"duration:{video_id}")
            entry_str = json.dumps({"data": duration, "time": time.time()})
            values.append((key, entry_str))

        # Execute a single batch operation with the database
        with self.lock:
            try:
                self.cursor.executemany(
                    """
                    INSERT OR REPLACE INTO cache_entries (key, entry)
                    VALUES (?, ?)
                    """,
                    values,
                )
                self.conn.commit()
                self.logger.info(
                    f"Cached {len(durations)} durations in a single batch operation"
                )
            except sqlite3.Error as e:
                self.logger.warning(
                    f"Failed to write batch durations to cache: {e.__class__.__name__}: {e}"
                )

    def get_entry_type(self, path: str) -> Optional[str]:
        """Retrieve the entry type (file or directory) for a path.

        Args:
            path: The path to check

        Returns:
            'file', 'directory', or None if the path is not in the cache
        """
        key = self.path_to_key(path)
        with self.lock:
            try:
                self.cursor.execute(
                    "SELECT entry_type FROM cache_entries WHERE key = ?", (key,)
                )
                row = self.cursor.fetchone()

                if row and row[0]:
                    return row[0]

                # Also check with different prefixes if not found directly
                for prefix in ["valid_dir:", "exact_path:"]:
                    prefixed_key = self.path_to_key(f"{prefix}{path}")
                    self.cursor.execute(
                        "SELECT entry_type FROM cache_entries WHERE key = ?",
                        (prefixed_key,),
                    )
                    row = self.cursor.fetchone()
                    if row and row[0]:
                        return row[0]

                return None
            except sqlite3.Error as e:
                self.logger.warning(
                    f"Failed to get entry type for {path}: {e.__class__.__name__}: {e}"
                )
                return None

    def is_directory(self, path: str) -> Optional[bool]:
        """Check if a path is a directory (faster in-memory check).

        Args:
            path: The path to check

        Returns:
            True if directory, False if file, None if unknown
        """
        # Check in-memory cache first for speed
        if hasattr(self, "path_types") and path in self.path_types:
            return self.path_types[path] == "directory"

        # Fallback to database lookup
        entry_type = self.get_entry_type(path)
        if entry_type:
            # Cache the result for future lookups
            if not hasattr(self, "path_types"):
                self.path_types = {}
            self.path_types[path] = entry_type
            return entry_type == "directory"

        return None  # Unknown path type

    def __del__(self):
        """Clean up resources when the object is deleted."""
        try:
            if hasattr(self, "conn") and self.conn:
                self.conn.close()
        except Exception as e:
            # We can't log here as the logger might be gone
            pass
