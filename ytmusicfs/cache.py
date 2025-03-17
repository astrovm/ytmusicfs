#!/usr/bin/env python3

from cachetools import LRUCache
from pathlib import Path
from typing import Any, Optional, Dict, List
import hashlib
import json
import logging
import os
import sqlite3
import threading
import time
import stat
import traceback


class CacheManager:
    """Manager for handling cache operations with simplified locking and caching."""

    def __init__(
        self,
        cache_dir: Optional[str] = None,
        cache_timeout: int = 2592000,
        maxsize: int = 1000,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize the cache manager with simplified caching strategy.

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

        # Initialize a single lock for database operations
        self.lock = threading.RLock()

        # Initialize SQLite database
        self.db_path = self.cache_dir / "cache.db"
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL;")

        with self.lock:
            self.cursor = self.conn.cursor()

            # Create tables if they don't exist
            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS cache_entries (
                    key TEXT PRIMARY KEY,
                    entry TEXT,
                    entry_type TEXT CHECK(entry_type IN ('file', 'directory')),
                    metadata TEXT
                )
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

        # Simplified in-memory cache for high-frequency lookups only
        self.hotcache = LRUCache(maxsize=maxsize)
        self.cache_timeout = cache_timeout

        # Path validation cache - keep this for fast validation
        self.valid_paths = set()
        self.path_types = {}
        self._load_valid_paths()

    def _load_valid_paths(self) -> None:
        """Load valid paths from SQLite into memory."""
        self.logger.debug("Loading valid paths from SQLite into memory...")
        valid_paths_count = 0

        try:
            # Load paths from valid_dir entries
            self.cursor.execute(
                "SELECT key, entry_type FROM cache_entries WHERE key LIKE 'valid_dir:%'"
            )
            for row in self.cursor.fetchall():
                path = self.key_to_path(row[0].replace("valid_dir:", ""))
                self.valid_paths.add(path)
                if row[1]:  # If entry_type exists
                    self.path_types[path] = row[1]
                valid_paths_count += 1

            # Also load paths from exact_path entries
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

    def mark_valid(
        self, path: str, is_directory: bool = None, metadata: dict = None
    ) -> None:
        """Mark a path as valid with its type.

        Args:
            path: The path to mark as valid
            is_directory: Whether this path represents a directory
            metadata: Optional metadata to store with the entry
        """
        # Add to in-memory cache
        self.valid_paths.add(path)

        if is_directory is not None:
            self.path_types[path] = "directory" if is_directory else "file"

        # Persist to database
        key_prefix = (
            "valid_dir:"
            if is_directory
            else "exact_path:" if is_directory is not None else "exact_path:"
        )
        db_key = self.path_to_key(f"{key_prefix}{path}")

        entry = {"data": True, "time": time.time()}
        entry_str = json.dumps(entry)
        entry_type = (
            "directory"
            if is_directory
            else "file" if is_directory is not None else None
        )
        metadata_str = json.dumps(metadata) if metadata else None

        with self.lock:
            try:
                self.cursor.execute(
                    """
                    INSERT OR REPLACE INTO cache_entries (key, entry, entry_type, metadata)
                    VALUES (?, ?, ?, ?)
                    """,
                    (db_key, entry_str, entry_type, metadata_str),
                )
                self.conn.commit()
            except sqlite3.Error as e:
                self.logger.warning(
                    f"Failed to write database cache for {path}: {e.__class__.__name__}: {e}"
                )

        # If this is a directory, mark parent directories as valid too
        if is_directory:
            parts = path.split("/")
            for i in range(1, len(parts)):
                parent = "/".join(parts[:i]) or "/"
                if parent != path:  # Avoid recursion
                    if parent not in self.valid_paths:
                        self.mark_valid(parent, is_directory=True)

    def is_valid_path(self, path: str, context: str = None) -> bool:
        """Enhanced path validation that consolidates all validation logic.

        Args:
            path: The path to validate
            context: Optional operation context (e.g., 'mkdir', 'readdir')

        Returns:
            Boolean indicating if the path is valid
        """
        # Special case for operation context
        if context == "mkdir" or (context and "mkdir" in context):
            self.logger.debug(f"Allowing path as valid due to mkdir context: {path}")
            return True

        # Static paths that are always valid
        if path == "/" or path in ["/playlists", "/liked_songs", "/albums"]:
            return True

        # Check entry type in the cache - most reliable method
        entry_type = self.get_entry_type(path)
        if entry_type:
            return True

        # Check in-memory valid paths set
        if path in self.valid_paths:
            return True

        # Check if parent directory is valid and contains this entry
        parent_dir = os.path.dirname(path)
        filename = os.path.basename(path)

        if parent_dir and self.is_valid_path(parent_dir):  # Recursive check for parent
            # Try to get directory listing with attributes
            dir_listing = self.get_directory_listing_with_attrs(parent_dir)
            if dir_listing and filename in dir_listing:
                # Mark this path as valid for future lookups
                is_dir = (
                    dir_listing[filename].get("st_mode", 0) & stat.S_IFDIR
                    == stat.S_IFDIR
                )
                self.mark_valid(path, is_directory=is_dir)
                return True

            # Also check valid_files key for backward compatibility
            valid_files = self.get(f"valid_files:{parent_dir}")
            if valid_files and filename in valid_files:
                # Mark as valid file (since it's in valid_files)
                self.mark_valid(path, is_directory=False)
                return True

        # Check special prefixed keys in database
        for prefix in ["valid_dir:", "exact_path:"]:
            db_key = self.path_to_key(f"{prefix}{path}")
            try:
                with self.lock:
                    self.cursor.execute(
                        "SELECT entry FROM cache_entries WHERE key = ?", (db_key,)
                    )
                    row = self.cursor.fetchone()
                    if row:
                        # Entry exists, mark as valid for future lookups
                        is_dir = prefix == "valid_dir:"
                        self.mark_valid(path, is_directory=is_dir)
                        return True
            except sqlite3.Error as e:
                self.logger.warning(
                    f"Error checking database for {prefix}{path}: {e.__class__.__name__}: {e}"
                )

        return False

    def get_entry_type(self, path: str) -> Optional[str]:
        """Retrieve the entry type (file or directory) for a path.

        Args:
            path: The path to check

        Returns:
            'file', 'directory', or None if the path is not in the cache
        """
        # Special static paths
        if path == "/" or path in ["/playlists", "/liked_songs", "/albums"]:
            return "directory"

        # Check in-memory path_types first for speed
        if path in self.path_types:
            return self.path_types[path]

        # Check database
        db_key = self.path_to_key(path)
        try:
            with self.lock:
                self.cursor.execute(
                    "SELECT entry_type FROM cache_entries WHERE key = ?", (db_key,)
                )
                row = self.cursor.fetchone()
                if row and row[0]:
                    # Cache in memory for future lookups
                    self.path_types[path] = row[0]
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
                        # Cache in memory for future lookups
                        self.path_types[path] = row[0]
                        return row[0]

                    # Additional check for keys that exist but don't have explicit type
                    if prefix in ["valid_dir:", "exact_path:"]:
                        self.cursor.execute(
                            "SELECT entry FROM cache_entries WHERE key = ?",
                            (prefixed_key,),
                        )
                        row = self.cursor.fetchone()
                        if row:
                            entry_type = (
                                "directory" if prefix == "valid_dir:" else "file"
                            )
                            # Cache in memory for future lookups
                            self.path_types[path] = entry_type
                            return entry_type

            # Try to infer from directory listings
            parent_dir = os.path.dirname(path)
            filename = os.path.basename(path)
            dir_listing = self.get_directory_listing_with_attrs(parent_dir)
            if dir_listing and filename in dir_listing:
                attr = dir_listing[filename]
                is_dir = attr.get("st_mode", 0) & stat.S_IFDIR == stat.S_IFDIR
                entry_type = "directory" if is_dir else "file"
                # Cache in memory for future lookups
                self.path_types[path] = entry_type
                return entry_type

            return None
        except sqlite3.Error as e:
            self.logger.warning(
                f"Failed to get entry type for {path}: {e.__class__.__name__}: {e}"
            )
            return None

    def get(self, path: str) -> Optional[Any]:
        """Get data from cache if it's still valid.

        Args:
            path: The path to retrieve from cache

        Returns:
            The cached data if valid, None otherwise
        """
        # Check hot cache for frequently accessed items
        hotcache_key = f"hotcache:{path}"
        if hotcache_key in self.hotcache:
            cache_entry = self.hotcache[hotcache_key]
            if time.time() - cache_entry["time"] < self.cache_timeout:
                self.logger.debug(f"Hot cache hit for {path}")
                return cache_entry["data"]

        # Fall back to database
        db_key = self.path_to_key(path)
        try:
            with self.lock:
                self.cursor.execute(
                    "SELECT entry FROM cache_entries WHERE key = ?", (db_key,)
                )
                row = self.cursor.fetchone()
                if row:
                    cache_data = json.loads(row[0])
                    if time.time() - cache_data["time"] < self.cache_timeout:
                        # Update hot cache for future lookups
                        self.hotcache[hotcache_key] = cache_data
                        return cache_data["data"]
            return None
        except sqlite3.Error as e:
            self.logger.warning(
                f"Failed to read database cache for {path}: {e.__class__.__name__}: {e}"
            )
            return None

    def set(self, key: str, value: Any) -> None:
        """Set data in cache.

        Args:
            key: The key to cache
            value: The value to cache
        """
        if key is None:
            return

        try:
            # Update memory cache
            with self.lock:
                self.hotcache[key] = value

            # Update database
            db_key = self.path_to_key(key)
            cache_entry = {"data": value, "time": time.time()}
            entry_str = json.dumps(cache_entry)

            with self.lock:
                self.cursor.execute(
                    """
                    INSERT OR REPLACE INTO cache_entries (key, entry)
                    VALUES (?, ?)
                    """,
                    (db_key, entry_str),
                )
                self.conn.commit()
        except Exception as e:
            self.logger.error(f"Failed to write database cache for {key}: {e}")
            self.logger.error(traceback.format_exc())

    def delete(self, path: str) -> None:
        """Delete data from cache.

        Args:
            path: The path to delete from cache
        """
        # Remove from hot cache
        hotcache_key = f"hotcache:{path}"
        if hotcache_key in self.hotcache:
            del self.hotcache[hotcache_key]

        # Remove from database
        db_key = self.path_to_key(path)
        try:
            with self.lock:
                self.cursor.execute(
                    "DELETE FROM cache_entries WHERE key = ?", (db_key,)
                )
                self.conn.commit()
        except sqlite3.Error as e:
            self.logger.warning(
                f"Failed to delete from database cache for {path}: {e.__class__.__name__}: {e}"
            )

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

    def _store_hash_mapping(self, hashed_key: str, original_path: str) -> None:
        """Store a mapping between a hashed key and its original path."""
        try:
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
        """Retrieve the original path for a hashed key."""
        try:
            with self.lock:
                self.cursor.execute(
                    "SELECT original_path FROM hash_mappings WHERE hashed_key = ?",
                    (hashed_key,),
                )
                row = self.cursor.fetchone()
                if row:
                    return row[0]
                return None
        except sqlite3.Error as e:
            self.logger.warning(
                f"Failed to retrieve hash mapping: {e.__class__.__name__}: {e}"
            )
            return None

    def get_duration(self, video_id: str) -> Optional[int]:
        """Retrieve cached duration for a video ID."""
        duration = self.get(f"duration:{video_id}")
        if duration is not None:
            self.logger.debug(f"Retrieved cached duration for {video_id}: {duration}s")
        return duration

    def set_durations_batch(self, durations: Dict[str, int]) -> None:
        """Store multiple durations in a batch operation."""
        if not durations:
            return

        # Prepare batch operation
        values = []
        for video_id, duration in durations.items():
            cache_entry = {"data": duration, "time": time.time()}
            db_key = self.path_to_key(f"duration:{video_id}")
            entry_str = json.dumps(cache_entry)
            values.append((db_key, entry_str))

            # Update hot cache
            hotcache_key = f"hotcache:duration:{video_id}"
            self.hotcache[hotcache_key] = cache_entry

        # Execute batch update
        try:
            with self.lock:
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

    def get_directory_listing_with_attrs(
        self, path: str
    ) -> Optional[Dict[str, Dict[str, Any]]]:
        """Get directory listing with file attributes."""
        return self.get(f"{path}_listing_with_attrs")

    def set_directory_listing_with_attrs(
        self, path: str, listing_with_attrs: Dict[str, Dict[str, Any]]
    ) -> None:
        """Set directory listing with file attributes."""
        self.set(f"{path}_listing_with_attrs", listing_with_attrs)

    def get_file_attrs_from_parent_dir(self, path: str) -> Optional[Dict[str, Any]]:
        """Try to get file attributes from the parent directory's cached listing."""
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
        """Update file attributes in the parent directory's cached listing."""
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

    def is_directory(self, path: str) -> Optional[bool]:
        """Check if a path is a directory.

        Args:
            path: The path to check

        Returns:
            True if directory, False if file, None if unknown
        """
        entry_type = self.get_entry_type(path)
        if entry_type:
            return entry_type == "directory"
        return None

    def get_last_refresh(self, key: str) -> Optional[float]:
        """Get the last refresh time for a key.

        Args:
            key: The key to get the last refresh time for

        Returns:
            The last refresh time as a timestamp, or None if not found
        """
        refresh_key = f"refresh_time:{key}"
        return self.get(refresh_key)

    def set_last_refresh(self, key: str, timestamp: float) -> None:
        """Set the last refresh time for a key.

        Args:
            key: The key to set the last refresh time for
            timestamp: The timestamp to set
        """
        refresh_key = f"refresh_time:{key}"
        self.set(refresh_key, timestamp)

    def close(self) -> None:
        """Close the cache and release resources."""
        try:
            with self.lock:
                if self.conn:
                    self.conn.commit()
                    self.conn.close()
                    self.conn = None
                    self.cursor = None
            self.logger.debug("Cache database connection closed")
        except Exception as e:
            self.logger.error(f"Error closing cache: {e}")
            self.logger.error(traceback.format_exc())

    def __del__(self):
        """Clean up resources when the object is deleted."""
        try:
            if hasattr(self, "conn") and self.conn:
                self.conn.close()
        except Exception:
            pass  # Can't log here as the logger might be gone
