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
import random


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

        # Enhanced in-memory cache for high-frequency lookups with larger size
        self.maxsize = maxsize * 2  # Double the size for more aggressive caching
        self.hotcache = LRUCache(maxsize=self.maxsize)
        self.cache_timeout = cache_timeout

        # Add dedicated caches for most frequently accessed items
        self.directory_listings_cache = LRUCache(
            maxsize=50
        )  # Most important directories
        self.path_validation_cache = LRUCache(maxsize=1000)  # Path validation results
        self.attrs_cache = LRUCache(maxsize=500)  # File attributes

        # Path validation cache - keep this for fast validation
        self.valid_paths = set()
        self.path_types = {}
        self._load_valid_paths()

        # Track cache hits/misses for performance monitoring
        self.stats = {"hits": 0, "misses": 0, "db_hits": 0, "db_misses": 0}

        # Add static commonly accessed paths to avoid repeated lookups
        self._preload_common_paths()

    def _preload_common_paths(self):
        """Preload common paths into the cache for faster access."""
        # Add root path and standard directories
        common_paths = ["/", "/playlists", "/albums", "/liked_songs"]

        for path in common_paths:
            self.mark_valid(path, is_directory=True)
            # Add to path validation cache with long expiry
            self.path_validation_cache[path] = {
                "valid": True,
                "is_directory": True,
                "time": time.time() + self.cache_timeout * 2,  # Double timeout
            }

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

    def mark_valid(self, path: str, is_directory: Optional[bool] = None) -> None:
        """Mark a path as valid in the cache with optimized storage.

        Args:
            path: The path to mark as valid
            is_directory: Flag indicating if this is a directory (None for unknown)
        """
        # Skip root as it's always valid
        if path == "/":
            return

        # Update in-memory valid_paths set
        self.valid_paths.add(path)

        # Add to path validation cache with 5-minute expiry
        self.path_validation_cache[path] = {
            "valid": True,
            "is_directory": is_directory,
            "time": time.time() + 300,  # 5-minute cache
        }

        # Update path_types if is_directory is specified
        if is_directory is not None:
            self.path_types[path] = "directory" if is_directory else "file"

        # Prepare database entry - construct once, reuse for different keys
        entry = {"data": True, "time": time.time()}
        entry_str = json.dumps(entry)

        # We're storing file type information in the database too
        entry_type = None
        if is_directory is not None:
            entry_type = "directory" if is_directory else "file"

        # Add metadata about the path
        metadata = {"valid_since": time.time()}
        metadata_str = json.dumps(metadata)

        # Store in database with appropriate prefix
        try:
            prefix = "valid_dir:" if is_directory else "exact_path:"
            if is_directory is None:
                # Use both for unknown types
                prefixes = ["valid_dir:", "exact_path:"]
            else:
                prefixes = [prefix]

            # Build batch operation
            values = []
            for prefix in prefixes:
                db_key = self.path_to_key(f"{prefix}{path}")
                values.append((db_key, entry_str, entry_type, metadata_str))

            with self.lock:
                self.cursor.executemany(
                    """
                    INSERT OR REPLACE INTO cache_entries (key, entry, entry_type, metadata)
                    VALUES (?, ?, ?, ?)
                    """,
                    values,
                )
                # Commit less frequently for performance
                if random.random() < 0.1:  # Only commit about 10% of the time
                    self.conn.commit()
        except sqlite3.Error as e:
            self.logger.warning(
                f"Failed to mark path as valid: {e.__class__.__name__}: {e}"
            )

    def is_valid_path(self, path: str, context: str = None) -> bool:
        """Enhanced path validation with improved caching for better performance.

        Args:
            path: The path to validate
            context: Optional context for logging

        Returns:
            Boolean indicating if the path is valid
        """
        # Special static paths are always valid
        if path == "/" or path in ["/playlists", "/liked_songs", "/albums"]:
            return True

        # Check fast validation cache first
        if path in self.path_validation_cache:
            cached = self.path_validation_cache[path]
            if time.time() < cached.get("time", 0):  # Check expiry
                self.stats["hits"] += 1
                return cached["valid"]

        # Check the in-memory valid paths set
        if path in self.valid_paths:
            self.stats["hits"] += 1
            # Also update the validation cache
            self.path_validation_cache[path] = {
                "valid": True,
                "is_directory": self.is_directory(path),
                "time": time.time() + 300,  # Cache for 5 minutes
            }
            return True

        # For short paths, check parent validation for efficiency
        parent_dir = os.path.dirname(path)
        filename = os.path.basename(path)

        # If parent is valid and in directory cache, check if child exists
        if parent_dir and parent_dir in self.directory_listings_cache:
            cached = self.directory_listings_cache[parent_dir]
            if time.time() - cached["time"] < self.cache_timeout:
                dir_listing = cached["data"]
                if filename in dir_listing:
                    # Mark path as valid for future lookups
                    is_dir = bool(
                        dir_listing[filename].get("st_mode", 0) & stat.S_IFDIR
                    )
                    self.mark_valid(path, is_directory=is_dir)

                    # Update validation cache
                    self.path_validation_cache[path] = {
                        "valid": True,
                        "is_directory": is_dir,
                        "time": time.time() + 300,  # Cache for 5 minutes
                    }
                    self.stats["hits"] += 1
                    return True

        # Fall back to original validation logic
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

                # Update validation cache
                self.path_validation_cache[path] = {
                    "valid": True,
                    "is_directory": is_dir,
                    "time": time.time() + 300,  # Cache for 5 minutes
                }
                self.stats["db_hits"] += 1
                return True

            # Also check valid_files key for backward compatibility
            valid_files = self.get(f"valid_files:{parent_dir}")
            if valid_files and filename in valid_files:
                # Mark as valid file (since it's in valid_files)
                self.mark_valid(path, is_directory=False)

                # Update validation cache
                self.path_validation_cache[path] = {
                    "valid": True,
                    "is_directory": False,
                    "time": time.time() + 300,  # Cache for 5 minutes
                }
                self.stats["db_hits"] += 1
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

                        # Update validation cache
                        self.path_validation_cache[path] = {
                            "valid": True,
                            "is_directory": is_dir,
                            "time": time.time() + 300,  # Cache for 5 minutes
                        }
                        self.stats["db_hits"] += 1
                        return True
            except sqlite3.Error as e:
                self.logger.warning(
                    f"Error checking database for {prefix}{path}: {e.__class__.__name__}: {e}"
                )

        # Path is not valid, cache this result too for a shorter time
        self.path_validation_cache[path] = {
            "valid": False,
            "time": time.time() + 60,  # Cache negative results for 1 minute
        }
        self.stats["misses"] += 1
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
        """Get data from cache if it's still valid with improved caching.

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
                self.stats["hits"] += 1
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
                        self.stats["db_hits"] += 1
                        return cache_data["data"]

                    self.stats["db_misses"] += 1
                    return None

                self.stats["db_misses"] += 1
                return None
        except sqlite3.Error as e:
            self.logger.warning(
                f"Failed to read database cache for {path}: {e.__class__.__name__}: {e}"
            )
            self.stats["db_misses"] += 1
            return None

    def set(self, key: str, value: Any) -> None:
        """Set data in cache with improved performance.

        Args:
            key: The key to cache
            value: The value to cache
        """
        if key is None:
            return

        try:
            # Update memory cache
            hotcache_key = f"hotcache:{key}"
            cache_entry = {"data": value, "time": time.time()}
            self.hotcache[hotcache_key] = cache_entry

            # Update database, but avoid frequent commits
            db_key = self.path_to_key(key)
            entry_str = json.dumps(cache_entry)

            with self.lock:
                self.cursor.execute(
                    """
                    INSERT OR REPLACE INTO cache_entries (key, entry)
                    VALUES (?, ?)
                    """,
                    (db_key, entry_str),
                )

                # Only commit every 50 writes or if it's a critical path
                if (
                    key.startswith("valid_")
                    or "_listing_with_attrs" in key
                    or random.random() < 0.02
                ):
                    self.conn.commit()
        except Exception as e:
            self.logger.error(f"Failed to write database cache for {key}: {e}")
            self.logger.error(traceback.format_exc())

    def set_batch(self, entries: Dict[str, Any]) -> None:
        """Set multiple cache entries in a single database transaction.

        Args:
            entries: Dictionary mapping keys to values
        """
        if not entries:
            return

        try:
            # Prepare values for batch insertion
            values = []
            now = time.time()

            for key, value in entries.items():
                if key is None:
                    continue

                # Update memory cache
                hotcache_key = f"hotcache:{key}"
                cache_entry = {"data": value, "time": now}
                self.hotcache[hotcache_key] = cache_entry

                # Prepare for database insert
                db_key = self.path_to_key(key)
                entry_str = json.dumps(cache_entry)
                values.append((db_key, entry_str))

            # Execute batch operation
            if values:
                with self.lock:
                    self.cursor.executemany(
                        """
                        INSERT OR REPLACE INTO cache_entries (key, entry)
                        VALUES (?, ?)
                        """,
                        values,
                    )
                    self.conn.commit()

                self.logger.debug(f"Batch cached {len(values)} entries")
        except Exception as e:
            self.logger.error(f"Failed to batch write to cache: {e}")
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
        """Store multiple durations in a batch operation with optimized performance.

        Args:
            durations: Dictionary mapping video IDs to duration in seconds
        """
        if not durations:
            return

        # Build a batch entries dictionary
        cache_entries = {}
        now = time.time()

        for video_id, duration in durations.items():
            # Update hot cache
            hotcache_key = f"hotcache:duration:{video_id}"
            self.hotcache[hotcache_key] = {"data": duration, "time": now}

            # Add to batch entries for database
            cache_entries[f"duration:{video_id}"] = duration

        # Use batch cache operation for efficiency
        self.set_batch(cache_entries)
        self.logger.info(
            f"Cached {len(durations)} durations in a single batch operation"
        )

    def get_directory_listing_with_attrs(
        self, path: str
    ) -> Optional[Dict[str, Dict[str, Any]]]:
        """Get cached directory listing with attributes with improved performance.

        Args:
            path: Directory path

        Returns:
            Dictionary mapping filenames to their attributes,
            or None if not cached or expired
        """
        # First check in memory cache for very fast access
        if path in self.directory_listings_cache:
            cached = self.directory_listings_cache[path]
            current_time = time.time()

            # Check if the cache is still fresh
            if current_time - cached["time"] < self.cache_timeout:
                self.logger.debug(f"In-memory cache hit for directory listing: {path}")
                self.stats["hits"] += 1
                return cached["data"]

        # Dedicated cache key for directory listings
        cache_key = f"{path}_listing_with_attrs"

        # Try hot cache first (faster than database)
        hot_key = f"hot:{cache_key}"
        hot_cached = self.hotcache.get(hot_key)
        if hot_cached and time.time() - hot_cached["time"] < self.cache_timeout:
            self.logger.debug(f"Hot cache hit for directory listing: {path}")
            self.stats["hits"] += 1
            # Update memory cache for future access
            self.directory_listings_cache[path] = {
                "data": hot_cached["data"],
                "time": hot_cached["time"],
            }
            return hot_cached["data"]

        # Try database as last resort
        db_key = self.path_to_key(cache_key)
        try:
            with self.lock:
                self.cursor.execute(
                    "SELECT entry FROM cache_entries WHERE key = ?", (db_key,)
                )
                row = self.cursor.fetchone()
                if row:
                    try:
                        cache_data = json.loads(row[0])
                        if time.time() - cache_data["time"] < self.cache_timeout:
                            # Valid cache entry, update memory caches for future lookups
                            listing = cache_data["data"]

                            # Update in-memory and hot caches
                            self.directory_listings_cache[path] = {
                                "data": listing,
                                "time": cache_data["time"],
                            }
                            self.hotcache[hot_key] = cache_data

                            self.logger.debug(
                                f"DB cache hit for directory listing: {path}"
                            )
                            self.stats["db_hits"] += 1
                            return listing
                    except (json.JSONDecodeError, KeyError) as e:
                        self.logger.warning(f"Failed to parse cache entry: {e}")

                self.stats["db_misses"] += 1
                return None
        except sqlite3.Error as e:
            self.logger.warning(
                f"Failed to read directory listing from cache: {e.__class__.__name__}: {e}"
            )
            self.stats["db_misses"] += 1
            return None

    def set_directory_listing_with_attrs(
        self, path: str, listing_with_attrs: Dict[str, Dict[str, Any]]
    ) -> None:
        """Cache directory listing with attributes using optimized storage.

        Args:
            path: Directory path
            listing_with_attrs: Dictionary mapping filenames to their attributes
        """
        if not listing_with_attrs:
            self.logger.debug(f"Skipping empty directory listing for {path}")
            return

        # Update both in-memory caches
        current_time = time.time()

        # Update quick-access cached directory listing
        self.directory_listings_cache[path] = {
            "data": listing_with_attrs,
            "time": current_time,
        }

        # Prepare batch entries for related paths - this improves validation performance
        batch_entries = {}

        # First ensure the directory itself is marked as valid
        # Use set_batch later rather than calling mark_valid to reduce overhead
        batch_entries[f"valid_dir:{path}"] = {"data": True, "time": current_time}

        # Create full path entries for each file for quick validation
        for filename, attrs in listing_with_attrs.items():
            # Skip special entries
            if filename in [".", ".."]:
                continue

            # Determine if this entry is a directory based on mode
            is_dir = bool(attrs.get("st_mode", 0) & stat.S_IFDIR == stat.S_IFDIR)

            # Create full child path
            child_path = f"{path}/{filename}"

            # Use the appropriate entry type for the validation cache
            entry_type = "valid_dir:" if is_dir else "exact_path:"
            batch_entries[f"{entry_type}{child_path}"] = {
                "data": True,
                "time": current_time,
            }

            # Also update attrs cache for quick lookups
            self.attrs_cache[child_path] = attrs

        # Dedicated cache key for directory listings
        cache_key = f"{path}_listing_with_attrs"

        # Update hot cache for quick access
        hot_key = f"hot:{cache_key}"
        self.hotcache[hot_key] = {
            "data": listing_with_attrs,
            "time": current_time,
        }

        # Prepare for database storage
        db_key = self.path_to_key(cache_key)
        entry = {
            "data": listing_with_attrs,
            "time": current_time,
        }

        # Update database in a background task to avoid blocking
        try:
            # Use the entry_type for directories
            entry_type = "directory"

            # Add metadata for improved stats tracking
            metadata = {
                "entries_count": len(listing_with_attrs),
                "cached_at": current_time,
                "path_length": len(path),
            }

            # Convert to string for storage
            entry_str = json.dumps(entry)
            metadata_str = json.dumps(metadata)

            with self.lock:
                self.cursor.execute(
                    """
                    INSERT OR REPLACE INTO cache_entries
                    (key, entry, entry_type, metadata)
                    VALUES (?, ?, ?, ?)
                    """,
                    (db_key, entry_str, entry_type, metadata_str),
                )
                self.conn.commit()

            # Use batch update for all related entries
            self.set_batch(batch_entries)

            self.logger.debug(
                f"Cached directory listing with {len(listing_with_attrs)} entries for {path}"
            )
        except (sqlite3.Error, json.JSONDecodeError) as e:
            self.logger.warning(
                f"Failed to cache directory listing: {e.__class__.__name__}: {e}"
            )

    def get_file_attrs_from_parent_dir(self, path: str) -> Optional[Dict[str, Any]]:
        """Get file attributes from parent directory cache with improved performance.

        Args:
            path: File path

        Returns:
            Dictionary of file attributes if found, None otherwise
        """
        # First check direct attrs cache for the fastest access
        if path in self.attrs_cache:
            self.stats["hits"] += 1
            self.logger.debug(f"Direct attrs cache hit for {path}")
            return self.attrs_cache[path]

        # Split path into parent directory and filename
        parent_dir = os.path.dirname(path)
        filename = os.path.basename(path)

        # Early return for root directory
        if not parent_dir or not filename:
            self.stats["misses"] += 1
            return None

        # Check if parent directory listing is cached
        dir_listing = self.get_directory_listing_with_attrs(parent_dir)
        if dir_listing and filename in dir_listing:
            attrs = dir_listing[filename]
            # Update attrs cache for future direct lookups
            self.attrs_cache[path] = attrs
            self.stats["hits"] += 1
            return attrs

        # For special file paths like /playlists/playlist_name, check the appropriate registry
        if (
            parent_dir in ["/playlists", "/albums", "/liked_songs"]
            and len(path.split("/")) == 3
        ):
            # This might be a playlist/album entry - create minimal attrs for directories
            if self.is_valid_path(path):
                # Create basic attributes for a directory
                attrs = self._create_directory_attrs()
                # Cache for future lookups
                self.attrs_cache[path] = attrs
                self.stats["hits"] += 1
                return attrs

        # Look in parent's parent for special cases (nested directory listings)
        if parent_dir != "/" and len(path.split("/")) > 3:
            parent_parent = os.path.dirname(parent_dir)
            parent_basename = os.path.basename(parent_dir)

            # Check if grandparent directory has the parent listed
            grandparent_listing = self.get_directory_listing_with_attrs(parent_parent)
            if grandparent_listing and parent_basename in grandparent_listing:
                # If parent is a directory, create default directory attrs for child
                parent_attrs = grandparent_listing[parent_basename]
                if parent_attrs.get("st_mode", 0) & stat.S_IFDIR:
                    # Create basic attributes for a directory or file based on path type
                    is_dir = self.is_directory(path)
                    if is_dir is not None:
                        attrs = (
                            self._create_directory_attrs()
                            if is_dir
                            else self._create_file_attrs()
                        )
                        self.attrs_cache[path] = attrs
                        self.stats["hits"] += 1
                        return attrs

        self.logger.debug(f"No attributes found for {path}")
        self.stats["misses"] += 1
        return None

    def _create_directory_attrs(self) -> Dict[str, Any]:
        """Create default attributes for a directory.

        Returns:
            Dictionary with default directory attributes
        """
        now = time.time()
        return {
            "st_mode": stat.S_IFDIR | 0o555,  # directory with read/execute permissions
            "st_nlink": 2,  # default for directories
            "st_size": 4096,  # standard size for directory
            "st_ctime": now,
            "st_mtime": now,
            "st_atime": now,
        }

    def _create_file_attrs(self) -> Dict[str, Any]:
        """Create default attributes for a file.

        Returns:
            Dictionary with default file attributes
        """
        now = time.time()
        return {
            "st_mode": stat.S_IFREG | 0o444,  # regular file with read permissions
            "st_nlink": 1,  # default for files
            "st_size": 0,  # empty file by default
            "st_ctime": now,
            "st_mtime": now,
            "st_atime": now,
        }

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

    def get_cache_stats(self) -> Dict[str, int]:
        """Get cache performance statistics.

        Returns:
            Dictionary with hit/miss statistics
        """
        hit_rate = 0
        if (self.stats["hits"] + self.stats["misses"]) > 0:
            hit_rate = (
                self.stats["hits"] / (self.stats["hits"] + self.stats["misses"]) * 100
            )

        db_hit_rate = 0
        if (self.stats["db_hits"] + self.stats["db_misses"]) > 0:
            db_hit_rate = (
                self.stats["db_hits"]
                / (self.stats["db_hits"] + self.stats["db_misses"])
                * 100
            )

        return {
            **self.stats,
            "memory_hit_rate": hit_rate,
            "db_hit_rate": db_hit_rate,
            "directory_cache_size": len(self.directory_listings_cache),
            "path_validation_cache_size": len(self.path_validation_cache),
            "attrs_cache_size": len(self.attrs_cache),
        }

    def update_file_attrs_in_parent_dir(self, path: str, attrs: Dict[str, Any]) -> None:
        """Update file attributes in the parent directory's cached listing with improved caching.

        Args:
            path: The file path
            attrs: The file attributes to update
        """
        parent_dir = os.path.dirname(path)
        filename = os.path.basename(path)

        # Update the in-memory attrs_cache first
        self.attrs_cache[path] = {"attrs": attrs, "time": time.time()}

        # Update the in-memory directory listing cache if present
        if parent_dir in self.directory_listings_cache:
            cached = self.directory_listings_cache[parent_dir]
            dir_listing = cached["data"]
            if dir_listing:
                dir_listing[filename] = attrs
                # No need to re-store in cache since dict is modified in-place
                self.logger.debug(
                    f"Updated attributes for {filename} in {parent_dir} memory cache"
                )

        # Get the database cached directory listing
        dir_listing = self.get_directory_listing_with_attrs(parent_dir)

        # Update the listing if it exists in the database
        if dir_listing is not None:
            dir_listing[filename] = attrs
            self.set_directory_listing_with_attrs(parent_dir, dir_listing)
            self.logger.debug(
                f"Updated attributes for {filename} in {parent_dir} database cache"
            )
