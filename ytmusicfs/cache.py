#!/usr/bin/env python3

import builtins
import hashlib
import json
import logging
import os
import shutil
import sqlite3
import stat
import time
import traceback
from contextlib import suppress
from pathlib import Path
from typing import Any, cast

from cachetools import LRUCache


class CacheManager:
    """Manager for handling cache operations with simplified locking and caching."""

    WRITE_COMMIT_INTERVAL = 50
    STATIC_DIRECTORIES = frozenset({"/", "/playlists", "/albums", "/liked_songs"})

    def __init__(
        self,
        thread_manager: Any,  # ThreadManager (required)
        cache_dir: str | None = None,
        cache_timeout: int = 2592000,
        maxsize: int = 1000,
        logger: logging.Logger | None = None,
    ) -> None:
        """
        Initialize the CacheManager.

        Args:
            thread_manager: ThreadManager instance for thread synchronization (required)
            cache_dir: Path to the cache directory. Defaults to ~/.cache/ytmusicfs.
            cache_timeout: Cache timeout in seconds (default: 30 days).
            maxsize: Maximum number of items to keep in memory cache (default: 1000)
            logger: Logger instance to use
        """
        self.logger = logger or logging.getLogger("CacheManager")

        if cache_dir:
            self.cache_dir = Path(cache_dir)
        else:
            self.cache_dir = Path.home() / ".cache" / "ytmusicfs"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Using cache directory: {self.cache_dir}")

        self.thread_manager = thread_manager

        self.lock = thread_manager.create_lock()
        self.logger.debug("Using ThreadManager for lock creation in CacheManager")

        self.db_path = self.cache_dir / "cache.db"
        self.conn: sqlite3.Connection = sqlite3.connect(
            str(self.db_path), check_same_thread=False
        )
        self._closed = False
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self._pending_writes = 0

        with self.lock:
            cursor = self.conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cache_entries (
                    key TEXT PRIMARY KEY,
                    entry TEXT,
                    entry_type TEXT CHECK(entry_type IN ('file', 'directory')),
                    metadata TEXT
                )
                """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS hash_mappings (
                    hashed_key TEXT PRIMARY KEY,
                    original_path TEXT
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS refresh_tracker (
                    key TEXT PRIMARY KEY,
                    last_refresh REAL,
                    status TEXT CHECK(status IN ('fresh', 'pending', 'stale'))
                )
                """)

            self.conn.commit()

        self.maxsize = maxsize * 2
        self.hotcache: LRUCache[str, Any] = LRUCache(maxsize=self.maxsize)
        self.cache_timeout = cache_timeout

        self.directory_listings_cache: LRUCache[str, Any] = LRUCache(maxsize=50)
        self.path_validation_cache: LRUCache[str, Any] = LRUCache(maxsize=1000)
        self.attrs_cache: LRUCache[str, Any] = LRUCache(maxsize=500)
        self.valid_paths: set[str] = set()
        self.path_types: dict[str, str] = {}
        self.unavailable_video_ids: set[str] = set()
        self.unavailable_paths: set[str] = set()
        self._load_valid_paths()
        self._load_unavailable_tracks()

        self.stats = {"hits": 0, "misses": 0, "db_hits": 0, "db_misses": 0}

        self._preload_common_paths()

    def _preload_common_paths(self) -> None:
        """Preload common paths into the cache for faster access."""
        common_paths = ["/", "/playlists", "/albums", "/liked_songs"]

        for path in common_paths:
            self.mark_valid(path, is_directory=True)
            self.path_validation_cache[path] = {
                "valid": True,
                "is_directory": True,
                "time": time.time() + self.cache_timeout * 2,
            }

    def _load_valid_paths(self) -> None:
        """Load valid paths from SQLite into memory."""
        self.logger.debug("Loading valid paths from SQLite into memory...")
        valid_paths_count = 0

        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT key, entry_type FROM cache_entries WHERE key LIKE 'valid_dir:%'"
            )
            for row in cursor.fetchall():
                path = self.key_to_path(row[0].replace("valid_dir:", ""))
                self.valid_paths.add(path)
                if row[1]:
                    self.path_types[path] = row[1]
                valid_paths_count += 1

            cursor.execute(
                "SELECT key, entry_type FROM cache_entries WHERE key LIKE 'exact_path:%'"
            )
            for row in cursor.fetchall():
                path = self.key_to_path(row[0].replace("exact_path:", ""))
                self.valid_paths.add(path)
                if row[1]:
                    self.path_types[path] = row[1]
                valid_paths_count += 1
        except sqlite3.Error as e:
            self.logger.warning(
                f"Failed to load valid paths: {e.__class__.__name__}: {e}"
            )

        self.logger.info(f"Loaded {valid_paths_count} valid paths into memory")

    def _load_unavailable_tracks(self) -> None:
        """Load unavailable video IDs from SQLite into memory."""
        try:
            with self.lock:
                cursor = self.conn.cursor()
                cursor.execute(
                    "SELECT key, entry FROM cache_entries WHERE key LIKE ?",
                    (self.path_to_key("unavailable:") + "%",),
                )
                for key, entry in cursor.fetchall():
                    self.unavailable_video_ids.add(
                        self.key_to_path(key).replace("unavailable:", "", 1)
                    )
                    with suppress(json.JSONDecodeError, TypeError):
                        metadata = json.loads(entry)
                        data = metadata.get("data")
                        path = data.get("path") if isinstance(data, dict) else None
                        if isinstance(path, str):
                            self.unavailable_paths.add(path)
        except sqlite3.Error as e:
            self.logger.warning(
                "Failed to load unavailable-track cache: %s: %s",
                e.__class__.__name__,
                e,
            )

    def mark_valid(self, path: str, is_directory: bool | None = None) -> None:
        """Persist a path and its known type for future lookups."""
        if path == "/":
            return

        self.valid_paths.add(path)
        self.path_validation_cache[path] = {
            "valid": True,
            "is_directory": is_directory,
            "time": time.time() + 300,
        }
        if is_directory is not None:
            self.path_types[path] = "directory" if is_directory else "file"

        entry = {"data": True, "time": time.time()}
        entry_str = json.dumps(entry)
        entry_type = (
            None if is_directory is None else "directory" if is_directory else "file"
        )
        metadata_str = json.dumps({"valid_since": time.time()})
        prefixes = (
            ["valid_dir:", "exact_path:"]
            if is_directory is None
            else ["valid_dir:" if is_directory else "exact_path:"]
        )
        values = [
            (
                self.path_to_key(f"{prefix}{path}"),
                entry_str,
                entry_type,
                metadata_str,
            )
            for prefix in prefixes
        ]
        try:
            with self.lock:
                cursor = self.conn.cursor()
                if is_directory is not None:
                    opposite_prefix = "exact_path:" if is_directory else "valid_dir:"
                    cursor.execute(
                        "DELETE FROM cache_entries WHERE key = ?",
                        (self.path_to_key(f"{opposite_prefix}{path}"),),
                    )
                cursor.executemany(
                    """
                    INSERT OR REPLACE INTO cache_entries (key, entry, entry_type, metadata)
                    VALUES (?, ?, ?, ?)
                    """,
                    values,
                )
                self._record_write(len(values))
        except sqlite3.Error as error:
            self.logger.warning(
                "Failed to mark path as valid: %s: %s",
                error.__class__.__name__,
                error,
            )

    def is_valid_path(self, path: str) -> bool:
        """Return whether a path exists in a memory or persistent listing."""
        if path in self.STATIC_DIRECTORIES:
            return True

        cached_result = self._cached_path_validation(path)
        if cached_result is not None:
            return cached_result

        if path in self.valid_paths:
            return self._remember_valid_path(path, self.is_directory(path), "hits")

        parent_dir = os.path.dirname(path)
        filename = os.path.basename(path)
        listing_result = self._validate_from_listings(path, parent_dir, filename)
        if listing_result:
            return True

        database_type = self._path_type_from_prefixed_entry(path)
        if database_type is not None:
            return self._remember_valid_path(path, database_type, "db_hits")

        self.path_validation_cache[path] = {
            "valid": False,
            "time": time.time() + 60,
        }
        self.stats["misses"] += 1
        return False

    def _cached_path_validation(self, path: str) -> bool | None:
        cached = self.path_validation_cache.get(path)
        if not cached or time.time() >= cached.get("time", 0):
            return None
        self.stats["hits"] += 1
        return bool(cached["valid"])

    def _validate_from_listings(
        self, path: str, parent_dir: str, filename: str
    ) -> bool:
        if not parent_dir:
            return False

        cached = self.directory_listings_cache.get(parent_dir)
        if cached and time.time() - cached["time"] < self.cache_timeout:
            attrs = cached["data"].get(filename)
            if attrs:
                return self._remember_valid_path(
                    path, self._attrs_are_directory(attrs), "hits"
                )

        if not self.is_valid_path(parent_dir):
            return False
        listing = self.get_directory_listing_with_attrs(parent_dir)
        if listing and filename in listing:
            return self._remember_valid_path(
                path, self._attrs_are_directory(listing[filename]), "db_hits"
            )

        valid_files = self.get(f"valid_files:{parent_dir}")
        if isinstance(valid_files, (list, set, tuple)) and filename in valid_files:
            return self._remember_valid_path(path, False, "db_hits")
        return False

    @staticmethod
    def _attrs_are_directory(attrs: dict[str, Any]) -> bool:
        mode = attrs.get("st_mode", 0)
        return isinstance(mode, int) and mode & stat.S_IFDIR == stat.S_IFDIR

    def _remember_valid_path(
        self, path: str, is_directory: bool | None, stat_key: str
    ) -> bool:
        self.mark_valid(path, is_directory=is_directory)
        self.stats[stat_key] += 1
        return True

    def _path_type_from_prefixed_entry(self, path: str) -> bool | None:
        try:
            with self.lock:
                cursor = self.conn.cursor()
                for prefix, is_directory in (
                    ("exact_path:", False),
                    ("valid_dir:", True),
                ):
                    cursor.execute(
                        "SELECT entry FROM cache_entries WHERE key = ?",
                        (self.path_to_key(f"{prefix}{path}"),),
                    )
                    if cursor.fetchone():
                        return is_directory
        except sqlite3.Error as error:
            self.logger.warning(
                "Error checking database for %s: %s: %s",
                path,
                error.__class__.__name__,
                error,
            )
        return None

    def get_entry_type(self, path: str) -> str | None:
        """Return a cached path type, preferring explicit file records."""
        if path in self.STATIC_DIRECTORIES:
            return "directory"

        cached_type = self.path_types.get(path)
        if cached_type:
            return cached_type

        try:
            entry_type = self._entry_type_from_database(path)
        except sqlite3.Error as error:
            self.logger.warning(
                "Failed to get entry type for %s: %s: %s",
                path,
                error.__class__.__name__,
                error,
            )
            return None
        if entry_type:
            return self._remember_entry_type(path, entry_type)

        listing = self.get_directory_listing_with_attrs(os.path.dirname(path))
        attrs = listing.get(os.path.basename(path)) if listing else None
        if attrs:
            inferred_type = "directory" if self._attrs_are_directory(attrs) else "file"
            return self._remember_entry_type(path, inferred_type)
        return None

    def _entry_type_from_database(self, path: str) -> str | None:
        keys = (
            self.path_to_key(path),
            self.path_to_key(f"exact_path:{path}"),
            self.path_to_key(f"valid_dir:{path}"),
        )
        with self.lock:
            cursor = self.conn.cursor()
            for key in keys:
                cursor.execute(
                    "SELECT entry_type FROM cache_entries WHERE key = ?", (key,)
                )
                row = cursor.fetchone()
                if row and row[0] in {"file", "directory"}:
                    return str(row[0])
        return None

    def _remember_entry_type(self, path: str, entry_type: str) -> str:
        self.path_types[path] = entry_type
        return entry_type

    def get(self, path: str) -> Any | None:
        """Get data from cache if it's still valid with improved caching.

        Args:
            path: The path to retrieve from cache

        Returns:
            The cached data if valid, None otherwise
        """
        hotcache_key = f"hotcache:{path}"
        if hotcache_key in self.hotcache:
            cache_entry = self.hotcache[hotcache_key]
            if time.time() - cache_entry["time"] < self.cache_timeout:
                self.stats["hits"] += 1
                self.logger.debug(f"Hot cache hit for {path}")
                return cache_entry["data"]

        db_key = self.path_to_key(path)
        try:
            with self.lock:
                cursor = self.conn.cursor()
                cursor.execute(
                    "SELECT entry FROM cache_entries WHERE key = ?", (db_key,)
                )
                row = cursor.fetchone()
                if row:
                    cache_data = json.loads(row[0])
                    if time.time() - cache_data["time"] < self.cache_timeout:
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
        """Store one value and commit on deterministic durability boundaries."""
        try:
            hotcache_key = f"hotcache:{key}"
            cache_entry = {"data": value, "time": time.time()}
            self.hotcache[hotcache_key] = cache_entry

            db_key = self.path_to_key(key)
            entry_str = json.dumps(cache_entry)
            with self.lock:
                cursor = self.conn.cursor()
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO cache_entries (key, entry)
                    VALUES (?, ?)
                    """,
                    (db_key, entry_str),
                )
                durable = key.startswith(("valid_", "unavailable:")) or (
                    "_listing_with_attrs" in key
                )
                self._record_write(force=durable)
        except Exception as error:
            self.logger.error("Failed to write database cache for %s: %s", key, error)
            self.logger.error(traceback.format_exc())

    def _record_write(self, count: int = 1, *, force: bool = False) -> None:
        """Commit after a fixed write count or at an explicit durability boundary."""
        self._pending_writes += count
        if force or self._pending_writes >= self.WRITE_COMMIT_INTERVAL:
            self.conn.commit()
            self._pending_writes = 0

    def flush(self) -> None:
        """Commit pending writes."""
        with self.lock:
            if self._pending_writes:
                self.conn.commit()
                self._pending_writes = 0

    def set_batch(self, entries: dict[str, Any]) -> None:
        """Set multiple cache entries in a single database transaction.

        Args:
            entries: Dictionary mapping keys to values
        """
        if not entries:
            return

        try:
            values = []
            now = time.time()

            for key, value in entries.items():
                hotcache_key = f"hotcache:{key}"
                cache_entry = {"data": value, "time": now}
                self.hotcache[hotcache_key] = cache_entry

                db_key = self.path_to_key(key)
                entry_str = json.dumps(cache_entry)
                values.append((db_key, entry_str))

            if values:
                with self.lock:
                    cursor = self.conn.cursor()
                    cursor.executemany(
                        """
                        INSERT OR REPLACE INTO cache_entries (key, entry)
                        VALUES (?, ?)
                        """,
                        values,
                    )
                    self.conn.commit()
                    self._pending_writes = 0

                self.logger.debug(f"Batch cached {len(values)} entries")
        except Exception as e:
            self.logger.error(f"Failed to batch write to cache: {e}")
            self.logger.error(traceback.format_exc())

    def delete(self, path: str) -> None:
        """Delete data from cache.

        Args:
            path: The path to delete from cache
        """
        for hotcache_key in (f"hotcache:{path}", f"hot:{path}"):
            if hotcache_key in self.hotcache:
                del self.hotcache[hotcache_key]

        db_key = self.path_to_key(path)
        try:
            with self.lock:
                cursor = self.conn.cursor()
                cursor.execute("DELETE FROM cache_entries WHERE key = ?", (db_key,))
                self.conn.commit()
                self._pending_writes = 0
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
        key = path.replace("/", "_").replace("'", "''").replace(" ", "_")

        MAX_KEY_LENGTH = 200
        if len(key) > MAX_KEY_LENGTH:
            prefix = key[:30]
            path_hash = hashlib.md5(path.encode("utf-8")).hexdigest()
            key = f"{prefix}_{path_hash}"

            self._store_hash_mapping(key, path)

        return key

    def key_to_path(self, key: str) -> str:
        """Convert a cache key back to a filesystem path.

        Args:
            key: The cache key

        Returns:
            Original filesystem path
        """
        if (
            "_" in key
            and len(key) > 30
            and key[30:31] == "_"
            and len(key) - key.rfind("_") == 33
        ):
            original_path = self.get_original_path(key)
            if original_path:
                return original_path
            self.logger.warning(f"Cannot convert hashed key back to path: {key}")
            return key

        return key.replace("_", "/").replace("''", "'")

    def _store_hash_mapping(self, hashed_key: str, original_path: str) -> None:
        """Store a mapping between a hashed key and its original path."""
        try:
            with self.lock:
                cursor = self.conn.cursor()
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO hash_mappings (hashed_key, original_path)
                    VALUES (?, ?)
                    """,
                    (hashed_key, original_path),
                )
                self.conn.commit()
                self._pending_writes = 0
        except sqlite3.Error as e:
            self.logger.warning(
                f"Failed to store hash mapping: {e.__class__.__name__}: {e}"
            )

    def get_original_path(self, hashed_key: str) -> str | None:
        """Retrieve the original path for a hashed key."""
        try:
            with self.lock:
                cursor = self.conn.cursor()
                cursor.execute(
                    "SELECT original_path FROM hash_mappings WHERE hashed_key = ?",
                    (hashed_key,),
                )
                row = cursor.fetchone()
                if row and isinstance(row[0], str):
                    return row[0]
                return None
        except sqlite3.Error as e:
            self.logger.warning(
                f"Failed to retrieve hash mapping: {e.__class__.__name__}: {e}"
            )
            return None

    def get_duration(self, video_id: str) -> int | None:
        """Retrieve cached duration for a video ID."""
        duration = self.get(f"duration:{video_id}")
        if duration is not None:
            self.logger.debug(f"Retrieved cached duration for {video_id}: {duration}s")
        return duration

    def mark_unavailable_track(
        self, video_id: str, path: str | None, reason: str
    ) -> None:
        """Persist that a track cannot be streamed."""
        if not video_id:
            return
        self.unavailable_video_ids.add(video_id)
        if path:
            self.unavailable_paths.add(path)
        self.set(
            f"unavailable:{video_id}",
            {
                "videoId": video_id,
                "path": path,
                "reason": reason,
                "timestamp": time.time(),
            },
        )
        self._invalidate_unavailable_track_path(path)

    def _invalidate_unavailable_track_path(self, path: str | None) -> None:
        """Drop cached path/listing entries that can keep dead tracks visible."""
        if not path:
            return

        parent_dir = os.path.dirname(path)
        if not parent_dir:
            return

        listing_key = f"{parent_dir}_listing_with_attrs"

        self.valid_paths.discard(path)
        self.path_types.pop(path, None)
        self.path_validation_cache.pop(path, None)
        self.attrs_cache.pop(path, None)
        self.directory_listings_cache.pop(parent_dir, None)

        self.delete(f"valid_dir:{path}")
        self.delete(f"exact_path:{path}")
        self.delete(f"video_id:{path}")
        self.delete(f"valid_files:{parent_dir}")
        self.delete(listing_key)

    def get_unavailable_track(self, video_id: str) -> dict[str, Any] | None:
        """Return persisted unavailable-track metadata for a video ID."""
        if not video_id:
            return None
        value = self.get(f"unavailable:{video_id}")
        return value if isinstance(value, dict) else None

    def get_unavailable_tracks(self) -> list[dict[str, Any]]:
        """Return all persisted unavailable-track metadata."""
        tracks = []
        try:
            with self.lock:
                cursor = self.conn.cursor()
                cursor.execute(
                    "SELECT entry FROM cache_entries WHERE key LIKE ?",
                    (self.path_to_key("unavailable:") + "%",),
                )
                rows = cursor.fetchall()
        except sqlite3.Error as e:
            self.logger.warning(
                "Failed to list unavailable-track cache: %s: %s",
                e.__class__.__name__,
                e,
            )
            return []

        for (entry,) in rows:
            with suppress(json.JSONDecodeError, KeyError, TypeError):
                cache_data = json.loads(entry)
                data = cache_data["data"]
                if isinstance(data, dict):
                    tracks.append(data)
        return tracks

    def clear_unavailable_track(self, video_id: str, path: str | None = None) -> None:
        """Remove unavailable-track metadata after a successful repair."""
        if not video_id:
            return
        self.unavailable_video_ids.discard(video_id)
        if path:
            self.unavailable_paths.discard(path)
        self.delete(f"unavailable:{video_id}")

    def is_track_unavailable(self, video_id: str) -> bool:
        """Return whether a video ID is marked unavailable."""
        return video_id in self.unavailable_video_ids

    def is_path_unavailable(self, path: str) -> bool:
        """Return whether a filesystem path is marked unavailable."""
        return path in self.unavailable_paths

    def get_unavailable_video_ids(self) -> builtins.set[str]:
        """Return a snapshot of unavailable video IDs."""
        return set(self.unavailable_video_ids)

    def set_durations_batch(self, durations: dict[str, int]) -> None:
        """Store multiple durations in a batch operation with optimized performance.

        Args:
            durations: Dictionary mapping video IDs to duration in seconds
        """
        if not durations:
            return

        cache_entries = {}
        now = time.time()

        for video_id, duration in durations.items():
            hotcache_key = f"hotcache:duration:{video_id}"
            self.hotcache[hotcache_key] = {"data": duration, "time": now}

            cache_entries[f"duration:{video_id}"] = duration

        self.set_batch(cache_entries)
        self.logger.info(
            f"Cached {len(durations)} durations in a single batch operation"
        )

    def get_directory_listing_with_attrs(
        self, path: str
    ) -> dict[str, dict[str, Any]] | None:
        """Get cached directory listing with attributes with improved performance.

        Args:
            path: Directory path

        Returns:
            Dictionary mapping filenames to their attributes,
            or None if not cached or expired
        """
        if path in self.directory_listings_cache:
            cached = self.directory_listings_cache[path]
            current_time = time.time()

            if current_time - cached["time"] < self.cache_timeout:
                self.logger.debug(f"In-memory cache hit for directory listing: {path}")
                self.stats["hits"] += 1
                return self._as_directory_listing(cached.get("data"))

        cache_key = f"{path}_listing_with_attrs"

        hot_key = f"hot:{cache_key}"
        hot_cached = self.hotcache.get(hot_key)
        if hot_cached and time.time() - hot_cached["time"] < self.cache_timeout:
            self.logger.debug(f"Hot cache hit for directory listing: {path}")
            self.stats["hits"] += 1
            self.directory_listings_cache[path] = {
                "data": hot_cached["data"],
                "time": hot_cached["time"],
            }
            return self._as_directory_listing(hot_cached.get("data"))

        db_key = self.path_to_key(cache_key)
        try:
            with self.lock:
                cursor = self.conn.cursor()
                cursor.execute(
                    "SELECT entry FROM cache_entries WHERE key = ?", (db_key,)
                )
                row = cursor.fetchone()
                if row:
                    try:
                        cache_data = json.loads(row[0])
                        if time.time() - cache_data["time"] < self.cache_timeout:
                            listing = self._as_directory_listing(cache_data.get("data"))
                            if listing is None:
                                self.stats["db_misses"] += 1
                                return None

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

    @staticmethod
    def _as_directory_listing(
        value: object,
    ) -> dict[str, dict[str, Any]] | None:
        if not isinstance(value, dict):
            return None
        return cast("dict[str, dict[str, Any]]", value)

    def set_directory_listing_with_attrs(
        self, path: str, listing_with_attrs: dict[str, dict[str, Any]]
    ) -> None:
        """Cache directory listing with attributes using optimized storage.

        Args:
            path: Directory path
            listing_with_attrs: Dictionary mapping filenames to their attributes
        """
        if not listing_with_attrs:
            self.logger.debug(f"Skipping empty directory listing for {path}")
            return

        current_time = time.time()

        self.directory_listings_cache[path] = {
            "data": listing_with_attrs,
            "time": current_time,
        }

        batch_entries = {}

        # Persist child validity in one batch with the listing.
        batch_entries[f"valid_dir:{path}"] = {"data": True, "time": current_time}

        for filename, attrs in listing_with_attrs.items():
            if filename in [".", ".."]:
                continue

            is_dir = bool(attrs.get("st_mode", 0) & stat.S_IFDIR == stat.S_IFDIR)
            child_path = f"{path}/{filename}"
            entry_type = "valid_dir:" if is_dir else "exact_path:"
            batch_entries[f"{entry_type}{child_path}"] = {
                "data": True,
                "time": current_time,
            }

            self.attrs_cache[child_path] = attrs

        cache_key = f"{path}_listing_with_attrs"
        hot_key = f"hot:{cache_key}"
        self.hotcache[hot_key] = {
            "data": listing_with_attrs,
            "time": current_time,
        }

        db_key = self.path_to_key(cache_key)
        entry = {
            "data": listing_with_attrs,
            "time": current_time,
        }

        try:
            entry_type = "directory"
            metadata = {
                "entries_count": len(listing_with_attrs),
                "cached_at": current_time,
                "path_length": len(path),
            }

            entry_str = json.dumps(entry)
            metadata_str = json.dumps(metadata)

            with self.lock:
                cursor = self.conn.cursor()
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO cache_entries
                    (key, entry, entry_type, metadata)
                    VALUES (?, ?, ?, ?)
                    """,
                    (db_key, entry_str, entry_type, metadata_str),
                )
                self.conn.commit()
                self._pending_writes = 0

            self.set_batch(batch_entries)

            self.logger.debug(
                f"Cached directory listing with {len(listing_with_attrs)} entries for {path}"
            )
        except (sqlite3.Error, json.JSONDecodeError) as e:
            self.logger.warning(
                f"Failed to cache directory listing: {e.__class__.__name__}: {e}"
            )

    def get_file_attrs_from_parent_dir(self, path: str) -> dict[str, Any] | None:
        """Return file attributes from the nearest cached directory listing."""
        cached_attrs = self.attrs_cache.get(path)
        if cached_attrs:
            return self._record_attrs_hit(path, cached_attrs)

        parent_dir = os.path.dirname(path)
        filename = os.path.basename(path)
        if not parent_dir or not filename:
            return self._record_attrs_miss(path)

        attrs = self._attrs_from_listing(parent_dir, filename)
        if attrs:
            return self._record_attrs_hit(path, attrs)

        attrs = self._attrs_for_registry_directory(path, parent_dir)
        if attrs:
            return self._record_attrs_hit(path, attrs)

        attrs = self._attrs_from_grandparent(path, parent_dir)
        if attrs:
            return self._record_attrs_hit(path, attrs)
        return self._record_attrs_miss(path)

    def _attrs_from_listing(
        self, parent_dir: str, filename: str
    ) -> dict[str, Any] | None:
        listing = self.get_directory_listing_with_attrs(parent_dir)
        return listing.get(filename) if listing else None

    def _attrs_for_registry_directory(
        self, path: str, parent_dir: str
    ) -> dict[str, Any] | None:
        if parent_dir not in {"/playlists", "/albums"} or len(path.split("/")) != 3:
            return None
        return self._create_directory_attrs() if self.is_valid_path(path) else None

    def _attrs_from_grandparent(
        self, path: str, parent_dir: str
    ) -> dict[str, Any] | None:
        if parent_dir == "/" or len(path.split("/")) <= 3:
            return None
        listing = self.get_directory_listing_with_attrs(os.path.dirname(parent_dir))
        parent_attrs = listing.get(os.path.basename(parent_dir)) if listing else None
        if not parent_attrs or not self._attrs_are_directory(parent_attrs):
            return None
        is_directory = self.is_directory(path)
        if is_directory is None:
            return None
        return (
            self._create_directory_attrs()
            if is_directory
            else self._create_file_attrs()
        )

    def _record_attrs_hit(self, path: str, attrs: dict[str, Any]) -> dict[str, Any]:
        self.attrs_cache[path] = attrs
        self.stats["hits"] += 1
        return attrs

    def _record_attrs_miss(self, path: str) -> dict[str, Any] | None:
        self.logger.debug("No attributes found for %s", path)
        self.stats["misses"] += 1
        return None

    def _create_directory_attrs(self) -> dict[str, Any]:
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

    def _create_file_attrs(self) -> dict[str, Any]:
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

    def is_directory(self, path: str) -> bool | None:
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

    def set_refresh_metadata(
        self, key: str, timestamp: float, status: str = "fresh"
    ) -> None:
        """Set refresh metadata for a cache key.

        Args:
            key: The key to set refresh metadata for
            timestamp: The timestamp of the refresh
            status: Status of the refresh ('fresh', 'pending', or 'stale')
        """
        if status not in ("fresh", "pending", "stale"):
            self.logger.warning(
                f"Invalid refresh status '{status}' for {key}, using 'fresh'"
            )
            status = "fresh"

        try:
            with self.lock:
                cursor = self.conn.cursor()
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO refresh_tracker (key, last_refresh, status)
                    VALUES (?, ?, ?)
                    """,
                    (self.path_to_key(key), timestamp, status),
                )
                self.conn.commit()
                self._pending_writes = 0
            self.logger.debug(
                f"Set refresh metadata for {key}: {status} at {timestamp}"
            )
        except sqlite3.Error as e:
            self.logger.warning(
                f"Failed to set refresh metadata for {key}: {e.__class__.__name__}: {e}"
            )

    def get_refresh_metadata(self, key: str) -> tuple[float | None, str | None]:
        """Get last refresh time and status for a cache key.

        Args:
            key: The key to get refresh metadata for

        Returns:
            Tuple of (timestamp, status) or (None, None) if not found
        """
        try:
            db_key = self.path_to_key(key)
            with self.lock:
                cursor = self.conn.cursor()
                cursor.execute(
                    "SELECT last_refresh, status FROM refresh_tracker WHERE key = ?",
                    (db_key,),
                )
                row = cursor.fetchone()
                if row:
                    return row[0], row[1]
            return None, None
        except sqlite3.Error as e:
            self.logger.warning(
                f"Failed to get refresh metadata for {key}: {e.__class__.__name__}: {e}"
            )
            return None, None

    def _atomic_write(self, path: Path, content: str) -> None:
        """Write content atomically via a temp file and rename."""
        tmp = path.with_suffix(path.suffix + ".tmp")
        try:
            tmp.write_text(content, encoding="utf-8")
            tmp.rename(path)
        except Exception:
            with suppress(OSError):
                tmp.unlink(missing_ok=True)
            raise

    def record_repair_trigger(self, repairs: list[dict[str, Any]]) -> None:
        """Write a repair trigger file for the mount process to pick up.

        Args:
            repairs: List of repair dicts with old_video_id, path, new_video_id.
        """
        if not repairs:
            return
        try:
            trigger = self.cache_dir / ".repair_trigger"
            data = {"timestamp": time.time(), "repairs": repairs}
            self._atomic_write(trigger, json.dumps(data))
            self.logger.debug("Wrote repair trigger with %d repairs", len(repairs))
        except OSError as e:
            self.logger.warning("Failed to write repair trigger: %s", e)

    def record_cache_trigger(self, action: str) -> None:
        """Write a cache refresh or clear trigger file.

        Args:
            action: One of 'refresh' or 'clear'.
        """
        if action not in ("refresh", "clear"):
            raise ValueError(f"Invalid cache trigger action: {action}")
        try:
            trigger = self.cache_dir / f".{action}_trigger"
            self._atomic_write(trigger, str(time.time()))
            self.logger.debug("Wrote %s trigger", action)
        except OSError as e:
            self.logger.warning("Failed to write %s trigger: %s", action, e)

    def get_pending_repair_trigger(self) -> dict[str, Any] | None:
        """Return pending repair trigger data if any.

        Returns:
            Trigger dict with timestamp and repairs, or None.
        """
        trigger = self.cache_dir / ".repair_trigger"
        if not trigger.exists():
            return None
        try:
            data = json.loads(trigger.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else None
        except (OSError, json.JSONDecodeError) as e:
            self.logger.warning("Failed to read repair trigger: %s", e)
            return None

    def clear_repair_trigger(self) -> None:
        """Remove the repair trigger file after processing."""
        trigger = self.cache_dir / ".repair_trigger"
        if trigger.exists():
            try:
                trigger.unlink()
            except OSError as e:
                self.logger.warning("Failed to remove repair trigger: %s", e)

    def get_pending_cache_trigger(self) -> str | None:
        """Return pending cache action ('clear' or 'refresh') if any.

        Returns:
            Action string or None.
        """
        for action in ("clear", "refresh"):
            trigger = self.cache_dir / f".{action}_trigger"
            if trigger.exists():
                return action
        return None

    def clear_cache_trigger(self, action: str) -> None:
        """Remove a cache trigger file after processing."""
        trigger = self.cache_dir / f".{action}_trigger"
        if trigger.exists():
            try:
                trigger.unlink()
            except OSError as e:
                self.logger.warning("Failed to remove %s trigger: %s", action, e)

    def invalidate_repaired_paths(self, repairs: list[dict[str, Any]]) -> None:
        """Surgically invalidate in-memory caches for repaired track paths.

        Args:
            repairs: List of repair dicts with old_video_id and path keys.
        """
        if not repairs:
            return
        changed = False
        parent_dirs = set()
        for repair in repairs:
            old_video_id = repair.get("old_video_id")
            path = repair.get("path")
            if not old_video_id or not path:
                continue
            parent_dir = os.path.dirname(path)
            if parent_dir:
                parent_dirs.add(parent_dir)
            self.unavailable_video_ids.discard(old_video_id)
            self.unavailable_paths.discard(path)
            self.valid_paths.discard(path)
            self.path_validation_cache.pop(path, None)
            self.attrs_cache.pop(path, None)
            for hot_key in (f"hotcache:video_id:{path}",):
                self.hotcache.pop(hot_key, None)
            self.delete(f"valid_dir:{path}")
            self.delete(f"video_id:{path}")
            changed = True
            self.logger.debug("Invalidated cache for repaired path %s", path)
        if changed:
            for parent_dir in parent_dirs:
                self.directory_listings_cache.pop(parent_dir, None)
                self.hotcache.pop(f"hotcache:{parent_dir}_processed", None)
                self.hotcache.pop(f"hot:{parent_dir}_listing_with_attrs", None)
                self.delete(f"{parent_dir}_listing_with_attrs")
                self.delete(f"{parent_dir}_listing")
            self.logger.info(
                "Applied repair invalidations for %d path(s)", len(repairs)
            )

    def clear_metadata(self) -> None:
        """Clear all metadata from the persistent cache and in-memory state."""
        try:
            with self.lock:
                cursor = self.conn.cursor()
                cursor.execute("DELETE FROM cache_entries")
                cursor.execute("DELETE FROM hash_mappings")
                cursor.execute("DELETE FROM refresh_tracker")
                self.conn.commit()
                self._pending_writes = 0

                self.hotcache.clear()
                self.directory_listings_cache.clear()
                self.path_validation_cache.clear()
                self.attrs_cache.clear()
                self.valid_paths.clear()
                self.path_types.clear()
                self.unavailable_video_ids.clear()
                self.unavailable_paths.clear()
        except sqlite3.Error as e:
            self.logger.warning(
                "Failed to clear metadata cache: %s: %s",
                e.__class__.__name__,
                e,
            )

        self.logger.info("Metadata cache cleared")

    def clear_all(self) -> None:
        """Clear metadata and audio/ranges caches."""
        self.clear_metadata()
        for subdir in ("audio", "ranges"):
            path = self.cache_dir / subdir
            if path.exists():
                try:
                    shutil.rmtree(path)
                    self.logger.info("Removed %s cache directory", subdir)
                except OSError as e:
                    self.logger.warning("Failed to remove %s: %s", subdir, e)

    def close(self) -> None:
        """Close the cache and release resources."""
        if self._closed:
            return
        try:
            with self.lock:
                self.conn.commit()
                self._pending_writes = 0
                self.conn.close()
                self._closed = True
            self.logger.debug("Cache database connection closed")
        except Exception as e:
            self.logger.error(f"Error closing cache: {e}")
            self.logger.error(traceback.format_exc())

    def __del__(self) -> None:
        """Attempt to close database connection during garbage collection."""
        with suppress(Exception):
            self.close()

    def mark_no_replacement(self, video_id: str, path: str, ttl: int = 86400) -> None:
        """Persist that a track has no verified replacement.

        Args:
            video_id: YouTube video ID
            path: Filesystem path
            ttl: Time-to-live in seconds (default: 24 hours)
        """
        self.set(
            f"no_replacement:{video_id}",
            {"videoId": video_id, "path": path, "timestamp": time.time(), "ttl": ttl},
        )

    def is_no_replacement(self, video_id: str) -> bool:
        """Return whether a track is known to have no replacement.

        Args:
            video_id: YouTube video ID

        Returns:
            True if a verified replacement is known to not exist.
        """
        data = self.get(f"no_replacement:{video_id}")
        if not isinstance(data, dict):
            return False
        timestamp = data.get("timestamp", 0)
        ttl = data.get("ttl", 86400)
        if time.time() - timestamp > ttl:
            self.delete(f"no_replacement:{video_id}")
            return False
        return True

    def get_cache_stats(self) -> dict[str, int | float]:
        """Get cache statistics.

        Returns:
            Dictionary with cache statistics.
        """
        hit_rate = 0.0
        if (self.stats["hits"] + self.stats["misses"]) > 0:
            hit_rate = (
                self.stats["hits"] / (self.stats["hits"] + self.stats["misses"]) * 100
            )

        db_hit_rate = 0.0
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

    def update_file_attrs_in_parent_dir(self, path: str, attrs: dict[str, Any]) -> None:
        """Update file attributes in the parent directory's cached listing with improved caching.

        Args:
            path: The file path
            attrs: The file attributes to update
        """
        parent_dir = os.path.dirname(path)
        filename = os.path.basename(path)

        self.attrs_cache[path] = {"attrs": attrs, "time": time.time()}

        if parent_dir in self.directory_listings_cache:
            cached = self.directory_listings_cache[parent_dir]
            dir_listing = cached["data"]
            if dir_listing:
                dir_listing[filename] = attrs
                self.logger.debug(
                    f"Updated attributes for {filename} in {parent_dir} memory cache"
                )

        dir_listing = self.get_directory_listing_with_attrs(parent_dir)

        if dir_listing is not None:
            dir_listing[filename] = attrs
            self.set_directory_listing_with_attrs(parent_dir, dir_listing)
            self.logger.debug(
                f"Updated attributes for {filename} in {parent_dir} database cache"
            )
