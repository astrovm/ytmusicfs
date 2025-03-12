#!/usr/bin/env python3

from cachetools import LRUCache
from contextlib import contextmanager
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

        # Initialize locks
        self.cache_lock = threading.RLock()  # Lock for in-memory cache access
        self.db_lock = threading.RLock()  # Dedicated lock for database operations

        # Initialize SQLite database
        self.db_path = self.cache_dir / "cache.db"
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL;")

        # Create a single cursor for all operations
        with self.db_lock:
            self.cursor = self.conn.cursor()
            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS cache_entries (
                    key TEXT PRIMARY KEY,
                    entry TEXT
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

        self.cache = LRUCache(
            maxsize=maxsize
        )  # In-memory cache with LRU eviction strategy
        self.cache_timeout = cache_timeout

    def path_to_key(self, path: str) -> str:
        """Convert a filesystem path to a cache key.

        Args:
            path: The filesystem path

        Returns:
            Sanitized cache key suitable for database storage
        """
        # Enhanced sanitization: replace slashes and special characters
        key = path.replace("/", "_").replace("'", "''").replace(" ", "_")

        # If the key is too long (> 200 chars), hash it to avoid key length issues
        # Keep a prefix to make it somewhat readable/debuggable
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
        # If this is a hashed key, try to look up the original path
        if self.is_hashed_key(key):
            original_path = self.get_original_path(key)
            if original_path:
                return original_path
            self.logger.warning(f"Cannot convert hashed key back to path: {key}")
            return key  # Return as is if we can't reconstruct the original

        # Regular key - just replace underscores with slashes
        # We need to reverse any special character handling here
        path = key.replace("_", "/")
        # Restore single quotes if they were escaped
        path = path.replace("''", "'")
        return path

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

        # Then check SQLite cache
        key = self.path_to_key(path)
        with self.db_lock:
            try:
                self.cursor.execute(
                    "SELECT entry FROM cache_entries WHERE key = ?", (key,)
                )
                row = self.cursor.fetchone()
                if row:
                    cache_data = json.loads(row[0])
                    if time.time() - cache_data["time"] < self.cache_timeout:
                        self.logger.debug(f"Cache hit (disk) for {path}")
                        # Also update memory cache
                        with self.cache_lock:
                            self.cache[path] = cache_data
                        return cache_data["data"]
            except sqlite3.Error as e:
                self.logger.debug(
                    f"Failed to read database cache for {path}: {e.__class__.__name__}: {e}"
                )

        return None

    def set(self, path: str, data: Any) -> None:
        """Set data in both memory and SQLite cache.

        Args:
            path: The path to cache
            data: The data to cache
        """
        cache_entry = {"data": data, "time": time.time()}

        # Update memory cache (with lock)
        with self.cache_lock:
            self.cache[path] = cache_entry

        # Update SQLite cache with a dedicated lock
        key = self.path_to_key(path)
        entry_str = json.dumps(cache_entry)

        with self.db_lock:
            try:
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
        with self.cache_lock:
            if path in self.cache:
                del self.cache[path]

        # Also delete from SQLite cache with a dedicated lock
        key = self.path_to_key(path)
        with self.db_lock:
            try:
                self.cursor.execute("DELETE FROM cache_entries WHERE key = ?", (key,))
                self.conn.commit()
            except sqlite3.Error as e:
                self.logger.warning(
                    f"Failed to delete from database cache for {path}: {e.__class__.__name__}: {e}"
                )

    def is_hashed_key(self, key: str) -> bool:
        """Check if a key is a hashed key.

        Args:
            key: The key to check

        Returns:
            True if the key is a hashed key, False otherwise
        """
        return (
            "_" in key
            and len(key) > 30
            and key[30:31] == "_"
            and len(key) - key.rfind("_") == 33
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

        # First check memory cache
        with self.cache_lock:
            for key in self.cache.keys():
                if re.match(f"^{pattern_re}$", key):
                    matching_keys.append(key)

        # Also check SQLite database for keys not in memory
        with self.db_lock:
            try:
                # Fetch all keys from the database
                self.cursor.execute("SELECT key FROM cache_entries")
                all_keys = [row[0] for row in self.cursor.fetchall()]

                for key in all_keys:
                    # For hashed keys, get the original path and check against pattern
                    if self.is_hashed_key(key):
                        original_path = self.get_original_path(key)
                        if original_path and re.match(f"^{pattern_re}$", original_path):
                            if original_path not in matching_keys:
                                matching_keys.append(original_path)
                    else:
                        # For regular keys, convert back to path and check against pattern
                        path = self.key_to_path(key)
                        if (
                            re.match(f"^{pattern_re}$", path)
                            and path not in matching_keys
                        ):
                            matching_keys.append(path)

            except sqlite3.Error as e:
                self.logger.warning(
                    f"Failed to get keys matching pattern {pattern}: {e.__class__.__name__}: {e}"
                )

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

        # First handle memory cache
        pattern_re = pattern.replace("*", ".*")
        with self.cache_lock:
            # Find all matching keys in memory cache
            keys_to_delete = [
                k for k in self.cache.keys() if re.match(f"^{pattern_re}$", k)
            ]
            for key in keys_to_delete:
                del self.cache[key]
                count += 1

        # Get all matching paths - leveraging our updated get_keys_by_pattern method
        matching_paths = self.get_keys_by_pattern(pattern)

        # Delete each matching path from the database within a single transaction
        with self.db_lock:
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
            with self.cache_lock:
                if not hasattr(self, "hash_to_path"):
                    self.hash_to_path = {}
                self.hash_to_path[hashed_key] = original_path

            # Store in SQLite database with a dedicated lock
            with self.db_lock:
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
        with self.cache_lock:
            if hasattr(self, "hash_to_path") and hashed_key in self.hash_to_path:
                return self.hash_to_path[hashed_key]

        # Then check in SQLite database with a dedicated lock
        with self.db_lock:
            try:
                self.cursor.execute(
                    "SELECT original_path FROM hash_mappings WHERE hashed_key = ?",
                    (hashed_key,),
                )
                row = self.cursor.fetchone()
                if row:
                    original_path = row[0]
                    # Also update in-memory cache for next time
                    with self.cache_lock:
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
        last_refresh_time = self.get_metadata(cache_key, "last_refresh_time")
        current_time = time.time()

        if (
            last_refresh_time is None
            or (current_time - last_refresh_time) > refresh_interval
        ):
            self.logger.info(f"Auto-refreshing cache for {cache_key}")
            refresh_method()
            # Mark the cache as freshly refreshed
            self.set_metadata(cache_key, "last_refresh_time", current_time)
            return True
        return False

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

    def refresh_cache_data(
        self,
        cache_key: str,
        fetch_func: Callable,
        processor: Optional[Any] = None,
        id_fields: List[str] = ["id"],
        check_updates: bool = False,
        update_field: str = None,
        clear_related_cache: bool = False,
        related_cache_prefix: str = None,
        related_cache_suffix: str = None,
        fetch_args: Dict = None,
        process_items: bool = False,
        processed_cache_key: str = None,
        extract_nested_items: str = None,
        prepend_new_items: bool = False,
    ) -> Dict[str, Any]:
        """Generic method to refresh any cache with a smart merging approach.

        Args:
            cache_key: The cache key to refresh
            fetch_func: Function to call to fetch new data
            processor: Optional processor object with process_tracks method
            id_fields: List of possible field names for ID in priority order
            check_updates: Whether to check for updates to existing items
            update_field: Field to check for updates if check_updates is True
            clear_related_cache: Whether to clear related caches for updated items
            related_cache_prefix: Prefix for related cache keys
            related_cache_suffix: Suffix for related cache keys
            fetch_args: Optional arguments to pass to the fetch function
            process_items: Whether to process items (e.g., for tracks)
            processed_cache_key: Cache key for processed items
            extract_nested_items: Key to extract nested items from response
            prepend_new_items: Whether to add new items to the beginning of the list

        Returns:
            Dictionary with info about the refresh (new_items, updated_items, etc.)
        """
        self.logger.info(f"Refreshing {cache_key} cache...")
        result = {
            "new_items": 0,
            "updated_items": 0,
            "total_items": 0,
            "success": False,
        }

        # Get existing cached data
        existing_items = self.get(cache_key)
        existing_processed_items = None
        if process_items and processed_cache_key:
            existing_processed_items = self.get(processed_cache_key)

        # Fetch recent data
        self.logger.debug(f"Fetching recent data for {cache_key}...")
        if fetch_args:
            recent_data = fetch_func(**fetch_args)
        else:
            recent_data = fetch_func()

        if not recent_data:
            self.logger.info(f"No data found or error fetching data for {cache_key}")
            return result

        # Extract nested items if needed
        recent_items = recent_data
        if (
            extract_nested_items
            and isinstance(recent_data, dict)
            and extract_nested_items in recent_data
        ):
            recent_items = recent_data[extract_nested_items]

        # Handle case where we have existing items
        if existing_items:
            self.logger.info(
                f"Found {len(existing_items if not extract_nested_items else existing_items.get(extract_nested_items, []))} existing items in {cache_key} cache"
            )

            # Build a set of existing IDs and a mapping for updates
            existing_ids = set()
            existing_item_map = {}

            # Extract nested items from existing data if needed
            existing_nested_items = existing_items
            if (
                extract_nested_items
                and isinstance(existing_items, dict)
                and extract_nested_items in existing_items
            ):
                existing_nested_items = existing_items[extract_nested_items]
            else:
                existing_nested_items = existing_items

            for item in existing_nested_items:
                # Find the first available ID field
                item_id = None
                for id_field in id_fields:
                    if item.get(id_field):
                        item_id = item.get(id_field)
                        existing_ids.add(item_id)
                        existing_item_map[item_id] = item
                        break

            # Find new items
            new_items = []
            updated_items = []

            for item in recent_items:
                # Find the item ID
                item_id = None
                for id_field in id_fields:
                    if item.get(id_field):
                        item_id = item.get(id_field)
                        break

                if not item_id:
                    continue

                if item_id not in existing_ids:
                    # New item
                    new_items.append(item)
                elif (
                    check_updates
                    and update_field
                    and item.get(update_field)
                    != existing_item_map[item_id].get(update_field)
                ):
                    # Updated item
                    updated_items.append(item)

            if new_items or updated_items:
                action_text = []
                if new_items:
                    action_text.append(f"{len(new_items)} new")
                if updated_items:
                    action_text.append(f"{len(updated_items)} updated")

                self.logger.info(
                    f"Found {' and '.join(action_text)} items to add to {cache_key} cache"
                )

                # Get list of updated and new item IDs
                changed_ids = set()
                for item in new_items + updated_items:
                    for id_field in id_fields:
                        if item.get(id_field):
                            changed_ids.add(item.get(id_field))
                            break

                # Handle nested structure update
                if extract_nested_items and isinstance(existing_items, dict):
                    # Filter out changed items
                    unchanged_items = [
                        item
                        for item in existing_nested_items
                        if not any(
                            item.get(id_field) in changed_ids
                            for id_field in id_fields
                            if item.get(id_field)
                        )
                    ]

                    # Determine merge order based on prepend_new_items
                    if prepend_new_items:
                        merged_nested_items = (
                            new_items + updated_items + unchanged_items
                        )
                    else:
                        merged_nested_items = (
                            unchanged_items + new_items + updated_items
                        )

                    # Update the nested structure
                    existing_items[extract_nested_items] = merged_nested_items
                    self.set(cache_key, existing_items)
                else:
                    # Filter out changed items
                    unchanged_items = [
                        item
                        for item in existing_nested_items
                        if not any(
                            item.get(id_field) in changed_ids
                            for id_field in id_fields
                            if item.get(id_field)
                        )
                    ]

                    # Determine merge order based on prepend_new_items
                    if prepend_new_items:
                        merged_items = new_items + updated_items + unchanged_items
                    else:
                        merged_items = unchanged_items + new_items + updated_items

                    self.set(cache_key, merged_items)

                self.logger.info(
                    f"Updated cache with {len(merged_nested_items if extract_nested_items else merged_items)} total items"
                )

                # Process items if needed (for tracks)
                if process_items and processed_cache_key and processor is not None:
                    if new_items or updated_items:
                        # Process just the new/updated items
                        items_to_process = new_items + updated_items
                        new_processed_items, _ = processor.process_tracks(
                            items_to_process
                        )

                        if existing_processed_items:
                            # Filter out processed items corresponding to changed items
                            unchanged_processed_items = [
                                item
                                for item in existing_processed_items
                                if not any(
                                    item.get(id_field) in changed_ids
                                    for id_field in id_fields
                                    if item.get(id_field)
                                )
                            ]

                            # Determine merge order based on prepend_new_items
                            if prepend_new_items:
                                merged_processed_items = (
                                    new_processed_items + unchanged_processed_items
                                )
                            else:
                                merged_processed_items = (
                                    unchanged_processed_items + new_processed_items
                                )

                            self.set(processed_cache_key, merged_processed_items)
                            self.logger.info(
                                f"Updated processed cache with {len(merged_processed_items)} total items"
                            )
                        else:
                            self.logger.warning(
                                f"Existing processed items not found at {processed_cache_key}, skipping update"
                            )

                # Clear related caches if needed
                if clear_related_cache and (
                    related_cache_prefix or related_cache_suffix
                ):
                    for item in updated_items:
                        for id_field in id_fields:
                            item_id = item.get(id_field)
                            if item_id:
                                if related_cache_prefix:
                                    related_key = f"{related_cache_prefix}{item_id}"
                                    self.delete(related_key)

                                if related_cache_suffix:
                                    related_key = f"{item_id}{related_cache_suffix}"
                                    self.delete(related_key)
                                break

                result.update(
                    {
                        "new_items": len(new_items),
                        "updated_items": len(updated_items),
                        "total_items": len(
                            merged_nested_items
                            if extract_nested_items
                            else merged_items
                        ),
                        "success": True,
                    }
                )
                return result
            else:
                self.logger.info(f"No changes detected for {cache_key} cache")
                result.update(
                    {"success": True, "total_items": len(existing_nested_items)}
                )
                return result
        else:
            self.logger.info(f"No existing {cache_key} cache found, will create cache")
            # Just cache the recent data to make next access faster
            self.set(cache_key, recent_data)

            # Process and cache items if needed
            if (
                process_items
                and processed_cache_key
                and recent_items
                and processor is not None
            ):
                processed_items, _ = processor.process_tracks(recent_items)
                self.set(processed_cache_key, processed_items)
                self.logger.info(
                    f"Created processed cache with {len(processed_items)} items"
                )
                result.update(
                    {
                        "new_items": len(recent_items),
                        "total_items": len(recent_items),
                        "success": True,
                    }
                )
            else:
                result.update(
                    {
                        "new_items": (
                            len(recent_items) if isinstance(recent_items, list) else 1
                        ),
                        "total_items": (
                            len(recent_items) if isinstance(recent_items, list) else 1
                        ),
                        "success": True,
                    }
                )

            return result

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
                [f"valid_files:{parent_dir}", f"valid_base_names:{parent_dir}"]
            )

        # Delete each key
        for key_pattern in key_patterns:
            # Delete exact match first
            self.delete(key_pattern)

            # Then delete any pattern match (for wildcards)
            if "*" in key_pattern:
                self.delete_pattern(key_pattern)

        self.logger.debug(f"Finished cleaning path metadata for: {path}")

    def __del__(self):
        """Clean up resources when the object is deleted."""
        try:
            if hasattr(self, "conn") and self.conn:
                self.conn.close()
        except Exception as e:
            # We can't log here as the logger might be gone
            pass
