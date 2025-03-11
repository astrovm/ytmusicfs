#!/usr/bin/env python3

from cachetools import LRUCache
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Optional, Callable, Dict, List
import json
import logging
import threading
import time
import hashlib


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

    def path_to_key(self, path: str) -> str:
        """Convert a filesystem path to a cache key.

        Args:
            path: The filesystem path

        Returns:
            Sanitized cache key suitable for filesystem storage
        """
        # First replace slashes with underscores for basic path conversion
        key = path.replace('/', '_')

        # If the key is too long (> 200 chars), hash it to avoid filename length issues
        # Keep a prefix to make it somewhat readable/debuggable
        MAX_KEY_LENGTH = 200
        if len(key) > MAX_KEY_LENGTH:
            # Get the first 30 chars for readability
            prefix = key[:30]
            # Hash the full path for uniqueness
            path_hash = hashlib.md5(path.encode('utf-8')).hexdigest()
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
        if '_' in key and len(key) > 30 and key[30:31] == '_' and len(key) - key.rfind('_') == 33:
            original_path = self.get_original_path(key)
            if original_path:
                return original_path
            self.logger.warning(f"Cannot convert hashed key back to path: {key}")
            return key  # Return as is if we can't reconstruct the original

        # Regular key - just replace underscores with slashes
        return key.replace('_', '/')

    def get_cache_file_path(self, path: str) -> Path:
        """Get the filesystem path for a cache entry.

        Args:
            path: The cache path

        Returns:
            Path object pointing to the cache file
        """
        # Get the sanitized key
        key = self.path_to_key(path)

        # If this is a hashed key (for long paths), store it in a 'hashed' subdirectory
        # to keep the cache organization clean
        if '_' in key and len(key) > 30 and key[30:31] == '_' and len(key) - key.rfind('_') == 33:
            # It's a hashed key - create in a hashed subdir for organization
            hashed_dir = self.cache_dir / "hashed"
            hashed_dir.mkdir(exist_ok=True)
            return hashed_dir / f"{key}.json"

        # Regular key
        return self.cache_dir / f"{key}.json"

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
        cache_file = self.get_cache_file_path(path)
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
            cache_file = self.get_cache_file_path(path)
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
            cache_file = self.get_cache_file_path(path)
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

    def auto_refresh_cache(self, cache_key: str, refresh_method: Callable, refresh_interval: int = 600) -> bool:
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

        if last_refresh_time is None or (current_time - last_refresh_time) > refresh_interval:
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
            "success": False
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
        if extract_nested_items and isinstance(recent_data, dict) and extract_nested_items in recent_data:
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
            if extract_nested_items and isinstance(existing_items, dict) and extract_nested_items in existing_items:
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
                elif check_updates and update_field and item.get(update_field) != existing_item_map[item_id].get(update_field):
                    # Updated item
                    updated_items.append(item)

            if new_items or updated_items:
                action_text = []
                if new_items:
                    action_text.append(f"{len(new_items)} new")
                if updated_items:
                    action_text.append(f"{len(updated_items)} updated")

                self.logger.info(f"Found {' and '.join(action_text)} items to add to {cache_key} cache")

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
                        item for item in existing_nested_items
                        if not any(item.get(id_field) in changed_ids for id_field in id_fields if item.get(id_field))
                    ]

                    # Determine merge order based on prepend_new_items
                    if prepend_new_items:
                        merged_nested_items = new_items + updated_items + unchanged_items
                    else:
                        merged_nested_items = unchanged_items + new_items + updated_items

                    # Update the nested structure
                    existing_items[extract_nested_items] = merged_nested_items
                    self.set(cache_key, existing_items)
                else:
                    # Filter out changed items
                    unchanged_items = [
                        item for item in existing_nested_items
                        if not any(item.get(id_field) in changed_ids for id_field in id_fields if item.get(id_field))
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
                        new_processed_items, _ = processor.process_tracks(items_to_process)

                        if existing_processed_items:
                            # Filter out processed items corresponding to changed items
                            unchanged_processed_items = [
                                item for item in existing_processed_items
                                if not any(item.get(id_field) in changed_ids for id_field in id_fields if item.get(id_field))
                            ]

                            # Determine merge order based on prepend_new_items
                            if prepend_new_items:
                                merged_processed_items = new_processed_items + unchanged_processed_items
                            else:
                                merged_processed_items = unchanged_processed_items + new_processed_items

                            self.set(processed_cache_key, merged_processed_items)
                            self.logger.info(f"Updated processed cache with {len(merged_processed_items)} total items")
                        else:
                            self.logger.warning(
                                f"Existing processed items not found at {processed_cache_key}, skipping update"
                            )

                # Clear related caches if needed
                if clear_related_cache and (related_cache_prefix or related_cache_suffix):
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

                result.update({
                    "new_items": len(new_items),
                    "updated_items": len(updated_items),
                    "total_items": len(merged_nested_items if extract_nested_items else merged_items),
                    "success": True
                })
                return result
            else:
                self.logger.info(f"No changes detected for {cache_key} cache")
                result.update({"success": True, "total_items": len(existing_nested_items)})
                return result
        else:
            self.logger.info(f"No existing {cache_key} cache found, will create cache")
            # Just cache the recent data to make next access faster
            self.set(cache_key, recent_data)

            # Process and cache items if needed
            if process_items and processed_cache_key and recent_items and processor is not None:
                processed_items, _ = processor.process_tracks(recent_items)
                self.set(processed_cache_key, processed_items)
                self.logger.info(f"Created processed cache with {len(processed_items)} items")
                result.update({
                    "new_items": len(recent_items),
                    "total_items": len(recent_items),
                    "success": True
                })
            else:
                result.update({
                    "new_items": len(recent_items) if isinstance(recent_items, list) else 1,
                    "total_items": len(recent_items) if isinstance(recent_items, list) else 1,
                    "success": True
                })

            return result

    def _store_hash_mapping(self, hashed_key: str, original_path: str) -> None:
        """Store a mapping between a hashed key and its original path.

        Args:
            hashed_key: The hashed key created for a long path
            original_path: The original path that was hashed
        """
        try:
            # Store in memory for quick lookups
            with self.cache_lock:
                if not hasattr(self, 'hash_to_path'):
                    self.hash_to_path = {}
                self.hash_to_path[hashed_key] = original_path

            # Also try to store on disk for persistence across restarts
            hash_map_file = self.cache_dir / "hash_mappings.json"

            # Load existing mappings if file exists
            mappings = {}
            if hash_map_file.exists():
                try:
                    with open(hash_map_file, "r") as f:
                        mappings = json.load(f)
                except json.JSONDecodeError:
                    # If file is corrupted, start fresh
                    mappings = {}

            # Add the new mapping and save
            mappings[hashed_key] = original_path
            with open(hash_map_file, "w") as f:
                json.dump(mappings, f)

        except Exception as e:
            self.logger.warning(f"Failed to store hash mapping: {e}")

    def get_original_path(self, hashed_key: str) -> Optional[str]:
        """Retrieve the original path for a hashed key.

        Args:
            hashed_key: The hashed key to look up

        Returns:
            The original path if found, None otherwise
        """
        # First check in-memory cache
        with self.cache_lock:
            if hasattr(self, 'hash_to_path') and hashed_key in self.hash_to_path:
                return self.hash_to_path[hashed_key]

        # Then check on-disk storage
        try:
            hash_map_file = self.cache_dir / "hash_mappings.json"
            if hash_map_file.exists():
                with open(hash_map_file, "r") as f:
                    mappings = json.load(f)
                    if hashed_key in mappings:
                        # Also update in-memory cache for next time
                        with self.cache_lock:
                            if not hasattr(self, 'hash_to_path'):
                                self.hash_to_path = {}
                            self.hash_to_path[hashed_key] = mappings[hashed_key]
                        return mappings[hashed_key]
        except Exception as e:
            self.logger.warning(f"Failed to retrieve hash mapping: {e}")

        return None
