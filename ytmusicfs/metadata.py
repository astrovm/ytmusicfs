#!/usr/bin/env python3

import errno
import os
from typing import Any, Dict, Optional


class MetadataManager:
    """
    Manages metadata operations, such as extracting video IDs from paths.

    Attributes:
        cache: CacheManager instance for accessing cached data.
        logger: Logger instance for logging.
        content_fetcher: ContentFetcher instance for accessing playlist data.
        thread_manager: ThreadManager instance for thread synchronization.
    """

    def __init__(self, cache, logger, thread_manager, content_fetcher=None):
        """
        Initialize the MetadataManager.

        Args:
            cache: CacheManager instance.
            logger: Logger instance.
            thread_manager: ThreadManager instance for thread synchronization.
            content_fetcher: Optional ContentFetcher instance for accessing playlists.
        """
        self.cache = cache
        self.logger = logger
        self.thread_manager = thread_manager
        self.content_fetcher = content_fetcher

        # Video ID cache with lock protection
        self.video_id_cache = {}
        self.video_id_cache_lock = thread_manager.create_lock()
        self.logger.debug("Using ThreadManager for lock creation in MetadataManager")

    def set_content_fetcher(self, content_fetcher):
        """
        Set the content fetcher instance.

        Args:
            content_fetcher: ContentFetcher instance.
        """
        self.content_fetcher = content_fetcher

    def _cache_video_id(self, path: str, video_id: str) -> None:
        """Store a discovered video ID in memory and persistent caches."""

        with self.video_id_cache_lock:
            self.video_id_cache[path] = video_id
        self.cache.set(f"video_id:{path}", video_id)

    def _find_video_id_in_cache(
        self, cache_key: str, filename: str, path: str
    ) -> Optional[str]:
        """Search cached track listings for a matching filename."""

        tracks = self.cache.get(cache_key)
        if not tracks:
            return None

        for track in tracks:
            if isinstance(track, dict) and track.get("filename") == filename:
                video_id = track.get("videoId")
                if video_id:
                    self.logger.debug(
                        f"Found video ID {video_id} for {filename} in {cache_key}"
                    )
                    self._cache_video_id(path, video_id)
                    return video_id

        return None

    def _refresh_playlist_entry(self, playlist_entry: Dict[str, Any], dir_path: str) -> bool:
        """Refresh playlist cache for the provided entry if possible."""

        playlist_id = playlist_entry.get("id")
        if not playlist_id:
            self.logger.warning(
                f"Cannot refresh playlist cache for {dir_path} without an ID"
            )
            return False

        try:
            self.content_fetcher.fetch_playlist_content(
                playlist_id, dir_path, force_refresh=True
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            self.logger.error(
                f"Failed to refresh playlist cache for {dir_path}: {exc}"
            )
            return False

        return True

    def get_video_id(self, path):
        """
        Get the video ID for a file path using cached data.

        Args:
            path (str): Filesystem path to the file.

        Returns:
            str: Video ID for the file.

        Raises:
            OSError: If the path is not a file or video ID cannot be found.
        """
        # Check if path is a file based on entry_type
        entry_type = self.cache.get_entry_type(path)
        if entry_type != "file":
            self.logger.warning(f"Attempting to get video ID for non-file: {path}")
            raise OSError(errno.EINVAL, "Not a music file")

        # Check if we already have the video ID for this path in memory cache
        with self.video_id_cache_lock:
            if path in self.video_id_cache:
                video_id = self.video_id_cache[path]
                self.logger.debug(f"Using cached video ID {video_id} for {path}")
                return video_id

        # Check for video ID in persistent cache
        cache_key = f"video_id:{path}"
        video_id = self.cache.get(cache_key)
        if video_id:
            self.logger.debug(
                f"Found video ID {video_id} in persistent cache for {path}"
            )
            self._cache_video_id(path, video_id)
            return video_id

        dir_path = os.path.dirname(path)
        filename = os.path.basename(path)
        self.logger.debug(f"Looking up video ID for {filename} in {dir_path}")

        # First try to get attributes from the parent directory's cached listing
        file_attrs = self.cache.get_file_attrs_from_parent_dir(path)
        if file_attrs and "videoId" in file_attrs:
            video_id = file_attrs["videoId"]
            self.logger.debug(f"Found video ID {video_id} in parent directory cache")
            self._cache_video_id(path, video_id)
            return video_id

        # If we don't have a content fetcher, we can't go further
        if not self.content_fetcher:
            self.logger.error(
                f"No content fetcher available to lookup video ID for {path}"
            )
            raise OSError(
                errno.ENOENT, "Video ID not found, no content fetcher available"
            )

        # Find the corresponding playlist using the new method
        playlist_entry = self.content_fetcher.get_playlist_entry_from_path(dir_path)

        if playlist_entry:
            playlist_cache_key = f"{dir_path}_processed"
            self.logger.debug(f"Using cache key {playlist_cache_key} for {path}")

            video_id = self._find_video_id_in_cache(
                playlist_cache_key, filename, path
            )
            if video_id:
                return video_id

        # If we didn't find the video ID, try to fetch the directory contents
        # This would usually be done by the filesystem readdir method
        self.logger.debug(f"Track not in cache for {dir_path}, attempting to fetch")
        # We can't directly call readdir here, but we can check if the playlist was refreshed

        # Try again with the unified approach after potential refresh
        if playlist_entry:
            refreshed = self._refresh_playlist_entry(playlist_entry, dir_path)
            if refreshed:
                video_id = self._find_video_id_in_cache(
                    playlist_cache_key, filename, path
                )
                if video_id:
                    return video_id

        self.logger.error(f"Could not find video ID for {filename} in {dir_path}")
        raise OSError(errno.ENOENT, "Video ID not found")

    def clear_cache(self):
        """
        Clear the video ID cache.
        """
        with self.video_id_cache_lock:
            self.video_id_cache.clear()
            self.logger.debug("Video ID cache cleared")
