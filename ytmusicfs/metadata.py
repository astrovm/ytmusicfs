#!/usr/bin/env python3

import errno
import logging
import os
from typing import Any


class MetadataManager:
    """Resolve stable video IDs from filesystem paths and cached listings."""

    def __init__(
        self,
        cache: Any,
        logger: logging.Logger,
        thread_manager: Any,
        content_fetcher: Any = None,
    ) -> None:
        self.cache = cache
        self.logger = logger
        self.thread_manager = thread_manager
        self.content_fetcher = content_fetcher
        self.video_id_cache: dict[str, str] = {}
        self.video_id_cache_lock: Any = thread_manager.create_lock()
        self.logger.debug("Using ThreadManager for lock creation in MetadataManager")

    def set_content_fetcher(self, content_fetcher: Any) -> None:
        self.content_fetcher = content_fetcher

    def _remember_video_id(self, path: str, video_id: str) -> str:
        with self.video_id_cache_lock:
            self.video_id_cache[path] = video_id
        self.cache.set(f"video_id:{path}", video_id)
        return video_id

    def _video_id_from_tracks(
        self, path: str, filename: str, tracks: Any
    ) -> str | None:
        if not isinstance(tracks, list):
            return None
        for track in tracks:
            if not isinstance(track, dict) or track.get("filename") != filename:
                continue
            video_id = track.get("videoId")
            if isinstance(video_id, str) and video_id:
                return self._remember_video_id(path, video_id)
        return None

    def get_video_id(self, path: str) -> str:
        """Resolve a music path without making a network request."""
        entry_type = self.cache.get_entry_type(path)
        if entry_type != "file" and not path.endswith(".m4a"):
            self.logger.warning(f"Attempting to get video ID for non-file: {path}")
            raise OSError(errno.EINVAL, "Not a music file")
        if entry_type != "file":
            self.cache.mark_valid(path, is_directory=False)

        with self.video_id_cache_lock:
            if path in self.video_id_cache:
                return self.video_id_cache[path]

        cache_key = f"video_id:{path}"
        video_id = self.cache.get(cache_key)
        if isinstance(video_id, str) and video_id:
            with self.video_id_cache_lock:
                self.video_id_cache[path] = video_id
            return video_id

        dir_path = os.path.dirname(path)
        filename = os.path.basename(path)

        file_attrs = self.cache.get_file_attrs_from_parent_dir(path)
        if file_attrs and isinstance(file_attrs.get("videoId"), str):
            video_id = file_attrs["videoId"]
            return self._remember_video_id(path, video_id)

        if not self.content_fetcher:
            self.logger.error(
                f"No content fetcher available to lookup video ID for {path}"
            )
            raise OSError(
                errno.ENOENT, "Video ID not found, no content fetcher available"
            )

        playlist_entry = self.content_fetcher.get_playlist_entry_from_path(dir_path)
        if playlist_entry:
            cache_key = f"{dir_path}_processed"
            self.logger.debug(f"Using cache key {cache_key} for {path}")
            video_id = self._video_id_from_tracks(
                path, filename, self.cache.get(cache_key)
            )
            if video_id:
                return video_id

        self.logger.error(f"Could not find video ID for {filename} in {dir_path}")
        raise OSError(errno.ENOENT, "Video ID not found")

    def clear_cache(self) -> None:
        with self.video_id_cache_lock:
            self.video_id_cache.clear()
            self.logger.debug("Video ID cache cleared")
