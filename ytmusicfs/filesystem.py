#!/usr/bin/env python3

import errno
import json
import logging
import os
import stat
import time
import traceback
from contextlib import suppress
from typing import Any

from fuse import FUSE, FuseOSError, Operations

from ytmusicfs import __version__
from ytmusicfs.auth_adapter import YTMusicAuthAdapter
from ytmusicfs.cache import CacheManager
from ytmusicfs.client import YouTubeMusicClient
from ytmusicfs.content_fetcher import ContentFetcher
from ytmusicfs.file_handler import FileHandler
from ytmusicfs.metadata import MetadataManager
from ytmusicfs.path_router import PathRouter
from ytmusicfs.processor import TrackProcessor
from ytmusicfs.thread_manager import ThreadManager
from ytmusicfs.yt_dlp_utils import YTDLPUtils


class YouTubeMusicFS(Operations):
    """YouTube Music FUSE filesystem implementation."""

    METADATA_DIR = "/.ytmusicfs"
    STATUS_FILE = "/.ytmusicfs/status.json"
    MIN_AUDIO_SIZE = 1024 * 1024
    ESTIMATED_BYTES_PER_SECOND = 16 * 1024

    def __init__(
        self,
        cache_dir: str | None = None,
        browser: str = "",
    ):
        """Initialize the FUSE filesystem with YouTube Music API.

        Args:
            cache_dir: Directory for persistent cache (optional)
            browser: Browser to use for cookies (e.g., 'chrome', 'firefox', 'brave')
        """
        # Get or create the logger
        self.logger = logging.getLogger("YTMusicFS")

        # Initialize the ThreadManager first
        self.thread_manager = ThreadManager(logger=self.logger)
        self.logger.info("ThreadManager initialized")

        # Initialize YTDLPUtils with the ThreadManager
        self.yt_dlp_utils = YTDLPUtils(
            thread_manager=self.thread_manager, logger=self.logger
        )
        self.logger.debug("YTDLPUtils initialized with ThreadManager")

        # Initialize the authentication adapter first
        auth_adapter = YTMusicAuthAdapter(
            browser=browser,
            yt_dlp_utils=self.yt_dlp_utils,
            logger=self.logger,
        )

        # Initialize the client component with the authentication adapter
        self.client = YouTubeMusicClient(
            auth_adapter=auth_adapter,
            logger=self.logger,
        )

        # Store the adapter reference for downstream consumers/tests
        self.auth_adapter = auth_adapter
        # Backwards compatibility with older attribute name used in tests
        self.oauth_adapter = auth_adapter

        # Initialize the cache component
        self.cache = CacheManager(
            thread_manager=self.thread_manager,
            cache_dir=cache_dir,
            logger=self.logger,
        )

        # Initialize the track processor component with cache access
        self.processor = TrackProcessor(logger=self.logger, cache_manager=self.cache)

        # Initialize the content fetcher component
        self.fetcher = ContentFetcher(
            client=self.client,
            processor=self.processor,
            cache=self.cache,
            logger=self.logger,
            yt_dlp_utils=self.yt_dlp_utils,
            browser=browser,
        )

        # Set the callback for caching directory listings with attributes
        self.fetcher.cache_directory_callback = self._cache_directory_listing_with_attrs

        # Initialize the path router
        self.router = PathRouter()

        # Set the content fetcher in the router
        self.router.set_fetcher(self.fetcher)

        # Also explicitly set the cache manager
        self.router.set_cache(self.cache)

        # Initialize the metadata manager
        self.metadata_manager = MetadataManager(
            cache=self.cache,
            logger=self.logger,
            thread_manager=self.thread_manager,
            content_fetcher=self.fetcher,
        )

        # Store parameters for future reference
        self.request_cooldown = 1.0  # Default to 1 second cooldown

        # Debounce mechanism for repeated requests - use ThreadManager for locks
        self.last_access_time = {}  # {operation_path: last_access_time}
        self.last_access_lock = self.thread_manager.create_lock()
        self.last_access_results = {}  # {operation_path: cached_result}
        self.read_error_log_times = {}
        self.read_error_log_cooldown = 60.0
        self.stats_lock = self.thread_manager.create_lock()
        self.stats = {
            "open": 0,
            "read": 0,
            "getattr": 0,
            "readdir": 0,
            "stream_extractions": 0,
            "probe_eof_skips": 0,
            "range_416_eof": 0,
            "background_downloads": 0,
            "unavailable_cache_hits": 0,
        }

        # Store the browser parameter
        self.browser = browser

        # Initialize the file handler component
        self.file_handler = FileHandler(
            thread_manager=self.thread_manager,
            cache_dir=self.cache.cache_dir,
            cache=self.cache,
            logger=self.logger,
            update_file_size_callback=self._update_file_size,
            yt_dlp_utils=self.yt_dlp_utils,
            browser=self.browser,
            record_stat_callback=self._record_stat,
            get_file_size_callback=self._get_advertised_file_size,
        )

        # Register exact path handlers
        self.router.register(
            "/",
            lambda: [
                ".",
                "..",
                "playlists",
                "liked_songs",
                "albums",
            ],
        )

        # Use the unified playlist handling for all playlist types
        self.router.register(
            "/playlists",
            lambda: self.fetcher.readdir_playlist_by_type("playlist", "/playlists"),
        )
        self.router.register(
            "/liked_songs",
            lambda: self.fetcher.readdir_playlist_by_type(
                "liked_songs", "/liked_songs"
            ),
        )
        self.router.register(
            "/albums",
            lambda: self.fetcher.readdir_playlist_by_type("album", "/albums"),
        )

        # Register dynamic handlers with wildcard capture
        # Use a unified approach for content fetching since all are just different playlist types
        self.router.register_dynamic(
            "/playlists/*",
            lambda path, playlist_name: [
                ".",
                "..",
                *self.fetcher.fetch_playlist_content(
                    self._get_playlist_id_from_name(playlist_name, "playlist"),
                    path,
                ),
            ],
        )
        self.router.register_dynamic(
            "/albums/*",
            lambda path, album_name: [
                ".",
                "..",
                *self.fetcher.fetch_playlist_content(
                    self._get_playlist_id_from_name(album_name, "album"),
                    path,
                ),
            ],
        )

        # Initialize path validation with common static paths
        self.cache.mark_valid("/", is_directory=True)
        self.cache.mark_valid("/playlists", is_directory=True)
        self.cache.mark_valid("/liked_songs", is_directory=True)
        self.cache.mark_valid("/albums", is_directory=True)

        self.logger.info("YTMusicFS initialized successfully")
        self.logger.debug(
            f"Using browser: {browser}, cache_dir: {self.cache.cache_dir}"
        )

    def _cache_directory_listing_with_attrs(
        self, dir_path: str, processed_tracks: list[dict[str, Any]]
    ) -> None:
        """Cache directory listing with file attributes for efficient getattr lookups.

        Args:
            dir_path: Directory path
            processed_tracks: List of processed track data with filename and metadata
        """
        now = time.time()
        listing_with_attrs = {}
        valid_filenames = set()
        unavailable_ids = self.cache.get_unavailable_video_ids()

        for track in processed_tracks:
            video_id = track.get("videoId")
            if video_id and video_id in unavailable_ids:
                self.logger.debug("Skipping unavailable track in listing: %s", video_id)
                continue

            filename = track.get("filename")
            if not filename:
                self.logger.warning(
                    f"Track missing filename in processed_tracks: {track}"
                )
                continue

            valid_filenames.add(filename)

            # Check if this is explicitly a directory
            is_directory = track.get("is_directory", False)
            if is_directory:
                attrs = {
                    "st_mode": stat.S_IFDIR | 0o755,
                    "st_atime": now,
                    "st_ctime": now,
                    "st_mtime": now,
                    "st_nlink": 2,
                    "st_size": 0,
                }
                # Mark as directory in cache system
                self.cache.mark_valid(f"{dir_path}/{filename}", is_directory=True)
                self.logger.debug(f"Cached directory: {dir_path}/{filename}")
            else:
                attrs = {
                    "st_mode": stat.S_IFREG | 0o644,
                    "st_atime": now,
                    "st_ctime": now,
                    "st_mtime": now,
                    "st_nlink": 1,
                }
                attrs["st_size"] = self._audio_size_for_path(
                    f"{dir_path}/{filename}",
                    track.get("duration_seconds"),
                )

            # Preserve important metadata fields from the original track
            for key, value in track.items():
                if key in ["browseId", "channelId", "id", "playlistId", "videoId"]:
                    attrs[key] = value

            listing_with_attrs[filename] = attrs

            # Mark path as valid and store metadata with explicit is_directory flag
            file_path = f"{dir_path}/{filename}"
            self.cache.mark_valid(file_path, is_directory=is_directory)

        # Cache the directory listing with attributes
        self.cache.set_directory_listing_with_attrs(dir_path, listing_with_attrs)

        # Also cache the filenames separately for backward compatibility
        self.cache.set(f"valid_files:{dir_path}", list(valid_filenames))

        # Mark this directory as valid
        self.cache.mark_valid(dir_path, is_directory=True)

    def readdir(self, path: str, fh: int | None = None) -> list[str]:
        """Read directory contents with optimized caching.

        Args:
            path: Directory path
            fh: File handle (unused)

        Returns:
            List of directory entries
        """
        self._record_stat("readdir")
        self.logger.debug(f"readdir: {path}")

        # Priority 1: Check fixed paths directly
        if path == "/":
            return [".", "..", "playlists", "liked_songs", "albums", ".ytmusicfs"]

        if path == self.METADATA_DIR:
            return [".", "..", "status.json"]

        if path in ["/playlists", "/albums", "/liked_songs"]:
            # Make a direct call to the appropriate content function
            if path == "/playlists":
                return self.fetcher.readdir_playlist_by_type("playlist", "/playlists")
            if path == "/albums":
                return self.fetcher.readdir_playlist_by_type("album", "/albums")
            if path == "/liked_songs":
                return self.fetcher.readdir_playlist_by_type(
                    "liked_songs", "/liked_songs"
                )

        # Priority 2: Check if we have a cached directory listing
        cache_key = f"{path}_listing_with_attrs"
        directory_listing = self.cache.get(cache_key)
        if directory_listing:
            self.logger.debug(f"Using cached directory listing for readdir: {path}")
            return [".", "..", *self._filter_unavailable_listing(directory_listing)]

        # Priority 3: Check operation cooldown cache for very recent requests
        operation_key = f"readdir:{path}"
        current_time = time.time()

        with self.last_access_lock:
            last_time = self.last_access_time.get(operation_key, 0)
            if (
                current_time - last_time < self.request_cooldown
                and operation_key in self.last_access_results
            ):
                self.logger.debug(
                    f"Using cached result for {operation_key} (within cooldown: {current_time - last_time:.3f}s)"
                )
                return self.last_access_results[operation_key]

            # Update the last access time for this operation
            self.last_access_time[operation_key] = current_time

        # Priority 4: Check entry type and validity
        entry_type = self.cache.get_entry_type(path)
        self.logger.debug(f"Entry type for {path}: {entry_type}")

        # If it's a file, return empty dir
        if entry_type == "file":
            self.logger.debug(f"Path is a file, not a directory: {path}")
            result = [".", ".."]
            with self.last_access_lock:
                self.last_access_results[operation_key] = result
            return result

        # Ignore hidden paths
        if any(part.startswith(".") for part in path.split("/") if part):
            self.logger.debug(f"Ignoring hidden path: {path}")
            result = [".", ".."]
            with self.last_access_lock:
                self.last_access_results[operation_key] = result
            return result

        # Priority 5: Use the router
        try:
            # VALIDATION: Use router's validation logic to reject invalid paths
            if not self.router.validate_path(path):
                self.logger.debug(f"Rejecting invalid path in readdir: {path}")
                result = [".", ".."]
                with self.last_access_lock:
                    self.last_access_results[operation_key] = result
                return result

            # Delegate directory listing to the router
            self.logger.debug(f"Routing path: {path}")
            try:
                result = self.router.route(path)
                self.logger.debug(f"Router returned {len(result)} entries for {path}")
            except Exception as router_error:
                self.logger.error(f"Router error for {path}: {router_error}")
                self.logger.error(traceback.format_exc())
                result = [".", ".."]
                with self.last_access_lock:
                    self.last_access_results[operation_key] = result
                return result

            # Mark this as a valid directory and cache all files for future validation
            if path != "/" and len(result) > 2:  # More than just "." and ".."
                # Mark the directory as valid in the cache
                self.cache.mark_valid(path, is_directory=True)

                # Cache valid filenames for simpler lookups
                filenames = [entry for entry in result if entry not in [".", ".."]]
                self.cache.set(f"valid_files:{path}", filenames)

                # Use batch caching for better performance
                batch_entries = {}
                for filename in filenames:
                    file_path = f"{path}/{filename}"
                    batch_entries[f"path_valid:{file_path}"] = True

                # Submit batch update in one operation if there are entries
                if batch_entries:
                    # Store batch of path validations at once
                    self.thread_manager.submit_task(
                        "io", self.cache.set_batch, batch_entries
                    )

            # Cache this result for the cooldown period
            with self.last_access_lock:
                self.last_access_results[operation_key] = result

            return result
        except Exception as e:
            self.logger.error(f"Error in readdir for {path}: {e}")
            self.logger.error(traceback.format_exc())
            result = [".", ".."]
            with self.last_access_lock:
                self.last_access_results[operation_key] = result
            return result

    def _get_video_id(self, path: str) -> str:
        """Get the video ID for a file using MetadataManager.

        Args:
            path: The path to get the video ID for

        Returns:
            Video ID for the file

        Raises:
            OSError: If the video ID could not be found
        """
        return self.metadata_manager.get_video_id(path)

    def getattr(self, path: str, fh: int | None = None) -> dict[str, Any]:
        """Get file attributes with optimized caching.

        Args:
            path: File path
            fh: File handle (unused)

        Returns:
            File attributes
        """
        if path != self.STATUS_FILE:
            self._record_stat("getattr")
        # Start timing for performance analysis
        start_time = time.time()
        operation_key = f"getattr:{path}"

        # Add default values
        attrs = {
            "st_uid": os.getuid(),
            "st_gid": os.getgid(),
            "st_atime": time.time(),
            "st_mtime": time.time(),
            "st_ctime": time.time(),
        }

        audio_video_id = None
        if path.endswith(".m4a"):
            with suppress(OSError):
                audio_video_id = self._get_video_id(path)
            if audio_video_id and self.cache.is_track_unavailable(audio_video_id):
                raise FuseOSError(errno.ENOENT)

        # CHECK COOLDOWN CACHE FIRST for frequent requests
        current_time = time.time()
        with self.last_access_lock:
            last_time = self.last_access_time.get(operation_key, 0)
            if (
                current_time - last_time < self.request_cooldown
                and operation_key in self.last_access_results
            ):
                self.logger.debug(
                    f"Using cached result for {operation_key} (within cooldown: {current_time - last_time:.3f}s)"
                )
                return self.last_access_results[operation_key]

            # Update the last access time for this operation
            self.last_access_time[operation_key] = current_time

        # CASE 1: Root directory
        if path == "/":
            attrs.update(
                {
                    "st_mode": stat.S_IFDIR | 0o555,
                    "st_nlink": 2,
                    "st_size": 4096,
                }
            )
            self.cache.mark_valid(path, is_directory=True)
            with self.last_access_lock:
                self.last_access_results[operation_key] = attrs
            return attrs

        if path == self.METADATA_DIR:
            attrs.update(
                {
                    "st_mode": stat.S_IFDIR | 0o555,
                    "st_nlink": 2,
                    "st_size": 4096,
                }
            )
            with self.last_access_lock:
                self.last_access_results[operation_key] = attrs
            return attrs

        if path == self.STATUS_FILE:
            status = self._get_status_json()
            attrs.update(
                {
                    "st_mode": stat.S_IFREG | 0o444,
                    "st_nlink": 1,
                    "st_size": len(status),
                }
            )
            with self.last_access_lock:
                self.last_access_results[operation_key] = attrs
            return attrs

        # CASE 2: Top-level directories
        if path in ["/playlists", "/albums", "/liked_songs"]:
            attrs.update(
                {
                    "st_mode": stat.S_IFDIR | 0o555,
                    "st_nlink": 2,
                    "st_size": 4096,
                }
            )
            self.cache.mark_valid(path, is_directory=True)
            with self.last_access_lock:
                self.last_access_results[operation_key] = attrs
            return attrs

        # CASE 3: Check cache directly - fastest path
        cached_attrs = self.cache.get_file_attrs_from_parent_dir(path)
        if cached_attrs:
            # We've found cached attributes for this path
            self.logger.debug(f"Using cached attributes for {path}")
            with self.last_access_lock:
                self.last_access_results[operation_key] = cached_attrs
            processing_time = time.time() - start_time
            if processing_time > 0.1:
                self.logger.info(
                    f"getattr for {path} took {processing_time:.3f}s (from cache)"
                )
            return cached_attrs

        # CASE 4: File in a playlist - try to get from playlist registry
        if path.count("/") == 2:  # /category/item format
            parts = path.split("/")
            category = parts[1]

            # VALIDATION: First check if this is a valid path
            if not self.router.validate_path(path):
                self.logger.debug(f"Rejecting invalid level 2 path in getattr: {path}")
                raise FuseOSError(errno.ENOENT)

            if category in ["playlists", "albums"]:
                # This is likely a directory
                attrs.update(
                    {
                        "st_mode": stat.S_IFDIR | 0o555,
                        "st_nlink": 2,
                        "st_size": 4096,
                    }
                )
                self.cache.mark_valid(path, is_directory=True)

                # Save these attrs to both caches
                self.cache.update_file_attrs_in_parent_dir(path, attrs)

                with self.last_access_lock:
                    self.last_access_results[operation_key] = attrs
                processing_time = time.time() - start_time
                if processing_time > 0.1:
                    self.logger.info(
                        f"getattr for {path} took {processing_time:.3f}s (playlist/album dir)"
                    )
                return attrs

        # CASE 5: Use the router to validate - fallback approach
        try:
            if not self.router.validate_path(path):
                self.logger.debug(f"Path not valid: {path}")
                raise FuseOSError(errno.ENOENT)

            # For normal files, we need to set appropriate metadata
            if path.endswith(".m4a"):
                # Audio files
                video_id = audio_video_id or self._get_video_id(path)
                duration = self.cache.get_duration(video_id) if video_id else None
                size = self._audio_size_for_path(path, duration)

                # Set file mode and type
                mode = stat.S_IFREG | 0o444  # Regular file, read-only

                attrs.update(
                    {
                        "st_mode": mode,
                        "st_nlink": 1,
                        "st_size": size,
                    }
                )

                # Mark this as a valid file path
                self.cache.mark_valid(path, is_directory=False)
            else:
                # Assume directory as default for non-media files
                attrs.update(
                    {
                        "st_mode": stat.S_IFDIR | 0o555,
                        "st_nlink": 2,
                        "st_size": 4096,
                    }
                )
                self.cache.mark_valid(path, is_directory=True)

            # Cache the result for future access
            self.cache.update_file_attrs_in_parent_dir(path, attrs)

            with self.last_access_lock:
                self.last_access_results[operation_key] = attrs

            # Log slow getattr operations
            processing_time = time.time() - start_time
            if processing_time > 0.1:
                self.logger.info(
                    f"getattr for {path} took {processing_time:.3f}s (from router)"
                )

            return attrs
        except Exception as e:
            if isinstance(e, FuseOSError) and getattr(e, "errno", None) == errno.ENOENT:
                self.logger.debug(f"getattr miss for {path}: {e}")
                raise
            self.logger.error(f"Error in getattr for {path}: {e}")
            self.logger.error(traceback.format_exc())
            raise FuseOSError(errno.ENOENT) from e

    def open(self, path: str, flags: int) -> int:
        """Open file and return file handle.

        Args:
            path: File path
            flags: Open flags

        Returns:
            File handle
        """
        try:
            self.logger.debug(f"open: {path} (flags={flags})")

            if path == self.STATUS_FILE:
                return 0
            self._record_stat("open")

            # Check entry type from cache
            entry_type = self.cache.get_entry_type(path)

            # If it's a directory, raise error
            if entry_type == "directory":
                self.logger.debug(f"Cannot open directory as file: {path}")
                raise FuseOSError(errno.EISDIR)

            # If the path is not valid, raise error
            if entry_type is None and not self.router.validate_path(path):
                self.logger.debug(f"Invalid path in open: {path}")
                raise FuseOSError(errno.ENOENT)

            # Mark this path as valid in the cache
            self.cache.mark_valid(path, is_directory=False)

            # Extract the video ID from the path
            try:
                video_id = self._get_video_id(path)
                if not video_id:
                    self.logger.error(f"Could not extract video ID from path: {path}")
                    raise FuseOSError(errno.ENOENT)
            except Exception as e:
                self.logger.error(f"Error extracting video ID for {path}: {e}")
                raise FuseOSError(errno.ENOENT) from e

            # Delegate to file handler
            return self.file_handler.open(path, video_id)

        except Exception as e:
            if isinstance(e, FuseOSError):
                raise
            self.logger.error(f"Error in open for {path}: {e}")
            self.logger.error(traceback.format_exc())
            raise FuseOSError(errno.ENOENT) from e

    def read(self, path: str, size: int, offset: int, fh: int) -> bytes:
        """Read data from file.

        Args:
            path: File path
            size: Number of bytes to read
            offset: Offset to start reading from
            fh: File handle

        Returns:
            Bytes read from file
        """
        try:
            self.logger.debug(f"read: {path} (size={size}, offset={offset}, fh={fh})")

            if path == self.STATUS_FILE:
                data = self._get_status_json()
                return data[offset : offset + size]
            self._record_stat("read")

            # Delegate to file handler
            return self.file_handler.read(path, size, offset, fh)

        except Exception as e:
            if isinstance(e, FuseOSError):
                raise
            if isinstance(e, OSError):
                error_code = e.errno or errno.EIO
                self._log_read_failure(path, e)
                raise FuseOSError(error_code) from e
            self.logger.error(f"Error reading {path}: {e}")
            self.logger.error(traceback.format_exc())
            raise FuseOSError(errno.EIO) from e

    def _filter_unavailable_listing(
        self, listing: dict[str, dict[str, Any]]
    ) -> dict[str, dict[str, Any]]:
        unavailable_ids = self.cache.get_unavailable_video_ids()
        return {
            filename: attrs
            for filename, attrs in listing.items()
            if not attrs.get("videoId") or attrs["videoId"] not in unavailable_ids
        }

    def _log_read_failure(self, path: str, error: OSError) -> None:
        now = time.time()
        last_logged = self.read_error_log_times.get(path, 0)
        if now - last_logged < self.read_error_log_cooldown:
            return
        self.read_error_log_times[path] = now
        self.logger.warning(f"Read failed for {path}: {error}")

    def _get_status_json(self) -> bytes:
        """Return lightweight filesystem status for mounted debug reads."""
        with self.stats_lock:
            stats = dict(self.stats)
        payload = {
            "version": __version__,
            "browser": self.browser,
            "cache_dir": str(self.cache.cache_dir),
            "generated_at": int(time.time()),
            "stats": stats,
        }
        return (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8")

    def _record_stat(self, name: str) -> None:
        with self.stats_lock:
            self.stats[name] = self.stats.get(name, 0) + 1

    def _get_real_file_size(self, path: str) -> int | None:
        cached_size = self.cache.get(f"filesize:{path}")
        if isinstance(cached_size, int):
            return cached_size
        return None

    def _get_advertised_file_size(self, path: str) -> int | None:
        real_size = self._get_real_file_size(path)
        if real_size is not None:
            return real_size
        cached_attrs = self.cache.get_file_attrs_from_parent_dir(path)
        if cached_attrs:
            size = cached_attrs.get("st_size")
            if isinstance(size, int):
                return size
        return None

    def _audio_size_for_path(
        self, path: str, duration_seconds: float | None = None
    ) -> int:
        real_size = self._get_real_file_size(path)
        if real_size is not None:
            return real_size
        if duration_seconds:
            return max(
                int(duration_seconds * self.ESTIMATED_BYTES_PER_SECOND),
                self.MIN_AUDIO_SIZE,
            )
        return self.MIN_AUDIO_SIZE

    def release(self, path: str, fh: int) -> int:
        """Close the file.

        Args:
            path: File path
            fh: File handle

        Returns:
            0 on success
        """
        try:
            self.logger.debug(f"release: {path} (fh={fh})")

            # Delegate to file handler
            return self.file_handler.release(path, fh)

        except Exception as e:
            self.logger.error(f"Error releasing {path}: {e}")
            self.logger.error(traceback.format_exc())
            return 0  # Always return success for release

    def _update_file_size(self, path: str, size: int) -> None:
        """Update the cached file size for a path.

        Args:
            path: The file path
            size: The new file size
        """
        file_size_cache_key = f"filesize:{path}"
        self.cache.set(file_size_cache_key, size)

        # Also update the file attributes in the parent directory's cached listing
        attr = {
            "st_mode": stat.S_IFREG | 0o644,
            "st_atime": time.time(),
            "st_ctime": time.time(),
            "st_mtime": time.time(),
            "st_nlink": 1,
            "st_size": size,
        }
        self.cache.update_file_attrs_in_parent_dir(path, attr)

    def mkdir(self, path, mode):
        """Create a directory.

        Args:
            path: The directory path
            mode: Directory permissions (unused)

        Returns:
            0 on success
        """
        self.logger.debug(f"mkdir: {path}")
        # For now, disallow creating directories anywhere
        self.logger.warning(f"mkdir not supported: {path}")
        raise OSError(errno.EPERM, "Directory creation not supported")

    def rmdir(self, path):
        """Remove a directory.

        Args:
            path: The directory path

        Returns:
            0 on success
        """
        self.logger.debug(f"rmdir: {path}")
        # For now, disallow removing directories anywhere
        self.logger.warning(f"rmdir not supported: {path}")
        raise OSError(errno.EPERM, "Directory removal not supported")

    def __call__(self, op, *args):
        """Override the __call__ method to ensure correct operation handling.

        This method is called by FUSE for each filesystem operation.
        """
        try:
            # Call the parent class implementation
            return super().__call__(op, *args)
        finally:
            # Nothing to clean up as thread-local cache is no longer used
            pass

    def destroy(self, path: str) -> None:
        """Clean up when filesystem is unmounted.

        Args:
            path: Mount point path
        """
        self.logger.info("Destroying YTMusicFS instance")

        if hasattr(self, "yt_dlp_utils") and self.yt_dlp_utils is not None:
            try:
                self.yt_dlp_utils.cleanup()
            except Exception as e:
                self.logger.error(f"Error cleaning up yt-dlp resources: {e}")

        # Shutdown thread pool via ThreadManager
        if hasattr(self, "thread_manager") and self.thread_manager is not None:
            try:
                self.logger.info("Shutting down ThreadManager")
                self.thread_manager.shutdown(wait=True, timeout=10.0)
            except Exception as e:
                self.logger.error(f"Error shutting down ThreadManager: {e}")

        # Close the cache if it exists
        if hasattr(self, "cache") and self.cache is not None:
            try:
                self.cache.close()
            except Exception as e:
                self.logger.error(f"Error closing cache: {e}")

        self.logger.info("YTMusicFS destroyed successfully")

    def _get_playlist_id_from_name(
        self, name: str, type_filter: str | None = None
    ) -> str | None:
        """Get playlist ID from its name using the ContentFetcher.

        Args:
            name: The sanitized name of the playlist/album
            type_filter: Optional type to filter by ('playlist', 'album', 'liked_songs')

        Returns:
            The playlist ID if found, None otherwise
        """
        return self.fetcher.get_playlist_id_from_name(name, type_filter)


def mount_ytmusicfs(
    mount_point: str,
    cache_dir: str | None = None,
    foreground: bool = False,
    browser: str = "",
) -> None:
    """Mount the YouTube Music filesystem.

    Args:
        mount_point: Directory where the filesystem will be mounted
        cache_dir: Directory to store cache files (default: None)
        foreground: Run in the foreground (for debugging)
        browser: Browser to use for cookies (e.g., 'chrome', 'firefox', 'brave')
    """
    # Set fuse logger to WARNING level to suppress debug messages about unsupported operations
    logging.getLogger("fuse").setLevel(logging.WARNING)

    # Define FUSE options
    fuse_options = {
        "foreground": foreground,
        "nothreads": False,
        "fsname": "ytmusicfs",
        "uid": os.getuid(),  # Set mount UID to current user
        "gid": os.getgid(),  # Set mount GID to current group
    }

    FUSE(
        YouTubeMusicFS(
            cache_dir=cache_dir,
            browser=browser,
        ),
        mount_point,
        **fuse_options,
    )
