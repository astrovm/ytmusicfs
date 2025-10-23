#!/usr/bin/env python3

from fuse import FUSE, Operations, FuseOSError
from typing import Dict, Any, Optional, List
from ytmusicfs.cache import CacheManager
from ytmusicfs.client import YouTubeMusicClient
from ytmusicfs.content_fetcher import ContentFetcher
from ytmusicfs.file_handler import FileHandler
from ytmusicfs.metadata import MetadataManager
from ytmusicfs.auth_adapter import YTMusicAuthAdapter
from ytmusicfs.path_router import PathRouter
from ytmusicfs.processor import TrackProcessor
from ytmusicfs.thread_manager import ThreadManager
from ytmusicfs.yt_dlp_utils import YTDLPUtils
import errno
import logging
import os
import stat
import time
import traceback


class YouTubeMusicFS(Operations):
    """YouTube Music FUSE filesystem implementation."""

    def __init__(
        self,
        auth_file: str,
        cache_dir: Optional[str] = None,
        browser: Optional[str] = None,
    ):
        """Initialize the FUSE filesystem with YouTube Music API.

        Args:
            auth_file: Path to authentication file containing browser headers
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
            auth_file=auth_file,
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
        self.auth_file = auth_file
        self.request_cooldown = 1.0  # Default to 1 second cooldown

        # Debounce mechanism for repeated requests - use ThreadManager for locks
        self.last_access_time = {}  # {operation_path: last_access_time}
        self.last_access_lock = self.thread_manager.create_lock()
        self.last_access_results = {}  # {operation_path: cached_result}

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
            lambda path, playlist_name: [".", ".."]
            + self.fetcher.fetch_playlist_content(
                self._get_playlist_id_from_name(playlist_name, "playlist"),
                path,
            ),
        )
        self.router.register_dynamic(
            "/albums/*",
            lambda path, album_name: [".", ".."]
            + self.fetcher.fetch_playlist_content(
                self._get_playlist_id_from_name(album_name, "album"),
                path,
            ),
        )

        # Initialize path validation with common static paths
        self.cache.mark_valid("/", is_directory=True)
        self.cache.mark_valid("/playlists", is_directory=True)
        self.cache.mark_valid("/liked_songs", is_directory=True)
        self.cache.mark_valid("/albums", is_directory=True)

        self.logger.info("YTMusicFS initialized successfully")
        self.logger.debug(
            f"Using auth_file: {auth_file}, cache_dir: {self.cache.cache_dir}"
        )

    def _cache_directory_listing_with_attrs(
        self, dir_path: str, processed_tracks: List[Dict[str, Any]]
    ) -> None:
        """Cache directory listing with file attributes for efficient getattr lookups.

        Args:
            dir_path: Directory path
            processed_tracks: List of processed track data with filename and metadata
        """
        now = time.time()
        listing_with_attrs = {}
        valid_filenames = set()

        # Collect durations for batch processing
        durations_batch = {}

        for track in processed_tracks:
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
                duration_seconds = track.get("duration_seconds")
                if duration_seconds:
                    # Estimate file size based on duration (128kbps = 16KB/sec)
                    attrs["st_size"] = duration_seconds * 16 * 1024
                else:
                    # Default placeholder size
                    attrs["st_size"] = 4096

                file_size_cache_key = f"filesize:{dir_path}/{filename}"
                cached_size = self.cache.get(file_size_cache_key)
                if cached_size is not None:
                    attrs["st_size"] = cached_size

            # Preserve important metadata fields from the original track
            for key, value in track.items():
                if key in ["browseId", "channelId", "id", "playlistId", "videoId"]:
                    attrs[key] = value

            listing_with_attrs[filename] = attrs

            # Mark path as valid and store metadata with explicit is_directory flag
            file_path = f"{dir_path}/{filename}"
            self.cache.mark_valid(file_path, is_directory=is_directory)

            # If it's a file, collect video ID to duration mapping if available
            if not is_directory:
                video_id = track.get("videoId")
                if video_id and track.get("duration_seconds"):
                    durations_batch[video_id] = track.get("duration_seconds")

        # Batch update all durations at once
        if durations_batch:
            self.logger.debug(
                f"Batch updating {len(durations_batch)} track durations from directory listing"
            )
            self.cache.set_durations_batch(durations_batch)

        # Cache the directory listing with attributes
        self.cache.set_directory_listing_with_attrs(dir_path, listing_with_attrs)

        # Also cache the filenames separately for backward compatibility
        self.cache.set(f"valid_files:{dir_path}", list(valid_filenames))

        # Mark this directory as valid
        self.cache.mark_valid(dir_path, is_directory=True)

    def readdir(self, path: str, fh: Optional[int] = None) -> List[str]:
        """Read directory contents with optimized caching.

        Args:
            path: Directory path
            fh: File handle (unused)

        Returns:
            List of directory entries
        """
        self.logger.debug(f"readdir: {path}")

        # Priority 1: Check fixed paths directly
        if path == "/":
            return [".", "..", "playlists", "liked_songs", "albums"]

        if path in ["/playlists", "/albums", "/liked_songs"]:
            # Make a direct call to the appropriate content function
            if path == "/playlists":
                return self.fetcher.readdir_playlist_by_type("playlist", "/playlists")
            elif path == "/albums":
                return self.fetcher.readdir_playlist_by_type("album", "/albums")
            elif path == "/liked_songs":
                return self.fetcher.readdir_playlist_by_type(
                    "liked_songs", "/liked_songs"
                )

        # Priority 2: Check if we have a cached directory listing
        cache_key = f"{path}_listing_with_attrs"
        directory_listing = self.cache.get(cache_key)
        if directory_listing:
            self.logger.debug(f"Using cached directory listing for readdir: {path}")
            return [".", ".."] + list(directory_listing.keys())

        # Priority 3: Check operation cooldown cache for very recent requests
        operation_key = f"readdir:{path}"
        current_time = time.time()

        with self.last_access_lock:
            last_time = self.last_access_time.get(operation_key, 0)
            if current_time - last_time < self.request_cooldown:
                # Request is within cooldown period, return cached result if available
                if operation_key in self.last_access_results:
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

    def getattr(self, path: str, fh: Optional[int] = None) -> Dict[str, Any]:
        """Get file attributes with optimized caching.

        Args:
            path: File path
            fh: File handle (unused)

        Returns:
            File attributes
        """
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

        # CHECK COOLDOWN CACHE FIRST for frequent requests
        current_time = time.time()
        with self.last_access_lock:
            last_time = self.last_access_time.get(operation_key, 0)
            if current_time - last_time < self.request_cooldown:
                # Request is within cooldown period, return cached result if available
                if operation_key in self.last_access_results:
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
            item_name = parts[2]

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
                is_audio = True
                size = 1024 * 1024  # Default size for audio files

                # Try to get a more accurate size for audio files
                video_id = self.fetcher.processor.extract_video_id_from_path(path)
                if video_id:
                    # Get duration from cache
                    duration = self.cache.get_duration(video_id)
                    if duration:
                        # Rough estimate: ~1MB per minute of audio
                        size = max(int(duration * 10 * 1024), size)

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
            self.logger.error(f"Error in getattr for {path}: {e}")
            self.logger.error(traceback.format_exc())
            raise FuseOSError(errno.ENOENT)

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
                raise FuseOSError(errno.ENOENT)

            # Delegate to file handler
            return self.file_handler.open(path, video_id)

        except Exception as e:
            if isinstance(e, FuseOSError):
                raise
            self.logger.error(f"Error in open for {path}: {e}")
            self.logger.error(traceback.format_exc())
            raise FuseOSError(errno.ENOENT)

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

            # Delegate to file handler
            return self.file_handler.read(path, size, offset, fh)

        except Exception as e:
            if isinstance(e, FuseOSError):
                raise
            self.logger.error(f"Error reading {path}: {e}")
            self.logger.error(traceback.format_exc())
            raise FuseOSError(errno.EIO)

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
        self, name: str, type_filter: str = None
    ) -> Optional[str]:
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
    auth_file: str,
    cache_dir: Optional[str] = None,
    foreground: bool = False,
    browser: Optional[str] = None,
) -> None:
    """Mount the YouTube Music filesystem.

    Args:
        mount_point: Directory where the filesystem will be mounted
        auth_file: Path to the browser headers authentication file
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
        "uid": os.getuid(),  # Set mount UID to current user
        "gid": os.getgid(),  # Set mount GID to current group
    }

    FUSE(
        YouTubeMusicFS(
            auth_file=auth_file,
            cache_dir=cache_dir,
            browser=browser,
        ),
        mount_point,
        **fuse_options,
    )
