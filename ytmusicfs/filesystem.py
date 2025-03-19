#!/usr/bin/env python3

from concurrent.futures import ThreadPoolExecutor
from fuse import FUSE, Operations, FuseOSError
from typing import Dict, Any, Optional, List, Union
from ytmusicfs.cache import CacheManager
from ytmusicfs.client import YouTubeMusicClient
from ytmusicfs.content_fetcher import ContentFetcher
from ytmusicfs.file_handler import FileHandler
from ytmusicfs.metadata import MetadataManager
from ytmusicfs.path_router import PathRouter
from ytmusicfs.processor import TrackProcessor
from ytmusicfs.oauth_adapter import YTMusicOAuthAdapter
import errno
import inspect
import logging
import os
import stat
import threading
import time
import traceback


class YouTubeMusicFS(Operations):
    """YouTube Music FUSE filesystem implementation."""

    def __init__(
        self,
        auth_file: str,
        client_id: str = None,
        client_secret: str = None,
        cache_dir: Optional[str] = None,
        browser: Optional[str] = None,
    ):
        """Initialize the FUSE filesystem with YouTube Music API.

        Args:
            auth_file: Path to authentication file (OAuth token)
            client_id: OAuth client ID (required for OAuth authentication)
            client_secret: OAuth client secret (required for OAuth authentication)
            cache_dir: Directory for persistent cache (optional)
            browser: Browser to use for cookies (e.g., 'chrome', 'firefox', 'brave')
        """
        # Get or create the logger
        self.logger = logging.getLogger("YTMusicFS")

        # Initialize the OAuth adapter first
        oauth_adapter = YTMusicOAuthAdapter(
            auth_file=auth_file,
            client_id=client_id,
            client_secret=client_secret,
            logger=self.logger,
            browser=browser,
        )

        # Initialize the client component with the OAuth adapter
        self.client = YouTubeMusicClient(
            oauth_adapter=oauth_adapter,
            logger=self.logger,
        )

        # Initialize the cache component
        self.cache = CacheManager(
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
            cache=self.cache, logger=self.logger, content_fetcher=self.fetcher
        )

        # Store parameters for future reference
        self.auth_file = auth_file
        self.client_id = client_id
        self.client_secret = client_secret
        self.request_cooldown = 1.0  # Default to 1 second cooldown

        # Debounce mechanism for repeated requests
        self.last_access_time = {}  # {operation_path: last_access_time}
        self.last_access_lock = (
            threading.RLock()
        )  # Lock for last_access_time operations
        self.last_access_results = {}  # {operation_path: cached_result}

        # Thread-related objects
        self.thread_pool = ThreadPoolExecutor(max_workers=8)
        self.logger.info("Thread pool initialized with 8 workers")

        # Store the browser parameter
        self.browser = browser

        # Initialize the file handler component
        self.file_handler = FileHandler(
            cache_dir=self.cache.cache_dir,
            cache=self.cache,
            logger=self.logger,
            update_file_size_callback=self._update_file_size,
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
        self.router.register(
            "/playlists", lambda: [".", ".."] + self.fetcher.readdir_playlists()
        )
        self.router.register(
            "/liked_songs", lambda: [".", ".."] + self.fetcher.readdir_liked_songs()
        )
        self.router.register(
            "/albums", lambda: [".", ".."] + self.fetcher.readdir_albums()
        )

        # Register dynamic handlers with wildcard capture
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

        # Flag to track initialization state
        self.initialized = False

        # Pre-warm the cache
        self._preload_cache()

        self.initialized = True
        self.logger.info("YTMusicFS initialized successfully")
        self.logger.debug(
            f"Using auth_file: {auth_file}, cache_dir: {self.cache.cache_dir}"
        )

    def _preload_cache(self) -> None:
        """Preload common paths and data into the cache."""
        self.logger.info("Preloading cache...")

        try:
            # Mark basic directories as valid
            self.cache.mark_valid("/", is_directory=True)
            self.cache.mark_valid("/playlists", is_directory=True)
            self.cache.mark_valid("/liked_songs", is_directory=True)
            self.cache.mark_valid("/albums", is_directory=True)

            # Fetch playlists in background thread
            def fetch_playlists():
                try:
                    self.router.route("/playlists")
                    self.logger.debug("Preloaded playlists")
                except Exception as e:
                    self.logger.error(f"Failed to preload playlists: {e}")

            self.thread_pool.submit(fetch_playlists)

        except Exception as e:
            self.logger.error(f"Error preloading cache: {e}")
            self.logger.error(traceback.format_exc())

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
        """Read directory contents.

        Args:
            path: Directory path
            fh: File handle (unused)

        Returns:
            List of directory entries
        """
        self.logger.debug(f"readdir: {path}")

        # For debugging, log the entry type from cache
        entry_type = self.cache.get_entry_type(path)
        self.logger.debug(f"Entry type for {path}: {entry_type}")

        # Check if this path is a file (not a directory)
        if entry_type == "file":
            self.logger.debug(f"Path is a file, not a directory: {path}")
            # Return empty directory for file paths
            return [".", ".."]

        # Check if this request is too soon after the last one
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

        # Ignore hidden paths
        if any(part.startswith(".") for part in path.split("/") if part):
            self.logger.debug(f"Ignoring hidden path: {path}")
            result = [".", ".."]
            with self.last_access_lock:
                self.last_access_results[operation_key] = result
            return result

        # Use the router to handle the path
        try:
            # Extra debugging for /playlists
            if path == "/playlists":
                self.logger.debug("Special handling for /playlists path")
                try:
                    # Check if playlists exist in the registry
                    if hasattr(self.fetcher, "PLAYLIST_REGISTRY"):
                        playlist_count = len(
                            [
                                p
                                for p in self.fetcher.PLAYLIST_REGISTRY
                                if p["type"] == "playlist"
                            ]
                        )
                        self.logger.debug(
                            f"Found {playlist_count} playlists in registry"
                        )

                    # Verify router registration
                    if (
                        not hasattr(self.router, "handlers")
                        or "/playlists" not in self.router.handlers
                    ):
                        self.logger.error("Router missing handler for /playlists")
                except Exception as debug_e:
                    self.logger.error(f"Debug inspection error: {debug_e}")

            # Simplified validation logic - use router and cache manager directly
            if not self.router.validate_path(path) and not self.cache.is_valid_path(
                path, "readdir"
            ):
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

                # Mark each file as valid
                for filename in filenames:
                    file_path = f"{path}/{filename}"
                    # We don't know if it's a directory yet, don't specify is_directory
                    self.cache.mark_valid(file_path)

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

    def getattr(
        self, path: str, fh: Optional[int] = None
    ) -> Dict[str, Union[int, float]]:
        """Get file attributes.

        Args:
            path: File path
            fh: File handle (unused)

        Returns:
            Dictionary with file attributes
        """
        self.logger.debug(f"getattr: {path}")

        # Check for root path
        if path == "/":
            return {
                "st_mode": stat.S_IFDIR | 0o755,
                "st_nlink": 2,
                "st_size": 0,
                "st_ctime": time.time(),
                "st_mtime": time.time(),
                "st_atime": time.time(),
                "st_uid": os.getuid(),
                "st_gid": os.getgid(),
            }

        # Check if path is valid according to cache manager
        if not self.cache.is_valid_path(path, "getattr"):
            self.logger.debug(f"Path not valid according to cache: {path}")
            raise FuseOSError(errno.ENOENT)

        # Check the entry type from cache
        entry_type = self.cache.get_entry_type(path)

        # Try to get file attributes from parent directory's cached listing
        parent_dir = os.path.dirname(path)
        filename = os.path.basename(path)
        attrs = self.cache.get_file_attrs_from_parent_dir(path)

        if attrs:
            self.logger.debug(f"Using attributes from parent dir cache for {path}")
            return attrs

        # If we have a known entry type, return appropriate attributes
        if entry_type == "directory":
            self.logger.debug(f"Known directory: {path}")
            return {
                "st_mode": stat.S_IFDIR | 0o755,
                "st_nlink": 2,
                "st_size": 0,
                "st_ctime": time.time(),
                "st_mtime": time.time(),
                "st_atime": time.time(),
                "st_uid": os.getuid(),
                "st_gid": os.getgid(),
            }
        elif entry_type == "file":
            self.logger.debug(f"Known file: {path}")
            return {
                "st_mode": stat.S_IFREG | 0o644,
                "st_nlink": 1,
                "st_size": 4096,  # Default size for most files
                "st_ctime": time.time(),
                "st_mtime": time.time(),
                "st_atime": time.time(),
                "st_uid": os.getuid(),
                "st_gid": os.getgid(),
            }

        # For unknown paths, use validation through router
        try:
            # Check if path is valid at the router level
            if self.router.validate_path(path):
                # Path is valid at router level, mark it in cache
                attr_data = self.router.get_attributes(path)
                if attr_data:
                    is_dir = attr_data.get("st_mode", 0) & stat.S_IFDIR == stat.S_IFDIR
                    # Update cache with this validated path
                    self.cache.mark_valid(path, is_directory=is_dir)
                    return attr_data
                else:
                    # Default attributes for valid directory paths
                    self.cache.mark_valid(path, is_directory=True)
                    return {
                        "st_mode": stat.S_IFDIR | 0o755,
                        "st_nlink": 2,
                        "st_size": 0,
                        "st_ctime": time.time(),
                        "st_mtime": time.time(),
                        "st_atime": time.time(),
                        "st_uid": os.getuid(),
                        "st_gid": os.getgid(),
                    }

            # Path appears to be a file by default if it has an extension
            is_file = "." in filename
            self.cache.mark_valid(path, is_directory=not is_file)

            if is_file:
                return {
                    "st_mode": stat.S_IFREG | 0o644,
                    "st_nlink": 1,
                    "st_size": 4096,
                    "st_ctime": time.time(),
                    "st_mtime": time.time(),
                    "st_atime": time.time(),
                    "st_uid": os.getuid(),
                    "st_gid": os.getgid(),
                }
            else:
                return {
                    "st_mode": stat.S_IFDIR | 0o755,
                    "st_nlink": 2,
                    "st_size": 0,
                    "st_ctime": time.time(),
                    "st_mtime": time.time(),
                    "st_atime": time.time(),
                    "st_uid": os.getuid(),
                    "st_gid": os.getgid(),
                }

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
            if entry_type is None and not self._is_valid_path(path):
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
            return self.file_handler.open(path, video_id, self.thread_pool)

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

    def refresh_cache(self) -> None:
        """Refresh all caches.

        This method updates all caches with any changes in the user's library.
        """
        self.logger.info("Refreshing all caches...")

        # Use the fetcher to refresh the caches
        self.fetcher.refresh_all_caches()

        self.logger.info("All caches refreshed successfully")

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

    def _is_valid_path(self, path: str) -> bool:
        """Check if a path is valid using the cache manager.

        Args:
            path: The path to validate

        Returns:
            Boolean indicating if the path is valid
        """
        # Get the context for better logging
        context = inspect.currentframe().f_back.f_code.co_name
        self.logger.debug(f"_is_valid_path called from {context} for {path}")

        # Use the cache manager's validation
        return self.cache.is_valid_path(path, context)

    def destroy(self, path: str) -> None:
        """Clean up when filesystem is unmounted.

        Args:
            path: Mount point path
        """
        self.logger.info("Destroying YTMusicFS instance")

        # Shutdown thread pool
        self.thread_pool.shutdown(wait=True)

        # Close the cache
        self.cache.close()

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
    client_id: str,
    client_secret: str,
    cache_dir: Optional[str] = None,
    foreground: bool = False,
    browser: Optional[str] = None,
) -> None:
    """Mount the YouTube Music filesystem.

    Args:
        mount_point: Directory where the filesystem will be mounted
        auth_file: Path to the OAuth token file
        client_id: OAuth client ID
        client_secret: OAuth client secret
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
            client_id=client_id,
            client_secret=client_secret,
            cache_dir=cache_dir,
            browser=browser,
        ),
        mount_point,
        **fuse_options,
    )
