#!/usr/bin/env python3

from concurrent.futures import ThreadPoolExecutor
from fuse import FUSE, Operations
from pathlib import Path
from typing import Dict, Any, Optional, List, Callable
from yt_dlp import YoutubeDL
from ytmusicfs.cache import CacheManager
from ytmusicfs.client import YouTubeMusicClient
from ytmusicfs.processor import TrackProcessor
from ytmusicfs.path_router import PathRouter
from ytmusicfs.content_fetcher import ContentFetcher
import errno
import inspect
import logging
import multiprocessing
import os
import re
import requests
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
        cache_timeout: int = 2592000,
        max_workers: int = 8,
        browser: Optional[str] = None,
        cache_maxsize: int = 10000,
        preload_cache: bool = True,
        request_cooldown: int = 1000,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize the FUSE filesystem with YouTube Music API.

        Args:
            auth_file: Path to authentication file (OAuth token)
            client_id: OAuth client ID (required for OAuth authentication)
            client_secret: OAuth client secret (required for OAuth authentication)
            cache_dir: Directory for persistent cache (optional)
            cache_timeout: Time in seconds before cached data expires (default: 30 days)
            max_workers: Maximum number of worker threads (default: 8)
            browser: Browser to use for cookies (e.g., 'chrome', 'firefox', 'brave')
            cache_maxsize: Maximum number of items to keep in memory cache (default: 10000)
            preload_cache: Whether to preload cache data at startup (default: True)
            request_cooldown: Time in milliseconds between allowed repeated requests to the same path (default: 1000)
            logger: Logger instance to use (default: creates a new logger)
        """
        # Get or create the logger
        self.logger = logger or logging.getLogger("YTMusicFS")

        # Initialize the client component
        self.client = YouTubeMusicClient(
            auth_file=auth_file,
            client_id=client_id,
            client_secret=client_secret,
            browser=browser,
            logger=self.logger,
        )

        # Initialize the cache component
        self.cache = CacheManager(
            cache_dir=cache_dir,
            cache_timeout=cache_timeout,
            maxsize=cache_maxsize,
            logger=self.logger,
        )

        # Initialize the track processor component
        self.processor = TrackProcessor(logger=self.logger)

        # Initialize the content fetcher component
        self.fetcher = ContentFetcher(
            client=self.client,
            processor=self.processor,
            cache=self.cache,
            logger=self.logger,
        )

        # Set the callback for caching directory listings with attributes
        self.fetcher.cache_directory_callback = self._cache_directory_listing_with_attrs

        # Initialize the path router
        self.router = PathRouter()

        # Store parameters for future reference
        self.auth_file = auth_file
        self.client_id = client_id
        self.client_secret = client_secret
        self.browser = browser
        self.request_cooldown = request_cooldown / 1000.0  # Convert to seconds

        # File handling state
        self.open_files = {}  # Store file handles: {handle: {'stream_url': ...}}
        self.next_fh = 1  # Next file handle to assign
        self.path_to_fh = {}  # Store path to file handle mapping

        # Debounce mechanism for repeated requests
        self.last_access_time = {}  # {operation_path: last_access_time}
        self.last_access_lock = (
            threading.RLock()
        )  # Lock for last_access_time operations
        self.last_access_results = {}  # {operation_path: cached_result}

        # Thread-related objects
        self.file_handle_lock = threading.RLock()  # Lock for file handle operations
        self.thread_pool = ThreadPoolExecutor(max_workers=max_workers)
        self.logger.info(f"Thread pool initialized with {max_workers} workers")

        self.download_progress = (
            {}
        )  # Track download progress: {video_id: bytes_downloaded or status}

        # Track download threads - needed for interrupting downloads
        self.download_threads = {}  # Track download threads: {video_id: Thread}

        # Track download processes
        self.download_processes = {}
        self.download_queues = {}

        # Register exact path handlers
        self.router.register(
            "/",
            lambda: [
                ".",
                "..",
                "playlists",
                "liked_songs",
                "artists",
                "albums",
                "search",
            ],
        )
        self.router.register(
            "/playlists", lambda: [".", ".."] + self.fetcher.readdir_playlists()
        )
        self.router.register(
            "/liked_songs", lambda: [".", ".."] + self.fetcher.readdir_liked_songs()
        )
        self.router.register(
            "/artists", lambda: [".", ".."] + self.fetcher.readdir_artists()
        )
        self.router.register(
            "/albums", lambda: [".", ".."] + self.fetcher.readdir_albums()
        )
        self.router.register(
            "/search", lambda: [".", ".."] + self.fetcher.readdir_search_categories()
        )
        # Add handlers for /search/library and /search/catalog to show categories
        self.router.register(
            "/search/library",
            lambda: [".", ".."] + self.fetcher.readdir_search_category_options(),
        )
        self.router.register(
            "/search/catalog",
            lambda: [".", ".."] + self.fetcher.readdir_search_category_options(),
        )

        # Register dynamic handlers with wildcard capture
        self.router.register_dynamic(
            "/playlists/*",
            lambda path, *args: [".", ".."]
            + self.fetcher.readdir_playlist_content(path),
        )
        self.router.register_dynamic(
            "/artists/*",
            lambda path, *args: [".", ".."] + self.fetcher.readdir_artist_content(path),
        )
        self.router.register_dynamic(
            "/artists/*/*",
            lambda path, *args: [".", ".."] + self.fetcher.readdir_album_content(path),
        )
        self.router.register_dynamic(
            "/albums/*",
            lambda path, *args: [".", ".."] + self.fetcher.readdir_album_content(path),
        )
        # Search routes - focused on library and catalog paths only
        self.router.register_dynamic(
            "/search/*",
            lambda path, *args: [".", ".."]
            + self.fetcher.readdir_search_results(path, *args),
        )
        self.router.register_dynamic(
            "/search/library/*",
            lambda path, *args: [".", ".."]
            + self.fetcher.readdir_search_results(path, *args, scope="library"),
        )
        self.router.register_dynamic(
            "/search/catalog/*",
            lambda path, *args: [".", ".."]
            + self.fetcher.readdir_search_results(path, *args, scope=None),
        )
        self.router.register_dynamic(
            "/search/library/*/*",
            lambda path, *args: [".", ".."]
            + self.fetcher.readdir_search_item_content(path, *args, scope="library"),
        )
        self.router.register_dynamic(
            "/search/catalog/*/*",
            lambda path, *args: [".", ".."]
            + self.fetcher.readdir_search_item_content(path, *args, scope=None),
        )
        self.router.register_dynamic(
            "/search/library/*/*/*",
            lambda path, *args: [".", ".."]
            + self.fetcher.readdir_search_item_content(path, *args, scope="library"),
        )
        self.router.register_dynamic(
            "/search/catalog/*/*/*",
            lambda path, *args: [".", ".."]
            + self.fetcher.readdir_search_item_content(path, *args, scope=None),
        )

        # Preload cache if requested
        if preload_cache:
            self.preload_cache()

    def preload_cache(self) -> None:
        """Preload important cache data at startup.

        This method loads playlist, liked songs, artists, and albums data into cache
        to avoid on-demand loading when the filesystem is accessed.
        """
        self.logger.info("Preloading cache data...")

        # Use a thread pool to load data in parallel
        futures = []

        # Start loading each data type
        futures.append(self.thread_pool.submit(self.fetcher.readdir_playlists))
        futures.append(self.thread_pool.submit(self.fetcher.readdir_liked_songs))
        futures.append(self.thread_pool.submit(self.fetcher.readdir_artists))
        futures.append(self.thread_pool.submit(self.fetcher.readdir_albums))
        futures.append(self.thread_pool.submit(self.fetcher.readdir_search_categories))
        # Also preload search category options
        futures.append(
            self.thread_pool.submit(self.fetcher.readdir_search_category_options)
        )

        # Wait for all preload tasks to complete
        for future in futures:
            try:
                future.result()
            except Exception as e:
                self.logger.error(f"Error during cache preloading: {e}")

        # Optimize path validation by caching known directory structure
        self.optimize_path_validation()

        self.logger.info("Cache preloading completed")

    def optimize_path_validation(self):
        """Optimize path validation by pre-caching known valid paths.

        This method creates a set of known valid paths to avoid repeated validation
        and improve performance. Static paths that are always valid are checked directly
        in the _is_valid_path method and don't need caching.
        """
        self.logger.info("Optimizing path validation...")

        # We no longer need to cache these static directories since they're checked directly in _is_valid_path
        # Only cache dynamic paths that need validation

        # Cache search category directories - these are still cached to preserve compatibility with other code
        for category in ["songs", "videos", "albums", "artists", "playlists"]:
            self._cache_valid_dir(f"/search/library/{category}")
            self._cache_valid_dir(f"/search/catalog/{category}")

        # Cache the valid filenames for root
        self._cache_valid_filenames(
            "/", ["playlists", "liked_songs", "artists", "albums", "search"]
        )

        # Cache the valid filenames for search
        self._cache_valid_filenames("/search", ["library", "catalog"])

        # Cache the valid filenames for search/library and search/catalog
        search_categories = ["songs", "videos", "albums", "artists", "playlists"]
        self._cache_valid_filenames("/search/library", search_categories)
        self._cache_valid_filenames("/search/catalog", search_categories)

        self.logger.info("Path validation optimization completed")

    def _is_valid_path(self, path: str) -> bool:
        """Quickly check if a path is potentially valid without expensive operations.

        Args:
            path: The path to validate

        Returns:
            Boolean indicating if the path might be valid
        """
        # Get the context for better decision making
        context = inspect.currentframe().f_back.f_code.co_name
        self.logger.debug(f"_is_valid_path called from {context} for {path}")

        # For mkdir operations, always return true to let mkdir's own validation handle it
        if context == "mkdir" or "mkdir" in context:
            self.logger.debug(f"Allowing path as valid due to mkdir context: {path}")
            return True

        # Root and main category directories are always valid - no need to cache these
        if path == "/" or path in [
            "/playlists",
            "/liked_songs",
            "/artists",
            "/albums",
            "/search",
            "/search/library",
            "/search/catalog",
        ]:
            return True

        # Search category paths are valid - simplified logic
        if path.startswith("/search/library/") or path.startswith("/search/catalog/"):
            parts = path.split("/")

            # Category paths like /search/library/songs
            if len(parts) == 4:
                valid_categories = ["songs", "videos", "albums", "artists", "playlists"]
                if parts[3] in valid_categories:
                    return True

            # Search query paths - simplified
            if len(parts) >= 5:
                valid_categories = ["songs", "videos", "albums", "artists", "playlists"]
                if parts[3] in valid_categories:
                    # For getattr, consider it potentially valid to allow further validation
                    if context == "getattr":
                        return True
                    # For other operations, rely on cache
                    return self.cache.is_valid_path(path)

        # For file paths, check if they exist in parent directory's listings with attributes
        if "." in path and path.lower().endswith(".m4a"):
            parent_dir = os.path.dirname(path)
            filename = os.path.basename(path)

            # Check if file exists in parent directory's cached listing with attributes
            dir_listing = self.cache.get_directory_listing_with_attrs(parent_dir)
            if dir_listing and filename in dir_listing:
                return True

        # Use the cache's optimized path validation for all other paths
        return self.cache.is_valid_path(path)

    def _cache_valid_path(self, path: str) -> None:
        """Cache a valid path to help with quick validation.

        Args:
            path: The valid path to cache
        """
        dir_path = os.path.dirname(path)
        filename = os.path.basename(path)

        # Mark this exact path as valid using the optimized method
        self.cache.add_valid_file(path)

        # Add to directory's valid files list
        valid_files_cached = self.cache.get(f"valid_files:{dir_path}")
        valid_files = set(valid_files_cached) if valid_files_cached else set()
        valid_files.add(filename)
        # Convert set to list for JSON serialization
        self.cache.set(f"valid_files:{dir_path}", list(valid_files))

        match = re.search(r"(.*?)\s*\((\d+)\)(\.[^.]+)?$", filename)
        if match:
            base_name, _, _ = match.groups()
            # Cache the specific valid path
            cache_key = f"valid_path:{dir_path}/{base_name}"
            self.cache.set(cache_key, path)

            # Also add this base name to our set of valid base names for this directory
            valid_base_names_key = f"valid_base_names:{dir_path}"
            valid_base_names_cached = self.cache.get(valid_base_names_key)
            valid_base_names = (
                set(valid_base_names_cached) if valid_base_names_cached else set()
            )
            valid_base_names.add(base_name)
            # Convert set to list for JSON serialization
            self.cache.set(valid_base_names_key, list(valid_base_names))

            self.logger.debug(f"Cached valid path: {path} with key {cache_key}")

    def _cache_valid_dir(self, dir_path: str) -> None:
        """Mark a directory as valid in the cache.

        Args:
            dir_path: The directory path to cache as valid
        """
        # Mark the directory as valid using the optimized method
        self.cache.add_valid_dir(dir_path)

        # Also mark parent directories as valid
        parts = dir_path.split("/")
        for i in range(1, len(parts)):
            parent = "/".join(parts[:i]) or "/"
            self.cache.add_valid_dir(parent)

    # Helper method to cache all valid filenames in a directory
    def _cache_valid_filenames(self, dir_path: str, filenames: List[str]) -> None:
        """Cache all valid filenames in a directory for efficient path validation.

        Args:
            dir_path: Directory path
            filenames: List of valid filenames in this directory
        """
        # Mark this directory as valid
        self._cache_valid_dir(dir_path)

        # Store the complete list of valid files - already a list, so no conversion needed
        self.cache.set(f"valid_files:{dir_path}", filenames)

    def _cache_directory_listing_with_attrs(
        self, dir_path: str, processed_tracks: List[Dict[str, Any]]
    ) -> None:
        """Cache directory listing with file attributes for efficient getattr lookups.

        Args:
            dir_path: Directory path
            processed_tracks: List of processed track data with filename and metadata
        """
        # Create a dictionary mapping filenames to their attributes
        now = time.time()
        listing_with_attrs = {}

        for track in processed_tracks:
            filename = track.get("filename")
            if not filename:
                continue

            # Create basic file attributes
            attrs = {
                "st_mode": stat.S_IFREG | 0o644,
                "st_atime": now,
                "st_ctime": now,
                "st_mtime": now,
                "st_nlink": 1,
            }

            # Try to get a more accurate file size if duration is available
            duration_seconds = track.get("duration_seconds")
            if duration_seconds:
                # Estimate file size based on duration (128kbps = 16KB/sec)
                estimated_size = duration_seconds * 16 * 1024
                attrs["st_size"] = estimated_size
            else:
                # Default placeholder size
                attrs["st_size"] = 4096

            # Check if we have an actual cached file size
            file_size_cache_key = f"filesize:{dir_path}/{filename}"
            cached_size = self.cache.get(file_size_cache_key)
            if cached_size is not None:
                attrs["st_size"] = cached_size

            # Add to the listing
            listing_with_attrs[filename] = attrs

            # Mark each individual file path as valid - THIS IS CRUCIAL!
            file_path = f"{dir_path}/{filename}"
            self.cache.add_valid_file(file_path)

        # Cache the directory listing with attributes
        self.cache.set_directory_listing_with_attrs(dir_path, listing_with_attrs)

        # Also cache the filenames separately for backward compatibility
        self.cache.set(f"valid_files:{dir_path}", list(listing_with_attrs.keys()))

        # Mark this directory as valid
        self._cache_valid_dir(dir_path)

    def readdir(self, path: str, fh: Optional[int] = None) -> List[str]:
        """Read directory contents.

        Args:
            path: Directory path
            fh: File handle (unused)

        Returns:
            List of directory entries
        """
        self.logger.debug(f"readdir: {path}")

        # Check if this is a file (contains a period) and not an m4a file
        if "." in os.path.basename(path) and not path.lower().endswith(".m4a"):
            self.logger.debug(f"Rejecting non-m4a file extension in readdir: {path}")
            # Just return empty directory for these special files
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

        # Check if we need to handle path validation
        if not self._is_valid_path(path):
            self.logger.debug(f"Rejecting invalid path in readdir: {path}")
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

        # Use the router to handle the path
        try:
            result = self.router.route(path)

            # Mark this as a valid directory and cache all valid filenames for future validation
            if path != "/" and len(result) > 2:  # More than just "." and ".."
                self._cache_valid_filenames(
                    path, [entry for entry in result if entry not in [".", ".."]]
                )
                self._cache_valid_dir(path)

                # Special handling for search scope directories to cache category directories
                if path in ["/search/library", "/search/catalog"]:
                    categories = [entry for entry in result if entry not in [".", ".."]]
                    for category in categories:
                        self._cache_valid_dir(f"{path}/{category}")

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

    def getattr(self, path: str, fh: Optional[int] = None) -> Dict[str, Any]:
        """Get file attributes.

        Args:
            path: The file or directory path
            fh: File handle (unused)

        Returns:
            File attributes dictionary
        """
        self.logger.debug(f"getattr: {path}")

        # Reject any file that is not an m4a file
        if "." in os.path.basename(path) and not path.lower().endswith(".m4a"):
            self.logger.debug(f"Rejecting non-m4a file extension in getattr: {path}")
            raise OSError(errno.ENOENT, "No such file or directory")

        # For debugging, check if we have a stack trace involved in a mkdir operation
        stack = traceback.format_stack()
        is_mkdir_in_stack = any("mkdir" in frame for frame in stack)
        if is_mkdir_in_stack:
            self.logger.debug(f"mkdir detected in call stack for getattr {path}")

        # Check if this request is too soon after the last one
        operation_key = f"getattr:{path}"
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

        # Check if this is coming from mkdir or another operation
        # that requires existence checking rather than creating
        # This is essential to avoid blocking directory creation
        context = (
            inspect.currentframe().f_back.f_code.co_name
            if inspect.currentframe().f_back
            else "unknown"
        )
        self.logger.debug(f"getattr called from context: {context}")

        # More aggressive detection for mkdir operations
        is_mkdir_context = context == "mkdir" or "mkdir" in context or is_mkdir_in_stack

        # Special handling for search paths during mkdir operations
        if is_mkdir_context and path.startswith("/search/"):
            parts = path.split("/")
            # Check if this is a search query path (like /search/catalog/songs/query)
            if len(parts) == 5 and parts[2] in ["library", "catalog"]:
                valid_categories = ["songs", "videos", "albums", "artists", "playlists"]
                if parts[3] in valid_categories:
                    # During mkdir, ALWAYS report the path doesn't exist
                    # This is critical to ensure mkdir operations can proceed
                    self.logger.debug(
                        f"mkdir context detected, forcing non-existence for {path}"
                    )
                    raise OSError(errno.ENOENT, "No such file or directory")

        # Quick validation to reject invalid paths
        if not self._is_valid_path(path):
            self.logger.debug(f"Rejecting invalid path in getattr: {path}")
            raise OSError(errno.ENOENT, "No such file or directory")

        now = time.time()
        attr = {
            "st_atime": now,
            "st_ctime": now,
            "st_mtime": now,
            "st_nlink": 2,
        }

        # Root directory
        if path == "/":
            attr["st_mode"] = stat.S_IFDIR | 0o755
            attr["st_size"] = 0
            with self.last_access_lock:
                self.last_access_results[operation_key] = attr.copy()
            return attr

        # Main categories
        if path in ["/playlists", "/liked_songs", "/artists", "/albums"]:
            attr["st_mode"] = stat.S_IFDIR | 0o755
            attr["st_size"] = 0
            with self.last_access_lock:
                self.last_access_results[operation_key] = attr.copy()
            return attr

        # Search paths - special handling for search functionality
        if path == "/search" or path == "/search/library" or path == "/search/catalog":
            attr["st_mode"] = stat.S_IFDIR | 0o755
            attr["st_size"] = 0
            with self.last_access_lock:
                self.last_access_results[operation_key] = attr.copy()
            return attr

        # Special handling for search category and search query paths
        if path.startswith("/search/library/") or path.startswith("/search/catalog/"):
            parts = path.split("/")
            # Category directories (like /search/catalog/songs)
            if len(parts) == 4:
                valid_categories = ["songs", "videos", "albums", "artists", "playlists"]
                if parts[3] in valid_categories:
                    attr["st_mode"] = stat.S_IFDIR | 0o755
                    attr["st_size"] = 0
                    with self.last_access_lock:
                        self.last_access_results[operation_key] = attr.copy()
                    return attr

            # Search query directories (like /search/catalog/songs/queen)
            if len(parts) == 5:
                # When checking if a directory exists, we need to be careful about mkdir operations
                if is_mkdir_context:
                    # During mkdir, we need to report that the directory doesn't exist
                    # so mkdir can create it
                    self.logger.debug(
                        f"mkdir context detected for {path}, reporting non-existence"
                    )
                    raise OSError(errno.ENOENT, "No such file or directory")

                # Check if this is a valid/existing search directory
                valid_dir_cached = self.cache.get(f"valid_dir:{path}")
                if valid_dir_cached:
                    attr["st_mode"] = stat.S_IFDIR | 0o755
                    attr["st_size"] = 0
                    with self.last_access_lock:
                        self.last_access_results[operation_key] = attr.copy()
                    return attr

                # If it's not in cache as a valid directory, it doesn't exist yet
                # This ensures mkdir can create new search directories without conflict
                raise OSError(errno.ENOENT, "No such file or directory")

        # First, check if we can get the file attributes from the parent directory's cached listing
        # This is the key optimization we're adding in this step
        if path.lower().endswith(".m4a"):
            parent_dir = os.path.dirname(path)
            filename = os.path.basename(path)

            # Try to get attributes from parent dir's cached listing
            cached_attrs = self.cache.get_file_attrs_from_parent_dir(path)
            if cached_attrs:
                self.logger.debug(
                    f"Using cached attributes from parent directory for {path}"
                )

                # Update with fresh timestamps
                cached_attrs["st_atime"] = now
                cached_attrs["st_ctime"] = now
                cached_attrs["st_mtime"] = now

                # Check for updated file size
                file_size_cache_key = f"filesize:{path}"
                cached_size = self.cache.get(file_size_cache_key)
                if cached_size is not None:
                    cached_attrs["st_size"] = cached_size

                with self.last_access_lock:
                    self.last_access_results[operation_key] = cached_attrs.copy()
                return cached_attrs

        # If we didn't find cached attributes, continue with the original logic
        # Check if this is a song file (ends with .m4a)
        if path.lower().endswith(".m4a"):
            # Check if it's a valid song file by examining its parent directory
            parent_dir = os.path.dirname(path)
            filename = os.path.basename(path)

            # Check if we have a cached file size for this path
            file_size_cache_key = f"filesize:{path}"
            cached_size = self.cache.get(file_size_cache_key)

            if cached_size is not None:
                self.logger.debug(f"Using cached file size for {path}: {cached_size}")
                attr["st_mode"] = stat.S_IFREG | 0o644
                attr["st_size"] = cached_size

                # Also update the cached attributes in parent dir for future lookups
                self.cache.update_file_attrs_in_parent_dir(path, attr)

                with self.last_access_lock:
                    self.last_access_results[operation_key] = attr.copy()
                return attr

            try:
                # Get directory listing of parent
                dirlist = self.readdir(parent_dir, None)
                if filename in dirlist:
                    # It's a valid song file
                    attr["st_mode"] = stat.S_IFREG | 0o644

                    # Get a more accurate file size estimate based on song duration if available
                    songs = self.cache.get(parent_dir)
                    if (
                        songs
                        and isinstance(songs, list)
                        and songs
                        and isinstance(songs[0], dict)
                    ):
                        for song in songs:
                            if song.get("filename") == filename:
                                # Check if we have an actual file size in cache
                                file_size_cache_key = f"filesize:{path}"
                                cached_size = self.cache.get(file_size_cache_key)

                                if cached_size is not None:
                                    self.logger.debug(
                                        f"Using cached actual file size for {path}: {cached_size}"
                                    )
                                    attr["st_size"] = cached_size

                                    # Update the parent dir cache with these attributes
                                    self.cache.update_file_attrs_in_parent_dir(
                                        path, attr
                                    )

                                    with self.last_access_lock:
                                        self.last_access_results[operation_key] = (
                                            attr.copy()
                                        )
                                    return attr

                                # No duration-based estimation - use minimal size
                                # This will be updated with actual size when file is opened
                                attr["st_size"] = 4096

                                # Update the parent dir cache with these attributes
                                self.cache.update_file_attrs_in_parent_dir(path, attr)

                                with self.last_access_lock:
                                    self.last_access_results[operation_key] = (
                                        attr.copy()
                                    )
                                return attr

                    # No duration-based fallback anymore - just use minimal size
                    attr["st_size"] = 4096  # Minimal placeholder size

                    # Update the parent dir cache with these attributes
                    self.cache.update_file_attrs_in_parent_dir(path, attr)

                    with self.last_access_lock:
                        self.last_access_results[operation_key] = attr.copy()
                    return attr
            except Exception as e:
                self.logger.debug(f"Error checking file existence: {e}")
                # Continue to other checks
                pass

        # Check if path is a directory by trying to list it
        try:
            if path.endswith("/"):
                path = path[:-1]

            entries = self.readdir(path, None)
            if entries:
                attr["st_mode"] = stat.S_IFDIR | 0o755
                attr["st_size"] = 0
                with self.last_access_lock:
                    self.last_access_results[operation_key] = attr.copy()
                return attr
        except Exception:
            pass

        # Check if path is a file
        parent_dir = os.path.dirname(path)
        filename = os.path.basename(path)

        if not filename:
            raise OSError(errno.ENOENT, "No such file or directory")

        # Check if file exists in parent directory
        try:
            dirlist = self.readdir(parent_dir, None)
            if filename in dirlist:
                # It's a file
                attr["st_mode"] = stat.S_IFREG | 0o644

                # Same but for other file paths - no estimation
                file_size_cache_key = f"filesize:{path}"
                cached_size = self.cache.get(file_size_cache_key)

                if cached_size is not None:
                    attr["st_size"] = cached_size
                else:
                    attr["st_size"] = (
                        4096  # Minimal placeholder size instead of default estimate
                    )

                # If we successfully determined attributes for a file path with a number pattern,
                # cache it as a valid path to help with future validation
                self._cache_valid_path(path)

                with self.last_access_lock:
                    self.last_access_results[operation_key] = attr.copy()
                return attr
        except Exception:
            pass

        # If we get here, the file or directory doesn't exist
        raise OSError(errno.ENOENT, "No such file or directory")

    def open(self, path: str, flags: int) -> int:
        """Open a file and return a file handle.

        Args:
            path: The file path
            flags: File open flags

        Returns:
            File handle
        """
        self.logger.debug(f"open: {path} with flags {flags}")

        # Cache this as a valid path since it's being opened
        self._cache_valid_path(path)

        if path == "/":
            raise OSError(errno.EISDIR, "Is a directory")

        # Extract directory path and filename
        dir_path = os.path.dirname(path)
        filename = os.path.basename(path)
        self.logger.debug(f"Looking for {filename} in {dir_path}")

        songs = None

        # Special handling for liked songs which use a different cache key
        if dir_path == "/liked_songs":
            songs = self.cache.get("/liked_songs_processed")
            self.logger.debug(f"Liked songs cache: {'Found' if songs else 'Not found'}")
        # Special handling for playlists which need to use the playlist ID in the cache key
        elif dir_path.startswith("/playlists/"):
            playlist_name = dir_path.split("/")[2]
            # Find the playlist ID
            playlists = self.cache.get("/playlists")
            if playlists:
                playlist_id = None
                for playlist in playlists:
                    if (
                        self.processor.sanitize_filename(playlist["title"])
                        == playlist_name
                    ):
                        playlist_id = playlist["playlistId"]
                        break

                if playlist_id:
                    processed_cache_key = f"/playlist/{playlist_id}_processed"
                    songs = self.cache.get(processed_cache_key)
                    self.logger.debug(
                        f"Playlist songs cache ({processed_cache_key}): {'Found' if songs else 'Not found'}"
                    )
        else:
            songs = self.cache.get(dir_path)
            self.logger.debug(
                f"Cache status for {dir_path}: {'Found' if songs else 'Not found'}"
            )

        if not songs:
            # Re-fetch if not in cache
            self.logger.debug(f"Re-fetching directory {dir_path}")
            self.readdir(dir_path, None)

            # Try to get from cache again after refetching
            if dir_path == "/liked_songs":
                songs = self.cache.get("/liked_songs_processed")
                self.logger.debug(
                    f"After re-fetch, liked songs cache: {'Found' if songs else 'Not found'}"
                )
            elif dir_path.startswith("/playlists/"):
                playlist_name = dir_path.split("/")[2]
                playlists = self.cache.get("/playlists")
                if playlists:
                    playlist_id = None
                    for playlist in playlists:
                        if (
                            self.processor.sanitize_filename(playlist["title"])
                            == playlist_name
                        ):
                            playlist_id = playlist["playlistId"]
                            break

                    if playlist_id:
                        processed_cache_key = f"/playlist/{playlist_id}_processed"
                        songs = self.cache.get(processed_cache_key)
                        self.logger.debug(
                            f"After re-fetch, playlist songs cache ({processed_cache_key}): {'Found' if songs else 'Not found'}"
                        )
            else:
                songs = self.cache.get(dir_path)
                self.logger.debug(
                    f"After re-fetch, songs: {'Found' if songs else 'Not found'}"
                )

        if not songs:
            self.logger.error(f"Could not find songs in directory {dir_path}")
            raise OSError(errno.ENOENT, "File not found")

        # Handle songs as either list of strings or list of dictionaries
        video_id = None

        # Check if the cache contains song objects (dictionaries) or just filenames (strings)
        if isinstance(songs, list) and songs and isinstance(songs[0], str):
            # Cache contains only filenames
            self.logger.debug(f"Cache contains string filenames")
            if filename in songs:
                # Matched by filename, but we don't have videoId
                self.logger.error(
                    f"File found in cache but no videoId available: {filename}"
                )
                raise OSError(errno.ENOENT, "No video ID for file")
        else:
            # Cache contains song dictionaries as expected
            for song in songs:
                # Check if song is a dictionary before trying to use .get()
                if isinstance(song, dict):
                    self.logger.debug(
                        f"Checking song: {song.get('filename', 'Unknown')} for match with {filename}"
                    )
                    if song.get("filename") == filename:
                        video_id = song.get("videoId")
                        self.logger.debug(f"Found matching song, videoId: {video_id}")
                        break
                else:
                    self.logger.debug(
                        f"Skipping non-dictionary song object: {type(song)}"
                    )

        if not video_id:
            self.logger.error(f"Could not find videoId for {filename}")

            # Special debug to see what's in the cache
            self.logger.debug(
                f"Cache contents for debugging: {songs[:5] if isinstance(songs, list) else songs}"
            )

            raise OSError(errno.ENOENT, "File not found")

        # First check if the audio is already cached
        cache_path = Path(self.cache.cache_dir / "audio" / f"{video_id}.m4a")
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        # Set up the file handle with initial information
        with self.file_handle_lock:
            fh = self.next_fh
            self.next_fh += 1
            self.open_files[fh] = {
                "cache_path": str(cache_path),
                "video_id": video_id,
                "stream_url": None,  # Will be populated in background
                "status": "initializing",  # Track status of file preparation
                "error": None,  # Store any errors that occur
                "path": path,  # Store path for reference
                "initialized_event": threading.Event(),  # Event to signal when initialization is complete
            }
            self.path_to_fh[path] = fh
            self.logger.debug(f"Assigned file handle {fh} to {path}")

        # Check if the audio file is already cached completely
        if self._check_cached_audio(video_id):
            self.logger.debug(f"Found complete cached audio for {video_id}")
            with self.file_handle_lock:
                self.open_files[fh]["status"] = "ready"
                self.open_files[fh]["initialized_event"].set()
            return fh

        # Start a background task to prepare the stream URL and file size
        self.thread_pool.submit(self._prepare_file_in_background, fh, video_id, path)

        # Return file handle immediately without waiting for background task
        return fh

    def _prepare_file_in_background(self, fh: int, video_id: str, path: str) -> None:
        """Prepare a file in the background by fetching its stream URL and starting download.

        Args:
            fh: File handle
            video_id: YouTube video ID
            path: File path
        """
        try:
            # Get file info from handle
            file_info = self.open_files.get(fh)
            if not file_info:
                self.logger.error(f"File handle {fh} no longer exists")
                return

            cache_path = file_info["cache_path"]

            # Create a queue for communication between processes
            result_queue = multiprocessing.Queue()

            # Define the process function to run yt-dlp
            def extract_stream_url_process(video_id, browser, result_queue):
                try:
                    # Use yt-dlp to get the audio stream URL
                    ydl_opts = {
                        "format": "141/bestaudio[ext=m4a]",
                        "extractor_args": {
                            "youtube": {
                                "formats": ["missing_pot"],
                            },
                        },
                    }

                    # Add browser cookies if a browser is specified
                    if browser:
                        ydl_opts["cookiesfrombrowser"] = (browser,)

                    url = f"https://music.youtube.com/watch?v={video_id}"

                    with YoutubeDL(ydl_opts) as ydl:
                        info = ydl.extract_info(url, download=False)
                        stream_url = info["url"]
                        result_queue.put(
                            {"status": "success", "stream_url": stream_url}
                        )
                except Exception as e:
                    result_queue.put({"status": "error", "error": str(e)})

            # Start the process
            process = multiprocessing.Process(
                target=extract_stream_url_process,
                args=(video_id, self.browser, result_queue),
            )
            process.daemon = True
            process.start()

            # Store the process and queue for potential termination
            self.download_processes[video_id] = process
            self.download_queues[video_id] = result_queue

            # Wait for the result with a timeout
            try:
                # Set a timeout to prevent hanging indefinitely
                result = result_queue.get(timeout=30)

                # Process is done, clean up
                if video_id in self.download_processes:
                    del self.download_processes[video_id]
                if video_id in self.download_queues:
                    del self.download_queues[video_id]

                if result["status"] == "error":
                    raise Exception(result["error"])

                stream_url = result["stream_url"]

                if not stream_url:
                    raise Exception("No suitable audio stream found")

                self.logger.debug(f"Successfully got stream URL for {video_id}")

                # Get the actual file size using a HEAD request
                actual_size = None
                try:
                    head_response = requests.head(stream_url, timeout=10)
                    if (
                        head_response.status_code == 200
                        and "content-length" in head_response.headers
                    ):
                        actual_size = int(head_response.headers["content-length"])
                        self.logger.debug(
                            f"Got actual file size for {video_id}: {actual_size} bytes"
                        )
                        # Update the file size cache
                        self._update_file_size(path, actual_size)
                    else:
                        self.logger.warning(
                            f"Couldn't get file size from HEAD request for {video_id}"
                        )
                except Exception as e:
                    self.logger.warning(
                        f"Error getting file size for {video_id}: {str(e)}"
                    )
                    # Continue even if we can't get the file size

                # Update the file handle with the stream URL and status
                with self.file_handle_lock:
                    if fh in self.open_files:
                        self.open_files[fh]["stream_url"] = stream_url
                        self.open_files[fh]["status"] = "ready"
                        self.open_files[fh]["initialized_event"].set()

                # Initialize download status and start the download only if not already interrupted
                if video_id not in self.download_progress or self.download_progress[
                    video_id
                ] not in ["interrupted", "complete"]:
                    self.download_progress[video_id] = 0
                    self._start_background_download(video_id, stream_url, cache_path)

            except multiprocessing.queues.Empty:
                # Timeout occurred
                self.logger.error(f"Timeout while extracting stream URL for {video_id}")
                # Clean up the process
                if video_id in self.download_processes:
                    process = self.download_processes[video_id]
                    if process.is_alive():
                        process.terminate()
                    del self.download_processes[video_id]
                if video_id in self.download_queues:
                    del self.download_queues[video_id]

                # Update file handle with error
                with self.file_handle_lock:
                    if fh in self.open_files:
                        self.open_files[fh]["status"] = "error"
                        self.open_files[fh][
                            "error"
                        ] = "Timeout while extracting stream URL"
                        self.open_files[fh]["initialized_event"].set()

        except Exception as e:
            self.logger.error(f"Error preparing file {video_id}: {str(e)}")
            # Update file handle with error
            with self.file_handle_lock:
                if fh in self.open_files:
                    self.open_files[fh]["status"] = "error"
                    self.open_files[fh]["error"] = str(e)
                    self.open_files[fh]["initialized_event"].set()

            # Clean up any processes that might be running
            if video_id in self.download_processes:
                process = self.download_processes[video_id]
                if process.is_alive():
                    process.terminate()
                del self.download_processes[video_id]
            if video_id in self.download_queues:
                del self.download_queues[video_id]

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

    def read(self, path: str, size: int, offset: int, fh: int) -> bytes:
        """Read data that might be partially downloaded."""
        if fh not in self.open_files:
            raise OSError(errno.EBADF, "Bad file descriptor")

        file_info = self.open_files[fh]
        cache_path = file_info["cache_path"]
        video_id = file_info["video_id"]
        status = file_info.get("status")

        # Check for errors first
        if status == "error":
            error_msg = file_info.get("error", "Unknown error preparing file")
            self.logger.error(f"Error reading {path}: {error_msg}")
            raise OSError(errno.EIO, error_msg)

        # If still initializing, wait for initialization to complete with periodic checks
        if status == "initializing":
            # Wait up to 5 seconds total, checking every 0.1 seconds
            max_wait_time = 5.0  # seconds
            check_interval = 0.1  # seconds
            wait_iterations = int(max_wait_time / check_interval)

            for i in range(wait_iterations):
                # Check if initialization is complete
                if file_info["initialized_event"].wait(check_interval):
                    # Initialization completed
                    break

                stream_url = file_info.get("stream_url")
                if stream_url:
                    # Don't stream if download was interrupted
                    if self.download_progress.get(video_id) == "interrupted":
                        self.logger.debug(
                            f"Download interrupted for {path}, stopping read"
                        )
                        raise OSError(errno.EIO, "File access interrupted")

                    self.logger.debug(
                        f"Streaming directly during initialization: {path}"
                    )
                    return self._stream_content(stream_url, offset, size)

            # Check status after waiting
            if file_info.get("status") == "error":
                error_msg = file_info.get("error", "Unknown error preparing file")
                self.logger.error(f"Error after waiting: {error_msg}")
                raise OSError(errno.EIO, error_msg)
            elif file_info.get("status") == "initializing":
                self.logger.error(f"Timeout waiting for file initialization: {path}")
                raise OSError(errno.EIO, "Timeout waiting for file initialization")

        # Now check download status - use video_id to get download progress
        download_status = self.download_progress.get(video_id)

        # If download was interrupted, stop the read
        if download_status == "interrupted":
            self.logger.debug(f"Download interrupted for {path}, stopping read")
            raise OSError(errno.EIO, "File access interrupted")

        # If download is complete, read from cache
        if download_status == "complete":
            with open(cache_path, "rb") as f:
                f.seek(offset)
                return f.read(size)

        # If we have enough of the file downloaded, read from cache
        if isinstance(download_status, int) and download_status > offset + size:
            with open(cache_path, "rb") as f:
                f.seek(offset)
                return f.read(size)

        # Get the stream URL (should be available by now)
        stream_url = file_info["stream_url"]
        if not stream_url:
            self.logger.error(f"Stream URL not available for {path}")
            raise OSError(errno.EIO, "Stream URL not available")

        # Otherwise, stream directly - no waiting
        self.logger.debug(
            f"Requested range not cached yet, streaming directly: offset={offset}, size={size}"
        )
        return self._stream_content(stream_url, offset, size)

    def release(self, path: str, fh: int) -> int:
        """Release (close) a file handle and stop any ongoing download.

        Args:
            path: The file path
            fh: File handle

        Returns:
            0 on success
        """
        self.logger.debug(f"Releasing file handle {fh} for {path}")
        with self.file_handle_lock:
            if fh in self.open_files:
                video_id = self.open_files[fh].get("video_id")
                del self.open_files[fh]
                # Remove path_to_fh entry for this path
                if path in self.path_to_fh and self.path_to_fh[path] == fh:
                    del self.path_to_fh[path]

                # Stop the download if it's ongoing
                if video_id:
                    self.logger.debug(f"Stopping download for {video_id}")
                    # Mark download as interrupted
                    self.download_progress[video_id] = "interrupted"

                    # Terminate the download process if it exists
                    if video_id in self.download_processes:
                        process = self.download_processes[video_id]
                        if process.is_alive():
                            # Send stop signal through the queue
                            if video_id in self.download_queues:
                                try:
                                    self.download_queues[video_id].put({"type": "stop"})
                                except Exception:
                                    pass
                            # Force terminate the process
                            try:
                                process.terminate()
                            except Exception as e:
                                self.logger.error(
                                    f"Error terminating process for {video_id}: {str(e)}"
                                )

                    # For compatibility with existing code
                    if video_id in self.download_threads:
                        del self.download_threads[video_id]
        return 0

    def _auto_refresh_cache(self, cache_key: str, refresh_interval: int = 600) -> None:
        """Automatically refresh the cache if the last refresh time exceeds the interval.

        Args:
            cache_key: The cache key to check (e.g., '/playlists').
            refresh_interval: Time in seconds before triggering a refresh (default: 600).
        """
        # Use the CacheManager's auto_refresh_cache method instead
        self.cache.auto_refresh_cache(
            cache_key=cache_key,
            refresh_method=self.refresh_cache,
            refresh_interval=refresh_interval,
        )

    def refresh_liked_songs_cache(self) -> None:
        """Refresh the cache for liked songs.

        This method updates the liked songs cache with any newly liked songs,
        without deleting the entire cache.
        """
        self.cache.refresh_cache_data(
            cache_key="/liked_songs",
            fetch_func=self.client.get_liked_songs,
            processor=self.processor,
            id_fields=["videoId"],
            fetch_args={"limit": 100},
            process_items=True,
            processed_cache_key="/liked_songs_processed",
            extract_nested_items="tracks",
            prepend_new_items=True,
        )

    def refresh_playlists_cache(self) -> None:
        """Refresh the cache for playlists.

        This method updates the playlists cache with any changes in playlists,
        without deleting the entire cache.
        """
        self.cache.refresh_cache_data(
            cache_key="/playlists",
            fetch_func=self.client.get_library_playlists,
            id_fields=["playlistId"],
            check_updates=True,
            update_field="title",
            clear_related_cache=True,
            related_cache_prefix="/playlist/",
            related_cache_suffix="_processed",
            fetch_args={"limit": 100},
        )

    def refresh_artists_cache(self) -> None:
        """Refresh the cache for artists.

        This method updates the artists cache with any changes in the user's library,
        without deleting the entire cache.
        """
        self.cache.refresh_cache_data(
            cache_key="/artists",
            fetch_func=self.client.get_library_artists,
            id_fields=["artistId", "browseId", "id"],
        )

    def refresh_albums_cache(self) -> None:
        """Refresh the cache for albums.

        This method updates the albums cache with any changes in the user's library,
        without deleting the entire cache.
        """
        self.cache.refresh_cache_data(
            cache_key="/albums",
            fetch_func=self.client.get_library_albums,
            id_fields=["albumId", "browseId", "id"],
        )

    def refresh_cache(self) -> None:
        """Refresh all caches.

        This method updates all caches with any changes in the user's library.
        It uses a unified approach to refresh all content types consistently,
        preserving existing cached data and only updating what's changed,
        making it efficient for large libraries.
        """
        self.logger.info("Refreshing all caches...")

        # Refresh individual caches using a consistent approach
        self.refresh_liked_songs_cache()
        self.refresh_playlists_cache()
        self.refresh_artists_cache()
        self.refresh_albums_cache()

        # Delete all keys that start with "/search/"
        self.cache.delete_pattern("/search/*")
        # Also clear processed search results
        self.cache.delete_pattern("/search/*/*_processed")

        current_time = time.time()

        # Mark all caches as freshly refreshed
        self.cache.set_metadata("/liked_songs", "last_refresh_time", current_time)
        self.cache.set_metadata("/playlists", "last_refresh_time", current_time)
        self.cache.set_metadata("/artists", "last_refresh_time", current_time)
        self.cache.set_metadata("/albums", "last_refresh_time", current_time)

        self.logger.info("All caches refreshed successfully")

    def _delete_from_cache(self, path: str) -> None:
        """Delete data from cache.

        Args:
            path: The path to delete from cache
        """
        self.cache.delete(path)

    def _start_background_download(self, video_id, stream_url, cache_path):
        """Start downloading a file in the background and track progress."""

        def download_task(video_id, stream_url, cache_path, progress_queue):
            try:
                self.logger.debug(f"Starting background download for {video_id}")
                with requests.get(stream_url, stream=True) as response:
                    # Update file size from content-length if available
                    if "content-length" in response.headers:
                        actual_size = int(response.headers["content-length"])
                        # Send file size to main process
                        progress_queue.put(
                            {"type": "size", "video_id": video_id, "size": actual_size}
                        )

                    with open(cache_path, "wb") as f:
                        for chunk in response.iter_content(chunk_size=4096):
                            # Check if we should stop (by checking if queue has a stop message)
                            try:
                                if not progress_queue.empty():
                                    msg = progress_queue.get_nowait()
                                    if msg.get("type") == "stop":
                                        self.logger.debug(
                                            f"Download interrupted for {video_id}"
                                        )
                                        return
                            except Exception:
                                pass  # Ignore errors checking the queue

                            f.write(chunk)
                            # Update download progress
                            progress = f.tell()
                            # Send progress update to main process
                            progress_queue.put(
                                {
                                    "type": "progress",
                                    "video_id": video_id,
                                    "progress": progress,
                                }
                            )

                # Mark download as complete
                progress_queue.put({"type": "complete", "video_id": video_id})

                # When a download completes:
                status_path = os.path.join(
                    os.path.dirname(cache_path), f"{video_id}.status"
                )
                with open(status_path, "w") as f:
                    f.write("complete")

            except Exception as e:
                # Send error to main process
                progress_queue.put(
                    {"type": "error", "video_id": video_id, "error": str(e)}
                )

        # Create a queue for communication between processes
        progress_queue = multiprocessing.Queue()

        # Start the download in a background process
        download_process = multiprocessing.Process(
            target=download_task,
            args=(video_id, stream_url, cache_path, progress_queue),
        )
        download_process.daemon = True
        download_process.start()

        # Store the process and queue references
        self.download_processes[video_id] = download_process
        self.download_queues[video_id] = progress_queue

        # Start a thread to monitor the progress queue
        def monitor_progress():
            while True:
                try:
                    # Check if the process is still alive
                    if not download_process.is_alive():
                        break

                    # Get progress updates from the queue
                    try:
                        msg = progress_queue.get(timeout=1)
                        msg_type = msg.get("type")

                        if msg_type == "progress":
                            # Update download progress
                            self.download_progress[video_id] = msg.get("progress", 0)
                        elif msg_type == "complete":
                            # Mark download as complete
                            self.download_progress[video_id] = "complete"
                            self.logger.debug(f"Download completed for {video_id}")
                            break
                        elif msg_type == "error":
                            # Mark download as failed
                            self.download_progress[video_id] = "failed"
                            self.logger.error(
                                f"Error downloading {video_id}: {msg.get('error')}"
                            )
                            break
                        elif msg_type == "size":
                            # Update file size for all paths using this video_id
                            actual_size = msg.get("size")
                            with self.file_handle_lock:
                                for handle, info in self.open_files.items():
                                    if info.get("video_id") == video_id:
                                        # Get the path from handle
                                        for path in self.path_to_fh:
                                            if self.path_to_fh[path] == handle:
                                                self._update_file_size(
                                                    path, actual_size
                                                )
                                                self.logger.debug(
                                                    f"Updated size for {path} to {actual_size} from download response"
                                                )
                                                break
                    except multiprocessing.queues.Empty:
                        # No messages in the queue, continue
                        continue

                except Exception as e:
                    self.logger.error(
                        f"Error monitoring download progress for {video_id}: {str(e)}"
                    )
                    break

            # Clean up resources when done
            if video_id in self.download_processes:
                process = self.download_processes[video_id]
                if process.is_alive():
                    try:
                        process.terminate()
                    except Exception:
                        pass
                del self.download_processes[video_id]
            if video_id in self.download_queues:
                del self.download_queues[video_id]

        # Start the monitor thread
        monitor_thread = threading.Thread(target=monitor_progress)
        monitor_thread.daemon = True
        monitor_thread.start()

        # Store the thread reference (for compatibility with existing code)
        self.download_threads[video_id] = monitor_thread

    def _stream_content(self, stream_url, offset, size):
        """Stream content directly from URL (fallback if download is too slow)."""
        headers = {"Range": f"bytes={offset}-{offset + size - 1}"}
        response = requests.get(stream_url, headers=headers, stream=False)

        if response.status_code in (200, 206):
            return response.content
        else:
            self.logger.error(f"Failed to stream: {response.status_code}")
            raise OSError(errno.EIO, "Failed to read stream")

    def _check_cached_audio(self, video_id):
        """Check if an audio file is already cached completely."""
        cache_path = self.cache.cache_dir / "audio" / f"{video_id}.m4a"
        status_path = self.cache.cache_dir / "audio" / f"{video_id}.status"

        # First check for status file (most reliable)
        if status_path.exists():
            try:
                with open(status_path, "r") as f:
                    status = f.read().strip()
                if status == "complete":
                    self.logger.debug(
                        f"Found status file indicating {video_id} is complete"
                    )
                    self.download_progress[video_id] = "complete"
                    return True
            except Exception as e:
                self.logger.debug(f"Error reading status file for {video_id}: {e}")

        # Fall back to checking if the file exists and has content
        if cache_path.exists() and cache_path.stat().st_size > 0:
            self.logger.debug(
                f"Found existing audio file for {video_id}, marking as complete"
            )
            self.download_progress[video_id] = "complete"

            # If file exists but status doesn't, create the status file
            if not status_path.exists():
                try:
                    with open(status_path, "w") as f:
                        f.write("complete")
                except Exception as e:
                    self.logger.debug(
                        f"Failed to create status file for {video_id}: {e}"
                    )

            return True

        return False

    def mkdir(self, path, mode):
        """Create a directory in the FUSE filesystem.

        This implementation primarily supports creating search query directories
        in the search path structure.

        Args:
            path: The directory path to create
            mode: Directory permissions mode

        Returns:
            0 on success
        """
        self.logger.debug(f"mkdir: {path} with mode {mode}")

        # For debugging, log the stack
        stack = traceback.format_stack()
        self.logger.debug(f"mkdir stack trace: {stack}")

        # Handle search path directories
        if path.startswith("/search/"):
            parts = path.split("/")

            # Don't allow creating directories with invalid path structure
            if len(parts) < 3:
                raise OSError(errno.EINVAL, "Invalid search path")

            # /search/library or /search/catalog are valid
            if len(parts) == 3 and parts[2] in ["library", "catalog"]:
                self.logger.debug(f"Creating search scope directory: {parts[2]}")
                # Allow creating these directories (they're already virtually handled)
                return 0

            # /search/library/songs, /search/catalog/songs, etc.
            if len(parts) == 4 and parts[2] in ["library", "catalog"]:
                valid_categories = ["songs", "videos", "albums", "artists", "playlists"]
                if parts[3] in valid_categories:
                    self.logger.debug(f"Creating search category directory: {parts[3]}")
                    # Allow creating category directories
                    return 0
                else:
                    raise OSError(errno.EINVAL, "Invalid search category")

            # /search/library/songs/query or /search/catalog/songs/query - this is the actual search query
            if len(parts) == 5 and parts[2] in ["library", "catalog"]:
                valid_categories = ["songs", "videos", "albums", "artists", "playlists"]
                if parts[3] in valid_categories:
                    search_query = parts[4]
                    scope = "library" if parts[2] == "library" else None
                    filter_type = parts[3]  # Keep plural form as expected by the API

                    # Log key cache state BEFORE cleanup for debugging
                    valid_dir_key = f"valid_dir:{path}"
                    has_valid_dir = self.cache.get(valid_dir_key)
                    self.logger.debug(
                        f"BEFORE cleanup: valid_dir:{path} exists = {has_valid_dir is not None}"
                    )

                    # Add extra logging to check for possible other causes
                    parent_dir = os.path.dirname(path)
                    parent_valid_files = self.cache.get(f"valid_files:{parent_dir}")
                    parent_base_names = self.cache.get(f"valid_base_names:{parent_dir}")
                    filename = os.path.basename(path)
                    self.logger.debug(f"Parent dir valid files: {parent_valid_files}")
                    self.logger.debug(f"Parent dir base names: {parent_base_names}")
                    self.logger.debug(f"Creating filename: {filename}")

                    # SUPER AGGRESSIVE CLEANUP - Clean ALL cache entries related to this path and search query
                    # First, use our structured cleanup method
                    self.logger.debug(f"STARTING aggressive cleanup for {path}")
                    self.cache.clean_path_metadata(path)

                    # Then, directly target ALL variations of cache keys
                    scope_suffix = f"_library" if scope == "library" else ""
                    filter_suffix = f"_{filter_type}" if filter_type else ""
                    cache_key = f"/search/{search_query}{scope_suffix}{filter_suffix}"
                    processed_cache_key = f"{path}_processed"

                    # Force cleaning EVERY cache key that might interfere
                    cache_keys_to_clear = [
                        valid_dir_key,
                        f"exact_path:{path}",
                        f"valid_path:{parent_dir}/{filename}",
                        cache_key,
                        processed_cache_key,
                        f"filesize:{path}",
                        f"valid_files:{parent_dir}",
                        f"valid_base_names:{parent_dir}",
                        f"valid_path:{parent_dir}",
                        f"last_access_time:getattr:{path}",
                        f"last_access_time:readdir:{path}",
                    ]

                    # Clean all the keys
                    for key in cache_keys_to_clear:
                        self.cache.delete(key)
                        # Also try pattern matching for similar keys
                        self.cache.delete_pattern(f"{key}*")

                    # Also eliminate all last_access_results data
                    with self.last_access_lock:
                        keys_to_remove = []
                        for k in self.last_access_results.keys():
                            if path in k:
                                keys_to_remove.append(k)
                        for k in keys_to_remove:
                            del self.last_access_results[k]

                    # Log key cache state AFTER cleanup for debugging
                    has_valid_dir_after = self.cache.get(valid_dir_key)
                    self.logger.debug(
                        f"AFTER cleanup: valid_dir:{path} exists = {has_valid_dir_after is not None}"
                    )

                    # Log the operation
                    self.logger.info(
                        f"Creating search query directory: {search_query} with scope {scope} and filter {filter_type}"
                    )

                    # Perform the search and cache the results
                    try:
                        # Use a direct, fresh search to ensure we're not getting cached results
                        search_results = self._perform_search(
                            search_query, scope, filter_type
                        )

                        # Set the cache with fresh results
                        self.cache.set(cache_key, search_results)
                        self.logger.debug(
                            f"Search results cached with key: {cache_key}"
                        )

                        # Only now mark the directory as valid - AFTER everything else is set up
                        self._cache_valid_dir(path)
                        self.logger.debug(
                            f"Successfully created and marked directory as valid: {path}"
                        )

                        # Verify the directory is properly marked as valid
                        has_valid_dir_final = self.cache.get(valid_dir_key)
                        self.logger.debug(
                            f"FINAL check: valid_dir:{path} exists = {has_valid_dir_final is not None}"
                        )

                        return 0
                    except Exception as e:
                        self.logger.error(f"Error performing search for mkdir: {e}")
                        raise OSError(errno.EIO, f"Search failed: {str(e)}")
                else:
                    raise OSError(errno.EINVAL, "Invalid search category")

        # Disallow creating directories in other parts of the filesystem
        self.logger.warning(f"Attempt to create directory not in search path: {path}")
        raise OSError(errno.EPERM, "Cannot create directory outside of search paths")

    def rmdir(self, path):
        """Remove a directory from the FUSE filesystem.

        This implementation primarily supports removing search query directories.

        Args:
            path: The directory path to remove

        Returns:
            0 on success
        """
        self.logger.debug(f"rmdir: {path}")

        # Handle search path directories
        if path.startswith("/search/"):
            parts = path.split("/")

            # Don't allow removing the base search directory
            if path == "/search":
                raise OSError(errno.EPERM, "Cannot remove base search directory")

            # /search/library or /search/catalog - can be removed
            if len(parts) == 3 and parts[2] in ["library", "catalog"]:
                self.logger.debug(f"Removing search scope directory: {parts[2]}")
                return 0

            # /search/library/songs, etc. - can be removed
            if len(parts) == 4 and parts[2] in ["library", "catalog"]:
                valid_categories = ["songs", "videos", "albums", "artists", "playlists"]
                if parts[3] in valid_categories:
                    self.logger.debug(f"Removing search category directory: {parts[3]}")
                    return 0
                else:
                    raise OSError(errno.EINVAL, "Invalid search category")

            # /search/library/songs/query - this is a search query, remove from cache
            if len(parts) == 5 and parts[2] in ["library", "catalog"]:
                valid_categories = ["songs", "videos", "albums", "artists", "playlists"]
                if parts[3] in valid_categories:
                    search_query = parts[4]
                    scope = "library" if parts[2] == "library" else None
                    filter_type = parts[3]

                    self.logger.info(f"Removing search query directory: {search_query}")

                    # Generate the cache key
                    scope_suffix = f"_library" if scope == "library" else ""
                    filter_suffix = f"_{filter_type}" if filter_type else ""
                    cache_key = f"/search/{search_query}{scope_suffix}{filter_suffix}"

                    # Delete the processed cache as well
                    processed_cache_key = f"{path}_processed"

                    # Remove from cache
                    self.cache.delete(cache_key)
                    self.cache.delete(processed_cache_key)
                    self.logger.debug(f"Removed search results from cache: {cache_key}")

                    # Clean up path validation caches
                    self.cache.delete(f"valid_dir:{path}")
                    self.cache.delete(f"exact_path:{path}")

                    # Remove from optimized valid paths set
                    self.cache.remove_valid_path(path)

                    return 0
                else:
                    raise OSError(errno.EINVAL, "Invalid search category")

        # Check if directory is empty
        try:
            entries = self.readdir(path, None)
            if len(entries) > 2:  # More than "." and ".."
                raise OSError(errno.ENOTEMPTY, "Directory not empty")
        except Exception as e:
            self.logger.error(f"Error checking if directory is empty: {e}")

        # Disallow removing directories in other parts of the filesystem
        self.logger.warning(f"Attempt to remove directory not in search path: {path}")
        raise OSError(errno.EPERM, "Cannot remove directory outside of search paths")


def mount_ytmusicfs(
    mount_point: str,
    auth_file: str,
    client_id: str,
    client_secret: str,
    foreground: bool = False,
    debug: bool = False,
    cache_dir: Optional[str] = None,
    cache_timeout: int = 2592000,
    max_workers: int = 8,
    browser: Optional[str] = None,
    credentials_file: Optional[str] = None,
    cache_maxsize: int = 10000,
    preload_cache: bool = True,
    request_cooldown: int = 1000,
    logger: Optional[logging.Logger] = None,
) -> None:
    """Mount the YouTube Music filesystem.

    Args:
        mount_point: Directory where the filesystem will be mounted
        auth_file: Path to the OAuth token file
        client_id: OAuth client ID
        client_secret: OAuth client secret
        foreground: Run in the foreground (for debugging)
        debug: Enable debug logging
        cache_dir: Directory to store cache files (default: ~/.cache/ytmusicfs)
        cache_timeout: Cache timeout in seconds (default: 2592000)
        max_workers: Maximum number of worker threads for parallel operations
        browser: Browser to use for cookies
        credentials_file: Path to the client credentials file (default: None)
        cache_maxsize: Maximum number of items to keep in memory cache (default: 10000)
        preload_cache: Whether to preload cache data at startup (default: True)
        request_cooldown: Time in milliseconds between allowed repeated requests to the same path (default: 1000)
        logger: Logger instance to use (default: None, creates a new logger)
    """
    # Set fuse logger to WARNING level to suppress debug messages about unsupported operations
    logging.getLogger("fuse").setLevel(logging.WARNING)

    FUSE(
        YouTubeMusicFS(
            auth_file=auth_file,
            client_id=client_id,
            client_secret=client_secret,
            cache_dir=cache_dir,
            cache_timeout=cache_timeout,
            max_workers=max_workers,
            browser=browser,
            cache_maxsize=cache_maxsize,
            preload_cache=preload_cache,
            request_cooldown=request_cooldown,
            logger=logger,
        ),
        mount_point,
        foreground=foreground,
        nothreads=False,
    )
