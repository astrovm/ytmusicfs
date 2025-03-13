#!/usr/bin/env python3

from concurrent.futures import ThreadPoolExecutor
from fuse import FUSE, Operations
from typing import Dict, Any, Optional, List
from ytmusicfs.cache import CacheManager
from ytmusicfs.client import YouTubeMusicClient
from ytmusicfs.content_fetcher import ContentFetcher
from ytmusicfs.file_handler import FileHandler
from ytmusicfs.path_router import PathRouter
from ytmusicfs.processor import TrackProcessor
from ytmusicfs.oauth_adapter import YTMusicOAuthAdapter
import errno
import inspect
import logging
import os
import re
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
        credentials_file: Optional[str] = None,
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
            credentials_file: Path to the client credentials file (default: None)
        """
        # Get or create the logger
        self.logger = logger or logging.getLogger("YTMusicFS")

        # Initialize the OAuth adapter first
        oauth_adapter = YTMusicOAuthAdapter(
            auth_file=auth_file,
            client_id=client_id,
            client_secret=client_secret,
            browser=browser,
            logger=self.logger,
            credentials_file=credentials_file,
        )

        # Initialize the client component with the OAuth adapter
        self.client = YouTubeMusicClient(
            oauth_adapter=oauth_adapter,
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

        # Debounce mechanism for repeated requests
        self.last_access_time = {}  # {operation_path: last_access_time}
        self.last_access_lock = (
            threading.RLock()
        )  # Lock for last_access_time operations
        self.last_access_results = {}  # {operation_path: cached_result}

        # Thread-related objects
        self.thread_pool = ThreadPoolExecutor(max_workers=max_workers)
        self.logger.info(f"Thread pool initialized with {max_workers} workers")

        # Initialize the file handler component
        self.file_handler = FileHandler(
            cache_dir=self.cache.cache_dir,
            browser=browser,
            cache=self.cache,
            logger=self.logger,
            update_file_size_callback=self._update_file_size,
        )

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

        # Ignore hidden paths
        if any(part.startswith(".") for part in path.split("/") if part):
            self.logger.debug(f"Ignoring hidden path: {path}")
            result = [".", ".."]
            with self.last_access_lock:
                self.last_access_results[operation_key] = result
            return result

        # Use the router to handle the path
        try:
            # Check if path is valid (simplified validation)
            if not self._is_valid_path(path):
                self.logger.debug(f"Rejecting invalid path in readdir: {path}")
                result = [".", ".."]
                with self.last_access_lock:
                    self.last_access_results[operation_key] = result
                return result

            # Delegate directory listing to the router
            result = self.router.route(path)

            # Mark this as a valid directory and cache all valid filenames for future validation
            if path != "/" and len(result) > 2:  # More than just "." and ".."
                self._cache_valid_filenames(
                    path, [entry for entry in result if entry not in [".", ".."]]
                )
                self._cache_valid_dir(path)

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

        # Check context for operations like mkdir that need to check existence
        context = (
            inspect.currentframe().f_back.f_code.co_name
            if inspect.currentframe().f_back
            else "unknown"
        )
        is_mkdir_context = context == "mkdir" or "mkdir" in context

        # For mkdir operations on search paths, report path doesn't exist to allow creation
        if is_mkdir_context and path.startswith("/search/"):
            parts = path.split("/")
            if len(parts) == 5 and parts[2] in ["library", "catalog"]:
                valid_categories = ["songs", "videos", "albums", "artists", "playlists"]
                if parts[3] in valid_categories:
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

        # Handle root and standard top-level directories
        if path == "/" or path in [
            "/playlists",
            "/liked_songs",
            "/artists",
            "/albums",
            "/search",
        ]:
            attr["st_mode"] = stat.S_IFDIR | 0o755
            attr["st_size"] = 0
            with self.last_access_lock:
                self.last_access_results[operation_key] = attr.copy()
            return attr

        # Handle search path directories
        if path in ["/search/library", "/search/catalog"]:
            attr["st_mode"] = stat.S_IFDIR | 0o755
            attr["st_size"] = 0
            with self.last_access_lock:
                self.last_access_results[operation_key] = attr.copy()
            return attr

        # Handle search category paths
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

            # Search query directories during mkdir operations
            if len(parts) == 5 and is_mkdir_context:
                self.logger.debug(
                    f"mkdir context detected for {path}, reporting non-existence"
                )
                raise OSError(errno.ENOENT, "No such file or directory")

            # Check if this is a valid/existing search directory
            if len(parts) == 5 and self.cache.get(f"valid_dir:{path}"):
                attr["st_mode"] = stat.S_IFDIR | 0o755
                attr["st_size"] = 0
                with self.last_access_lock:
                    self.last_access_results[operation_key] = attr.copy()
                return attr

        # Check cached attributes for files
        if path.lower().endswith(".m4a"):
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

            # If no cached attributes, check for cached file size
            file_size_cache_key = f"filesize:{path}"
            cached_size = self.cache.get(file_size_cache_key)
            if cached_size is not None:
                attr["st_mode"] = stat.S_IFREG | 0o644
                attr["st_size"] = cached_size

                # Update the cached attributes in parent dir for future lookups
                self.cache.update_file_attrs_in_parent_dir(path, attr)

                with self.last_access_lock:
                    self.last_access_results[operation_key] = attr.copy()
                return attr

        # Try to check if path is a directory by listing contents
        try:
            dir_path = path[:-1] if path.endswith("/") else path
            entries = self.readdir(dir_path, None)
            if entries and len(entries) > 0:
                attr["st_mode"] = stat.S_IFDIR | 0o755
                attr["st_size"] = 0
                with self.last_access_lock:
                    self.last_access_results[operation_key] = attr.copy()
                return attr
        except Exception:
            pass

        # Check if path is a valid file by examining parent directory
        parent_dir = os.path.dirname(path)
        filename = os.path.basename(path)

        try:
            dirlist = self.readdir(parent_dir, None)
            if filename in dirlist:
                attr["st_mode"] = stat.S_IFREG | 0o644
                attr["st_size"] = 4096  # Default size

                # Cache the path for future validation
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

        # Delegate to file handler
        return self.file_handler.open(path, video_id, self.thread_pool)

    def read(self, path: str, size: int, offset: int, fh: int) -> bytes:
        """Read data from a file.

        Args:
            path: The file path
            size: Number of bytes to read
            offset: Offset to start reading from
            fh: File handle

        Returns:
            The requested bytes
        """
        # Delegate to file handler
        return self.file_handler.read(path, size, offset, fh)

    def release(self, path: str, fh: int) -> int:
        """Release (close) a file handle.

        Args:
            path: The file path
            fh: File handle

        Returns:
            0 on success
        """
        # Delegate to file handler
        return self.file_handler.release(path, fh)

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

        # Delete all search-related cache entries
        self.cache.delete_pattern("/search/*")
        self.cache.delete_pattern("/search/*/*_processed")

        # Mark all caches as freshly refreshed
        current_time = time.time()
        for cache_key in ["/liked_songs", "/playlists", "/artists", "/albums"]:
            self.cache.set_metadata(cache_key, "last_refresh_time", current_time)

        self.logger.info("All caches refreshed successfully")

    def _perform_search(self, search_query, scope, filter_type):
        """Perform a search operation.

        Args:
            search_query: Query string to search for
            scope: Search scope (library or None for all)
            filter_type: Type of items to filter for (songs, albums, etc.)

        Returns:
            Search results
        """
        # Delegate to ContentFetcher for search operations
        return self.fetcher._perform_search(search_query, scope, filter_type)

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
            credentials_file=credentials_file,
        ),
        mount_point,
        foreground=foreground,
        nothreads=False,
    )
