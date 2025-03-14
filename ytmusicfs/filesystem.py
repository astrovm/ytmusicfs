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

        # Set the content fetcher in the router to enable the search path handlers
        self.router.set_fetcher(self.fetcher)

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

        # Initialize thread-local storage for per-operation caching
        self._thread_local = threading.local()

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

        # Search routes - focused on library and catalog paths
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

        # Note: The PathRouter now also has a dynamic handler for the categorized search paths
        # registered via the router.set_fetcher(self.fetcher) call above

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

        # For any existing search result paths in cache, make sure they're marked as valid
        search_metadata_keys = self.cache.get_keys_by_pattern("*_search_metadata")
        for key in search_metadata_keys:
            if key.endswith("_search_metadata"):
                # Extract the path from the metadata key
                path = key[:-16]  # Remove "_search_metadata" suffix
                self.logger.debug(f"Pre-validating search path from metadata: {path}")
                # Mark both the directory and parent directories as valid
                self._cache_valid_dir(path)

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

        # NEW: Explicitly handle artist paths with dots as directories
        if path.startswith("/artists/"):
            # Split the path into components
            parts = path.split("/")
            # Check if this is a top-level artist directory (e.g., /artists/t.A.T.u)
            if len(parts) == 3 and parts[1] == "artists":
                # If it doesn't end with .m4a, treat it as a directory
                if not path.lower().endswith(".m4a"):
                    self.logger.debug(f"Recognized {path} as a valid artist directory")
                    return True
            # For deeper paths under artists (e.g., /artists/t.A.T.u/Albums), rely on cache
            parent_dir = os.path.dirname(path)
            filename = os.path.basename(path)
            if self.cache.is_valid_path(parent_dir):
                # If no .m4a extension, assume it's a directory
                if not path.lower().endswith(".m4a"):
                    return True
                # For files, check directory listing
                dir_listing = self.cache.get_directory_listing_with_attrs(parent_dir)
                if dir_listing and filename in dir_listing:
                    return True

        # Explicitly validate subdirectories under /albums, /artists, and /playlists
        if path.startswith(("/albums/", "/artists/", "/playlists/")):
            parent_dir = os.path.dirname(path)
            filename = os.path.basename(path)

            # If the parent dir is one of our root dirs, assume directory is valid
            # This handles paths like /albums/AlbumName
            if parent_dir in ["/albums", "/artists", "/playlists"]:
                if "." not in filename or (
                    path.startswith("/artists/") and not path.lower().endswith(".m4a")
                ):  # Modified to handle artist directories with dots
                    return True

            # For subdirectories deeper than first level
            # First, check if parent directory is valid
            if self.cache.is_valid_path(parent_dir):
                # For directories (no extension or artist directory with dots), assume valid if parent is valid
                if "." not in filename or (
                    path.startswith("/artists/") and not path.lower().endswith(".m4a")
                ):  # Modified to handle artist directories with dots
                    return True

                # For files, check the directory listing if available
                dir_listing = self.cache.get_directory_listing_with_attrs(parent_dir)
                if dir_listing and filename in dir_listing:
                    return True

        # Initialize thread-local validation cache if needed
        if not hasattr(self._thread_local, "validated_dirs"):
            self._thread_local.validated_dirs = set()

        # Check if this path's parent directory has already been validated in this operation
        parent_dir = os.path.dirname(path)
        if parent_dir in self._thread_local.validated_dirs:
            # If parent is validated and we have the directory listing cached,
            # we can directly check if the file exists in the listing
            if (
                hasattr(self._thread_local, "temp_dir_listings")
                and parent_dir in self._thread_local.temp_dir_listings
            ):

                filename = os.path.basename(path)
                dir_listing = self._thread_local.temp_dir_listings[parent_dir]

                # If this is a directory, it's always valid if parent is valid
                if "." not in filename or (
                    path.startswith("/artists/") and not path.lower().endswith(".m4a")
                ):  # Modified to handle artist directories with dots
                    return True

                # For files, check the cached listing
                if dir_listing and filename in dir_listing:
                    return True

                # File not found in valid parent directory
                return False

            # Parent is valid but we don't have listing cached
            # For directories, assume valid
            if "." not in os.path.basename(path) or (
                path.startswith("/artists/") and not path.lower().endswith(".m4a")
            ):  # Modified to handle artist directories with dots
                return True

        # Search category paths are valid - simplified logic
        if path.startswith("/search/library/") or path.startswith("/search/catalog/"):
            parts = path.split("/")

            # Category paths like /search/library/songs
            if len(parts) == 4:
                valid_categories = ["songs", "videos", "albums", "artists", "playlists"]
                if parts[3] in valid_categories:
                    # Mark this directory as validated for future checks
                    self._thread_local.validated_dirs.add(path)
                    return True

            # Search query paths - simplified
            if len(parts) >= 5:
                valid_categories = ["songs", "videos", "albums", "artists", "playlists"]
                if parts[3] in valid_categories:
                    # For getattr, consider it potentially valid to allow further validation
                    if context == "getattr":
                        return True
                    # For other operations, rely on cache
                    is_valid = self.cache.is_valid_path(path)
                    if is_valid and "." not in os.path.basename(path):
                        # Mark directories as validated for future checks
                        self._thread_local.validated_dirs.add(path)
                    return is_valid

        # Use thread-local storage to cache directory listings during a single operation
        if not hasattr(self._thread_local, "temp_dir_listings"):
            self._thread_local.temp_dir_listings = {}

        # For file paths, check if they exist in parent directory's listings with attributes
        if "." in path and path.lower().endswith(".m4a"):
            # First check the thread-local cache to avoid repeated lookups
            if parent_dir in self._thread_local.temp_dir_listings:
                dir_listing = self._thread_local.temp_dir_listings[parent_dir]
                filename = os.path.basename(path)
                if dir_listing and filename in dir_listing:
                    # Mark the parent directory as validated
                    self._thread_local.validated_dirs.add(parent_dir)
                    return True
            else:
                # Get from main cache and store in thread-local for quick access
                dir_listing = self.cache.get_directory_listing_with_attrs(parent_dir)
                if dir_listing:
                    self._thread_local.temp_dir_listings[parent_dir] = dir_listing
                    filename = os.path.basename(path)
                    if filename in dir_listing:
                        # Mark the parent directory as validated
                        self._thread_local.validated_dirs.add(parent_dir)
                        return True

        # Final fallback to cache
        is_valid = self.cache.is_valid_path(path)
        if is_valid and (
            "." not in os.path.basename(path)
            or (path.startswith("/artists/") and not path.lower().endswith(".m4a"))
        ):  # Modified to handle artist directories with dots
            self._thread_local.validated_dirs.add(path)
        return is_valid

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
        now = time.time()
        listing_with_attrs = {}
        valid_filenames = set()

        for track in processed_tracks:
            filename = track.get("filename")
            if not filename:
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
                self.cache.add_valid_dir(f"{dir_path}/{filename}")
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
            self.cache.add_valid_path(file_path, is_directory=is_directory)

            # If it's a file, handle video ID to duration mapping if available
            if not is_directory:
                video_id = track.get("videoId")
                if video_id and track.get("duration_seconds"):
                    self.cache.set_duration(video_id, track.get("duration_seconds"))

        # Cache the directory listing with attributes
        self.cache.set_directory_listing_with_attrs(dir_path, listing_with_attrs)

        # Also cache the filenames separately for backward compatibility
        self.cache.set(f"valid_files:{dir_path}", list(valid_filenames))

        # Mark this directory as valid
        self._cache_valid_dir(dir_path)

        # Pre-populate thread-local caches if they exist
        # This helps maintain optimizations across operations
        if hasattr(self._thread_local, "temp_dir_listings"):
            self._thread_local.temp_dir_listings[dir_path] = listing_with_attrs

        if hasattr(self._thread_local, "validated_dirs"):
            self._thread_local.validated_dirs.add(dir_path)

        if hasattr(self._thread_local, "validated_paths"):
            for filename in valid_filenames:
                file_path = f"{dir_path}/{filename}"
                self._thread_local.validated_paths[file_path] = True

    def readdir(self, path: str, fh: Optional[int] = None) -> List[str]:
        """Read directory contents.

        Args:
            path: Directory path
            fh: File handle (unused)

        Returns:
            List of directory entries
        """
        self.logger.debug(f"readdir: {path}")

        # Clear thread-local cache at the beginning of a new directory operation
        self._clear_thread_local_cache()

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
                # Pre-fetch the directory listing with attributes and store in thread-local cache
                dir_listing = self.cache.get_directory_listing_with_attrs(path)
                if dir_listing:
                    # Store in thread-local cache for subsequent getattr calls
                    if not hasattr(self._thread_local, "temp_dir_listings"):
                        self._thread_local.temp_dir_listings = {}
                    self._thread_local.temp_dir_listings[path] = dir_listing

                    # Mark this directory as validated
                    if not hasattr(self._thread_local, "validated_dirs"):
                        self._thread_local.validated_dirs = set()
                    self._thread_local.validated_dirs.add(path)

                    # Pre-validate all files in this directory
                    if not hasattr(self._thread_local, "validated_paths"):
                        self._thread_local.validated_paths = {}

                    for filename in result:
                        if filename not in [".", ".."]:
                            file_path = f"{path}/{filename}"
                            self._thread_local.validated_paths[file_path] = True

                # Continue with normal caching
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

    def _get_video_id(self, path: str) -> str:
        """Helper to extract videoId from a path.

        Args:
            path: The file path

        Returns:
            Video ID for the file

        Raises:
            OSError: If the video ID could not be found
        """
        # Create a thread-local cache for video IDs if it doesn't exist
        if not hasattr(self._thread_local, "video_id_cache"):
            self._thread_local.video_id_cache = {}

        # Check if we already have the video ID for this path in thread-local cache
        if path in self._thread_local.video_id_cache:
            return self._thread_local.video_id_cache[path]

        dir_path = os.path.dirname(path)
        filename = os.path.basename(path)

        # Check if we have the directory listing with file attributes in thread-local cache
        if (
            hasattr(self._thread_local, "temp_dir_listings")
            and dir_path in self._thread_local.temp_dir_listings
        ):

            dir_listing = self._thread_local.temp_dir_listings[dir_path]
            # The listing maps filenames to attributes - we need to find the song data

            # Try to find the song data to extract the video ID
            if dir_path == "/liked_songs":
                songs = self.cache.get("/liked_songs_processed")
                if songs:
                    for song in songs:
                        if isinstance(song, dict) and song.get("filename") == filename:
                            video_id = song.get("videoId")
                            if video_id:
                                # Cache for future use
                                self._thread_local.video_id_cache[path] = video_id
                                return video_id
            elif dir_path.startswith("/playlists/"):
                # For playlists, we need to find the playlist ID first
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
                        if songs:
                            for song in songs:
                                if (
                                    isinstance(song, dict)
                                    and song.get("filename") == filename
                                ):
                                    video_id = song.get("videoId")
                                    if video_id:
                                        # Cache for future use
                                        self._thread_local.video_id_cache[path] = (
                                            video_id
                                        )
                                        return video_id
            else:
                # For other directories (like artists or albums)
                songs = self.cache.get(dir_path)
                if songs:
                    for song in songs:
                        if isinstance(song, dict) and song.get("filename") == filename:
                            video_id = song.get("videoId")
                            if video_id:
                                # Cache for future use
                                self._thread_local.video_id_cache[path] = video_id
                                return video_id

        # If we didn't find it in thread-local cache, use the original implementation
        # Determine where to look for the song data
        songs = None
        if dir_path == "/liked_songs":
            songs = self.cache.get("/liked_songs_processed")
        elif dir_path.startswith("/playlists/"):
            # Special handling for playlists which need the playlist ID
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
        else:
            songs = self.cache.get(dir_path)

        # Try to fetch directory contents if not in cache
        if not songs:
            self.logger.debug(f"Songs not in cache for {dir_path}, attempting to fetch")
            self.readdir(dir_path, None)

            # Try again after refetching
            if dir_path == "/liked_songs":
                songs = self.cache.get("/liked_songs_processed")
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
            else:
                songs = self.cache.get(dir_path)

        # Find the video ID in the songs list
        if songs:
            for song in songs:
                if isinstance(song, dict) and song.get("filename") == filename:
                    video_id = song.get("videoId")
                    if video_id:
                        # Cache for future use
                        self._thread_local.video_id_cache[path] = video_id
                        return video_id

        self.logger.error(f"Could not find video ID for {filename} in {dir_path}")
        raise OSError(errno.ENOENT, "Video ID not found")

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

        # Get current user's UID and GID
        uid = os.getuid()
        gid = os.getgid()

        now = time.time()
        attr = {
            "st_atime": now,
            "st_ctime": now,
            "st_mtime": now,
            "st_nlink": 2,
            "st_uid": uid,  # Set to current user
            "st_gid": gid,  # Set to current group
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

        # NEW: Handle artist directories with dots
        if path.startswith("/artists/"):
            parts = path.split("/")
            # Top-level artist directory (e.g., /artists/t.A.T.u)
            if len(parts) == 3 and parts[1] == "artists":
                if not path.lower().endswith(".m4a"):  # Ensure it's not a file
                    attr["st_mode"] = stat.S_IFDIR | 0o755
                    attr["st_size"] = 0
                    with self.last_access_lock:
                        self.last_access_results[operation_key] = attr.copy()
                    self.logger.debug(
                        f"Returning directory attributes for artist: {path}"
                    )
                    return attr
            # Subdirectories under artists (e.g., /artists/t.A.T.u/Albums)
            elif len(parts) > 3:
                parent_dir = os.path.dirname(path)
                if self._is_valid_path(parent_dir):
                    filename = os.path.basename(path)
                    if not path.lower().endswith(".m4a"):
                        attr["st_mode"] = stat.S_IFDIR | 0o755
                        attr["st_size"] = 0
                        with self.last_access_lock:
                            self.last_access_results[operation_key] = attr.copy()
                        return attr

        # Handle search path directories
        if path.startswith("/search/"):
            parts = path.split("/")

            # Basic search paths like /search/library or /search/catalog
            if len(parts) == 3 and parts[2] in ["library", "catalog"]:
                attr["st_mode"] = stat.S_IFDIR | 0o755
                attr["st_size"] = 0
                with self.last_access_lock:
                    self.last_access_results[operation_key] = attr.copy()
                return attr

            # Search category paths like /search/library/songs
            if len(parts) == 4 and parts[2] in ["library", "catalog"]:
                valid_categories = ["songs", "videos", "albums", "artists", "playlists"]
                if parts[3] in valid_categories:
                    attr["st_mode"] = stat.S_IFDIR | 0o755
                    attr["st_size"] = 0
                    with self.last_access_lock:
                        self.last_access_results[operation_key] = attr.copy()
                    return attr

            if len(parts) >= 5 and parts[2] in ["library", "catalog"]:
                # Check for search query directories (like /search/library/songs/query)
                if len(parts) == 5:
                    # Only report existing if it's in the cache or if mkdir is in progress
                    if self.cache.is_valid_path(path) or is_mkdir_context:
                        attr["st_mode"] = stat.S_IFDIR | 0o755
                        attr["st_size"] = 0
                        with self.last_access_lock:
                            self.last_access_results[operation_key] = attr.copy()
                        return attr

                # Check for categorized search results (like /search/library/songs/query/top_album)
                if len(parts) == 6:
                    if self.cache.is_valid_path(path):
                        attr["st_mode"] = stat.S_IFDIR | 0o755
                        attr["st_size"] = 0
                        with self.last_access_lock:
                            self.last_access_results[operation_key] = attr.copy()
                        return attr

        # Initialize thread-local path validation cache if needed
        if not hasattr(self._thread_local, "validated_paths"):
            self._thread_local.validated_paths = {}

        # Check thread-local validation cache first
        if path in self._thread_local.validated_paths:
            if self._thread_local.validated_paths[path] is False:
                # We've already determined this path is invalid
                raise OSError(errno.ENOENT, "No such file or directory")

        # Check for regular files with .m4a extension
        if path.lower().endswith(".m4a"):
            parent_dir = os.path.dirname(path)
            filename = os.path.basename(path)

            # Fast path: if we have already validated the parent directory and have
            # the directory listing in thread-local cache, we can skip the validity check
            if (
                hasattr(self._thread_local, "validated_dirs")
                and parent_dir in self._thread_local.validated_dirs
                and hasattr(self._thread_local, "temp_dir_listings")
                and parent_dir in self._thread_local.temp_dir_listings
            ):

                dir_listing = self._thread_local.temp_dir_listings[parent_dir]
                if dir_listing and filename in dir_listing:
                    # Use the cached attributes directly
                    self.logger.debug(
                        f"Fast path: using validated directory for {path}"
                    )
                    cached_attrs = dir_listing[filename].copy()

                    # Update timestamps and check for file size updates
                    cached_attrs["st_atime"] = now
                    cached_attrs["st_ctime"] = now
                    cached_attrs["st_mtime"] = now

                    file_size_cache_key = f"filesize:{path}"
                    cached_size = self.cache.get(file_size_cache_key)
                    if cached_size is not None:
                        cached_attrs["st_size"] = cached_size

                    with self.last_access_lock:
                        self.last_access_results[operation_key] = cached_attrs.copy()

                    # Cache validation result
                    self._thread_local.validated_paths[path] = True

                    return cached_attrs
                else:
                    # File not found in validated directory
                    self._thread_local.validated_paths[path] = False
                    raise OSError(errno.ENOENT, "No such file or directory")

            # Otherwise fallback to the normal path validation
            if not self._is_valid_path(path):
                self.logger.debug(f"Invalid path in getattr: {path}")
                self._thread_local.validated_paths[path] = False
                raise OSError(errno.ENOENT, "No such file or directory")

            # Mark this path as valid for future reference
            self._thread_local.validated_paths[path] = True

            # Use thread-local cached directory listing if available
            if (
                hasattr(self._thread_local, "temp_dir_listings")
                and parent_dir in self._thread_local.temp_dir_listings
            ):
                dir_listing = self._thread_local.temp_dir_listings[parent_dir]
                if dir_listing and filename in dir_listing:
                    self.logger.debug(
                        f"Using thread-local cached attributes for {path}"
                    )
                    cached_attrs = dir_listing[filename].copy()

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

            # If not in thread-local cache, try to get from main cache
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

                # Store in thread-local cache for future lookups during this operation
                if not hasattr(self._thread_local, "temp_dir_listings"):
                    self._thread_local.temp_dir_listings = {}

                if parent_dir not in self._thread_local.temp_dir_listings:
                    # Retrieve and store the entire directory listing
                    dir_listing = self.cache.get_directory_listing_with_attrs(
                        parent_dir
                    )
                    if dir_listing:
                        self._thread_local.temp_dir_listings[parent_dir] = dir_listing

                with self.last_access_lock:
                    self.last_access_results[operation_key] = cached_attrs.copy()
                return cached_attrs

            # If no cached attributes, we'll try to get video_id and check for duration
            try:
                # Get the video ID
                video_id = self._get_video_id(path)

                # Check if we already have cached duration
                duration = self.cache.get_duration(video_id)

                if duration is not None:
                    # Estimate file size based on duration (128kbps = 16KB/sec)
                    estimated_size = duration * 16 * 1024
                else:
                    # Default size if we don't know duration
                    estimated_size = 4096

                # Create basic file attributes
                attr["st_mode"] = stat.S_IFREG | 0o644
                attr["st_size"] = estimated_size
                attr["st_nlink"] = 1

                with self.last_access_lock:
                    self.last_access_results[operation_key] = attr.copy()

                return attr
            except Exception as e:
                self.logger.error(f"Error getting file attributes for {path}: {e}")
                raise OSError(errno.ENOENT, f"No such file or directory: {str(e)}")

        # Regular directory
        else:
            # Quick check if the path is valid
            if not self._is_valid_path(path):
                self.logger.debug(f"Invalid directory path in getattr: {path}")
                self._thread_local.validated_paths[path] = False
                raise OSError(errno.ENOENT, "No such file or directory")

            # Mark this path as valid for future reference
            self._thread_local.validated_paths[path] = True

            # Directory attributes
            attr["st_mode"] = stat.S_IFDIR | 0o755
            attr["st_size"] = 0

            with self.last_access_lock:
                self.last_access_results[operation_key] = attr.copy()

            return attr

    def open(self, path: str, flags: int) -> int:
        """Open a file.

        Args:
            path: The file path
            flags: Open flags

        Returns:
            File handle
        """
        self.logger.debug(f"open: {path} with flags {flags}")

        # Skip validation if we know the path is valid from thread-local cache
        valid_from_cache = False
        if (
            hasattr(self._thread_local, "validated_paths")
            and path in self._thread_local.validated_paths
        ):
            valid_from_cache = self._thread_local.validated_paths[path]

        if not valid_from_cache and not self._is_valid_path(path):
            self.logger.warning(f"Rejecting invalid path in open: {path}")
            raise OSError(errno.ENOENT, "No such file or directory")

        try:
            # Get video ID
            video_id = self._get_video_id(path)

            # Delegate to file handler
            fh = self.file_handler.open(path, video_id, self.thread_pool)
            return fh
        except Exception as e:
            self.logger.error(f"Error opening file {path}: {e}")
            raise OSError(errno.EIO, str(e))

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
        """Release (close) a file.

        Args:
            path: The file path
            fh: File handle

        Returns:
            0 on success
        """
        self.logger.debug(f"release: {path} (fh={fh})")

        # Clear thread-local cache to prevent memory leaks
        self._clear_thread_local_cache()

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

        # Also delete search metadata entries
        self.cache.delete_pattern("*_search_metadata")
        self.logger.debug("Deleted search metadata entries")

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
        """Create a directory.

        Args:
            path: The directory path
            mode: Directory permissions (unused)

        Returns:
            0 on success
        """
        self.logger.debug(f"mkdir: {path}")

        # Only allow creating directories under the /search path
        if not path.startswith("/search/"):
            self.logger.warning(f"Rejecting mkdir for non-search path: {path}")
            raise OSError(errno.EPERM, "Can only create directories under /search")

        # Validate the search path structure
        parts = path.split("/")
        if len(parts) < 4 or parts[2] not in ["library", "catalog"]:
            self.logger.warning(f"Invalid search path structure for mkdir: {path}")
            raise OSError(
                errno.EINVAL,
                "Invalid search path format (must be /search/{library|catalog}/...)",
            )

        # For /search/{library|catalog}/{songs|videos|albums|artists|playlists}
        if len(parts) == 4:
            valid_categories = ["songs", "videos", "albums", "artists", "playlists"]
            if parts[3] not in valid_categories:
                self.logger.warning(f"Invalid search category for mkdir: {parts[3]}")
                raise OSError(
                    errno.EINVAL,
                    f"Invalid search category (must be one of {', '.join(valid_categories)})",
                )
            # These directories already exist virtually
            self._cache_valid_dir(path)
            # Clear thread-local cache
            self._clear_thread_local_cache()
            return 0

        # For /search/{library|catalog}/{songs|videos|albums|artists|playlists}/{query}
        if len(parts) == 5:
            valid_categories = ["songs", "videos", "albums", "artists", "playlists"]
            if parts[3] not in valid_categories:
                self.logger.warning(f"Invalid search category for mkdir: {parts[3]}")
                raise OSError(
                    errno.EINVAL,
                    f"Invalid search category (must be one of {', '.join(valid_categories)})",
                )

            # This is a search query directory - perform the search
            search_query = parts[4]
            scope = parts[2]  # 'library' or 'catalog'
            filter_type = parts[3]  # category

            # Store search metadata for later use
            self.cache.store_search_metadata(path, search_query, scope, filter_type)

            try:
                # Perform the search
                self._perform_search(search_query, scope, filter_type)
                # Mark as valid
                self._cache_valid_dir(path)
                # Clear thread-local cache
                self._clear_thread_local_cache()
                return 0
            except Exception as e:
                self.logger.error(f"Search failed for {path}: {e}")
                raise OSError(errno.EIO, f"Search failed: {str(e)}")

        self.logger.warning(f"Rejecting mkdir for unsupported path depth: {path}")
        raise OSError(
            errno.EINVAL, "Can only create directories at the category or query level"
        )

    def rmdir(self, path):
        """Remove a directory.

        Args:
            path: The directory path

        Returns:
            0 on success
        """
        self.logger.debug(f"rmdir: {path}")

        # Only allow removing directories under the /search path
        if not path.startswith("/search/"):
            self.logger.warning(f"Rejecting rmdir for non-search path: {path}")
            raise OSError(errno.EPERM, "Can only remove directories under /search")

        # Validate the search path structure
        parts = path.split("/")
        if len(parts) < 4 or parts[2] not in ["library", "catalog"]:
            self.logger.warning(f"Invalid search path structure for rmdir: {path}")
            raise OSError(
                errno.EINVAL,
                "Invalid search path format (must be /search/{library|catalog}/...)",
            )

        # For /search/{library|catalog}/{songs|videos|albums|artists|playlists}
        if len(parts) == 4:
            valid_categories = ["songs", "videos", "albums", "artists", "playlists"]
            if parts[3] not in valid_categories:
                self.logger.warning(f"Invalid search category for rmdir: {parts[3]}")
                raise OSError(
                    errno.EINVAL,
                    f"Invalid search category (must be one of {', '.join(valid_categories)})",
                )
            # These directories are virtual and permanent
            self.logger.warning(f"Cannot remove category directory: {path}")
            raise OSError(errno.EPERM, "Cannot remove category directories")

        # For /search/{library|catalog}/{songs|videos|albums|artists|playlists}/{query}
        if len(parts) == 5:
            valid_categories = ["songs", "videos", "albums", "artists", "playlists"]
            if parts[3] not in valid_categories:
                self.logger.warning(f"Invalid search category for rmdir: {parts[3]}")
                raise OSError(
                    errno.EINVAL,
                    f"Invalid search category (must be one of {', '.join(valid_categories)})",
                )

            # This is a search query directory - check if it's a valid path
            if not self.cache.is_valid_path(path):
                self.logger.warning(f"Cannot remove non-existent directory: {path}")
                raise OSError(errno.ENOENT, "No such directory")

            # Find all child paths in the cache that start with this path
            search_pattern = f"{path}/*"
            child_keys = self.cache.get_keys_by_pattern(search_pattern)

            # Clear the child paths from cache
            for key in child_keys:
                self.cache.delete(key)

            # Remove the directory itself from cache
            self.cache.remove_valid_path(path)
            self.cache.delete(path)

            # Clean up associated metadata
            self.cache.delete(f"{path}_listing_with_attrs")
            self.cache.delete(f"valid_files:{path}")

            # Clear thread-local cache
            self._clear_thread_local_cache()

            return 0

        self.logger.warning(f"Rejecting rmdir for unsupported path depth: {path}")
        raise OSError(errno.EPERM, "Cannot remove directory outside of search paths")

    def _clear_thread_local_cache(self):
        """Clear the thread-local cache to prevent memory leaks.
        This should be called at the end of major operations.
        """
        # Clear all thread-local caches
        if hasattr(self._thread_local, "temp_dir_listings"):
            self._thread_local.temp_dir_listings = {}
        if hasattr(self._thread_local, "validated_dirs"):
            self._thread_local.validated_dirs = set()
        if hasattr(self._thread_local, "validated_paths"):
            self._thread_local.validated_paths = {}
        if hasattr(self._thread_local, "video_id_cache"):
            self._thread_local.video_id_cache = {}

    def __call__(self, op, *args):
        """Override the __call__ method to ensure thread-local cache cleanup.

        This method is called by FUSE for each filesystem operation.
        """
        try:
            # Call the parent class implementation
            return super().__call__(op, *args)
        finally:
            # Clean up thread-local cache after every operation to prevent memory leaks
            self._clear_thread_local_cache()


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
        **fuse_options,
    )
