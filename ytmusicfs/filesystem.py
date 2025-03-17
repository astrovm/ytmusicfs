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

        # Set the content fetcher in the router
        self.router.set_fetcher(self.fetcher)

        # Also explicitly set the cache manager
        self.router.set_cache(self.cache)

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
                next(
                    p["id"]
                    for p in self.fetcher.cache.get("/playlists", [])
                    if p["name"] == playlist_name
                ),
                path,
            ),
        )
        self.router.register_dynamic(
            "/albums/*",
            lambda path, album_name: [".", ".."]
            + self.fetcher.fetch_playlist_content(
                next(
                    a["id"]
                    for a in self.fetcher.cache.get("/albums", [])
                    if a["name"] == album_name
                ),
                path,
            ),
        )

        # Initialize path validation with common static paths
        self.cache.mark_valid("/", is_directory=True)
        self.cache.mark_valid("/playlists", is_directory=True)
        self.cache.mark_valid("/liked_songs", is_directory=True)
        self.cache.mark_valid("/albums", is_directory=True)

        # Preload cache if requested
        if preload_cache:
            self.preload_cache()

    def preload_cache(self) -> None:
        """Preload common API data to improve initial filesystem performance."""
        self.logger.info("Preloading cache...")

        # Use thread pool to preload caches concurrently
        futures = []
        futures.append(self.thread_pool.submit(self.fetcher.readdir_playlists))
        futures.append(self.thread_pool.submit(self.fetcher.readdir_liked_songs))
        futures.append(self.thread_pool.submit(self.fetcher.readdir_albums))

        # Wait for all preload tasks to complete
        for future in futures:
            try:
                future.result()
            except Exception as e:
                self.logger.error(f"Error during cache preloading: {e}")

        # Optimize path validation by caching known valid paths
        self.optimize_path_validation()

        self.logger.info("Cache preloading completed")

    def optimize_path_validation(self):
        """Optimize path validation by pre-caching known valid paths.

        This method creates a set of known valid paths to avoid repeated validation
        and improve performance. Static paths that are always valid are checked directly
        in the _is_valid_path method and don't need caching.
        """
        self.logger.info("Optimizing path validation...")

        # Cache the valid filenames for root
        self._cache_valid_filenames("/", ["playlists", "liked_songs", "albums"])

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

        # Use the enhanced validation in the cache manager
        return self.cache.is_valid_path(path, context=context)

    def _cache_valid_path(self, path: str) -> None:
        """Cache a valid path to help with quick validation.

        Args:
            path: The valid path to cache
        """
        # Use the enhanced mark_valid method in the cache manager
        self.cache.mark_valid(path, is_directory=False)

        # Handle special filename pattern for base names if needed
        dir_path = os.path.dirname(path)
        filename = os.path.basename(path)

        match = re.search(r"(.*?)\s*\((\d+)\)(\.[^.]+)?$", filename)
        if match:
            base_name, _, _ = match.groups()
            # Store the base name in metadata
            self.cache.set_metadata(path, "base_name", base_name)
            # Also store in the old format for backward compatibility
            cache_key = f"valid_path:{dir_path}/{base_name}"
            self.cache.set(cache_key, path)

            # Add this base name to valid base names for this directory (for backward compatibility)
            valid_base_names_key = f"valid_base_names:{dir_path}"
            valid_base_names_cached = self.cache.get(valid_base_names_key)
            valid_base_names = (
                set(valid_base_names_cached) if valid_base_names_cached else set()
            )
            valid_base_names.add(base_name)
            self.cache.set(valid_base_names_key, list(valid_base_names))

            self.logger.debug(f"Cached valid path: {path} with key {cache_key}")

    def _cache_valid_dir(self, dir_path: str) -> None:
        """Mark a directory as valid in the cache.

        Args:
            dir_path: The directory path to cache as valid
        """
        # Use the enhanced mark_valid method in the cache manager
        self.cache.mark_valid(dir_path, is_directory=True)

    def _cache_valid_filenames(self, dir_path: str, filenames: List[str]) -> None:
        """Cache all valid filenames in a directory for efficient path validation.

        Args:
            dir_path: Directory path
            filenames: List of valid filenames in this directory
        """
        # Mark the directory as valid
        self.cache.mark_valid(dir_path, is_directory=True)

        # Store the complete list of valid files - already a list, so no conversion needed
        self.cache.set(f"valid_files:{dir_path}", filenames)

        # Mark each filename as valid in the parent directory
        for filename in filenames:
            if filename not in [".", ".."]:
                file_path = f"{dir_path}/{filename}"
                # We don't know if it's a directory, so let mark_valid infer it
                self.cache.mark_valid(file_path)

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

        # For debugging, log the entry type from cache
        entry_type = self.cache.get_entry_type(path)
        self.logger.debug(f"Entry type for {path}: {entry_type}")

        # Clear thread-local cache at the beginning of a new directory operation
        self._clear_thread_local_cache()

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
            # Use the router's validate_path method first for a quick check
            if not self.router.validate_path(path):
                # If router doesn't know about it, use our full validation
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
                # Mark the directory as valid in the cache
                self.cache.mark_valid(path, is_directory=True)

                # Pre-fetch the directory listing with attributes
                dir_listing = self.cache.get_directory_listing_with_attrs(path)

                # Store in thread-local cache for subsequent getattr calls
                if dir_listing:
                    if not hasattr(self._thread_local, "temp_dir_listings"):
                        self._thread_local.temp_dir_listings = {}
                    self._thread_local.temp_dir_listings[path] = dir_listing

                    # Mark this directory as validated in thread-local
                    if not hasattr(self._thread_local, "validated_dirs"):
                        self._thread_local.validated_dirs = set()
                    self._thread_local.validated_dirs.add(path)

                    # Pre-validate all files in this directory in thread-local
                    if not hasattr(self._thread_local, "validated_paths"):
                        self._thread_local.validated_paths = {}

                    for filename in result:
                        if filename not in [".", ".."]:
                            file_path = f"{path}/{filename}"
                            self._thread_local.validated_paths[file_path] = True

                            # Determine if this entry is a directory based on attributes
                            is_dir = False
                            if filename in dir_listing:
                                is_dir = (
                                    dir_listing[filename].get("st_mode", 0)
                                    & stat.S_IFDIR
                                    == stat.S_IFDIR
                                )

                            # Mark as valid in the cache manager
                            self.cache.mark_valid(file_path, is_directory=is_dir)

                # Cache valid filenames even if no directory listing available
                filenames = [entry for entry in result if entry not in [".", ".."]]
                self.cache.set(f"valid_files:{path}", filenames)

                # Mark each file as valid (we might not know if it's a directory so let mark_valid infer it)
                for filename in filenames:
                    file_path = f"{path}/{filename}"
                    # If we don't have dir_listing, we use None for is_directory to let mark_valid infer it
                    if not dir_listing:
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
        """Get the video ID for a file.

        Args:
            path: The path to get the video ID for

        Returns:
            Video ID for the file

        Raises:
            OSError: If the video ID could not be found
        """
        # Check if path is a file based on entry_type
        entry_type = self.cache.get_entry_type(path)
        if entry_type != "file":
            self.logger.warning(f"Attempting to get video ID for non-file: {path}")
            raise OSError(errno.EINVAL, "Not a music file")

        # Create a thread-local cache for video IDs if it doesn't exist
        if not hasattr(self._thread_local, "video_id_cache"):
            self._thread_local.video_id_cache = {}

        # Check if we already have the video ID for this path in thread-local cache
        if path in self._thread_local.video_id_cache:
            video_id = self._thread_local.video_id_cache[path]
            self.logger.debug(f"Using cached video ID {video_id} for {path}")
            return video_id

        dir_path = os.path.dirname(path)
        filename = os.path.basename(path)
        self.logger.debug(f"Looking up video ID for {filename} in {dir_path}")

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
                # For other directories (like albums)
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
            path: The path to get attributes for
            fh: File handle (unused)

        Returns:
            Dictionary containing file attributes
        """
        self.logger.debug(f"getattr: {path}")

        # Check cache for recent result
        operation_key = f"getattr:{path}"
        now = time.time()

        with self.last_access_lock:
            last_time = self.last_access_time.get(operation_key, 0)
            if (
                now - last_time < self.request_cooldown
                and operation_key in self.last_access_results
            ):
                self.logger.debug(f"Using cached result for {operation_key}")
                return self.last_access_results[operation_key]
            self.last_access_time[operation_key] = now

        # Initialize default attributes
        attr = {
            "st_atime": now,
            "st_ctime": now,
            "st_mtime": now,
            "st_nlink": 2,
            "st_uid": os.getuid(),
            "st_gid": os.getgid(),
        }

        # Get the entry type directly from the cache manager
        entry_type = self.cache.get_entry_type(path)

        # Case 1: Root or top-level directories
        if path == "/" or path in ["/playlists", "/liked_songs", "/albums"]:
            attr["st_mode"] = stat.S_IFDIR | 0o755
            attr["st_size"] = 0
            self._cache_result(operation_key, attr)
            return attr.copy()

        # Case 2: Known directories from entry_type
        if entry_type == "directory":
            attr["st_mode"] = stat.S_IFDIR | 0o755
            attr["st_size"] = 0
            self._cache_result(operation_key, attr)
            return attr.copy()

        # Case 3: Known files from entry_type
        if entry_type == "file":
            # Try to get file attributes from cached directory listings
            parent_dir = os.path.dirname(path)
            filename = os.path.basename(path)

            # First check the thread-local directory cache
            if (
                hasattr(self._thread_local, "temp_dir_listings")
                and parent_dir in self._thread_local.temp_dir_listings
            ):
                dir_listing = self._thread_local.temp_dir_listings[parent_dir]
                if filename in dir_listing:
                    cached_attrs = dir_listing[filename].copy()
                    cached_attrs["st_atime"] = now
                    cached_attrs["st_ctime"] = now
                    cached_attrs["st_mtime"] = now

                    # Update file size if we have a more precise value
                    file_size_cache_key = f"filesize:{path}"
                    cached_size = self.cache.get(file_size_cache_key)
                    if cached_size is not None:
                        cached_attrs["st_size"] = cached_size

                    self._cache_result(operation_key, cached_attrs)
                    return cached_attrs

            # Then check the persistent cache
            cached_attrs = self.cache.get_file_attrs_from_parent_dir(path)
            if cached_attrs:
                cached_attrs["st_atime"] = now
                cached_attrs["st_ctime"] = now
                cached_attrs["st_mtime"] = now

                file_size_cache_key = f"filesize:{path}"
                cached_size = self.cache.get(file_size_cache_key)
                if cached_size is not None:
                    cached_attrs["st_size"] = cached_size

                self._cache_result(operation_key, cached_attrs)
                return cached_attrs

            # Case 4: For files we know exist but don't have attributes for,
            # get video ID and estimate file size
            try:
                video_id = self._get_video_id(path)
                duration = self.cache.get_duration(video_id)

                # Estimate file size based on duration (128kbps = 16KB/sec)
                if duration is not None:
                    attr["st_size"] = duration * 16 * 1024
                else:
                    attr["st_size"] = 4096  # Default size

                attr["st_mode"] = stat.S_IFREG | 0o644
                attr["st_nlink"] = 1

                self._cache_result(operation_key, attr)
                return attr.copy()

            except Exception as e:
                self.logger.error(f"Error getting file attributes for {path}: {e}")
                raise OSError(errno.ENOENT, "No such file or directory")

        # Case 5: Path not found in cache but might be valid
        # Check with the fuller validation
        if not self._is_valid_path(path):
            self.logger.debug(f"Invalid path in getattr: {path}")
            raise OSError(errno.ENOENT, "No such file or directory")

        # Case 6: Path is valid but we don't know if it's a file or directory
        # Try getting the video ID to determine if it's a file
        try:
            video_id = self._get_video_id(path)
            if video_id:
                # It's a file, mark it as such
                self.cache.mark_valid(path, is_directory=False)

                duration = self.cache.get_duration(video_id)
                # Estimate file size based on duration (128kbps = 16KB/sec)
                if duration is not None:
                    attr["st_size"] = duration * 16 * 1024
                else:
                    attr["st_size"] = 4096  # Default size

                attr["st_mode"] = stat.S_IFREG | 0o644
                attr["st_nlink"] = 1

                self._cache_result(operation_key, attr)
                return attr.copy()
        except:
            # If we can't get a video ID, assume it's a directory
            self.cache.mark_valid(path, is_directory=True)
            attr["st_mode"] = stat.S_IFDIR | 0o755
            attr["st_size"] = 0
            self._cache_result(operation_key, attr)
            return attr.copy()

        # If all else fails, default to directory
        self.logger.debug(f"Defaulting to directory for {path}")
        self.cache.mark_valid(path, is_directory=True)
        attr["st_mode"] = stat.S_IFDIR | 0o755
        attr["st_size"] = 0
        self._cache_result(operation_key, attr)
        return attr.copy()

    def _cache_result(self, key: str, value: Dict[str, Any]) -> None:
        """Helper method to cache operation results.

        Args:
            key: Cache key
            value: Value to cache
        """
        with self.last_access_lock:
            self.last_access_results[key] = value.copy()

    def open(self, path: str, flags: int) -> int:
        """Open a file.

        Args:
            path: The file path
            flags: Open flags

        Returns:
            File handle
        """
        self.logger.debug(f"open: {path} with flags {flags}")

        # Check if this is a valid file path
        entry_type = self.cache.get_entry_type(path)

        # If it's a directory, we can't open it as a file
        if entry_type == "directory":
            self.logger.warning(f"Attempting to open directory as file: {path}")
            raise OSError(errno.EISDIR, "Is a directory")

        # If it's not known to be a file, use our validation
        if entry_type != "file":
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

            # Mark it as a file in the cache since we're opening it
            self.cache.mark_valid(path, is_directory=False)

        try:
            # Get video ID
            video_id = self._get_video_id(path)
            if not video_id:
                self.logger.error(f"Empty video ID extracted for {path}")
                raise OSError(errno.EINVAL, "Invalid video ID")

            self.logger.debug(
                f"Successfully extracted video ID: {video_id} for path: {path}"
            )

            # Delegate to file handler
            fh = self.file_handler.open(path, video_id, self.thread_pool)
            return fh
        except OSError as e:
            # Preserve the original OSError without wrapping it
            self.logger.error(f"OS Error opening file {path}: {e}")
            raise
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
