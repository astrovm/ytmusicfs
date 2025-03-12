#!/usr/bin/env python3

from concurrent.futures import ThreadPoolExecutor
from fuse import FUSE, Operations
from pathlib import Path
from typing import Dict, Any, Optional, List, Callable
from yt_dlp import YoutubeDL
from ytmusicfs.cache import CacheManager
from ytmusicfs.client import YouTubeMusicClient
from ytmusicfs.processor import TrackProcessor
import errno
import logging
import os
import re
import requests
import stat
import threading
import time
import inspect
import traceback


class PathRouter:
    """Router for handling FUSE filesystem paths."""

    def __init__(self):
        """Initialize the path router with empty handler collections."""
        self.handlers: Dict[str, Callable] = {}
        self.subpath_handlers: List[tuple[str, Callable]] = []
        self.pattern_handlers: List[tuple[str, Callable]] = []  # For wildcard patterns

    def register(self, path: str, handler: Callable) -> None:
        """Register a handler for an exact path match.

        Args:
            path: The exact path to match
            handler: The handler function to call
        """
        self.handlers[path] = handler

    def register_subpath(self, prefix: str, handler: Callable) -> None:
        """Register a handler for a path prefix match.

        Args:
            prefix: The path prefix to match
            handler: The handler function to call with the full path
        """
        self.subpath_handlers.append((prefix, handler))

    def register_dynamic(self, pattern: str, handler: Callable) -> None:
        """Register a handler for a path pattern with wildcards.

        Wildcards:
        - * matches any sequence of characters within a path segment
        - ** matches any sequence of characters across multiple path segments

        Args:
            pattern: The path pattern to match (e.g., "/playlists/*", "/artists/**/tracks")
            handler: The handler function to call with the full path
        """
        self.pattern_handlers.append((pattern, handler))

    def _match_wildcard_pattern(self, pattern: str, path: str) -> tuple[bool, list]:
        """Check if a path matches a wildcard pattern and extract wildcard values.

        Args:
            pattern: The pattern with wildcards to match against
            path: The path to check

        Returns:
            Tuple of (match_success, captured_values)
        """
        # Convert pattern to regex
        import re

        # Escape special regex characters except * which we'll handle specially
        regex_pattern = (
            re.escape(pattern).replace("\\*\\*", "(.+)").replace("\\*", "([^/]+)")
        )

        # Add start and end anchors
        regex_pattern = f"^{regex_pattern}$"

        # Match the path against the pattern
        match = re.match(regex_pattern, path)
        if match:
            # Return captured values
            return True, list(match.groups())
        return False, []

    def route(self, path: str) -> List[str]:
        """Route a path to the appropriate handler.

        Args:
            path: The path to route

        Returns:
            List of directory entries from the handler
        """
        # First try exact matches
        if path in self.handlers:
            return self.handlers[path]()

        # Then try prefix matches
        for prefix, handler in self.subpath_handlers:
            if path.startswith(prefix):
                return handler(path)

        # Finally try pattern matches
        for pattern, handler in self.pattern_handlers:
            match_success, captured_values = self._match_wildcard_pattern(pattern, path)
            if match_success:
                # Pass both the full path and the captured values
                if captured_values:
                    return handler(path, *captured_values)
                else:
                    return handler(path)

        return [".", ".."]


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
        """
        # Get the logger
        self.logger = logging.getLogger("YTMusicFS")

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
            "/playlists", lambda: [".", ".."] + self._readdir_playlists()
        )
        self.router.register(
            "/liked_songs", lambda: [".", ".."] + self._readdir_liked_songs()
        )
        self.router.register("/artists", lambda: [".", ".."] + self._readdir_artists())
        self.router.register("/albums", lambda: [".", ".."] + self._readdir_albums())
        self.router.register(
            "/search", lambda: [".", ".."] + self._readdir_search_categories()
        )
        # Add handlers for /search/library and /search/catalog to show categories
        self.router.register(
            "/search/library",
            lambda: [".", ".."] + self._readdir_search_category_options(),
        )
        self.router.register(
            "/search/catalog",
            lambda: [".", ".."] + self._readdir_search_category_options(),
        )

        # Register dynamic handlers with wildcard capture
        self.router.register_dynamic(
            "/playlists/*",
            lambda path, *args: [".", ".."] + self._readdir_playlist_content(path),
        )
        self.router.register_dynamic(
            "/artists/*",
            lambda path, *args: [".", ".."] + self._readdir_artist_content(path),
        )
        self.router.register_dynamic(
            "/artists/*/*",
            lambda path, *args: [".", ".."] + self._readdir_album_content(path),
        )
        self.router.register_dynamic(
            "/albums/*",
            lambda path, *args: [".", ".."] + self._readdir_album_content(path),
        )
        # Search routes - focused on library and catalog paths only
        self.router.register_dynamic(
            "/search/*",
            lambda path, *args: [".", ".."] + self._readdir_search_results(path, *args),
        )
        self.router.register_dynamic(
            "/search/library/*",
            lambda path, *args: [".", ".."]
            + self._readdir_search_results(path, *args, scope="library"),
        )
        self.router.register_dynamic(
            "/search/catalog/*",
            lambda path, *args: [".", ".."]
            + self._readdir_search_results(path, *args, scope=None),
        )
        self.router.register_dynamic(
            "/search/library/*/*",
            lambda path, *args: [".", ".."]
            + self._readdir_search_item_content(path, *args, scope="library"),
        )
        self.router.register_dynamic(
            "/search/catalog/*/*",
            lambda path, *args: [".", ".."]
            + self._readdir_search_item_content(path, *args, scope=None),
        )
        self.router.register_dynamic(
            "/search/library/*/*/*",
            lambda path, *args: [".", ".."]
            + self._readdir_search_item_content(path, *args, scope="library"),
        )
        self.router.register_dynamic(
            "/search/catalog/*/*/*",
            lambda path, *args: [".", ".."]
            + self._readdir_search_item_content(path, *args, scope=None),
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
        futures.append(self.thread_pool.submit(self._readdir_playlists))
        futures.append(self.thread_pool.submit(self._readdir_liked_songs))
        futures.append(self.thread_pool.submit(self._readdir_artists))
        futures.append(self.thread_pool.submit(self._readdir_albums))
        futures.append(self.thread_pool.submit(self._readdir_search_categories))
        # Also preload search category options
        futures.append(self.thread_pool.submit(self._readdir_search_category_options))

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
        and improve performance.
        """
        self.logger.info("Optimizing path validation...")

        # Cache the basic directory structure
        self._cache_valid_dir("/")
        self._cache_valid_dir("/playlists")
        self._cache_valid_dir("/liked_songs")
        self._cache_valid_dir("/artists")
        self._cache_valid_dir("/albums")
        self._cache_valid_dir("/search")
        self._cache_valid_dir("/search/library")
        self._cache_valid_dir("/search/catalog")

        # Cache search category directories
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

        # Check for mkdir in stack trace for better detection
        stack = traceback.format_stack()
        is_mkdir_in_stack = any("mkdir" in frame for frame in stack)

        # For mkdir operations, most paths should be considered valid
        # This allows the mkdir operation to proceed and then fail at its own validation
        # instead of getting blocked early by path validation
        if context == "mkdir" or "mkdir" in context or is_mkdir_in_stack:
            self.logger.debug(f"Allowing path as valid due to mkdir context: {path}")
            return True

        # Root and main category directories are always valid
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

        # Search category paths are valid
        if path.startswith("/search/library/") or path.startswith("/search/catalog/"):
            parts = path.split("/")
            # Category paths like /search/library/songs
            if len(parts) == 4:
                valid_categories = ["songs", "videos", "albums", "artists", "playlists"]
                if parts[3] in valid_categories:
                    return True

            # Search query paths like /search/library/songs/query
            if len(parts) == 5:
                valid_categories = ["songs", "videos", "albums", "artists", "playlists"]
                if parts[3] in valid_categories:
                    # For search paths, use the cache's optimized path validation
                    if self.cache.is_valid_path(path):
                        return True

                    # If the directory doesn't exist in our cache,
                    # we'll still consider it potentially valid for certain operations
                    if context == "getattr":
                        # For getattr, let it decide existence based on cache
                        self.logger.debug(
                            f"Directing path validation to getattr: {path}"
                        )
                        return True

        # For file paths, check if they exist in parent directory's listings with attributes
        if "." in path and path.lower().endswith(".m4a"):
            parent_dir = os.path.dirname(path)
            filename = os.path.basename(path)

            # Check if file exists in parent directory's cached listing with attributes
            dir_listing = self.cache.get_directory_listing_with_attrs(parent_dir)
            if dir_listing and filename in dir_listing:
                self.logger.debug(
                    f"Found {filename} in {parent_dir} directory listing with attributes"
                )
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

    def _readdir_playlists(self) -> List[str]:
        """Handle listing playlists.

        Returns:
            List of playlist names
        """
        # Use the helper method to handle cache auto-refreshing
        self._auto_refresh_cache("/playlists")

        # Check if we have cached playlists
        playlists = self.cache.get("/playlists")
        if not playlists:
            # Fetch playlists data outside of any locks
            self.logger.debug("Fetching playlists data from API")
            playlists = self.client.get_library_playlists(limit=100)
            self.cache.set("/playlists", playlists)

        # Process playlist data outside of locks
        playlist_names = [
            self.processor.sanitize_filename(playlist["title"])
            for playlist in playlists
        ]

        return playlist_names

    def _readdir_liked_songs(self) -> List[str]:
        """Handle listing liked songs.

        Returns:
            List of liked song filenames
        """
        # Use the helper method to handle cache auto-refreshing
        self._auto_refresh_cache("/liked_songs")

        # First check if we have processed tracks in cache
        processed_tracks = self.cache.get("/liked_songs_processed")
        if processed_tracks:
            self.logger.debug(
                f"Using {len(processed_tracks)} cached processed tracks for /liked_songs"
            )
            # Cache directory listing with attributes for efficient getattr lookups
            self._cache_directory_listing_with_attrs("/liked_songs", processed_tracks)
            return [track["filename"] for track in processed_tracks]

        # If no processed tracks in cache, fetch data outside of any locks
        self.logger.debug("Fetching liked songs data from API")
        liked_songs = self.client.get_liked_songs(limit=10000)

        # Process the raw liked songs data outside of any locks
        self.logger.debug(f"Processing raw liked songs data: {type(liked_songs)}")

        # Handle both possible response formats from the API:
        # 1. A dictionary with a 'tracks' key containing the list of tracks
        # 2. A direct list of tracks
        tracks_to_process = liked_songs
        if isinstance(liked_songs, dict) and "tracks" in liked_songs:
            tracks_to_process = liked_songs["tracks"]

        # Use the track processor to process tracks and create filenames (outside of locks)
        processed_tracks, filenames = self.processor.process_tracks(tracks_to_process)

        # Cache the processed song list with filename mappings
        self.logger.debug(
            f"Caching {len(processed_tracks)} processed tracks for /liked_songs"
        )
        self.cache.set("/liked_songs_processed", processed_tracks)

        # Cache directory listing with attributes for efficient getattr lookups
        self._cache_directory_listing_with_attrs("/liked_songs", processed_tracks)

        return filenames

    def _readdir_playlist_content(self, path: str) -> List[str]:
        """Handle listing contents of a specific playlist.

        Args:
            path: Playlist path

        Returns:
            List of track filenames in the playlist
        """
        # Check for cached processed tracks first
        processed_tracks = self.cache.get(path)
        if processed_tracks:
            self.logger.debug(
                f"Using {len(processed_tracks)} cached processed tracks for {path}"
            )
            # Cache directory listing with attributes for efficient getattr lookups
            self._cache_directory_listing_with_attrs(path, processed_tracks)
            return [track["filename"] for track in processed_tracks]

        # Extract playlist ID from path
        playlist_name = path.split("/")[2]

        # Locate the playlist ID using the sanitized name - do this outside of locks
        playlist_id = None

        # Fetch playlists data outside of locks
        playlists = self.cache.get("/playlists")
        if not playlists:
            self.logger.debug("Fetching playlists data from API")
            playlists = self.client.get_library_playlists(limit=10000)
            self.cache.set("/playlists", playlists)

        # Find matching playlist ID
        for playlist in playlists:
            if self.processor.sanitize_filename(playlist["title"]) == playlist_name:
                playlist_id = playlist["playlistId"]
                break

        if not playlist_id:
            self.logger.error(f"Could not find playlist ID for playlist: {path}")
            return []

        # Fetch the playlist tracks outside of locks
        self.logger.debug(f"Fetching tracks for playlist ID: {playlist_id}")
        playlist_data = self.client.get_playlist(playlist_id, limit=10000)

        # Process the playlist tracks outside of locks
        tracks_to_process = playlist_data.get("tracks", [])
        processed_tracks, filenames = self.processor.process_tracks(tracks_to_process)

        # Cache the processed playlist tracks
        self.cache.set(path, processed_tracks)

        # Cache directory listing with attributes for efficient getattr lookups
        self._cache_directory_listing_with_attrs(path, processed_tracks)

        return filenames

    def _readdir_artists(self) -> List[str]:
        """Handle listing artists.

        Returns:
            List of artist names
        """
        # Use the helper method to handle cache auto-refreshing
        self._auto_refresh_cache("/artists")

        # First check if we have cached artists
        artists = self.cache.get("/artists")
        if not artists:
            # Fetch artists data outside of any locks
            self.logger.debug("Fetching artists data from API")
            artists = self.client.get_library_artists()
            self.cache.set("/artists", artists)

        # Process artist data outside of locks
        artist_names = [
            self.processor.sanitize_filename(artist["artist"]) for artist in artists
        ]

        return artist_names

    def _readdir_albums(self) -> List[str]:
        """Handle listing albums.

        Returns:
            List of album names
        """
        # Use the helper method to handle cache auto-refreshing
        self._auto_refresh_cache("/albums")

        # First check if we have cached albums
        albums = self.cache.get("/albums")
        if not albums:
            # Fetch albums data outside of any locks
            self.logger.debug("Fetching albums data from API")
            albums = self.client.get_library_albums()
            self.cache.set("/albums", albums)

        # Process album data outside of locks
        album_names = [
            self.processor.sanitize_filename(album["title"]) for album in albums
        ]

        return album_names

    def _readdir_artist_content(self, path: str) -> List[str]:
        """Handle listing contents of a specific artist.

        Args:
            path: Artist path

        Returns:
            List of directory entries for artist content
        """
        # Check if we have cached content
        cached_content = self.cache.get(f"{path}_content")
        if cached_content:
            self.logger.debug(f"Using cached content for {path}")
            result = list(cached_content)  # Make a copy to avoid modifying cache

            # For directories, cache directory listings with attributes
            for entry in cached_content:
                if not entry.endswith(".m4a"):  # This is a subdirectory
                    subdir_path = f"{path}/{entry}"
                    # We need to preemptively mark this as a valid directory
                    self._cache_valid_dir(subdir_path)

            return result

        # Extract artist ID from path
        artist_name = os.path.basename(path)

        # Find artist ID - do this outside of locks
        artist_id = None

        # Fetch artists data first (check cache, then API if needed)
        artists = self.cache.get("/artists")
        if not artists:
            self.logger.debug("Fetching artists data from API")
            artists = self.client.get_library_artists(limit=10000)
            self.cache.set("/artists", artists)

        # Search for matching artist ID
        for artist in artists:
            if (
                self.processor.sanitize_filename(
                    artist.get("name") or artist.get("artist")
                )
                == artist_name
            ):
                artist_id = artist["browseId"]
                break

        if not artist_id:
            self.logger.error(f"Could not find artist ID for {artist_name}")
            return []

        # Get artist's singles and albums
        artist_content = []
        discography = []

        # Add standard subdirectories
        artist_content.extend(["singles", "albums"])

        # Cache the singles and albums for this artist
        artist_singles_cache_key = f"{path}/singles"
        artist_albums_cache_key = f"{path}/albums"

        # Fetch artist albums outside of locks
        artist_albums = self.cache.get(artist_albums_cache_key)
        if not artist_albums:
            self.logger.debug(f"Fetching albums for artist ID: {artist_id}")
            artist_albums = self.client.get_artist_albums(artist_id, limit=50)
            self.cache.set(artist_albums_cache_key, artist_albums)

        # Process album information outside of locks
        if artist_albums:
            # Create album directories
            albums = []
            for album in artist_albums:
                if album.get("title"):
                    album_name = self.processor.sanitize_filename(album["title"])
                    albums.append(album_name)

            # Cache album directories for album listing
            album_dirs = list(set(albums))  # Remove duplicates
            self.cache.set(f"{path}/albums_dirs", album_dirs)

            # Mark this content as valid for future validation
            self._cache_valid_dir(f"{path}/albums")
            self._cache_valid_filenames(f"{path}/albums", album_dirs)

            # Cache directory listing with attributes for efficient getattr lookups
            album_listing_with_attrs = {}
            now = time.time()
            for album_name in album_dirs:
                album_listing_with_attrs[album_name] = {
                    "st_mode": stat.S_IFDIR | 0o755,
                    "st_atime": now,
                    "st_ctime": now,
                    "st_mtime": now,
                    "st_nlink": 2,
                    "st_size": 0,
                }
            self.cache.set_directory_listing_with_attrs(
                f"{path}/albums", album_listing_with_attrs
            )

        # Cache the content list
        self.cache.set(f"{path}_content", artist_content)

        return artist_content

    def _readdir_album_content(self, path: str) -> List[str]:
        """Handle listing contents of a specific album.

        Args:
            path: Album path

        Returns:
            List of track filenames in the album
        """
        # Check for cached processed tracks first
        processed_tracks = self.cache.get(path)
        if processed_tracks:
            self.logger.debug(
                f"Using {len(processed_tracks)} cached processed tracks for {path}"
            )
            # Cache directory listing with attributes for efficient getattr lookups
            self._cache_directory_listing_with_attrs(path, processed_tracks)
            return [track["filename"] for track in processed_tracks]

        # Determine if this is an artist's album or a library album
        is_artist_album = path.startswith("/artists/")
        album_id = None
        album_title = None  # Store the album title for metadata
        artist_albums = []  # Initialize as empty list by default

        # Check for hidden files
        path_parts = path.split("/")
        for part in path_parts:
            if part and part.startswith("."):
                self.logger.debug(f"Ignoring hidden album path: {path}")
                return []

        if is_artist_album:
            parts = path.split("/")
            artist_name = parts[2]
            album_name = parts[3]

            # Find the artist ID - do this outside of locks
            artist_id = None

            # Get artists from cache or API
            artists = self.cache.get("/artists")
            if not artists:
                self.logger.debug("Fetching artists data from API")
                artists = self.client.get_library_artists(limit=10000)
                self.cache.set("/artists", artists)

            # Find matching artist
            for artist in artists:
                if self.processor.sanitize_filename(artist["artist"]) == artist_name:
                    # Safely access ID fields with fallbacks
                    artist_id = artist.get("artistId")
                    if not artist_id:
                        artist_id = artist.get("browseId")
                    if not artist_id:
                        artist_id = artist.get("id")
                    break

            if not artist_id:
                self.logger.error(f"Could not find artist ID for {artist_name}")
                return []

            # Get the artist's albums - outside of locks
            artist_cache_key = f"/artist/{artist_id}"

            # Check if we have cached artist albums
            artist_albums = self.cache.get(artist_cache_key)
            if not artist_albums:
                # Fetch artist data outside of locks
                self.logger.debug(f"Fetching data for artist ID: {artist_id}")
                artist_data = self.client.get_artist(artist_id)
                artist_albums = []

                # Get albums
                if "albums" in artist_data:
                    for album in artist_data["albums"]["results"]:
                        artist_albums.append(
                            {
                                "title": album.get("title", "Unknown Album"),
                                "year": album.get("year", ""),
                                "type": "album",
                                "browseId": album.get("browseId"),
                            }
                        )

                # Get singles
                if "singles" in artist_data:
                    for single in artist_data["singles"]["results"]:
                        artist_albums.append(
                            {
                                "title": single.get("title", "Unknown Single"),
                                "year": single.get("year", ""),
                                "type": "single",
                                "browseId": single.get("browseId"),
                            }
                        )

                # Cache the artist albums
                self.cache.set(artist_cache_key, artist_albums)

            # Find the album ID
            for album in artist_albums:
                if self.processor.sanitize_filename(album["title"]) == album_name:
                    album_id = album["browseId"]
                    album_title = album["title"]
                    break
        else:
            # Regular album path
            album_name = path.split("/")[2]

            # Find the album ID - do this outside of locks
            # Get albums from cache or API
            albums = self.cache.get("/albums")
            if not albums:
                self.logger.debug("Fetching albums data from API")
                albums = self.client.get_library_albums(limit=10000)
                self.cache.set("/albums", albums)

            # Find matching album
            for album in albums:
                if self.processor.sanitize_filename(album["title"]) == album_name:
                    album_id = album["browseId"]
                    album_title = album["title"]
                    break

        if not album_id:
            self.logger.error(f"Could not find album ID for album in path: {path}")
            return []

        # Get the album tracks - outside of locks
        album_cache_key = f"/album/{album_id}"

        # Check if we have cached album tracks
        album_tracks = self.cache.get(album_cache_key)
        if not album_tracks:
            # Fetch album tracks outside of locks
            self.logger.debug(f"Fetching tracks for album ID: {album_id}")
            album_data = self.client.get_album(album_id)
            album_tracks = album_data.get("tracks", [])
            self.cache.set(album_cache_key, album_tracks)

        # Process tracks outside of locks
        processed_tracks, filenames = self.processor.process_tracks(album_tracks)

        # Add additional album-specific information
        for track in processed_tracks:
            # Override album title with the one from the path
            if album_title:
                track["album"] = album_title

            # For artist albums, try to find the album year if available
            if is_artist_album and artist_albums and not track.get("year"):
                for album in artist_albums:
                    if self.processor.sanitize_filename(album["title"]) == album_name:
                        track["year"] = album.get("year")
                        break

        # Cache the processed track list for this album
        self.cache.set(path, processed_tracks)

        # Cache directory listing with attributes for efficient getattr lookups
        self._cache_directory_listing_with_attrs(path, processed_tracks)

        return filenames

    def _readdir_search_categories(self) -> List[str]:
        """Handle listing search categories.

        Returns:
            List of search categories
        """
        # Return only the two scope options - library and catalog
        categories = [
            "library",  # Search within your library
            "catalog",  # Search the entire YouTube Music catalog
        ]

        return categories

    def _readdir_search_category_options(self) -> List[str]:
        """Handle listing search category options (songs, videos, etc.).

        Returns:
            List of search category options
        """
        # Return the search category options
        categories = [
            "songs",  # Search for songs
            "videos",  # Search for videos
            "albums",  # Search for albums
            "artists",  # Search for artists
            "playlists",  # Search for playlists
        ]

        # Make sure these categories are recognized as valid directories
        path_parts = inspect.stack()[1].frame.f_locals.get("path", "").split("/")
        if len(path_parts) >= 3:
            prefix = f"/search/{path_parts[2]}"
            for category in categories:
                self._cache_valid_dir(f"{prefix}/{category}")

        return categories

    def _readdir_search_results(
        self, path: str, *args, scope: Optional[str] = None
    ) -> List[str]:
        """Handle listing search results.

        Args:
            path: Path to the search query
            scope: Optional scope ('library' or 'catalog') for the search

        Returns:
            List of directory entries for search results
        """
        # Extract query from path
        path_parts = path.split("/")
        if len(path_parts) < 5:
            self.logger.error(f"Invalid search path: {path}")
            return []

        # Check if we have a scope and type
        scope = scope or path_parts[2]
        search_type = path_parts[3]
        query = path_parts[4]

        # Handle empty query
        if not query:
            return []

        # Validate search category
        valid_categories = ["songs", "videos", "albums", "artists", "playlists"]
        if search_type not in valid_categories:
            self.logger.error(f"Invalid search type: {search_type}")
            return []

        # Check if we have cached results
        cache_key = f"{path}_results"
        cached_results = self.cache.get(cache_key)
        if cached_results:
            self.logger.debug(f"Using cached search results for {path}")

            # For search result directories, cache directory listings with attributes
            if search_type in ["albums", "artists", "playlists"]:
                # These search results are directories
                now = time.time()
                listing_with_attrs = {}
                for entry in cached_results:
                    if not entry.endswith(".m4a"):  # This is a subdirectory
                        listing_with_attrs[entry] = {
                            "st_mode": stat.S_IFDIR | 0o755,
                            "st_atime": now,
                            "st_ctime": now,
                            "st_mtime": now,
                            "st_nlink": 2,
                            "st_size": 0,
                        }
                self.cache.set_directory_listing_with_attrs(path, listing_with_attrs)

            return cached_results

        # Perform the search
        search_results = self._perform_search(query, scope, search_type)

        # Process the results based on the category
        results = self._process_search_result_items(search_results, search_type, path)

        # Cache the results
        self.cache.set(cache_key, results)

        # Mark this directory as valid to avoid future validation overhead
        self._cache_valid_dir(path)
        self._cache_valid_filenames(path, results)

        # For search result directories, cache directory listings with attributes
        if search_type in ["albums", "artists", "playlists"]:
            # These search results are directories
            now = time.time()
            listing_with_attrs = {}
            for entry in results:
                if not entry.endswith(".m4a"):  # This is a subdirectory
                    listing_with_attrs[entry] = {
                        "st_mode": stat.S_IFDIR | 0o755,
                        "st_atime": now,
                        "st_ctime": now,
                        "st_mtime": now,
                        "st_nlink": 2,
                        "st_size": 0,
                    }
            self.cache.set_directory_listing_with_attrs(path, listing_with_attrs)

        return results

    def _readdir_search_item_content(
        self, path: str, *args, scope: Optional[str] = None
    ) -> List[str]:
        """Handle listing contents of a specific search result item.

        Args:
            path: Search result item path
            args: Additional arguments from wildcard match
            scope: Search scope (None for entire catalog, 'library' for just library)

        Returns:
            List of track filenames in the search result item
        """
        # Check for cached processed tracks first
        processed_tracks = self.cache.get(path)
        if processed_tracks:
            self.logger.debug(
                f"Using {len(processed_tracks)} cached processed tracks for {path}"
            )
            # Cache directory listing with attributes for efficient getattr lookups
            self._cache_directory_listing_with_attrs(path, processed_tracks)
            return [track["filename"] for track in processed_tracks]

        # Extract path parts
        parts = path.split("/")
        if len(parts) < 4:
            self.logger.error(f"Invalid search item path: {path}")
            return []

        filter_type = None
        search_query = None
        item_category = None

        # Handle nested library/catalog paths with categories (like /search/catalog/songs/Linkin)
        if parts[2] == "library" or parts[2] == "catalog":
            # Set scope based on which path we're in
            scope = "library" if parts[2] == "library" else None

            if len(parts) == 5:  # /search/catalog/songs/Linkin
                category = parts[3]
                search_query = parts[4]

                # Get filter type based on category - use the category directly without changing to singular
                if category in ["songs", "videos", "albums", "artists", "playlists"]:
                    filter_type = category  # Keep plural form as expected by the API

                # For direct category searches, we need to return the search results
                # in the category folder directly, not as a subcategory
                scope_suffix = f"_library" if scope == "library" else ""
                filter_suffix = f"_{filter_type}" if filter_type else ""
                cache_key = f"/search/{search_query}{scope_suffix}{filter_suffix}"

                # Get cached search results or perform search
                search_results = self.cache.get(cache_key)
                if not search_results:
                    # Need to perform the search
                    search_results = self._perform_search(
                        search_query, scope, filter_type
                    )
                    self.cache.set(cache_key, search_results)

                # For a direct category search, use the category as the item_category
                item_category = category
                result_items = self._process_search_result_items(
                    search_results, item_category, path
                )

                return result_items

            elif len(parts) >= 6:  # /search/catalog/songs/query/results
                category = parts[3]
                search_query = parts[4]
                item_category = parts[5]

                # Get filter type based on category - keep plural form
                if category in ["songs", "videos", "albums", "artists", "playlists"]:
                    filter_type = category  # Keep plural form as expected by the API
            else:
                self.logger.error(f"Invalid nested search path: {path}")
                return []
        else:
            # Standard path (like /search/query/results)
            search_query = parts[2]
            item_category = parts[3]

            # If scope was not explicitly passed, try to determine it from the path
            if scope is None:
                # Default is to search entire catalog (None)
                pass

        # Generate the appropriate cache key based on scope and filter
        scope_suffix = f"_library" if scope == "library" else ""
        filter_suffix = f"_{filter_type}" if filter_type else ""
        cache_key = f"/search/{search_query}{scope_suffix}{filter_suffix}"

        # Get cached search results
        search_results = self.cache.get(cache_key)

        if not search_results:
            # Need to perform the search again
            search_results = self._perform_search(search_query, scope, filter_type)
            self.cache.set(cache_key, search_results)

        # Process the items based on category
        result_items = self._process_search_result_items(
            search_results, item_category, path
        )

        return result_items

    def _process_search_results_to_dirs(
        self, search_results: Dict[str, List]
    ) -> List[str]:
        """Process search results into directory names.

        Args:
            search_results: Categorized search results

        Returns:
            List of directory names
        """
        result_dirs = []

        # Add a directory for each category that has results
        for category, items in search_results.items():
            if items:
                count = len(items)
                result_dirs.append(f"{category}_{count}")

        return result_dirs

    def _perform_search(
        self,
        search_query: str,
        scope: Optional[str] = None,
        filter_type: Optional[str] = None,
    ) -> Dict[str, List]:
        """Perform a search and organize results by category.

        Args:
            search_query: The search query
            scope: The search scope (None for entire catalog, 'library' for just library)
            filter_type: The filter type for specific content (songs, videos, albums, etc.)

        Returns:
            Dictionary mapping categories to results
        """
        self.logger.info(
            f"Performing search for '{search_query}' with scope '{scope}' and filter '{filter_type}'"
        )

        # Perform search with appropriate scope and filter
        results = self.client.search(
            search_query, filter_type=filter_type, scope=scope, limit=100
        )

        # Organize results by category
        categorized_results = {
            "songs": [],
            "videos": [],
            "albums": [],
            "artists": [],
            "playlists": [],
        }

        # Map between API result types and our category names
        result_type_map = {
            "song": "songs",
            "video": "videos",
            "album": "albums",
            "artist": "artists",
            "playlist": "playlists",
        }

        # Process and categorize results
        for item in results:
            result_type = item.get("resultType")
            if result_type in result_type_map:
                category = result_type_map[result_type]
                categorized_results[category].append(item)

        self.logger.debug(
            f"Search categorized results: {[(k, len(v)) for k, v in categorized_results.items()]}"
        )
        return categorized_results

    def _process_search_result_items(
        self, search_results: Dict[str, List], item_category: str, path: str
    ) -> List[str]:
        """Process search result items for a specific category.

        Args:
            search_results: Categorized search results
            item_category: The category to process
            path: The full path (used for caching)

        Returns:
            List of filenames
        """
        # Check if we already processed this category
        processed_cache_key = f"{path}_processed"
        processed_items = self.cache.get(processed_cache_key)
        if processed_items:
            self.logger.debug(f"Using cached processed items for {item_category}")
            return [item["filename"] for item in processed_items]

        # Parse the category (it may include a count suffix)
        category_base = item_category.split("_")[0]

        # Get items for the specific category
        items_to_process = search_results.get(category_base, [])

        if not items_to_process:
            self.logger.warning(f"No items found for category: {category_base}")
            return []

        # Process the items based on their type
        if category_base in ["songs", "videos"]:
            # For songs and videos, process them as tracks
            processed_tracks, filenames = self.processor.process_tracks(
                items_to_process
            )

            # Cache the processed tracks
            self.cache.set(processed_cache_key, processed_tracks)

            return filenames

        elif category_base == "albums":
            # For albums, create a list of album names
            album_names = []
            processed_albums = []

            for i, album in enumerate(items_to_process):
                album_title = album.get("title", f"Unknown Album {i+1}")
                album_name = self.processor.sanitize_filename(album_title)

                # Store the browse ID to fetch album contents later
                processed_album = {
                    "title": album_title,
                    "browseId": album.get("browseId"),
                    "filename": album_name,
                }
                processed_albums.append(processed_album)
                album_names.append(album_name)

            # Cache the processed albums
            self.cache.set(processed_cache_key, processed_albums)

            return album_names

        elif category_base == "artists":
            # For artists, create a list of artist names
            artist_names = []
            processed_artists = []

            for i, artist in enumerate(items_to_process):
                artist_name = artist.get("artist", f"Unknown Artist {i+1}")
                sanitized_name = self.processor.sanitize_filename(artist_name)

                # Store the browse ID to fetch artist contents later
                processed_artist = {
                    "artist": artist_name,
                    "browseId": artist.get("browseId"),
                    "filename": sanitized_name,
                }
                processed_artists.append(processed_artist)
                artist_names.append(sanitized_name)

            # Cache the processed artists
            self.cache.set(processed_cache_key, processed_artists)

            return artist_names

        elif category_base == "playlists":
            # For playlists, create a list of playlist names
            playlist_names = []
            processed_playlists = []

            for i, playlist in enumerate(items_to_process):
                playlist_title = playlist.get("title", f"Unknown Playlist {i+1}")
                playlist_name = self.processor.sanitize_filename(playlist_title)

                # Store the browse ID to fetch playlist contents later
                processed_playlist = {
                    "title": playlist_title,
                    "playlistId": playlist.get("playlistId"),
                    "browseId": playlist.get("browseId"),
                    "filename": playlist_name,
                }
                processed_playlists.append(processed_playlist)
                playlist_names.append(playlist_name)

            # Cache the processed playlists
            self.cache.set(processed_cache_key, processed_playlists)

            return playlist_names

        return []

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
            if self.browser:
                ydl_opts["cookiesfrombrowser"] = (self.browser,)
                self.logger.debug(f"Using cookies from browser: {self.browser}")

            url = f"https://music.youtube.com/watch?v={video_id}"
            self.logger.debug(f"Extracting stream URL for {url}")

            with YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                stream_url = info["url"]

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
                self.logger.warning(f"Error getting file size for {video_id}: {str(e)}")
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

        except Exception as e:
            self.logger.error(f"Error preparing file {video_id}: {str(e)}")
            # Update file handle with error
            with self.file_handle_lock:
                if fh in self.open_files:
                    self.open_files[fh]["status"] = "error"
                    self.open_files[fh]["error"] = str(e)
                    self.open_files[fh]["initialized_event"].set()

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

        # If still initializing, wait for a short time for background initialization to complete
        if status == "initializing":
            # Wait up to 1 second for initialization to complete
            init_wait_result = file_info["initialized_event"].wait(1.0)
            if not init_wait_result:
                # If initialization is taking too long, try direct streaming if possible
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
                else:
                    # Fall back to waiting longer if we must (up to 5 more seconds)
                    self.logger.debug(f"Waiting for initialization to complete: {path}")
                    file_info["initialized_event"].wait(5.0)

                    # Check status again
                    if file_info.get("status") == "error":
                        error_msg = file_info.get(
                            "error", "Unknown error preparing file"
                        )
                        self.logger.error(f"Error after waiting: {error_msg}")
                        raise OSError(errno.EIO, error_msg)
                    elif file_info.get("status") == "initializing":
                        self.logger.error(
                            f"Timeout waiting for file initialization: {path}"
                        )
                        raise OSError(
                            errno.EIO, "Timeout waiting for file initialization"
                        )

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
                if video_id and video_id in self.download_threads:
                    self.logger.debug(f"Stopping download for {video_id}")
                    # Mark download as interrupted
                    self.download_progress[video_id] = "interrupted"
                    # Note: Threading in Python can't be forcefully stopped; we rely on the task checking status
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

        def download_task():
            try:
                self.logger.debug(f"Starting background download for {video_id}")
                with requests.get(stream_url, stream=True) as response:
                    # Update file size from content-length if available
                    if "content-length" in response.headers:
                        actual_size = int(response.headers["content-length"])
                        # Find all paths that use this video_id to update their file sizes
                        with self.file_handle_lock:
                            for handle, info in self.open_files.items():
                                if info.get("video_id") == video_id:
                                    # Get the path from handle
                                    for path in self.path_to_fh:
                                        if self.path_to_fh[path] == handle:
                                            self._update_file_size(path, actual_size)
                                            self.logger.debug(
                                                f"Updated size for {path} to {actual_size} from download response"
                                            )
                                            break

                    with open(cache_path, "wb") as f:
                        for chunk in response.iter_content(chunk_size=4096):
                            # Check if download should be interrupted
                            if self.download_progress.get(video_id) == "interrupted":
                                self.logger.debug(
                                    f"Download interrupted for {video_id}"
                                )
                                return
                            f.write(chunk)
                            # Update download progress
                            self.download_progress[video_id] = f.tell()

                # Mark download as complete
                self.download_progress[video_id] = "complete"

                self.logger.debug(f"Download task completed for {video_id}")

                # When a download completes:
                status_path = self.cache.cache_dir / "audio" / f"{video_id}.status"
                with open(status_path, "w") as f:
                    f.write("complete")

            except Exception as e:
                self.logger.error(f"Error downloading {video_id}: {e}")
                # Mark download as failed
                self.download_progress[video_id] = "failed"

        # Start the download in a background thread and store the reference
        download_thread = threading.Thread(target=download_task)
        download_thread.start()
        self.download_threads[video_id] = download_thread

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
        ),
        mount_point,
        foreground=foreground,
        nothreads=False,  # Enable threading for better performance
        big_writes=True,  # Use larger buffer sizes for better performance
        max_read=4194304,  # 4MB max read size for better performance
        max_write=4194304,  # 4MB max write size for better performance
        max_readahead=4194304,  # Optimize readahead for streaming media
        direct_io=False,  # Allow kernel caching for better performance
        kernel_cache=True,  # Enable kernel caching
        auto_cache=True,  # Enable automatic cache for data
    )
