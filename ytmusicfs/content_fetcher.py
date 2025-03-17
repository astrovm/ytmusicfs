#!/usr/bin/env python3

from typing import List, Callable, Optional, Dict, Any
from ytmusicfs.client import YouTubeMusicClient
from ytmusicfs.processor import TrackProcessor
from ytmusicfs.cache import CacheManager
import logging
import os
import time
import threading


class ContentFetcher:
    """Handles fetching and processing of YouTube Music content."""

    # Centralized registry for all playlist-like items
    PLAYLIST_REGISTRY = [
        {
            "name": "liked_songs",
            "id": "LM",
            "type": "liked_songs",
            "path": "/liked_songs",
        }
    ]

    def __init__(
        self,
        client: YouTubeMusicClient,
        processor: TrackProcessor,
        cache: CacheManager,
        logger: logging.Logger,
        browser: str = None,
    ):
        """Initialize the ContentFetcher.

        Args:
            client: YouTube Music API client
            processor: Track processor for handling track data
            cache: Cache manager for storing fetched data
            logger: Logger instance
            browser: Browser to use for cookies (optional)
        """
        self.client = client
        self.processor = processor
        self.cache = cache
        self.logger = logger
        self.browser = browser
        # Initialize playlist registry with liked songs and fetch others
        self._initialize_playlist_registry()
        # Start auto-refresh in a background thread
        threading.Thread(target=self._run_auto_refresh, daemon=True).start()

    def _initialize_playlist_registry(self):
        """Initialize the playlist registry with playlists and albums."""
        # Fetch playlists
        playlists = self.client.get_library_playlists(
            limit=1000
        )  # Initial fetch for IDs
        for p in playlists:
            sanitized_name = self.processor.sanitize_filename(p["title"])
            self.PLAYLIST_REGISTRY.append(
                {
                    "name": sanitized_name,
                    "id": p["playlistId"],
                    "type": "playlist",
                    "path": f"/playlists/{sanitized_name}",
                }
            )

        # Fetch albums
        albums = self.client.get_library_albums(limit=1000)  # Initial fetch for IDs
        for a in albums:
            sanitized_name = self.processor.sanitize_filename(a["title"])
            self.PLAYLIST_REGISTRY.append(
                {
                    "name": sanitized_name,
                    "id": a["browseId"],  # Albums use browseId as playlist ID
                    "type": "album",
                    "path": f"/albums/{sanitized_name}",
                }
            )

        self.logger.info(
            f"Initialized playlist registry with {len(self.PLAYLIST_REGISTRY)} entries"
        )

    def fetch_and_cache(
        self,
        path: str,
        fetch_func: Callable,
        limit: int = 10000,
        process_func: Optional[Callable] = None,
        auto_refresh: bool = True,
    ):
        """Centralized helper to fetch and cache data with consistent logic.

        Args:
            path: The cache path to use
            fetch_func: Function to call to fetch the data if not cached
            limit: Limit parameter to pass to fetch_func
            process_func: Optional function to process data after fetching
            auto_refresh: Whether to enable auto-refresh for this cache entry

        Returns:
            The cached or fetched data, optionally processed
        """
        # Handle cache auto-refreshing if enabled
        if auto_refresh:
            self._auto_refresh_cache()

        # Check if we have cached data
        data = self.cache.get(path)
        if not data:
            # Fetch data outside of any locks
            self.logger.debug(f"Fetching data from API for {path}")
            data = fetch_func(limit=limit)
            self.cache.set(path, data)

        # Process data if a processing function is provided
        if process_func and data:
            return process_func(data)

        return data

    def _auto_refresh_cache(self, refresh_interval: int = 600) -> None:
        """Auto-refresh all playlist caches every 10 minutes."""
        now = time.time()
        for playlist in self.PLAYLIST_REGISTRY:
            cache_key = f"{playlist['path']}_processed"
            last_refresh = self.cache.get_last_refresh(cache_key)
            if last_refresh and (now - last_refresh) < refresh_interval:
                continue
            self.logger.debug(
                f"Auto-refreshing {playlist['path']} with ID {playlist['id']}"
            )
            self.fetch_playlist_content(playlist["id"], playlist["path"], limit=100)
            self.cache.set_last_refresh(cache_key, now)

    def get_playlist_info(self, source: str, data: Dict) -> Dict[str, str]:
        """Standardize playlist metadata for playlists, liked songs, and albums.

        Args:
            source: Source of the data ('playlists', 'liked_songs', 'albums')
            data: Raw data from API or static definition

        Returns:
            Dict with 'name', 'id', and 'type'
        """
        if source == "playlists":
            return {
                "name": self.processor.sanitize_filename(data["title"]),
                "id": data["playlistId"],
                "type": "playlist",
            }
        elif source == "liked_songs":
            return {
                "name": "liked_songs",
                "id": "LM",  # YouTube Music's liked songs playlist ID
                "type": "liked_songs",
            }
        elif source == "albums":
            return {
                "name": self.processor.sanitize_filename(data["title"]),
                "id": data["browseId"],  # Albums use browseId as playlist identifier
                "type": "album",
            }
        else:
            self.logger.error(f"Unknown playlist source: {source}")
            return {"name": "", "id": "", "type": ""}

    def fetch_playlist_content(
        self, playlist_id: str, path: str, limit: int = 10000
    ) -> List[str]:
        """Fetch playlist content using yt-dlp with a specified limit and cache durations.

        Args:
            playlist_id: Playlist ID (e.g., 'PL123', 'LM', 'MPREb_abc123')
            path: Filesystem path for caching
            limit: Maximum number of tracks to fetch (default: 10000)

        Returns:
            List of track filenames
        """
        cache_key = f"{path}_processed"
        processed_tracks = self.cache.get(cache_key)
        if processed_tracks:
            self.logger.debug(f"Using {len(processed_tracks)} cached tracks for {path}")
            for track in processed_tracks:
                track["is_directory"] = False
            self._cache_directory_listing_with_attrs(path, processed_tracks)
            return [track["filename"] for track in processed_tracks]

        playlist_url = f"https://music.youtube.com/playlist?list={playlist_id}"
        ydl_opts = {
            "extract_flat": True,
            "quiet": True,
            "no_warnings": True,
            "ignoreerrors": True,
            "playlistend": limit,  # Limit the number of entries fetched
        }
        if self.browser:
            ydl_opts["cookiesfrombrowser"] = (self.browser,)

        self.logger.debug(
            f"Fetching up to {limit} tracks for playlist ID: {playlist_id} via yt-dlp"
        )
        from yt_dlp import YoutubeDL

        try:
            with YoutubeDL(ydl_opts) as ydl:
                result = ydl.extract_info(playlist_url, download=False)
                if not result or "entries" not in result:
                    self.logger.warning(
                        f"No tracks found for playlist ID: {playlist_id}"
                    )
                    return []

                tracks = result["entries"][:limit]  # Ensure we respect the limit
                self.logger.info(f"Fetched {len(tracks)} tracks for {playlist_id}")

                processed_tracks = []
                for entry in tracks:
                    if not entry:
                        continue

                    video_id = entry.get("id")
                    duration_seconds = (
                        int(entry.get("duration", 0))
                        if entry.get("duration") is not None
                        else None
                    )

                    if video_id and duration_seconds is not None:
                        self.cache.set_duration(video_id, duration_seconds)

                    track_info = {
                        "title": entry.get("title", "Unknown Title"),
                        "artist": entry.get("uploader", "Unknown Artist"),
                        "videoId": video_id,
                        "duration_seconds": duration_seconds,
                        "is_directory": False,
                    }

                    filename = self.processor.sanitize_filename(
                        f"{track_info['artist']} - {track_info['title']}.m4a"
                    )
                    track_info["filename"] = filename

                    processed_track = self.processor.extract_track_info(track_info)
                    processed_track["filename"] = filename
                    processed_track["is_directory"] = False
                    processed_tracks.append(processed_track)

                self.cache.set(cache_key, processed_tracks)
                self._cache_directory_listing_with_attrs(path, processed_tracks)
                return [track["filename"] for track in processed_tracks]
        except Exception as e:
            self.logger.error(f"Error fetching playlist content: {str(e)}")
            return []

    def readdir_playlists(self) -> List[str]:
        """List all user playlists from the registry."""
        self.logger.info("Fetching playlists for /playlists directory")
        playlist_entries = [
            p for p in self.PLAYLIST_REGISTRY if p["type"] == "playlist"
        ]
        processed_entries = []
        for p in playlist_entries:
            # Fetch initial content (up to 10000 tracks)
            self.fetch_playlist_content(p["id"], p["path"], limit=10000)
            processed_entries.append(
                {"filename": p["name"], "id": p["id"], "is_directory": True}
            )
        self._cache_directory_listing_with_attrs("/playlists", processed_entries)
        return [".", ".."] + [p["name"] for p in playlist_entries]

    def _cache_directory_listing_with_attrs(
        self, dir_path: str, processed_tracks: List[Dict[str, Any]]
    ) -> None:
        """Cache directory listing with file attributes for efficient lookups.

        Args:
            dir_path: Directory path
            processed_tracks: List of processed track dictionaries
        """
        # This is a callback to the filesystem class
        # The actual implementation is in the filesystem class
        # We'll need to set a callback function from the filesystem
        if hasattr(self, "cache_directory_callback") and callable(
            self.cache_directory_callback
        ):
            # Pass processed_tracks with explicit is_directory flag unchanged
            self.cache_directory_callback(dir_path, processed_tracks)
        else:
            self.logger.warning(
                "No callback set for caching directory listings with attributes"
            )

    def readdir_artists(self) -> List[str]:
        """Handle listing artists.

        Returns:
            List of artist names
        """

        # Define a processing function for the artists data
        def process_artists(artists):
            sanitized_names = []
            processed_artists = []

            # More robust handling with error checking
            for artist in artists:
                # Skip invalid entries
                if not isinstance(artist, dict):
                    self.logger.warning(f"Skipping invalid artist entry: {artist}")
                    continue

                # Use .get() with default value to safely handle missing 'artist' keys
                name = artist.get("artist", "Unknown Artist")

                # YouTube Music uses browseId for artists, not artistId
                browse_id = artist.get("browseId")

                # Skip artists without browseId
                if not browse_id:
                    self.logger.warning(f"Artist {name} has no browseId, skipping")
                    continue

                self.logger.debug(f"Processing artist - Name: {name}, ID: {browse_id}")
                sanitized_name = self.processor.sanitize_filename(name)

                sanitized_names.append(sanitized_name)

                # Add to processed artists list for directory caching
                processed_artists.append(
                    {
                        "filename": sanitized_name,
                        "browseId": browse_id,  # Store browseId instead of artistId
                        "is_directory": True,  # Artists are always directories
                    }
                )

            # Cache directory listing with attributes
            if hasattr(self, "cache_directory_callback") and callable(
                self.cache_directory_callback
            ):
                self.cache_directory_callback("/artists", processed_artists)
                self.logger.debug(
                    f"Cached {len(processed_artists)} artists as directories"
                )

            return sanitized_names

        # Use the centralized helper to fetch and process artists
        return self.fetch_and_cache(
            path="/artists",
            fetch_func=self.client.get_library_artists,
            limit=1000,
            process_func=process_artists,
        )

    def _log_object_structure(self, obj, indent=0, max_depth=3, current_depth=0):
        """Helper method to log the structure of an object with indentation.

        Args:
            obj: The object to log
            indent: Current indentation level
            max_depth: Maximum depth to traverse
            current_depth: Current depth in the traversal
        """
        if current_depth > max_depth:
            self.logger.info("  " * indent + "... (max depth reached)")
            return

        if isinstance(obj, dict):
            for key, value in obj.items():
                if isinstance(value, (dict, list)) and value:
                    self.logger.info("  " * indent + f"{key}: {type(value).__name__}")
                    self._log_object_structure(
                        value, indent + 1, max_depth, current_depth + 1
                    )
                else:
                    # For simple values, log the actual value
                    value_str = str(value)
                    # Truncate long values
                    if len(value_str) > 100:
                        value_str = value_str[:97] + "..."
                    self.logger.info(
                        "  " * indent + f"{key}: {value_str} ({type(value).__name__})"
                    )
        elif isinstance(obj, list):
            if obj:
                # For lists, log the first few items
                for i, item in enumerate(obj[:3]):
                    if i == 0:
                        self.logger.info(
                            "  " * indent + f"[{i}]: {type(item).__name__}"
                        )
                    else:
                        self.logger.info(
                            "  " * indent + f"[{i}]: {type(item).__name__}"
                        )
                    if isinstance(item, (dict, list)):
                        self._log_object_structure(
                            item, indent + 1, max_depth, current_depth + 1
                        )
                if len(obj) > 3:
                    self.logger.info("  " * indent + f"... ({len(obj) - 3} more items)")
            else:
                self.logger.info("  " * indent + "[] (empty list)")
        else:
            self.logger.info("  " * indent + str(obj))

    def readdir_albums(self) -> List[str]:
        """List all albums from the registry."""
        self.logger.info("Fetching albums for /albums directory")
        album_entries = [a for a in self.PLAYLIST_REGISTRY if a["type"] == "album"]
        processed_entries = []
        for a in album_entries:
            # Fetch initial content (up to 10000 tracks)
            self.fetch_playlist_content(a["id"], a["path"], limit=10000)
            processed_entries.append(
                {"filename": a["name"], "id": a["id"], "is_directory": True}
            )
        self._cache_directory_listing_with_attrs("/albums", processed_entries)
        return [".", ".."] + [a["name"] for a in album_entries]

    def readdir_liked_songs(self) -> List[str]:
        """List liked songs from the registry."""
        self.logger.info("Fetching liked songs for /liked_songs directory")
        liked_songs_entry = next(
            (p for p in self.PLAYLIST_REGISTRY if p["type"] == "liked_songs"), None
        )
        if not liked_songs_entry:
            self.logger.error("Liked songs not found in registry")
            return [".", ".."]
        # Fetch initial content (up to 10000 tracks)
        filenames = self.fetch_playlist_content(
            liked_songs_entry["id"], liked_songs_entry["path"], limit=10000
        )
        return [".", ".."] + filenames

    def readdir_artist_content(self, path: str) -> List[str]:
        """Handle listing artist content (albums, singles, songs).

        Args:
            path: Path to the artist directory

        Returns:
            List of content item names
        """
        # Extract artist name from path
        if not path.startswith("/artists/"):
            self.logger.error(f"Invalid artist path: {path}")
            return []

        artist_name = os.path.basename(path)
        self.logger.debug(f"Handling artist content for: {artist_name}")

        # Check cached directory listing for artist ID
        dir_listing = self.cache.get_directory_listing_with_attrs("/artists")
        browse_id = None
        if dir_listing and artist_name in dir_listing:
            browse_id = dir_listing[artist_name].get(
                "browseId"
            )  # Use browseId instead of artistId
            self.logger.debug(f"Retrieved artist browseId from cache: {browse_id}")
        else:
            self.logger.debug(
                f"Artist {artist_name} not in cached listing, falling back to search"
            )
            # Fall back to the original method if not in directory listing
            artists = self.cache.get("/artists")
            if not artists:
                self.logger.warning("No cached artists found")
                return []

            # Find the artist by name with fallback for missing 'name'
            for artist in artists:
                # Use .get() with default to avoid KeyError
                name = artist.get("name", artist.get("artist", "Unknown Artist"))
                sanitized_name = self.processor.sanitize_filename(name)
                self.logger.debug(
                    f"Comparing artist names: '{sanitized_name}' vs '{artist_name}'"
                )
                if sanitized_name == artist_name:
                    browse_id = artist.get(
                        "browseId"
                    )  # Use browseId instead of artistId
                    self.logger.debug(
                        f"Found matching artist - Name: {name}, ID: {browse_id}"
                    )
                    break

        if not browse_id:
            self.logger.warning(f"Artist browse ID not found for: {artist_name}")
            return []

        # First check cache for the artist data
        cache_key = f"{path}_data"
        artist_data = self.cache.get(cache_key)

        if not artist_data:
            # Fetch the artist content
            self.logger.debug(f"Fetching content for artist ID: {browse_id}")
            artist_data = self.client.get_artist(browse_id)  # Pass browseId

            if artist_data:
                self.cache.set(cache_key, artist_data)
            else:
                self.logger.warning(f"No data fetched for artist ID: {browse_id}")
                return []

        if not artist_data:
            self.logger.warning(f"No data found for artist: {artist_name}")
            return []

        # Extract content categories
        categories = ["Albums", "Singles", "Songs"]
        content_items = []
        processed_content = []

        # Add albums
        if "albums" in artist_data and artist_data["albums"].get("results"):
            albums = artist_data["albums"]["results"]
            if albums:
                content_items.append("Albums")
                # Add Albums to processed content
                processed_content.append(
                    {
                        "filename": "Albums",
                        "is_directory": True,  # Albums is a directory
                    }
                )

                # Add individual albums for direct access
                for album in albums:
                    album_name = self.processor.sanitize_filename(album["title"])
                    # Cache the album data for later use
                    album_cache_key = f"{path}/Albums/{album_name}_data"
                    self.cache.set(album_cache_key, album)

        # Add singles
        if "singles" in artist_data and artist_data["singles"].get("results"):
            singles = artist_data["singles"]["results"]
            if singles:
                content_items.append("Singles")
                # Add Singles to processed content
                processed_content.append(
                    {
                        "filename": "Singles",
                        "is_directory": True,  # Singles is a directory
                    }
                )

                # Cache singles data for later use
                singles_cache_key = f"{path}/Singles_data"
                self.cache.set(singles_cache_key, singles)

        # Add songs
        if "songs" in artist_data and artist_data["songs"].get("results"):
            songs = artist_data["songs"]["results"]
            if songs:
                content_items.append("Songs")
                # Add Songs to processed content
                processed_content.append(
                    {"filename": "Songs", "is_directory": True}  # Songs is a directory
                )

                # Process and cache the songs
                processed_songs = self.processor.process_tracks(songs)
                # Set is_directory flag for each song
                for song in processed_songs:
                    song["is_directory"] = False  # Individual songs are files

                songs_cache_key = f"{path}/Songs_processed"
                self.cache.set(songs_cache_key, processed_songs)

                # Cache directory listing with attributes for efficient getattr lookups
                songs_dir_path = f"{path}/Songs"
                self._cache_directory_listing_with_attrs(
                    songs_dir_path, processed_songs
                )

        # Cache the directory listing for the artist path
        if processed_content:
            self._cache_directory_listing_with_attrs(path, processed_content)

        return content_items

    def readdir_search_categories(self) -> List[str]:
        """Handle listing search categories.

        Returns:
            List of search categories
        """
        # Return hardcoded list of search categories
        categories = ["library", "catalog"]

        # Create processed entries with is_directory flags
        processed_categories = []
        for category in categories:
            processed_categories.append(
                {
                    "filename": category,
                    "is_directory": True,  # Categories are directories
                }
            )

        # Cache directory listing
        if processed_categories:
            self._cache_directory_listing_with_attrs("/search", processed_categories)

        return categories

    def readdir_search_category_options(self) -> List[str]:
        """Handle listing search category options.

        Returns:
            List of search category options
        """
        # Return hardcoded list of search category options
        options = [
            "songs",
            "albums",
            "artists",
            "playlists",
            "videos",
            "podcasts",
        ]

        return options

    def _cache_search_results(
        self,
        path: str,
        results: List[Dict],
        scope: Optional[str],
        filter_type: str,
        query: str,
    ) -> None:
        """Cache search results and metadata.

        Args:
            path: Full search path
            results: Search results from API
            scope: 'library' or None
            filter_type: Result type (e.g., 'songs')
            query: Search query string
        """
        cache_key = f"{path}_results"
        self.cache.set(cache_key, results)
        self.cache.store_search_metadata(path, query, scope, filter_type)
        self.cache.add_valid_dir(path)  # Mark directory as valid
        self.logger.debug(f"Cached search results and metadata for {path}")

    def readdir_search_results(
        self, path: str, *args, scope: Optional[str] = None
    ) -> List[str]:
        """Handle listing search results with categorization.

        Args:
            path: Search path (e.g., '/search/library/songs/query')
            scope: 'library' or None for catalog

        Returns:
            List of categorized directory entries
        """
        # Parse the search path
        parts = path.split("/")
        self.logger.debug(f"Search path parts: {parts}")

        # Handle direct scope path like /search/library or /search/catalog
        if len(parts) == 3 and parts[2] in ["library", "catalog"]:
            # Create directory entries for category options
            categories = self.readdir_search_category_options()
            processed_categories = []
            for category in categories:
                processed_categories.append(
                    {
                        "filename": category,
                        "is_directory": True,  # All search categories are directories
                    }
                )

            # Cache directory listing
            if processed_categories:
                self._cache_directory_listing_with_attrs(path, processed_categories)

            return categories

        # Handle scope and filter type like /search/library/songs
        if len(parts) >= 4:
            search_scope = parts[2]  # library or catalog
            filter_type = parts[3]  # songs, albums, etc.

            # If there's no search query provided
            if len(parts) == 4:
                # We could cache this as a directory, but it's just a placeholder
                return ["search_query_placeholder"]

            # Extract search query from path
            if len(parts) >= 5:
                # The search query is the 5th part
                search_query = parts[4]
                self.logger.info(
                    f"Performing search: '{search_query}' in {search_scope}/{filter_type}"
                )

                # Generate the full path for caching
                full_path = f"/search/{search_scope}/{filter_type}/{search_query}"
                cache_key = f"{full_path}_results"

                # Check cache with metadata
                cached_data = self.cache.get(cache_key)
                metadata = self.cache.get(f"{full_path}_search_metadata")

                if (
                    cached_data
                    and metadata
                    and metadata.get("last_refresh", 0) > time.time() - 3600
                ):  # 1-hour validity
                    self.logger.debug(f"Using cached search results for {full_path}")
                    search_results = cached_data
                else:
                    # Perform the search
                    search_results = self._perform_search(
                        search_query,
                        scope="library" if search_scope == "library" else None,
                        filter_type=filter_type,
                    )

                    # Cache the results with metadata
                    if search_results:
                        self._cache_search_results(
                            full_path,
                            search_results,
                            "library" if search_scope == "library" else None,
                            filter_type,
                            search_query,
                        )

                # Process the search results into categorized directory listings
                if search_results:
                    # Categorize results
                    dirs = []
                    processed_dirs = []
                    for category, items in search_results.items():
                        if items:
                            if category == "top_result" and items:
                                item = items[0]
                                category_name = (
                                    item.get("type", "unknown")
                                    .lower()
                                    .replace(" ", "_")
                                )
                                dir_name = f"top_{category_name}"
                                dirs.append(dir_name)
                                processed_dirs.append(
                                    {
                                        "filename": dir_name,
                                        "is_directory": True,  # Always a directory
                                    }
                                )
                            elif items:
                                # Normalize the category name
                                category_name = (
                                    category.replace("_", "").lower().replace(" ", "_")
                                )
                                if category_name not in dirs:  # Avoid duplicates
                                    dirs.append(category_name)
                                    processed_dirs.append(
                                        {
                                            "filename": category_name,
                                            "is_directory": True,  # Always a directory
                                        }
                                    )

                    # Cache the directory listing
                    if processed_dirs:
                        self._cache_directory_listing_with_attrs(
                            full_path, processed_dirs
                        )

                    return [".", ".."] + dirs

        return [".", ".."]

    def readdir_search_item_content(
        self, path: str, *args, scope: Optional[str] = None
    ) -> List[str]:
        """Handle listing content of a search result item.

        Args:
            path: Path to the search result item
            *args: Additional arguments
            scope: Search scope (library or None for general)

        Returns:
            List of item content filenames
        """
        # Parse the search path
        parts = path.split("/")
        self.logger.debug(f"Search item path parts: {parts}")

        # We need at least 5 parts: /search/scope/filter/query/item
        if len(parts) < 6:
            self.logger.error(f"Invalid search item path: {path}")
            return []

        search_scope = parts[2]  # library or catalog
        filter_type = parts[3]  # songs, albums, etc.
        search_query = parts[4]  # the search query

        # The item category depends on the filter type
        # For songs filter, the result will be a list of songs
        # For albums filter, we get a specific album
        # For artists filter, we get a specific artist

        # Extract item name/category
        item_name = parts[5]

        # For subsequent parts in the path, handle artist albums, etc.
        item_category = filter_type[:-1] if filter_type.endswith("s") else filter_type

        # Perform the search (if not already cached)
        cache_key = f"/search/{search_scope}/{filter_type}/{search_query}_results"
        search_results = self.cache.get(cache_key)

        if not search_results:
            search_results = self._perform_search(
                search_query,
                scope="library" if search_scope == "library" else None,
                filter_type=filter_type,
            )

        if not search_results:
            self.logger.warning(f"No search results found for: {search_query}")
            return []

        # Process the search result items
        return self._process_search_result_items(search_results, item_category, path)

    def _process_search_results_to_dirs(
        self, search_results: Dict[str, List]
    ) -> List[str]:
        """Process search results into directory listings.

        Args:
            search_results: Dictionary of search results

        Returns:
            List of directory names
        """
        dirs = []

        # Process each result category
        for category, items in search_results.items():
            if items:
                # Add a category if it has items
                if category == "top_result" and items:
                    item = items[0]
                    category_name = item.get("type", "Unknown")
                    dirs.append(f"top_{category_name}")
                elif items:
                    # Normalize the category name
                    category_name = category.replace("_", "")
                    dirs.append(category_name)

        return dirs

    def _perform_search(
        self,
        search_query: str,
        scope: Optional[str] = None,
        filter_type: Optional[str] = None,
    ) -> Dict[str, List]:
        """Perform a search and return the results.

        Args:
            search_query: The search query
            scope: The search scope (library or None for general)
            filter_type: The filter type (songs, albums, etc.)

        Returns:
            Dictionary of search results
        """
        # Normalize the filter type to match API expectations
        api_filter = None
        if filter_type:
            # Convert plural to singular
            filter_singular = (
                filter_type[:-1] if filter_type.endswith("s") else filter_type
            )
            # Map to YTMusic filter values
            filter_map = {
                "song": "songs",
                "album": "albums",
                "artist": "artists",
                "playlist": "playlists",
                "video": "videos",
                "podcast": "podcasts",
            }
            api_filter = filter_map.get(filter_singular)

        # Full path for consistency with readdir_search_results
        full_path = f"/search/{scope or 'catalog'}/{filter_type}/{search_query}"
        cache_key = f"{full_path}_results"

        # Check if we have cached results with fresh metadata
        cached_data = self.cache.get(cache_key)
        metadata = self.cache.get(f"{full_path}_search_metadata")

        if (
            cached_data
            and metadata
            and metadata.get("last_refresh", 0) > time.time() - 3600
        ):  # 1-hour validity
            self.logger.debug(f"Using cached search results for: {search_query}")
            return cached_data

        # Perform the search
        self.logger.info(
            f"Searching for '{search_query}' with filter: {api_filter}, scope: {scope}"
        )

        try:
            if scope == "library":
                results = self.client.search_library(
                    query=search_query, filter=api_filter
                )
            else:
                results = self.client.search(query=search_query, filter=api_filter)

            # Cache results using our dedicated method
            if results:
                self._cache_search_results(
                    full_path, results, scope, filter_type or "", search_query
                )
                return results
        except Exception as e:
            self.logger.error(f"Search error for '{search_query}': {e}")

        return {}

    def _process_search_result_items(
        self, search_results: Dict[str, List], item_category: str, path: str
    ) -> List[str]:
        """Process search result items into filenames.

        Args:
            search_results: Dictionary of search results
            item_category: The item category (song, album, artist)
            path: The current path

        Returns:
            List of filenames
        """
        # Extract path components
        parts = path.split("/")
        if len(parts) < 6:
            self.logger.error(f"Invalid search item path: {path}")
            return []

        # Get the item name from the path
        item_name = parts[5]

        # Different processing based on item category
        if item_category == "song":
            # For songs, find and process the song
            for category, items in search_results.items():
                if not items:
                    continue

                if category in ["songs", "top_result"]:
                    for song in items:
                        if song.get("type") == "song":
                            sanitized_title = self.processor.sanitize_filename(
                                song.get("title", "")
                            )
                            if sanitized_title == item_name:
                                # Process the track
                                processed_track = self.processor.process_track(song)
                                if processed_track:
                                    # Set is_directory flag
                                    processed_track["is_directory"] = (
                                        False  # Songs are files
                                    )

                                    # Cache processed track for later lookups
                                    cache_key = f"{path}_processed"
                                    self.cache.set(cache_key, [processed_track])

                                    # Cache directory listing with attributes
                                    self._cache_directory_listing_with_attrs(
                                        os.path.dirname(path), [processed_track]
                                    )

                                    return [processed_track["filename"]]

        elif item_category == "album":
            # For albums, find the album and process its tracks
            album_browse_id = None

            # Find the album in the search results
            for category, items in search_results.items():
                if not items:
                    continue

                if category in ["albums", "top_result"]:
                    for album in items:
                        if album.get("type") == "album":
                            sanitized_title = self.processor.sanitize_filename(
                                album.get("title", "")
                            )
                            if sanitized_title == item_name:
                                album_browse_id = album.get("browseId")
                                break

            if not album_browse_id:
                self.logger.warning(f"Album not found: {item_name}")
                return []

            # Check if we have processed tracks in cache
            cache_key = f"{path}_processed"
            processed_tracks = self.cache.get(cache_key)
            if processed_tracks:
                self.logger.debug(
                    f"Using cached processed tracks for album: {item_name}"
                )
                # Ensure all tracks have is_directory flag
                for track in processed_tracks:
                    track["is_directory"] = False  # Songs are files

                # Cache directory listing with attributes
                self._cache_directory_listing_with_attrs(path, processed_tracks)
                return [track["filename"] for track in processed_tracks]

            # Fetch album data
            album_data = self.client.get_album(album_browse_id)

            if not album_data or "tracks" not in album_data:
                self.logger.warning(f"No tracks found for album: {item_name}")
                return []

            # Process tracks
            tracks = album_data.get("tracks", [])
            self.logger.info(f"Processing {len(tracks)} tracks from album: {item_name}")
            processed_tracks = self.processor.process_tracks(tracks)

            # Set is_directory flag for each track
            for track in processed_tracks:
                track["is_directory"] = False  # Songs are files

            # Cache processed tracks
            self.cache.set(cache_key, processed_tracks)

            # Cache directory listing with attributes for efficient getattr lookups
            self._cache_directory_listing_with_attrs(path, processed_tracks)

            return [track["filename"] for track in processed_tracks]

        elif item_category == "artist":
            # For artists, find the artist and return categories
            artist_browse_id = None

            # Find the artist in the search results
            for category, items in search_results.items():
                if not items:
                    continue

                if category in ["artists", "top_result"]:
                    for artist in items:
                        if artist.get("type") == "artist":
                            sanitized_name = self.processor.sanitize_filename(
                                artist.get("name", "")
                            )
                            if sanitized_name == item_name:
                                artist_browse_id = artist.get("browseId")
                                break

            if not artist_browse_id:
                self.logger.warning(f"Artist not found: {item_name}")
                return []

            # Check if we have artist data in cache
            cache_key = f"{path}_data"
            artist_data = self.cache.get(cache_key)

            if not artist_data:
                # Fetch artist data
                artist_data = self.client.get_artist(artist_browse_id)
                if artist_data:
                    self.cache.set(cache_key, artist_data)

            if not artist_data:
                self.logger.warning(f"No data found for artist: {item_name}")
                return []

            # Extract content categories
            categories = []
            processed_categories = []

            # Add albums if available
            if "albums" in artist_data and artist_data["albums"].get("results"):
                categories.append("Albums")
                processed_categories.append(
                    {
                        "filename": "Albums",
                        "is_directory": True,  # Albums category is a directory
                    }
                )

            # Add singles if available
            if "singles" in artist_data and artist_data["singles"].get("results"):
                categories.append("Singles")
                processed_categories.append(
                    {
                        "filename": "Singles",
                        "is_directory": True,  # Singles category is a directory
                    }
                )

            # Add songs if available
            if "songs" in artist_data and artist_data["songs"].get("results"):
                categories.append("Songs")
                processed_categories.append(
                    {
                        "filename": "Songs",
                        "is_directory": True,  # Songs category is a directory
                    }
                )

            # Cache the categories listing
            if processed_categories:
                self._cache_directory_listing_with_attrs(path, processed_categories)

            return categories

        # Handle additional parts of the path for artist content
        if item_category == "artist" and len(parts) >= 7:
            category = parts[6]  # Albums, Singles, Songs

            # Check if we have artist data in cache
            cache_key = f"{path[:path.rfind('/' + category)]}_data"
            artist_data = self.cache.get(cache_key)

            if not artist_data:
                self.logger.warning(f"Artist data not found in cache: {item_name}")
                return []

            if category == "Songs":
                # Get songs from artist data
                songs = artist_data.get("songs", {}).get("results", [])

                # Process and cache the songs
                processed_songs = self.processor.process_tracks(songs)
                # Set is_directory flag for each song
                for song in processed_songs:
                    song["is_directory"] = False  # Songs are files

                songs_cache_key = f"{path}_processed"
                self.cache.set(songs_cache_key, processed_songs)

                # Cache directory listing with attributes
                self._cache_directory_listing_with_attrs(path, processed_songs)

                return [song["filename"] for song in processed_songs]

            elif category in ["Albums", "Singles"]:
                # For albums or singles category, return list of album names
                category_key = category.lower()
                items = artist_data.get(category_key, {}).get("results", [])

                # Create processed items with is_directory flags
                processed_items = []
                for item in items:
                    sanitized_title = self.processor.sanitize_filename(item["title"])
                    processed_items.append(
                        {
                            "filename": sanitized_title,
                            "browseId": item.get("browseId"),
                            "is_directory": True,  # Albums/singles are directories
                        }
                    )

                # Cache the directory listing
                if processed_items:
                    self._cache_directory_listing_with_attrs(path, processed_items)

                return [
                    self.processor.sanitize_filename(item["title"]) for item in items
                ]

            # For specific album, if path continues
            if category == "Albums" and len(parts) >= 8:
                album_name = parts[7]

                # Find the album in artist data
                album_browse_id = None
                albums = artist_data.get("albums", {}).get("results", [])

                for album in albums:
                    sanitized_title = self.processor.sanitize_filename(
                        album.get("title", "")
                    )
                    if sanitized_title == album_name:
                        album_browse_id = album.get("browseId")
                        break

                if not album_browse_id:
                    self.logger.warning(f"Album not found: {album_name}")
                    return []

                # Check if we have processed tracks in cache
                album_path = f"{path[:path.rfind('/' + album_name)]}/{album_name}"
                cache_key = f"{album_path}_processed"
                processed_tracks = self.cache.get(cache_key)

                if processed_tracks:
                    # Ensure all tracks have is_directory flag
                    for track in processed_tracks:
                        track["is_directory"] = False  # Songs are files

                    # Cache directory listing with attributes
                    self._cache_directory_listing_with_attrs(
                        album_path, processed_tracks
                    )
                    return [track["filename"] for track in processed_tracks]

                # If tracks aren't cached, we need to fetch the album data
                album_data = self.client.get_album(album_browse_id)

                if not album_data or "tracks" not in album_data:
                    self.logger.warning(f"No tracks found for album: {album_name}")
                    return []

                # Process tracks
                tracks = album_data.get("tracks", [])
                self.logger.info(
                    f"Processing {len(tracks)} tracks from album: {album_name}"
                )
                processed_tracks = self.processor.process_tracks(tracks)

                # Set is_directory flag for each track
                for track in processed_tracks:
                    track["is_directory"] = False  # Songs are files

                # Cache processed tracks
                self.cache.set(cache_key, processed_tracks)

                # Cache directory listing with attributes
                self._cache_directory_listing_with_attrs(album_path, processed_tracks)

                return [track["filename"] for track in processed_tracks]

        return []

    def refresh_all_caches(self) -> None:
        """Refresh all caches with the latest 100 songs from each playlist."""
        self.logger.info("Refreshing all content caches...")
        for playlist in self.PLAYLIST_REGISTRY:
            self.logger.debug(
                f"Refreshing {playlist['type']} at {playlist['path']} with ID {playlist['id']}"
            )
            self.fetch_playlist_content(playlist["id"], playlist["path"], limit=100)
        self.logger.info("All content caches refreshed successfully")

    def _run_auto_refresh(self):
        """Run the auto-refresh loop every 10 minutes."""
        while True:
            self._auto_refresh_cache(refresh_interval=600)
            time.sleep(600)  # Sleep for 10 minutes
