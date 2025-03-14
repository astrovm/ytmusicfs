#!/usr/bin/env python3

from typing import List, Callable, Optional, Dict, Any
from ytmusicfs.client import YouTubeMusicClient
from ytmusicfs.processor import TrackProcessor
from ytmusicfs.cache import CacheManager
from ytmusicfs.duration_fetcher import DurationFetcher
import logging
import os
import time
import threading


class ContentFetcher:
    """Handles fetching and processing of YouTube Music content."""

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

        # Initialize the DurationFetcher
        self.duration_fetcher = DurationFetcher(browser, logger)

        # Used to control background duration fetching
        self.duration_fetch_lock = threading.Lock()
        self.duration_fetch_tasks = {}  # Track ongoing fetches by playlist ID

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
            self._auto_refresh_cache(path)

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

    def _auto_refresh_cache(self, cache_key: str, refresh_interval: int = 600) -> None:
        """Auto-refresh cache in a background thread after a delay.

        Args:
            cache_key: The cache key to refresh
            refresh_interval: Time in seconds before refresh (default: 10 minutes)
        """
        # Check when this cache key was last refreshed
        last_refresh = self.cache.get_last_refresh(cache_key)
        now = time.time()

        # Only schedule refresh if sufficient time has passed
        if last_refresh and (now - last_refresh) < refresh_interval:
            return

        # Set last refresh time to prevent multiple refreshes
        self.cache.set_last_refresh(cache_key, now)

    def readdir_playlists(self) -> List[str]:
        """Handle listing playlists.

        Returns:
            List of playlist names
        """

        # Define a processing function for the playlists data
        def process_playlists(playlists):
            sanitized_names = []
            processed_playlists = []

            # More robust handling with error checking
            for playlist in playlists:
                # Skip invalid entries
                if not isinstance(playlist, dict):
                    continue

                # Use .get() with default value to safely handle missing 'title' keys
                title = playlist.get("title", "Unknown Playlist")
                sanitized_name = self.processor.sanitize_filename(title)

                sanitized_names.append(sanitized_name)

                # Add to processed playlists list for directory caching
                processed_playlists.append(
                    {
                        "filename": sanitized_name,
                        "playlistId": playlist.get("playlistId"),
                    }
                )

            # Cache directory listing with attributes
            if hasattr(self, "cache_directory_callback") and callable(
                self.cache_directory_callback
            ):
                self.cache_directory_callback("/playlists", processed_playlists)

            return sanitized_names

        # Use the centralized helper to fetch and process playlists
        return self.fetch_and_cache(
            path="/playlists",
            fetch_func=self.client.get_library_playlists,
            limit=1000,
            process_func=process_playlists,
        )

    def readdir_liked_songs(self) -> List[str]:
        """Handle listing liked songs with pre-fetched durations.

        Returns:
            List of liked song filenames
        """
        # First check if we have processed tracks in cache
        processed_tracks = self.cache.get("/liked_songs_processed")
        if processed_tracks:
            self.logger.debug(
                f"Using {len(processed_tracks)} cached processed tracks for /liked_songs"
            )
            # Cache directory listing with attributes for efficient getattr lookups
            self._cache_directory_listing_with_attrs("/liked_songs", processed_tracks)

            return [track["filename"] for track in processed_tracks]

        # Use the centralized helper to fetch liked songs
        liked_songs = self.fetch_and_cache(
            path="/liked_songs",
            fetch_func=self.client.get_liked_songs,
            limit=10000,
            auto_refresh=True,
        )

        # Continue with processing the data
        if not liked_songs or "tracks" not in liked_songs:
            self.logger.warning("No liked songs found or invalid response format")
            return []

        tracks = liked_songs.get("tracks", [])
        self.logger.info(f"Fetching durations for {len(tracks)} liked songs")

        # Synchronously fetch durations for all liked songs
        durations = self.duration_fetcher.fetch_durations_for_liked_songs(
            update_callback=lambda video_id, duration: self.cache.set_duration(
                video_id, duration
            )
        )
        self.logger.info(f"Fetched {len(durations)} durations for liked songs")

        # Process the tracks outside of any locks
        processed_tracks = self.processor.process_tracks(tracks)

        # Apply durations to processed tracks
        for track in processed_tracks:
            video_id = track.get("videoId")
            if video_id and video_id in durations:
                track["duration_seconds"] = durations[video_id]
                track["duration_formatted"] = self.processor._format_duration(
                    durations[video_id]
                )

        # Cache the processed tracks
        self.cache.set("/liked_songs_processed", processed_tracks)

        # Cache directory listing with attributes for efficient getattr lookups
        self._cache_directory_listing_with_attrs("/liked_songs", processed_tracks)

        return [track["filename"] for track in processed_tracks]

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
            self.cache_directory_callback(dir_path, processed_tracks)
        else:
            self.logger.warning(
                "No callback set for caching directory listings with attributes"
            )

    def readdir_playlist_content(self, path: str) -> List[str]:
        """Handle listing playlist contents using yt-dlp.

        Args:
            path: Path to the playlist directory

        Returns:
            List of track filenames
        """
        # Extract playlist name from path
        if not path.startswith("/playlists/"):
            self.logger.error(f"Invalid playlist path: {path}")
            return []

        playlist_name = os.path.basename(path)
        self.logger.debug(f"Handling playlist content for: {playlist_name}")

        # First check cache for the processed tracks
        cache_key = f"{path}_processed"
        processed_tracks = self.cache.get(cache_key)
        if processed_tracks:
            self.logger.debug(
                f"Using {len(processed_tracks)} cached processed tracks for {path}"
            )
            # Cache directory listing with attributes for efficient getattr lookups
            self._cache_directory_listing_with_attrs(path, processed_tracks)
            return [track["filename"] for track in processed_tracks]

        # Get the list of playlists to find the ID
        playlists = self.cache.get("/playlists")
        if not playlists:
            self.logger.warning("No cached playlists found")
            return []

        # Find the playlist by name
        playlist_id = None
        for playlist in playlists:
            sanitized_title = self.processor.sanitize_filename(playlist["title"])
            if sanitized_title == playlist_name:
                playlist_id = playlist["playlistId"]
                break

        if not playlist_id:
            self.logger.warning(f"Playlist not found: {playlist_name}")
            return []

        # Use yt-dlp to fetch playlist content
        self.logger.debug(
            f"Fetching content for playlist ID: {playlist_id} using yt-dlp"
        )
        playlist_url = f"https://music.youtube.com/playlist?list={playlist_id}"
        ydl_opts = {
            "extract_flat": True,  # Get metadata without downloading
            "quiet": True,
            "no_warnings": True,
            "ignoreerrors": True,
        }
        if self.browser:
            ydl_opts["cookiesfrombrowser"] = (self.browser,)

        from yt_dlp import YoutubeDL

        try:
            with YoutubeDL(ydl_opts) as ydl:
                result = ydl.extract_info(playlist_url, download=False)

                if not result or "entries" not in result:
                    self.logger.warning(
                        f"No tracks found for playlist: {playlist_name}"
                    )
                    return []

                tracks = result["entries"]
                self.logger.info(
                    f"Fetched {len(tracks)} tracks from playlist: {playlist_name}"
                )

                # Process tracks with yt-dlp metadata
                processed_tracks = []
                for entry in tracks:
                    if not entry:
                        continue

                    track_info = {
                        "title": entry.get("title", "Unknown Title"),
                        "artist": entry.get("uploader", "Unknown Artist"),
                        "videoId": entry.get("id"),
                        "duration_seconds": (
                            int(entry.get("duration", 0))
                            if entry.get("duration")
                            else None
                        ),
                    }

                    # Format the filename
                    filename = self.processor.sanitize_filename(
                        f"{track_info['artist']} - {track_info['title']}.m4a"
                    )
                    track_info["filename"] = filename

                    # Process through the track processor to ensure consistent format
                    processed_track = self.processor.extract_track_info(track_info)
                    processed_track["filename"] = filename
                    processed_tracks.append(processed_track)
        except Exception as e:
            self.logger.error(f"Error fetching playlist content with yt-dlp: {str(e)}")
            return []

        # Cache the processed tracks without stream URLs
        self.cache.set(cache_key, processed_tracks)

        # Cache directory listing with attributes for efficient getattr lookups
        self._cache_directory_listing_with_attrs(path, processed_tracks)

        # Start background duration fetching for this playlist
        self._fetch_durations_for_playlist(playlist_id)

        return [track["filename"] for track in processed_tracks]

    def _fetch_durations_for_playlist(self, playlist_id: str) -> None:
        """Start background duration fetching for a playlist if not already running.

        Args:
            playlist_id: The playlist ID to fetch durations for
        """
        with self.duration_fetch_lock:
            # Check if we're already fetching durations for this playlist
            if playlist_id in self.duration_fetch_tasks:
                self.logger.debug(
                    f"Duration fetching for playlist {playlist_id} already in progress"
                )
                return

            self.logger.info(
                f"Starting background duration fetching for playlist {playlist_id}"
            )

            def on_complete(durations):
                self.logger.info(
                    f"Completed fetching {len(durations)} durations for playlist {playlist_id}"
                )
                with self.duration_fetch_lock:
                    if playlist_id in self.duration_fetch_tasks:
                        del self.duration_fetch_tasks[playlist_id]

            # Start the background fetch
            self.duration_fetcher.fetch_durations_background(
                playlist_id, self.cache, on_complete
            )
            self.duration_fetch_tasks[playlist_id] = True

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
                    continue

                # Use .get() with default value to safely handle missing 'artist' keys
                name = artist.get("artist", "Unknown Artist")
                self.logger.debug(f"Processing artist: {name}")
                sanitized_name = self.processor.sanitize_filename(name)

                sanitized_names.append(sanitized_name)

                # Add to processed artists list for directory caching
                processed_artists.append(
                    {
                        "filename": sanitized_name,
                        "artistId": artist.get("artistId"),
                        "is_directory": True,  # Add flag for clarity
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

    def readdir_albums(self) -> List[str]:
        """Handle listing albums.

        Returns:
            List of album names
        """

        # Define a processing function for the albums data
        def process_albums(albums):
            sanitized_names = []
            processed_albums = []

            # More robust handling with error checking
            for album in albums:
                # Skip invalid entries
                if not isinstance(album, dict):
                    continue

                # Use .get() with default value to safely handle missing 'title' keys
                title = album.get("title", "Unknown Album")
                sanitized_name = self.processor.sanitize_filename(title)

                sanitized_names.append(sanitized_name)

                # Add to processed albums list for directory caching
                processed_albums.append(
                    {"filename": sanitized_name, "browseId": album.get("browseId")}
                )

            # Cache directory listing with attributes
            if hasattr(self, "cache_directory_callback") and callable(
                self.cache_directory_callback
            ):
                self.cache_directory_callback("/albums", processed_albums)

            return sanitized_names

        # Use the centralized helper to fetch and process albums
        return self.fetch_and_cache(
            path="/albums",
            fetch_func=self.client.get_library_albums,
            limit=1000,
            process_func=process_albums,
        )

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

        # First check cache for the artist data
        cache_key = f"{path}_data"
        artist_data = self.cache.get(cache_key)

        if not artist_data:
            # Get the list of artists to find the ID
            artists = self.cache.get("/artists")
            if not artists:
                self.logger.warning("No cached artists found")
                return []

            # Find the artist by name with fallback for missing 'name'
            artist_id = None
            for artist in artists:
                # Use .get() with default to avoid KeyError
                name = artist.get("name", artist.get("artist", "Unknown Artist"))
                sanitized_name = self.processor.sanitize_filename(name)
                if sanitized_name == artist_name:
                    artist_id = artist.get("artistId")
                    break

            if not artist_id:
                self.logger.warning(f"Artist not found: {artist_name}")
                return []

            # Fetch the artist content
            self.logger.debug(f"Fetching content for artist ID: {artist_id}")
            artist_data = self.client.get_artist(artist_id)

            if artist_data:
                self.cache.set(cache_key, artist_data)

        if not artist_data:
            self.logger.warning(f"No data found for artist: {artist_name}")
            return []

        # Extract content categories
        categories = ["Albums", "Singles", "Songs"]
        content_items = []

        # Add albums
        if "albums" in artist_data and artist_data["albums"].get("results"):
            albums = artist_data["albums"]["results"]
            if albums:
                content_items.append("Albums")
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
                # Cache singles data for later use
                singles_cache_key = f"{path}/Singles_data"
                self.cache.set(singles_cache_key, singles)

        # Add songs
        if "songs" in artist_data and artist_data["songs"].get("results"):
            songs = artist_data["songs"]["results"]
            if songs:
                content_items.append("Songs")
                # Process and cache the songs
                processed_songs = self.processor.process_tracks(songs)
                songs_cache_key = f"{path}/Songs_processed"
                self.cache.set(songs_cache_key, processed_songs)

                # Cache directory listing with attributes for efficient getattr lookups
                songs_dir_path = f"{path}/Songs"
                self._cache_directory_listing_with_attrs(
                    songs_dir_path, processed_songs
                )

        return content_items

    def readdir_album_content(self, path: str) -> List[str]:
        """Handle listing album tracks.

        Args:
            path: Path to the album directory

        Returns:
            List of track filenames
        """
        # Extract album path components
        if path.startswith("/artists/"):
            # Path format: /artists/{artist_name}/Albums/{album_name}
            parts = path.split("/")
            if len(parts) < 5:
                self.logger.error(f"Invalid artist album path: {path}")
                return []

            artist_name = parts[2]
            album_type = parts[3]  # Should be "Albums"
            album_name = parts[4]

            # Get album data from cache
            album_data_key = f"/artists/{artist_name}/Albums/{album_name}_data"
            album_data = self.cache.get(album_data_key)

            if not album_data:
                self.logger.warning(f"Album data not found in cache: {album_name}")
                return []

            album_id = album_data.get("browseId")

        elif path.startswith("/albums/"):
            # Path format: /albums/{album_name}
            album_name = os.path.basename(path)
            self.logger.debug(f"Handling album content for: {album_name}")

            # Get the list of albums to find the ID
            albums = self.cache.get("/albums")
            if not albums:
                self.logger.warning("No cached albums found")
                return []

            # Find the album by name
            album_id = None
            for album in albums:
                sanitized_title = self.processor.sanitize_filename(album["title"])
                if sanitized_title == album_name:
                    album_id = album["browseId"]
                    break

            if not album_id:
                self.logger.warning(f"Album not found: {album_name}")
                return []
        else:
            self.logger.error(f"Invalid album path: {path}")
            return []

        # First check cache for the processed tracks
        cache_key = f"{path}_processed"
        processed_tracks = self.cache.get(cache_key)
        if processed_tracks:
            self.logger.debug(
                f"Using {len(processed_tracks)} cached processed tracks for {path}"
            )
            # Cache directory listing with attributes for efficient getattr lookups
            self._cache_directory_listing_with_attrs(path, processed_tracks)
            return [track["filename"] for track in processed_tracks]

        # Fetch the album content
        self.logger.debug(f"Fetching content for album ID: {album_id}")
        album_data = self.client.get_album(album_id)

        if not album_data or "tracks" not in album_data:
            self.logger.warning(
                f"No tracks found or invalid response format for album: {album_name}"
            )
            return []

        # Process the tracks
        tracks = album_data.get("tracks", [])
        self.logger.info(f"Processing {len(tracks)} tracks from album: {album_name}")
        processed_tracks = self.processor.process_tracks(tracks)

        # Cache the processed tracks
        self.cache.set(cache_key, processed_tracks)

        # Cache directory listing with attributes for efficient getattr lookups
        self._cache_directory_listing_with_attrs(path, processed_tracks)

        return [track["filename"] for track in processed_tracks]

    def readdir_search_categories(self) -> List[str]:
        """Handle listing search categories.

        Returns:
            List of search categories
        """
        # Return hardcoded list of search categories
        return ["library", "catalog"]

    def readdir_search_category_options(self) -> List[str]:
        """Handle listing search category options.

        Returns:
            List of search category options
        """
        # Return hardcoded list of search category options
        return [
            "songs",
            "albums",
            "artists",
            "playlists",
            "videos",
            "podcasts",
        ]

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
            return self.readdir_search_category_options()

        # Handle scope and filter type like /search/library/songs
        if len(parts) >= 4:
            search_scope = parts[2]  # library or catalog
            filter_type = parts[3]  # songs, albums, etc.

            # If there's no search query provided
            if len(parts) == 4:
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
                    for category, items in search_results.items():
                        if items:
                            if category == "top_result" and items:
                                item = items[0]
                                category_name = (
                                    item.get("type", "unknown")
                                    .lower()
                                    .replace(" ", "_")
                                )
                                dirs.append(f"top_{category_name}")
                            elif items:
                                # Normalize the category name
                                category_name = (
                                    category.replace("_", "").lower().replace(" ", "_")
                                )
                                if category_name not in dirs:  # Avoid duplicates
                                    dirs.append(category_name)

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

            # Add albums if available
            if "albums" in artist_data and artist_data["albums"].get("results"):
                categories.append("Albums")

            # Add singles if available
            if "singles" in artist_data and artist_data["singles"].get("results"):
                categories.append("Singles")

            # Add songs if available
            if "songs" in artist_data and artist_data["songs"].get("results"):
                categories.append("Songs")

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
                songs_cache_key = f"{path}_processed"
                self.cache.set(songs_cache_key, processed_songs)

                # Cache directory listing with attributes
                self._cache_directory_listing_with_attrs(path, processed_songs)

                return [song["filename"] for song in processed_songs]

            elif category in ["Albums", "Singles"]:
                # For albums or singles category, return list of album names
                category_key = category.lower()
                items = artist_data.get(category_key, {}).get("results", [])

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
                    # Cache directory listing with attributes
                    self._cache_directory_listing_with_attrs(
                        album_path, processed_tracks
                    )
                    return [track["filename"] for track in processed_tracks]

                # Fetch album data
                album_data = self.client.get_album(album_browse_id)

                if not album_data or "tracks" not in album_data:
                    self.logger.warning(f"No tracks found for album: {album_name}")
                    return []

                # Process tracks
                tracks = album_data.get("tracks", [])
                processed_tracks = self.processor.process_tracks(tracks)

                # Cache processed tracks
                self.cache.set(cache_key, processed_tracks)

                # Cache directory listing with attributes
                self._cache_directory_listing_with_attrs(album_path, processed_tracks)

                return [track["filename"] for track in processed_tracks]

        return []

    def refresh_all_caches(self) -> None:
        """Refresh all caches in a consistent manner.

        This method updates all caches (liked songs, playlists, artists, albums)
        with any changes in the user's library, preserving existing cached data
        and only updating what's changed.
        """
        self.logger.info("Refreshing all content caches...")

        # Refresh liked songs
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

        # Refresh durations for liked songs
        self._refresh_durations_for_liked_songs()

        # Refresh playlists
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

        # Refresh artists
        self.cache.refresh_cache_data(
            cache_key="/artists",
            fetch_func=self.client.get_library_artists,
            id_fields=["artistId", "browseId", "id"],
        )

        # Refresh albums
        self.cache.refresh_cache_data(
            cache_key="/albums",
            fetch_func=self.client.get_library_albums,
            id_fields=["albumId", "browseId", "id"],
        )

        self.logger.info("All content caches refreshed successfully")

    def _refresh_durations_for_liked_songs(self) -> None:
        """Refresh durations for liked songs synchronously.

        This fetches durations for all liked songs in the foreground and updates cached tracks.
        """
        self.logger.info("Refreshing durations for liked songs synchronously...")

        # Fetch durations synchronously
        durations = self.duration_fetcher.fetch_durations_for_liked_songs(
            update_callback=lambda video_id, duration: self.cache.set_duration(
                video_id, duration
            )
        )
        self.logger.info(f"Fetched {len(durations)} durations for liked songs")

        # Update any cached processed tracks with the new durations
        processed_tracks = self.cache.get("/liked_songs_processed")
        if processed_tracks:
            updated = False
            for track in processed_tracks:
                video_id = track.get("videoId")
                if video_id and video_id in durations:
                    duration_seconds = durations[video_id]
                    if track.get("duration_seconds") != duration_seconds:
                        track["duration_seconds"] = duration_seconds
                        track["duration_formatted"] = self.processor._format_duration(
                            duration_seconds
                        )
                        updated = True

            # Only update the cache if tracks were changed
            if updated:
                self.logger.info("Updating cached processed tracks with new durations")
                self.cache.set("/liked_songs_processed", processed_tracks)
                self._cache_directory_listing_with_attrs(
                    "/liked_songs", processed_tracks
                )
