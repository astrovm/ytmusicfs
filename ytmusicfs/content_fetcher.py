#!/usr/bin/env python3

from typing import List, Optional, Dict, Any
from ytmusicfs.processor import TrackProcessor
from ytmusicfs.cache import CacheManager
import logging
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
        client: Any,  # YouTubeMusicClient
        processor: TrackProcessor,
        cache: CacheManager,
        logger: logging.Logger,
        browser: Optional[str] = None,
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
            # Skip podcast playlist type (SE)
            if p.get("playlistId") == "SE":
                self.logger.info(
                    "Skipping podcast playlist (SE) - podcasts not supported"
                )
                continue

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
        # Simple check for the podcast playlist ID "SE"
        if playlist_id == "SE":
            self.logger.info("Skipping podcast playlist (SE) - podcasts not supported")
            return []

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
                # Collect all durations in a batch
                durations_batch = {}

                for entry in tracks:
                    if not entry:
                        continue

                    video_id = entry.get("id")
                    duration_seconds = (
                        int(entry.get("duration", 0))
                        if entry.get("duration") is not None
                        else None
                    )

                    # Collect durations for batch processing
                    if video_id and duration_seconds is not None:
                        durations_batch[video_id] = duration_seconds

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

                # Cache all durations in a single batch operation
                if durations_batch:
                    self.cache.set_durations_batch(durations_batch)

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
