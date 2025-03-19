#!/usr/bin/env python3

from typing import List, Optional, Dict, Any
from ytmusicfs.processor import TrackProcessor
from ytmusicfs.cache import CacheManager
from ytmusicfs.yt_dlp_utils import extract_playlist_content
import logging
import time
import threading
import traceback


class ContentFetcher:
    """Handles fetching and processing of YouTube Music content."""

    # Initialize with empty registry - all entries will be added during initialization
    PLAYLIST_REGISTRY = []

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
        # Initialize playlist registry with all playlist types
        self._initialize_playlist_registry()
        # Start auto-refresh in a background thread
        threading.Thread(target=self._run_auto_refresh, daemon=True).start()

    def get_playlist_id_from_name(
        self, name: str, type_filter: Optional[str] = None
    ) -> Optional[str]:
        """Get playlist ID from its name using the PLAYLIST_REGISTRY.

        Args:
            name: The sanitized name of the playlist/album
            type_filter: Optional type to filter by ('playlist', 'album', 'liked_songs')

        Returns:
            The playlist ID if found, None otherwise
        """
        for entry in self.PLAYLIST_REGISTRY:
            if entry["name"] == name:
                if type_filter is None or entry["type"] == type_filter:
                    return entry["id"]
        return None

    def get_playlist_entry_from_path(self, path: str) -> Optional[Dict[str, Any]]:
        """Get playlist entry from its path using the PLAYLIST_REGISTRY.

        Args:
            path: The filesystem path of the playlist

        Returns:
            The playlist entry dictionary if found, None otherwise
        """
        for entry in self.PLAYLIST_REGISTRY:
            if entry["path"] == path:
                return entry
        return None

    def _initialize_playlist_registry(self):
        """Initialize the playlist registry with all playlist types."""
        # Clear any existing entries
        self.PLAYLIST_REGISTRY = []

        # Add liked songs entry
        self.PLAYLIST_REGISTRY.append(
            {
                "name": "liked_songs",
                "id": "LM",  # YouTube Music's liked songs playlist ID
                "type": "liked_songs",
                "path": "/liked_songs",
            }
        )

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
            path = f"/playlists/{sanitized_name}"
            self.PLAYLIST_REGISTRY.append(
                {
                    "name": sanitized_name,
                    "id": p["playlistId"],
                    "type": "playlist",
                    "path": path,
                }
            )

        # Fetch albums
        albums = self.client.get_library_albums(limit=1000)  # Initial fetch for IDs
        for a in albums:
            sanitized_name = self.processor.sanitize_filename(a["title"])
            path = f"/albums/{sanitized_name}"
            self.PLAYLIST_REGISTRY.append(
                {
                    "name": sanitized_name,
                    "id": a["browseId"],  # Albums use browseId as playlist ID
                    "type": "album",
                    "path": path,
                }
            )

        self.logger.info(
            f"Initialized playlist registry with {len(self.PLAYLIST_REGISTRY)} entries"
        )

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

        # CONSISTENT CACHE KEY: Always use path_processed regardless of playlist type
        cache_key = f"{path}_processed"

        processed_tracks = self.cache.get(cache_key)
        if (
            processed_tracks is not None
        ):  # Check for None specifically to handle empty lists
            self.logger.debug(f"Using {len(processed_tracks)} cached tracks for {path}")
            for track in processed_tracks:
                track["is_directory"] = False
            self._cache_directory_listing_with_attrs(path, processed_tracks)
            return [track["filename"] for track in processed_tracks]

        self.logger.debug(
            f"Fetching up to {limit} tracks for playlist ID: {playlist_id} via yt-dlp"
        )

        try:
            tracks = extract_playlist_content(playlist_id, limit, self.browser)
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
        try:
            # First, ensure PLAYLIST_REGISTRY is initialized
            if not self.PLAYLIST_REGISTRY:
                self.logger.warning(
                    "Playlist registry is empty, attempting to initialize"
                )
                self._initialize_playlist_registry()
                if not self.PLAYLIST_REGISTRY:
                    self.logger.error("Failed to initialize playlist registry")
                    return [".", ".."]

            # Get playlist entries of type "playlist"
            playlist_entries = [
                p for p in self.PLAYLIST_REGISTRY if p["type"] == "playlist"
            ]

            if not playlist_entries:
                self.logger.warning("No playlist entries found in registry")
                return [".", ".."]

            self.logger.debug(f"Found {len(playlist_entries)} playlist entries")

            processed_entries = []
            for p in playlist_entries:
                try:
                    # Make sure we have an ID and a name
                    if not p.get("id") or not p.get("name"):
                        self.logger.warning(f"Skipping invalid playlist entry: {p}")
                        continue

                    # Fetch initial content (up to 10000 tracks)
                    self.logger.debug(
                        f"Fetching content for playlist: {p['name']} (ID: {p['id']})"
                    )
                    self.fetch_playlist_content(p["id"], p["path"], limit=10000)

                    processed_entries.append(
                        {"filename": p["name"], "id": p["id"], "is_directory": True}
                    )
                except Exception as e:
                    self.logger.error(
                        f"Error processing playlist {p.get('name', 'Unknown')}: {str(e)}"
                    )
                    # Continue with next playlist instead of failing completely

            # Only cache if we have entries
            if processed_entries:
                self.logger.debug(
                    f"Caching {len(processed_entries)} processed playlist entries"
                )
                try:
                    self._cache_directory_listing_with_attrs(
                        "/playlists", processed_entries
                    )
                except Exception as e:
                    self.logger.error(f"Error caching directory listing: {str(e)}")
                    # Still continue to return the result even if caching fails
            else:
                self.logger.warning("No playlists processed successfully")

            # Return directory listing
            result = [".", ".."] + [
                p["name"] for p in playlist_entries if p.get("name")
            ]
            self.logger.debug(f"Returning {len(result)-2} playlist entries")
            return result

        except Exception as e:
            self.logger.error(f"Fatal error in readdir_playlists: {str(e)}")
            self.logger.error(traceback.format_exc())
            return [".", ".."]

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

        # Unlike albums/playlists which are directories containing songs,
        # liked_songs directly shows the songs themselves
        # Fetch initial content (up to 10000 tracks)
        filenames = self.fetch_playlist_content(
            liked_songs_entry["id"], liked_songs_entry["path"], limit=10000
        )

        # Return the full directory listing
        return [".", ".."] + filenames

    def refresh_all_caches(self) -> None:
        """Refresh all caches with the latest 100 songs from each playlist."""
        self.logger.info("Refreshing all content caches...")
        for playlist in self.PLAYLIST_REGISTRY:
            # Get all information consistently from the registry
            playlist_id = playlist["id"]
            path = playlist["path"]
            playlist_type = playlist["type"]

            self.logger.debug(
                f"Refreshing {playlist_type} at {path} with ID {playlist_id}"
            )
            # Use consistent caching pattern with path
            self.fetch_playlist_content(playlist_id, path, limit=100)

        self.logger.info("All content caches refreshed successfully")

    def _run_auto_refresh(self):
        """Run the auto-refresh loop every 10 minutes."""
        while True:
            self._auto_refresh_cache(refresh_interval=600)
            time.sleep(600)  # Sleep for 10 minutes
