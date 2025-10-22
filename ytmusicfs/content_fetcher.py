#!/usr/bin/env python3

from typing import List, Optional, Dict, Any, Callable
from ytmusicfs.cache import CacheManager
from ytmusicfs.processor import TrackProcessor
from ytmusicfs.yt_dlp_utils import YTDLPUtils
import logging
import time
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
        yt_dlp_utils: YTDLPUtils,
        browser: Optional[str] = None,
    ):
        """Initialize the ContentFetcher.

        Args:
            client: YouTube Music API client
            processor: Track processor for handling track data
            cache: Cache manager for storing fetched data
            logger: Logger instance
            yt_dlp_utils: YTDLPUtils instance for YouTube interaction
            browser: Browser to use for cookies (optional)
        """
        self.client = client
        self.processor = processor
        self.cache = cache
        self.logger = logger
        self.browser = browser
        self.yt_dlp_utils = yt_dlp_utils
        # Preload playlist registry at startup
        self._initialize_playlist_registry()
        self.logger.info("Preloaded playlist registry at initialization")

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

    def _add_registry_entries(
        self,
        fetch_fn: Callable[..., List[Dict[str, Any]]],
        id_key: str,
        type_name: str,
        prefix: str,
        skip_condition: Optional[Callable[[Dict[str, Any]], bool]] = None,
        id_transform: Optional[Callable[[Dict[str, Any]], str]] = None,
        limit: int = 1000,
    ) -> None:
        """Fetch and sanitize entries before appending them to the registry."""

        entries = fetch_fn(limit=limit)
        for entry in entries:
            if skip_condition and skip_condition(entry):
                continue

            entry_id = id_transform(entry) if id_transform else entry.get(id_key)
            if entry_id is None:
                self.logger.debug(
                    "Skipping %s entry without %s", type_name, id_key
                )
                continue

            sanitized_name = self.processor.sanitize_filename(entry["title"])
            path = f"{prefix}/{sanitized_name}"
            self.PLAYLIST_REGISTRY.append(
                {
                    "name": sanitized_name,
                    "id": entry_id,
                    "type": type_name,
                    "path": path,
                }
            )

    def _should_skip_podcast_playlist(self, playlist: Dict[str, Any]) -> bool:
        """Return True when the playlist represents a podcast feed to skip."""

        if playlist.get("playlistId") == "SE":
            self.logger.info("Skipping podcast playlist (SE) - podcasts not supported")
            return True
        return False

    def _initialize_playlist_registry(self, force_refresh: bool = False) -> None:
        """Initialize or refresh the playlist registry with all playlist types.

        Args:
            force_refresh: Whether to force a refresh even if the registry was recently refreshed
        """
        # Check if registry refresh is needed
        cache_key = "playlist_registry"
        last_refresh, status = self.cache.get_refresh_metadata(cache_key)
        refresh_interval = 3600  # 1 hour default

        if (
            not force_refresh
            and last_refresh
            and (time.time() - last_refresh < refresh_interval)
        ):
            self.logger.debug(
                f"Using existing playlist registry (last refreshed: {int(time.time() - last_refresh)}s ago)"
            )
            return

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
        self._add_registry_entries(
            fetch_fn=self.client.get_library_playlists,
            id_key="playlistId",
            type_name="playlist",
            prefix="/playlists",
            skip_condition=self._should_skip_podcast_playlist,
        )

        # Fetch albums
        self._add_registry_entries(
            fetch_fn=self.client.get_library_albums,
            id_key="browseId",
            type_name="album",
            prefix="/albums",
        )

        self.logger.info(
            f"Initialized playlist registry with {len(self.PLAYLIST_REGISTRY)} entries"
        )

        # Record refresh time with status
        self.cache.set_refresh_metadata(cache_key, time.time(), "fresh")

    def fetch_playlist_content(
        self,
        playlist_id: str,
        path: str,
        limit: int = 10000,
        force_refresh: bool = False,
    ) -> List[str]:
        """Fetch playlist content using yt-dlp with a specified limit and cache durations.

        Args:
            playlist_id: Playlist ID (e.g., 'PL123', 'LM', 'MPREb_abc123')
            path: Filesystem path for caching
            limit: Maximum number of tracks to fetch (default: 10000)
            force_refresh: If True, fetch fresh data and merge with existing cache (default: False)

        Returns:
            List of track filenames
        """
        # Simple check for the podcast playlist ID "SE"
        if playlist_id == "SE":
            self.logger.info("Skipping podcast playlist (SE) - podcasts not supported")
            return []

        # CONSISTENT CACHE KEY: Always use path_processed regardless of playlist type
        cache_key = f"{path}_processed"

        # Define the fetch function to be passed to refresh_content
        def fetch_tracks(lim):
            return self.yt_dlp_utils.extract_playlist_content(
                playlist_id, lim, self.browser
            )

        # Use the centralized refresh method
        tracks = self.refresh_content(
            cache_key, fetch_tracks, path, limit, force_refresh
        )

        # Return just the filenames
        return [track["filename"] for track in tracks]

    def readdir_playlist_by_type(
        self, playlist_type: str = None, directory_path: str = None
    ) -> List[str]:
        """List playlists/albums/liked_songs instantly using cached data."""
        if not directory_path:
            directory_path = {
                "playlist": "/playlists",
                "album": "/albums",
                "liked_songs": "/liked_songs",
            }.get(playlist_type, "")
            if not directory_path:
                self.logger.error(f"Invalid playlist type: {playlist_type}")
                return [".", ".."]

        cache_key = f"{directory_path}_listing"
        cached_listing = self.cache.get_directory_listing_with_attrs(directory_path)
        if cached_listing:
            self.logger.debug(f"Instant cache hit for {directory_path}")
            return [".", ".."] + list(cached_listing.keys())

        if playlist_type == "liked_songs":
            entry = next(
                (p for p in self.PLAYLIST_REGISTRY if p["type"] == "liked_songs"), None
            )
            if not entry:
                self.logger.error("Liked songs not found")
                return [".", ".."]
            tracks = self.refresh_content(
                f"{entry['path']}_processed",
                lambda lim: self.yt_dlp_utils.extract_playlist_content(
                    entry["id"], lim, self.browser
                ),
                entry["path"],
            )
            return [".", ".."] + [track["filename"] for track in tracks]

        # For playlists and albums, list directories
        entries = [p for p in self.PLAYLIST_REGISTRY if p["type"] == playlist_type]
        if not entries:
            self.logger.warning(f"No {playlist_type} entries found")
            return [".", ".."]

        processed_entries = [
            {"filename": e["name"], "is_directory": True} for e in entries
        ]
        self._cache_directory_listing_with_attrs(directory_path, processed_entries)
        self.cache.set_refresh_metadata(cache_key, time.time(), "fresh")
        return [".", ".."] + [e["name"] for e in entries]

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

    def check_refresh_needed(
        self, cache_key: str, refresh_interval: int = 3600
    ) -> bool:
        """Check if a cache entry needs refreshing based on its age.

        Args:
            cache_key: The cache key to check
            refresh_interval: Time in seconds before refresh is needed (default: 3600s = 1 hour)

        Returns:
            True if refresh is needed, False otherwise
        """
        now = time.time()
        last_refresh, status = self.cache.get_refresh_metadata(cache_key)

        # Refresh is needed if:
        # - Never refreshed (last_refresh is None)
        # - Older than refresh_interval
        # - Status is "stale"
        if not last_refresh:
            return True
        if status == "stale":
            return True
        if now - last_refresh >= refresh_interval:
            return True

        return False

    def refresh_content(
        self,
        cache_key: str,
        fetch_func: Callable,
        path: str,
        limit: int = 10000,
        force_refresh: bool = False,
    ) -> List[Dict[str, Any]]:
        """Refresh content for a given cache key if needed, returning processed tracks.

        Args:
            cache_key: The cache key to store the content under
            fetch_func: Callable function that retrieves the content
            path: The path for which we're refreshing content
            limit: Maximum number of items to fetch
            force_refresh: Whether to force a refresh even if not needed

        Returns:
            List of processed tracks with metadata
        """
        # Check if refresh is needed
        needs_refresh = force_refresh or self.check_refresh_needed(cache_key)

        # Get existing cached tracks
        existing_tracks = self.cache.get(cache_key) or []

        # If no refresh needed and we have data, use it
        if not needs_refresh and existing_tracks:
            last_refresh, _ = self.cache.get_refresh_metadata(cache_key)
            last_refresh_age = (
                "unknown"
                if not last_refresh
                else f"{int(time.time() - last_refresh)}s ago"
            )
            self.logger.debug(
                f"Using {len(existing_tracks)} cached tracks for {path} (last refresh: {last_refresh_age})"
            )
            for track in existing_tracks:
                track["is_directory"] = False
            self._cache_directory_listing_with_attrs(path, existing_tracks)
            return existing_tracks

        # Set status to pending while we're refreshing
        self.logger.info(f"Refreshing content for {path}")
        self.cache.set_refresh_metadata(cache_key, time.time(), "pending")

        try:
            # Fetch new content
            tracks = fetch_func(limit)
            self.logger.info(f"Fetched {len(tracks)} items for {path}")

            # If no tracks were returned and we have existing tracks, return them
            if not tracks and existing_tracks:
                self.logger.warning(
                    f"No content returned for {path}, using cached data"
                )
                self.cache.set_refresh_metadata(cache_key, time.time(), "stale")
                return existing_tracks

            # Process the fetched tracks
            new_tracks = []
            durations_batch = {}
            existing_ids = (
                {t.get("videoId") for t in existing_tracks if t.get("videoId")}
                if force_refresh
                else set()
            )

            for entry in tracks:
                if not entry:
                    continue

                video_id = entry.get("id")

                # Skip if we already have this track and we're doing a force refresh
                if force_refresh and video_id in existing_ids:
                    continue

                # Process duration for batch update
                duration_seconds = (
                    int(entry.get("duration", 0))
                    if entry.get("duration") is not None
                    else None
                )
                if video_id and duration_seconds is not None:
                    durations_batch[video_id] = duration_seconds

                # Create track info
                track_info = {
                    "title": entry.get("title", "Unknown Title"),
                    "artist": entry.get("uploader", "Unknown Artist"),
                    "videoId": video_id,
                    "duration_seconds": duration_seconds,
                    "is_directory": False,
                }

                # Process the track info
                processed_track = self.processor.extract_track_info(track_info)

                # Generate filename
                filename = self.processor.sanitize_filename(
                    f"{processed_track['artist']} - {processed_track['title']}.m4a"
                )
                processed_track["filename"] = filename
                processed_track["is_directory"] = False
                new_tracks.append(processed_track)

            # Update durations cache
            if durations_batch:
                self.cache.set_durations_batch(durations_batch)

            # Update the cache
            if force_refresh and existing_tracks:
                # Merge new tracks with existing ones (new tracks first)
                result_tracks = new_tracks + existing_tracks
                self.logger.info(
                    f"Added {len(new_tracks)} new tracks to existing {len(existing_tracks)} cached tracks"
                )
            else:
                # Just use the new tracks
                result_tracks = new_tracks

            # Cache the tracks and update directory listing
            self.cache.set(cache_key, result_tracks)
            self._cache_directory_listing_with_attrs(path, result_tracks)

            # Update refresh metadata
            self.cache.set_refresh_metadata(cache_key, time.time(), "fresh")

            return result_tracks

        except Exception as e:
            self.logger.error(f"Refresh failed for {path}: {str(e)}")
            self.logger.error(traceback.format_exc())
            # Mark as stale since refresh failed
            self.cache.set_refresh_metadata(cache_key, time.time(), "stale")
            # Return cached data if available
            return existing_tracks if existing_tracks else []
