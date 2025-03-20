#!/usr/bin/env python3

from typing import List, Optional, Dict, Any
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
        # Initialize playlist registry with all playlist types
        self._initialize_playlist_registry()
        # No auto-refresh - we'll refresh on demand when content is accessed
        self.logger.info("Using on-demand refresh for cache updates")

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

    def _initialize_playlist_registry(self, force_refresh=False):
        """Initialize the playlist registry with all playlist types.

        Args:
            force_refresh: Whether to force a refresh even if the registry was recently refreshed
        """
        # Check if registry refresh is needed
        cache_key = "playlist_registry"
        needs_refresh = force_refresh or self.check_refresh_needed(cache_key)

        # If registry exists and no refresh needed, we can skip
        if self.PLAYLIST_REGISTRY and not needs_refresh:
            self.logger.debug("Using cached playlist registry (not yet expired)")
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

        # Record refresh time
        self.cache.set_last_refresh("playlist_registry", time.time())

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

        # Get existing cached tracks
        existing_tracks = self.cache.get(cache_key) or []

        # Check if refresh is needed based on cache age (10 minutes = 600 seconds)
        refresh_interval = 600
        now = time.time()
        last_refresh = self.cache.get_last_refresh(cache_key)
        needs_refresh = (not last_refresh) or (now - last_refresh >= refresh_interval)

        # If refresh is needed or explicitly requested, do it
        if needs_refresh or force_refresh:
            self.logger.info(
                f"On-demand refresh needed for {path} (age: {(now - (last_refresh or 0)):.0f}s)"
            )
            force_refresh = True  # Set force_refresh to true if needed
        elif existing_tracks:
            # Not time to refresh yet and we have data
            self.logger.debug(
                f"Using {len(existing_tracks)} cached tracks for {path} (age: {(now - (last_refresh or 0)):.0f}s)"
            )
            for track in existing_tracks:
                track["is_directory"] = False
            self._cache_directory_listing_with_attrs(path, existing_tracks)
            return [track["filename"] for track in existing_tracks]

        # We're either forcing a refresh or don't have cached data
        self.logger.debug(
            f"Fetching up to {limit} tracks for playlist ID: {playlist_id} via yt-dlp"
        )

        try:
            # Fetch tracks from YouTube Music
            tracks = self.yt_dlp_utils.extract_playlist_content(
                playlist_id, limit, self.browser
            )
            self.logger.info(f"Fetched {len(tracks)} tracks for {playlist_id}")

            # If no tracks were returned and we have existing tracks, return them
            if not tracks and existing_tracks:
                self.logger.warning(
                    f"No tracks returned for {playlist_id}, using cached data"
                )
                return [track["filename"] for track in existing_tracks]

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

                # Process the track info first to get clean artist name
                processed_track = self.processor.extract_track_info(track_info)

                # Generate filename AFTER processing the track (with cleaned artist name)
                filename = self.processor.sanitize_filename(
                    f"{processed_track['artist']} - {processed_track['title']}.m4a"
                )
                processed_track["filename"] = filename
                processed_track["is_directory"] = False
                new_tracks.append(processed_track)

            # Update durations cache
            if durations_batch:
                self.cache.set_durations_batch(durations_batch)

            # Update the cache and return tracks
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

            # Update the last refresh time
            self.cache.set_last_refresh(cache_key, time.time())

            return [track["filename"] for track in result_tracks]

        except Exception as e:
            self.logger.error(f"Error fetching playlist content: {str(e)}")
            # If we have existing tracks, return them rather than an empty list on error
            if existing_tracks:
                return [track["filename"] for track in existing_tracks]
            return []

    def readdir_playlist_by_type(
        self, playlist_type: str = None, directory_path: str = None
    ) -> List[str]:
        """Generic method to list playlists/albums/liked_songs based on type with optimized caching.

        Args:
            playlist_type: Type of playlists to filter ('playlist', 'album', 'liked_songs')
            directory_path: Directory path where these items are shown

        Returns:
            List of directory entries
        """
        if not directory_path:
            if playlist_type == "playlist":
                directory_path = "/playlists"
            elif playlist_type == "album":
                directory_path = "/albums"
            elif playlist_type == "liked_songs":
                directory_path = "/liked_songs"
            else:
                self.logger.error(f"Invalid playlist type: {playlist_type}")
                return [".", ".."]

        # Cache key for this directory
        cache_key = f"{directory_path}_listing"

        # Check if we have this directory listing cached already
        dir_listing = self.cache.get_directory_listing_with_attrs(directory_path)

        # Check if refresh is needed based on cache age
        needs_refresh = self.check_refresh_needed(cache_key)

        if dir_listing is not None and not needs_refresh:
            # Use cached data if it exists and doesn't need refreshing
            self.logger.debug(f"Using cached directory listing for {directory_path}")
            # Return just the filenames
            return [".", ".."] + [name for name in dir_listing.keys()]
        elif dir_listing is not None and needs_refresh:
            self.logger.info(f"On-demand refresh needed for directory {directory_path}")
            # We'll proceed with refreshing but still have the dir_listing as fallback

        # Special handling for liked_songs which directly shows songs rather than folders
        if playlist_type == "liked_songs":
            self.logger.info(f"Fetching liked songs for {directory_path} directory")
            liked_songs_entry = next(
                (p for p in self.PLAYLIST_REGISTRY if p["type"] == "liked_songs"), None
            )
            if not liked_songs_entry:
                self.logger.error("Liked songs not found in registry")
                return [".", ".."]

            # Directly show the songs as files
            filenames = self.fetch_playlist_content(
                liked_songs_entry["id"], liked_songs_entry["path"], limit=10000
            )
            return [".", ".."] + filenames

        # Regular handling for playlists and albums (which show as directories)
        self.logger.info(f"Fetching {playlist_type}s for {directory_path} directory")
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

            # Get entries of the specified type
            entries = [p for p in self.PLAYLIST_REGISTRY if p["type"] == playlist_type]

            if not entries:
                self.logger.warning(f"No {playlist_type} entries found in registry")
                return [".", ".."]

            self.logger.debug(f"Found {len(entries)} {playlist_type} entries")

            processed_entries = []

            # Prepare batch cache entries
            batch_entries = {}

            for entry in entries:
                try:
                    # Make sure we have an ID and a name
                    if not entry.get("id") or not entry.get("name"):
                        self.logger.warning(
                            f"Skipping invalid {playlist_type} entry: {entry}"
                        )
                        continue

                    # Only fetch metadata for directory listing, not content - crucial optimization
                    processed_entries.append(
                        {
                            "filename": entry["name"],
                            "id": entry["id"],
                            "is_directory": True,
                        }
                    )

                    # Cache path validity for quick future lookups
                    child_path = f"{directory_path}/{entry['name']}"
                    batch_entries[f"valid_dir:{child_path}"] = True

                except Exception as e:
                    self.logger.error(
                        f"Error processing {playlist_type} {entry.get('name', 'Unknown')}: {str(e)}"
                    )
                    # Continue with next entry instead of failing completely

            # Batch update path validations
            if batch_entries:
                self.cache.set_batch(batch_entries)

            # Only cache if we have entries
            if processed_entries:
                self.logger.debug(
                    f"Caching {len(processed_entries)} processed {playlist_type} entries"
                )
                try:
                    self._cache_directory_listing_with_attrs(
                        directory_path, processed_entries
                    )
                except Exception as e:
                    self.logger.error(f"Error caching directory listing: {str(e)}")
                    # Still continue to return the result even if caching fails
            else:
                self.logger.warning(f"No {playlist_type}s processed successfully")

            # Record the refresh time
            cache_key = f"{directory_path}_listing"
            self.cache.set_last_refresh(cache_key, time.time())

            # Return directory listing
            result = [".", ".."] + [p["name"] for p in entries if p.get("name")]
            self.logger.debug(f"Returning {len(result)-2} {playlist_type} entries")
            return result

        except Exception as e:
            self.logger.error(f"Fatal error in readdir_{playlist_type}s: {str(e)}")
            self.logger.error(traceback.format_exc())
            return [".", ".."]

    def readdir_playlists(self) -> List[str]:
        """List all user playlists from the registry."""
        return self.readdir_playlist_by_type("playlist", "/playlists")

    def readdir_albums(self) -> List[str]:
        """List all albums from the registry."""
        return self.readdir_playlist_by_type("album", "/albums")

    def readdir_liked_songs(self) -> List[str]:
        """List liked songs from the registry."""
        return self.readdir_playlist_by_type("liked_songs", "/liked_songs")

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

    def check_refresh_needed(self, cache_key: str, refresh_interval: int = 600) -> bool:
        """Check if a cache entry needs refreshing based on its age.

        Args:
            cache_key: The cache key to check
            refresh_interval: Time in seconds before refresh is needed (default: 600s = 10min)

        Returns:
            True if refresh is needed, False otherwise
        """
        now = time.time()
        last_refresh = self.cache.get_last_refresh(cache_key)

        # Refresh needed if never refreshed or older than refresh_interval
        return (not last_refresh) or (now - last_refresh >= refresh_interval)
