#!/usr/bin/env python3

from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Dict, Any
from ytmusicfs.cache import CacheManager
from ytmusicfs.processor import TrackProcessor
from ytmusicfs.yt_dlp_utils import extract_playlist_content
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
        browser: Optional[str] = None,
        thread_pool: ThreadPoolExecutor = None,
    ):
        """Initialize the ContentFetcher.

        Args:
            client: YouTube Music API client
            processor: Track processor for handling track data
            cache: Cache manager for storing fetched data
            logger: Logger instance
            browser: Browser to use for cookies (optional)
            thread_pool: ThreadPoolExecutor to use for background tasks
        """
        self.client = client
        self.processor = processor
        self.cache = cache
        self.logger = logger
        self.browser = browser
        self.thread_pool = thread_pool
        self._running = False  # Flag for controlling the auto-refresh thread
        # Initialize playlist registry with all playlist types
        self._initialize_playlist_registry()
        # Start auto-refresh in a background thread using thread pool
        if not self.thread_pool:
            self.logger.warning("No thread pool provided, auto-refresh will not start")
            return

        self.logger.info("Starting auto-refresh task using thread pool")
        self.refresh_future = self.thread_pool.submit(self._run_auto_refresh)

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

        # Check if we have this directory listing cached already and return if found
        # This is the most significant optimization - avoid re-processing already cached entries
        dir_listing = self.cache.get_directory_listing_with_attrs(directory_path)
        if dir_listing is not None:
            self.logger.debug(f"Using cached directory listing for {directory_path}")
            # Return just the filenames
            return [".", ".."] + [name for name in dir_listing.keys()]

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

    def _auto_refresh_cache(self, refresh_interval: int = 600) -> None:
        """Auto-refresh all playlist caches every 10 minutes."""
        now = time.time()

        # Group playlists by type for more structured processing
        playlist_counts = {"playlist": 0, "album": 0, "liked_songs": 0}

        # Count refreshed items by type
        for playlist in self.PLAYLIST_REGISTRY:
            cache_key = f"{playlist['path']}_processed"
            last_refresh = self.cache.get_last_refresh(cache_key)

            # Skip if refreshed recently
            if last_refresh and (now - last_refresh) < refresh_interval:
                continue

            playlist_type = playlist["type"]
            if playlist_type in playlist_counts:
                self.logger.debug(
                    f"Auto-refreshing {playlist_type} at {playlist['path']} with ID {playlist['id']}"
                )
                self.fetch_playlist_content(playlist["id"], playlist["path"], limit=100)
                self.cache.set_last_refresh(cache_key, now)
                playlist_counts[playlist_type] += 1

        # Log summary
        total_refreshed = sum(playlist_counts.values())
        if total_refreshed > 0:
            self.logger.info(
                f"Auto-refreshed {total_refreshed} items: "
                + f"{playlist_counts['playlist']} playlists, "
                + f"{playlist_counts['album']} albums, "
                + f"{playlist_counts['liked_songs']} liked songs collections"
            )

    def _run_auto_refresh(self):
        """Run the auto-refresh loop every 10 minutes with proper termination handling."""
        self.logger.info("Auto-refresh background task started")
        self._running = True

        try:
            while self._running:
                try:
                    self._auto_refresh_cache(refresh_interval=600)
                except Exception as e:
                    self.logger.error(f"Error in auto refresh: {str(e)}")
                    self.logger.error(traceback.format_exc())

                # Sleep in small increments to allow for graceful termination
                for _ in range(60):
                    if not self._running:
                        break
                    time.sleep(
                        10
                    )  # 10-second sleep intervals (60 * 10 = 600 seconds total)
        except Exception as e:
            self.logger.error(f"Fatal error in auto-refresh thread: {str(e)}")
            self.logger.error(traceback.format_exc())
        finally:
            self.logger.info("Auto-refresh background task terminated")

    def stop_auto_refresh(self):
        """Stop the auto-refresh background task gracefully."""
        self.logger.info("Stopping auto-refresh background task")
        self._running = False

        # Cancel the future if it exists and is still running
        if hasattr(self, "refresh_future") and self.refresh_future:
            if not self.refresh_future.done():
                self.logger.debug("Attempting to cancel refresh future")
                # This won't forcibly stop the thread, but marks it for cancellation
                # when it reaches the next cancellation point
                self.refresh_future.cancel()
