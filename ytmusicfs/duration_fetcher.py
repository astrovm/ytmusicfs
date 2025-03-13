#!/usr/bin/env python3

from typing import Dict, Optional, List
from yt_dlp import YoutubeDL
import logging
import threading


class DurationFetcher:
    """Handles fetching durations for tracks from YouTube Music using yt-dlp."""

    def __init__(self, browser: Optional[str], logger: Optional[logging.Logger] = None):
        """Initialize the DurationFetcher.

        Args:
            browser: Browser to use for cookies (e.g., 'chrome', 'firefox', 'brave')
            logger: Optional logger instance
        """
        self.browser = browser
        self.logger = logger or logging.getLogger("DurationFetcher")
        self.lock = threading.Lock()
        self.ongoing_fetches = set()  # Set of playlist IDs currently being fetched

    def fetch_durations_for_playlist(
        self, playlist_id: str, update_callback=None
    ) -> Dict[str, int]:
        """Fetch durations for all tracks in a playlist using yt-dlp.

        Args:
            playlist_id: YouTube Music playlist ID
            update_callback: Optional callback function to call with durations as they are fetched

        Returns:
            Dictionary mapping video IDs to durations in seconds
        """
        with self.lock:
            # Check if we're already fetching this playlist
            if playlist_id in self.ongoing_fetches:
                self.logger.debug(
                    f"Already fetching durations for playlist {playlist_id}"
                )
                return {}
            self.ongoing_fetches.add(playlist_id)

        try:
            playlist_url = f"https://music.youtube.com/playlist?list={playlist_id}"
            self.logger.info(f"Fetching durations for playlist: {playlist_url}")

            ydl_opts = {
                "extract_flat": True,
                "quiet": True,
                "no_warnings": True,
                "ignoreerrors": True,
            }

            # Add browser cookies if specified
            if self.browser:
                ydl_opts["cookiesfrombrowser"] = (self.browser,)

            durations = {}
            try:
                with YoutubeDL(ydl_opts) as ydl:
                    result = ydl.extract_info(playlist_url, download=False)

                    if "entries" in result:
                        for entry in result["entries"]:
                            video_id = entry.get("id")
                            duration = entry.get("duration")
                            if video_id and duration is not None:
                                durations[video_id] = int(duration)
                                # Update callback if provided
                                if update_callback:
                                    update_callback(video_id, int(duration))
                        self.logger.info(
                            f"Fetched {len(durations)} durations for playlist {playlist_id}"
                        )
                    else:
                        self.logger.warning(
                            f"No entries found in playlist {playlist_id}"
                        )
            except Exception as e:
                self.logger.error(f"Failed to extract info from YoutubeDL: {str(e)}")
                return {}

            return durations
        except Exception as e:
            self.logger.error(f"Exception fetching durations: {str(e)}")
            return {}
        finally:
            with self.lock:
                self.ongoing_fetches.discard(playlist_id)

    def fetch_durations_for_liked_songs(self, update_callback=None) -> Dict[str, int]:
        """Fetch durations for all liked songs using yt-dlp.

        Args:
            update_callback: Optional callback function to call with durations as they are fetched

        Returns:
            Dictionary mapping video IDs to durations in seconds
        """
        # For liked songs, the playlist ID is "LM"
        return self.fetch_durations_for_playlist("LM", update_callback)

    def fetch_duration_for_video(self, video_id: str) -> Optional[int]:
        """Fetch duration for a single video.

        Args:
            video_id: YouTube video ID

        Returns:
            Duration in seconds if available, None otherwise
        """
        try:
            video_url = f"https://music.youtube.com/watch?v={video_id}"
            self.logger.debug(f"Fetching duration for video: {video_url}")

            ydl_opts = {
                "quiet": True,
                "no_warnings": True,
                "skip_download": True,
            }

            # Add browser cookies if specified
            if self.browser:
                ydl_opts["cookiesfrombrowser"] = (self.browser,)

            self.logger.debug(f"Fetching info for video: {video_url}")
            try:
                with YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(video_url, download=False)
                    duration = info.get("duration")

                    if duration is not None:
                        return int(duration)
                    else:
                        self.logger.error(f"No duration found for video {video_id}")
                        return None
            except Exception as e:
                self.logger.error(f"Error extracting info: {str(e)}")
                return None
        except Exception as e:
            self.logger.error(f"Exception fetching duration: {str(e)}")
            return None

    def fetch_durations_background(
        self, playlist_id: str, cache_manager, on_complete=None
    ) -> None:
        """Fetch durations in a background thread and update the cache.

        Args:
            playlist_id: YouTube Music playlist ID
            cache_manager: CacheManager instance to update with durations
            on_complete: Optional callback to call when fetching is complete
        """

        def update_cache(video_id, duration):
            cache_manager.set_duration(video_id, duration)

        def background_task():
            durations = self.fetch_durations_for_playlist(playlist_id, update_cache)
            if on_complete:
                on_complete(durations)

        thread = threading.Thread(target=background_task)
        thread.daemon = True
        thread.start()
        self.logger.info(
            f"Started background thread to fetch durations for playlist {playlist_id}"
        )
