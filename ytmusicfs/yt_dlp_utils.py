#!/usr/bin/env python3

from concurrent.futures import Future
from yt_dlp import YoutubeDL
import logging
import re


class YTDLPUtils:
    """
    Utility class for interacting with YouTube using yt-dlp.
    Provides methods for fetching playlist content and extracting stream URLs.

    This class is the central point for all YouTube DLP interactions in the application.
    ThreadManager is a required dependency for asynchronous operations.
    """

    def __init__(self, thread_manager=None, logger=None):
        """
        Initialize the YTDLPUtils.

        Args:
            thread_manager: ThreadManager instance for asynchronous operations
            logger: Logger instance for logging
        """
        self.thread_manager = thread_manager
        self.logger = logger or logging.getLogger("YTDLPUtils")
        self.logger.debug("YTDLPUtils initialized")

    def extract_playlist_content(self, playlist_id, limit=10000, browser=None):
        """
        Fetch playlist or album content using yt-dlp, handling YouTube Music's redirects.

        Args:
            playlist_id (str): Playlist ID (e.g., 'PL123', 'LM') or Album ID (e.g., 'MPREb_123').
            limit (int): Maximum number of tracks to fetch (default: 10000).
            browser (str, optional): Browser name for cookie extraction.

        Returns:
            list: List of track entries from yt-dlp.
        """
        # Determine if this is an album ID
        is_album = playlist_id.startswith("MPREb_")
        url = None

        if is_album:
            # For albums, start with browse URL
            url = f"https://music.youtube.com/browse/{playlist_id}"
            self.logger.debug(f"Handling album ID: {playlist_id}")

            # Try to follow any redirects to get OLAK5uy playlist ID
            try:
                redirect_opts = {
                    "quiet": True,
                    "skip_download": True,
                    "extract_flat": True,
                    "ignoreerrors": True,
                }
                if browser:
                    redirect_opts["cookiesfrombrowser"] = (browser,)

                with YoutubeDL(redirect_opts) as ydl:
                    info = ydl.extract_info(url, download=False, process=False)

                    # Check if we got a redirect
                    if info and info.get("_type") == "url" and "url" in info:
                        redirect_url = info["url"]
                        self.logger.debug(f"Album redirect detected: {redirect_url}")

                        # Extract OLAK5uy ID from redirect URL
                        match = re.search(
                            r"list=(OLAK5uy_[a-zA-Z0-9_-]+)", redirect_url
                        )
                        if match:
                            olak5_id = match.group(1)
                            url = f"https://music.youtube.com/playlist?list={olak5_id}"
                            self.logger.debug(
                                f"Using playlist URL with OLAK5uy ID: {olak5_id}"
                            )
            except Exception as e:
                self.logger.warning(f"Error detecting album redirect: {e}")

        # If URL wasn't set by album logic or there was an error, use default playlist URL
        if not url:
            url = f"https://music.youtube.com/playlist?list={playlist_id}"

        # Common extraction options
        ydl_opts = {
            "extract_flat": True,
            "quiet": True,
            "no_warnings": True,
            "ignoreerrors": True,
            "playlistend": limit,
            "extractor_args": {"youtubetab": {"skip": ["authcheck"]}},
        }
        if browser:
            ydl_opts["cookiesfrombrowser"] = (browser,)

        try:
            with YoutubeDL(ydl_opts) as ydl:
                result = ydl.extract_info(url, download=False)
                if not result or "entries" not in result:
                    self.logger.warning(
                        f"No tracks found for {'album' if is_album else 'playlist'} ID: {playlist_id}"
                    )
                    return []

                tracks = result.get("entries", [])[:limit]
                self.logger.debug(
                    f"Found {len(tracks)} tracks from {'album' if is_album else 'playlist'}"
                )
                return tracks
        except Exception as e:
            self.logger.warning(f"Error extracting content: {e}")
            return []

    def extract_stream_url(self, video_id, browser=None):
        """
        Extract stream URL and duration for a video using yt-dlp.

        Args:
            video_id (str): YouTube video ID.
            browser (str, optional): Browser name for cookie extraction.

        Returns:
            dict: Dictionary with stream URL and duration (if available).
        """
        url = f"https://music.youtube.com/watch?v={video_id}"
        ydl_opts = {
            "format": "141/bestaudio[ext=m4a]",
            "extractor_args": {"youtube": {"formats": ["missing_pot"]}},
        }
        if browser:
            ydl_opts["cookiesfrombrowser"] = (browser,)

        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            result = {"stream_url": info["url"]}

            # Extract and include duration if available
            if "duration" in info and info["duration"] is not None:
                try:
                    result["duration"] = int(info["duration"])
                except (ValueError, TypeError):
                    pass

            return result

    def extract_stream_url_async(self, video_id, browser=None) -> Future:
        """
        Extract stream URL asynchronously using ThreadManager.

        Args:
            video_id (str): YouTube video ID.
            browser (str, optional): Browser name for cookie extraction.

        Returns:
            Future: Future object that will contain the extraction result.
        """
        self.logger.debug(f"Submitting async extraction task for video ID: {video_id}")

        # ThreadManager must be set before calling this function
        if not self.thread_manager:
            raise RuntimeError("ThreadManager not set in YTDLPUtils")

        return self.thread_manager.submit_task(
            "extraction", self._extract_stream_url_worker, video_id, browser
        )

    def _extract_stream_url_worker(self, video_id, browser=None):
        """
        Worker function to extract stream URL for a thread pool task.

        Args:
            video_id (str): YouTube video ID.
            browser (str, optional): Browser name for cookie extraction.

        Returns:
            dict: Dictionary with extraction status and data.
        """
        try:
            result = self.extract_stream_url(video_id, browser)
            return {"status": "success", **result}
        except Exception as e:
            self.logger.error(f"Error extracting stream URL for {video_id}: {str(e)}")
            return {"status": "error", "error": str(e)}
