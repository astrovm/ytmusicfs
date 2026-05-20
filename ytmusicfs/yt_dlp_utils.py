#!/usr/bin/env python3

import logging
import re
import tempfile
import threading
from concurrent.futures import Future
from pathlib import Path

from yt_dlp import YoutubeDL

YOUTUBE_MUSIC_AUDIO_FORMAT = "141/140/bestaudio[ext=m4a]"
YT_DLP_JS_RUNTIMES = {
    "deno": {},
    "node": {},
    "bun": {},
    "quickjs": {},
}
UNAVAILABLE_ERRORS = (
    "Video unavailable",
    "This video is not available",
)


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
        self._browser_cookie_files = {}
        self._cookie_lock = threading.Lock()
        self.logger.debug("YTDLPUtils initialized")

    def _add_cookie_options(self, ydl_opts, browser):
        if not browser:
            return

        with self._cookie_lock:
            cookie_file = self._browser_cookie_files.get(browser)
            if cookie_file and Path(cookie_file).exists():
                ydl_opts["cookiefile"] = cookie_file
                return

        ydl_opts["cookiesfrombrowser"] = (browser,)

    def _cache_browser_cookies(self, browser, ydl):
        if not browser:
            return

        cookiejar = getattr(ydl, "cookiejar", None)
        if not cookiejar or not hasattr(cookiejar, "save"):
            return

        with self._cookie_lock:
            cookie_file = self._browser_cookie_files.get(browser)
            if not cookie_file:
                tmp = tempfile.NamedTemporaryFile(
                    prefix=f"ytmusicfs-{browser}-", suffix=".cookies", delete=False
                )
                cookie_file = tmp.name
                tmp.close()
                self._browser_cookie_files[browser] = cookie_file

        cookiejar.save(cookie_file, ignore_discard=True, ignore_expires=True)

    def extract_browser_cookies(self, browser):
        """Return YouTube cookies from a local browser profile."""
        if not browser:
            return {}

        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "cookiesfrombrowser": (browser,),
        }
        with YoutubeDL(ydl_opts) as ydl:
            cookiejar = getattr(ydl, "cookiejar", None)
            if not cookiejar:
                return {}

            cookies = {}
            for cookie in cookiejar:
                domain = getattr(cookie, "domain", "") or ""
                value = getattr(cookie, "value", None)
                if "youtube.com" not in domain or value is None:
                    continue
                cookies[str(cookie.name)] = str(value)
            return cookies

    def cleanup(self):
        with self._cookie_lock:
            cookie_files = list(self._browser_cookie_files.values())
            self._browser_cookie_files.clear()

        for cookie_file in cookie_files:
            try:
                Path(cookie_file).unlink(missing_ok=True)
            except OSError as exc:
                self.logger.debug("Failed to remove temporary cookie file: %s", exc)

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
                self._add_cookie_options(redirect_opts, browser)

                with YoutubeDL(redirect_opts) as ydl:
                    info = ydl.extract_info(url, download=False, process=False)
                    self._cache_browser_cookies(browser, ydl)

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
        self._add_cookie_options(ydl_opts, browser)

        try:
            with YoutubeDL(ydl_opts) as ydl:
                result = ydl.extract_info(url, download=False)
                self._cache_browser_cookies(browser, ydl)
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
            "format": YOUTUBE_MUSIC_AUDIO_FORMAT,
            "extractor_args": {"youtube": {"formats": ["missing_pot"]}},
            "js_runtimes": {
                name: dict(config) for name, config in YT_DLP_JS_RUNTIMES.items()
            },
        }
        self._add_cookie_options(ydl_opts, browser)

        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            self._cache_browser_cookies(browser, ydl)
            http_headers = info.get("http_headers") or {}
            http_headers = dict(http_headers)
            result = {"stream_url": info["url"], "http_headers": http_headers}

            cookies = info.get("cookies")
            if cookies:
                cookie_dict = None
                try:
                    cookie_dict = {cookie.name: cookie.value for cookie in cookies}
                except Exception:
                    if isinstance(cookies, dict):
                        cookie_dict = dict(cookies)
                    elif isinstance(cookies, (list, tuple)):
                        try:
                            cookie_dict = {
                                cookie["name"]: cookie["value"]
                                for cookie in cookies
                                if isinstance(cookie, dict)
                                and "name" in cookie
                                and "value" in cookie
                            }
                        except Exception:
                            cookie_dict = None
                if cookie_dict:
                    result["cookies"] = cookie_dict

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
            message = str(e)
            if any(marker in message for marker in UNAVAILABLE_ERRORS):
                self.logger.warning(f"Stream unavailable for {video_id}: {message}")
            else:
                self.logger.error(
                    f"Error extracting stream URL for {video_id}: {message}"
                )
            return {"status": "error", "error": str(e)}
