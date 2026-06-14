#!/usr/bin/env python3

from __future__ import annotations

import logging
import re
import shutil
import tempfile
import threading
import time
from contextlib import suppress
from http.cookiejar import MozillaCookieJar
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from yt_dlp import YoutubeDL

from ytmusicfs.retry import RetryPolicy

if TYPE_CHECKING:
    from concurrent.futures import Future

    from ytmusicfs.protocols import ThreadManagerProtocol

YOUTUBE_MUSIC_AUDIO_FORMAT = "141/140/bestaudio[ext=m4a]"
PREFERRED_YOUTUBE_MUSIC_AUDIO_FORMAT = "141"


def _detect_js_runtimes() -> dict[str, dict[str, str]]:
    runtimes: dict[str, dict[str, str]] = {}
    for name, cmd in (
        ("deno", "deno"),
        ("node", "node"),
        ("bun", "bun"),
        ("quickjs", "qjs"),
    ):
        path = shutil.which(cmd)
        if path:
            runtimes[name] = {"path": path}
    return runtimes


YT_DLP_JS_RUNTIMES = _detect_js_runtimes()
UNAVAILABLE_ERRORS = (
    "Video unavailable",
    "This video is not available",
)
TRANSIENT_STREAM_ERRORS = ("Requested format is not available",)
PARTIAL_PLAYLIST_RETRY_ATTEMPTS = 4
PARTIAL_PLAYLIST_COMPLETE_RATIO = 0.95
STREAM_EXTRACTION_ATTEMPTS = 3
BROWSER_COOKIEFILE_TTL = 20 * 60


class YTDLPUtils:
    """
    Utility class for interacting with YouTube using yt-dlp.
    Provides methods for fetching playlist content and extracting stream URLs.

    This class is the central point for all YouTube DLP interactions in the application.
    ThreadManager is a required dependency for asynchronous operations.
    """

    def __init__(
        self,
        thread_manager: ThreadManagerProtocol | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        """
        Initialize the YTDLPUtils.

        Args:
            thread_manager: ThreadManager instance for asynchronous operations
            logger: Logger instance for logging
        """
        self.thread_manager = thread_manager
        self.logger = logger or logging.getLogger("YTDLPUtils")
        self._browser_cookie_files: dict[str, str] = {}
        self._browser_cookie_file_times: dict[str, float] = {}
        self._cookie_lock = threading.Lock()
        self._playlist_total_counts: dict[str, int] = {}
        self.logger.debug("YTDLPUtils initialized")

    def _add_cookie_options(self, ydl_opts: dict[str, object], browser: str) -> None:
        if not browser:
            raise ValueError("Browser auth is required")

        cookie_file = self.ensure_browser_cookiefile(browser)
        ydl_opts["cookiefile"] = cookie_file

    @staticmethod
    def _has_auth_cookies(cookie_file: str) -> bool:
        try:
            with open(cookie_file) as f:
                content = f.read()
        except OSError:
            return False
        return "SAPISID" in content and "APISID" in content

    def ensure_browser_cookiefile(self, browser: str) -> str:
        """Ensure this browser has one reusable yt-dlp cookie file."""
        if not browser:
            raise ValueError("Browser auth is required")

        with self._cookie_lock:
            cookie_file = self._browser_cookie_files.get(browser)
            cookie_time = self._browser_cookie_file_times.get(browser, 0.0)
            if (
                cookie_file
                and Path(cookie_file).exists()
                and time.time() - cookie_time < BROWSER_COOKIEFILE_TTL
                and self._has_auth_cookies(cookie_file)
            ):
                return cookie_file

            if not cookie_file:
                cookie_file = self._new_cookie_file(browser)
                self._browser_cookie_files[browser] = cookie_file

        self.logger.info("Refreshing browser cookies from %s", browser)
        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "cookiesfrombrowser": (browser,),
        }
        with YoutubeDL(ydl_opts) as ydl:
            self._save_cookiejar(browser, ydl, cookie_file, refreshed_from_browser=True)
        if not self._has_auth_cookies(cookie_file):
            self.logger.warning(
                "Browser cookie extraction from %s did not include auth cookies "
                "(SAPISID, APISID). YouTube Music premium formats (141) may not be available.",
                browser,
            )
        return cookie_file

    def _new_cookie_file(self, browser: str) -> str:
        with tempfile.NamedTemporaryFile(
            prefix=f"ytmusicfs-{browser}-", suffix=".cookies", delete=False
        ) as tmp:
            return tmp.name

    def _save_cookiejar(
        self,
        browser: str,
        ydl: YoutubeDL,
        cookie_file: str,
        refreshed_from_browser: bool = False,
    ) -> bool:
        cookiejar = getattr(ydl, "cookiejar", None)
        if cookiejar is None or not hasattr(cookiejar, "save"):
            return False

        cookiejar.save(cookie_file, ignore_discard=True, ignore_expires=True)
        with self._cookie_lock:
            self._browser_cookie_files[browser] = cookie_file
            if refreshed_from_browser:
                self._browser_cookie_file_times[browser] = time.time()
        return True

    def _stream_extraction_options(self, browser: str) -> dict[str, object]:
        ydl_opts: dict[str, object] = {
            "format": YOUTUBE_MUSIC_AUDIO_FORMAT,
            "extractor_args": {"youtube": {"formats": ["missing_pot"]}},
            "js_runtimes": {
                name: dict(config) for name, config in YT_DLP_JS_RUNTIMES.items()
            },
        }
        self._add_cookie_options(ydl_opts, browser)
        return ydl_opts

    def _cache_browser_cookies(self, _browser: str, _ydl: object) -> bool:
        """No-op: do not overwrite the browser cookie file with post-extraction cookies.

        yt-dlp's cookiejar after extraction may be missing critical auth cookies
        (SAPISID, APISID, etc.) that are required for YouTube Music premium formats.
        Overwriting the browser-extracted file would permanently lose these cookies.
        The browser cookie file is refreshed periodically by ensure_browser_cookiefile.
        """
        return False

    def extract_browser_cookies(self, browser: str) -> dict[str, str]:
        """Return YouTube cookies from a local browser profile."""
        if not browser:
            raise ValueError("Browser auth is required")

        cookie_file = self.ensure_browser_cookiefile(browser)
        cookiejar = MozillaCookieJar(cookie_file)
        try:
            cookiejar.load(ignore_discard=True, ignore_expires=True)
        except OSError:
            return {}

        cookies = {}
        for cookie in cookiejar:
            domain = getattr(cookie, "domain", "") or ""
            value = getattr(cookie, "value", None)
            if "youtube.com" not in domain or value is None:
                continue
            cookies[str(cookie.name)] = str(value)
        return cookies

    def cleanup(self) -> None:
        with self._cookie_lock:
            cookie_files = list(self._browser_cookie_files.values())
            self._browser_cookie_files.clear()
            self._browser_cookie_file_times.clear()

        for cookie_file in cookie_files:
            try:
                Path(cookie_file).unlink(missing_ok=True)
            except OSError as exc:
                self.logger.debug("Failed to remove temporary cookie file: %s", exc)

    def extract_playlist_content(
        self, playlist_id: str, limit: int, browser: str
    ) -> list[dict[str, Any]]:
        """Fetch a playlist, keeping the largest result when retries are partial."""
        is_album = playlist_id.startswith("MPREb_")
        url = self._playlist_url(playlist_id, browser)
        ydl_opts = self._playlist_options(limit, browser)

        best_tracks: list[dict[str, Any]] = []
        best_playlist_count: int | None = None
        for attempt in RetryPolicy(PARTIAL_PLAYLIST_RETRY_ATTEMPTS, base_delay=0):
            try:
                result = self._extract_playlist_result(url, ydl_opts, browser)
            except Exception as error:
                self.logger.warning("Error extracting content: %s", error)
                return best_tracks

            if not result or "entries" not in result:
                self.logger.warning(
                    "No tracks found for %s ID: %s",
                    "album" if is_album else "playlist",
                    playlist_id,
                )
                return best_tracks

            entries = result.get("entries")
            tracks = (
                [entry for entry in entries if isinstance(entry, dict)][:limit]
                if isinstance(entries, list)
                else []
            )
            raw_playlist_count = result.get("playlist_count")
            playlist_count = (
                raw_playlist_count if isinstance(raw_playlist_count, int) else None
            )
            if playlist_count is not None:
                self._playlist_total_counts[playlist_id] = playlist_count
            if len(tracks) > len(best_tracks):
                best_tracks = tracks
                best_playlist_count = playlist_count

            if not self._is_partial_playlist(tracks, playlist_count):
                self.logger.debug(
                    "Found %d tracks from %s",
                    len(tracks),
                    "album" if is_album else "playlist",
                )
                return tracks

            self.logger.warning(
                "Playlist %s returned %d of %s tracks on attempt %d. "
                "YouTube rate limiting may have reduced the result.",
                playlist_id,
                len(tracks),
                playlist_count,
                attempt.number,
            )

        if best_playlist_count:
            self._playlist_total_counts[playlist_id] = best_playlist_count
        return best_tracks

    def _playlist_url(self, playlist_id: str, browser: str) -> str:
        default_url = f"https://music.youtube.com/playlist?list={playlist_id}"
        if not playlist_id.startswith("MPREb_"):
            return default_url

        browse_url = f"https://music.youtube.com/browse/{playlist_id}"
        self.logger.debug("Resolving album ID: %s", playlist_id)
        try:
            redirect_url = self._extract_album_redirect(browse_url, browser)
        except Exception as error:
            self.logger.warning("Error detecting album redirect: %s", error)
            return browse_url

        match = re.search(r"list=(OLAK5uy_[a-zA-Z0-9_-]+)", redirect_url or "")
        if not match:
            return browse_url
        playlist_url = f"https://music.youtube.com/playlist?list={match.group(1)}"
        self.logger.debug("Resolved album playlist URL: %s", playlist_url)
        return playlist_url

    def _extract_album_redirect(self, url: str, browser: str) -> str | None:
        options: dict[str, object] = {
            "quiet": True,
            "skip_download": True,
            "extract_flat": True,
            "ignoreerrors": True,
        }
        self._add_cookie_options(options, browser)
        with YoutubeDL(options) as ydl:
            info = ydl.extract_info(url, download=False, process=False)
            self._cache_browser_cookies(browser, ydl)
        if info and info.get("_type") == "url":
            redirect_url = info.get("url")
            return redirect_url if isinstance(redirect_url, str) else None
        return None

    def _playlist_options(self, limit: int, browser: str) -> dict[str, Any]:
        options = {
            "extract_flat": True,
            "quiet": True,
            "no_warnings": True,
            "ignoreerrors": True,
            "playlist_items": f"1-{limit}",
            "extractor_args": {"youtubetab": {"skip": ["authcheck"]}},
        }
        self._add_cookie_options(options, browser)
        return options

    def _extract_playlist_result(
        self, url: str, options: dict[str, Any], browser: str
    ) -> dict[str, Any] | None:
        with YoutubeDL(options) as ydl:
            result = ydl.extract_info(url, download=False)
            self._cache_browser_cookies(browser, ydl)
        return result if isinstance(result, dict) else None

    def _is_partial_playlist(
        self, tracks: list[dict[str, Any]], playlist_count: int | None
    ) -> bool:
        return bool(
            playlist_count
            and len(tracks) < playlist_count * PARTIAL_PLAYLIST_COMPLETE_RATIO
        )

    def get_last_playlist_total_count(self, playlist_id: str) -> int | None:
        total_count = self._playlist_total_counts.get(playlist_id)
        return total_count if isinstance(total_count, int) else None

    def extract_stream_url(self, video_id: str, browser: str) -> dict[str, Any]:
        """
        Extract stream URL and duration for a video using yt-dlp.

        Args:
            video_id (str): YouTube video ID.
            browser (str): Browser name for cookie extraction.

        Returns:
            dict: Dictionary with stream URL and duration (if available).
        """
        url = f"https://music.youtube.com/watch?v={video_id}"
        ydl_opts = self._stream_extraction_options(browser)

        info = None
        for attempt in RetryPolicy(STREAM_EXTRACTION_ATTEMPTS, base_delay=0):
            try:
                with YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                    self._cache_browser_cookies(browser, ydl)
                break
            except Exception as exc:
                if not self._is_transient_stream_error(exc):
                    raise
                if attempt.is_last:
                    raise
                self.logger.warning(
                    "Transient stream extraction failure for %s on attempt %s/%s: %s",
                    video_id,
                    attempt.number,
                    attempt.total,
                    exc,
                )

        if info is None:
            raise RuntimeError(f"Failed to extract stream URL for {video_id}")

        result = self._stream_result_from_info(info)
        if self._should_retry_with_cookiefile(result, browser):
            retry_result = self._retry_stream_url_with_cached_cookies(video_id, browser)
            if retry_result:
                self.logger.info(
                    "Selected stream format %s for %s after quality retry",
                    retry_result.get("format_id", "unknown"),
                    video_id,
                )
                return retry_result

        self.logger.info(
            "Selected stream format %s for %s",
            result.get("format_id", "unknown"),
            video_id,
        )
        return result

    def _should_retry_with_cookiefile(
        self, result: dict[str, Any], browser: str
    ) -> bool:
        if result.get("format_id") == PREFERRED_YOUTUBE_MUSIC_AUDIO_FORMAT:
            return False
        with self._cookie_lock:
            cookie_file = self._browser_cookie_files.get(browser)
        return bool(cookie_file and Path(cookie_file).exists())

    def _is_transient_stream_error(self, exc: Exception) -> bool:
        error = str(exc)
        if any(unavailable in error for unavailable in UNAVAILABLE_ERRORS):
            return False
        return any(transient in error for transient in TRANSIENT_STREAM_ERRORS)

    def _retry_stream_url_with_cached_cookies(
        self, video_id: str, browser: str
    ) -> dict[str, Any] | None:
        with self._cookie_lock:
            cookie_file = self._browser_cookie_files.get(browser)
        if not cookie_file or not Path(cookie_file).exists():
            return None

        url = f"https://music.youtube.com/watch?v={video_id}"
        ydl_opts = self._stream_extraction_options(browser)
        ydl_opts.pop("cookiesfrombrowser", None)
        ydl_opts["cookiefile"] = cookie_file

        try:
            with YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                self._cache_browser_cookies(browser, ydl)
            result = self._stream_result_from_info(info)
        except Exception as exc:
            self.logger.warning(
                "Retry with cached cookies failed for %s: %s", video_id, exc
            )
            return None

        if result.get("format_id") == PREFERRED_YOUTUBE_MUSIC_AUDIO_FORMAT:
            self.logger.info(
                "Cached-cookie retry upgraded %s to format %s",
                video_id,
                result["format_id"],
            )
            return result
        self.logger.warning(
            "Quality retry kept fallback stream format %s for %s",
            result.get("format_id", "unknown"),
            video_id,
        )
        return None

    def _stream_result_from_info(self, info: dict[str, Any]) -> dict[str, Any]:
        http_headers = info.get("http_headers") or {}
        http_headers = dict(http_headers)
        result = {"stream_url": info["url"], "http_headers": http_headers}

        if "format_id" in info and info["format_id"] is not None:
            result["format_id"] = str(info["format_id"])

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

        if "duration" in info and info["duration"] is not None:
            with suppress(ValueError, TypeError):
                result["duration"] = int(info["duration"])

        return result

    def extract_stream_url_async(self, video_id: str, browser: str) -> Future[object]:
        """
        Extract stream URL asynchronously using ThreadManager.

        Args:
            video_id (str): YouTube video ID.
            browser (str): Browser name for cookie extraction.

        Returns:
            Future: Future object that will contain the extraction result.
        """
        self.logger.debug(f"Submitting async extraction task for video ID: {video_id}")

        if not self.thread_manager:
            raise RuntimeError("ThreadManager not set in YTDLPUtils")

        return cast(
            "Future[object]",
            self.thread_manager.submit_task(
                "extraction", self._extract_stream_url_worker, video_id, browser
            ),
        )

    def _extract_stream_url_worker(self, video_id: str, browser: str) -> dict[str, Any]:
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
