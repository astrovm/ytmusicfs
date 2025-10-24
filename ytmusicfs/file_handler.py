#!/usr/bin/env python3

from pathlib import Path
from typing import Optional, Any, Callable, Dict, Tuple
from ytmusicfs.downloader import Downloader
from ytmusicfs.http_utils import ensure_headers_and_cookies
from ytmusicfs.yt_dlp_utils import YTDLPUtils
import errno
import json
import logging
import requests
import threading
import time


class FileHandler:
    """Handles file operations for the YouTube Music filesystem."""

    def __init__(
        self,
        thread_manager: Any,  # ThreadManager
        cache_dir: Path,
        cache: Any,  # CacheManager
        logger: logging.Logger,
        update_file_size_callback: Callable[[str, int], None],
        yt_dlp_utils: YTDLPUtils,
        browser: Optional[str] = None,
        auth_file: Optional[str] = None,
    ):
        """Initialize the FileHandler.

        Args:
            thread_manager: ThreadManager instance for thread synchronization
            cache_dir: Directory for persistent cache
            cache: CacheManager instance for caching
            logger: Logger instance to use
            update_file_size_callback: Callback to update file size in filesystem cache
            yt_dlp_utils: YTDLPUtils instance for YouTube interaction
            browser: Browser to use for cookies (e.g., 'chrome', 'firefox')
            auth_file: Path to persisted browser headers for fallback auth
        """
        self.cache_dir = cache_dir
        self.browser = browser
        self.cache = cache
        self.logger = logger
        self.update_file_size_callback = update_file_size_callback
        self.thread_manager = thread_manager
        self.yt_dlp_utils = yt_dlp_utils
        self.browser_auth = self._load_browser_auth(auth_file)

        # File handling state
        self.open_files = {}  # {fh: {'stream_url': str, 'video_id': str, ...}}
        self.next_fh = 1  # Next file handle to assign
        self.path_to_fh = {}  # Store path to file handle mapping
        self.file_handle_lock = thread_manager.create_lock()
        self.logger.debug("Using ThreadManager for lock creation in FileHandler")
        self.futures = {}  # Store futures for async operations by video_id

        # Initialize Downloader
        self.downloader = Downloader(
            thread_manager, cache_dir, logger, update_file_size_callback
        )

    def _load_browser_auth(
        self, auth_file: Optional[str]
    ) -> Optional[Dict[str, Optional[Dict[str, str]]]]:
        """Load sanitized headers/cookies from the auth file for fallback streaming."""

        if not auth_file:
            return None

        path = Path(auth_file)
        try:
            raw_data = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            self.logger.debug("Auth fallback skipped; file not found: %s", path)
            return None
        except Exception as exc:
            self.logger.warning(
                "Failed to parse auth headers from %s: %s", path, exc
            )
            return None

        cookie_entry = raw_data.pop("cookie", None)
        cookie_map: Optional[Dict[str, str]]

        if isinstance(cookie_entry, str):
            cookie_map = {}
            for part in cookie_entry.split(";"):
                name, sep, value = part.strip().partition("=")
                if not sep:
                    continue
                cookie_map[name] = value
        elif isinstance(cookie_entry, dict):
            cookie_map = {str(k): str(v) for k, v in cookie_entry.items()}
        else:
            cookie_map = None

        header_map = {str(k): str(v) for k, v in raw_data.items()}
        sapisid_keys = list(cookie_map.keys())[:5] if cookie_map else []
        self.logger.debug(
            "Loaded auth fallback headers=%s cookies=%s (from %s)",
            list(header_map.keys()),
            sapisid_keys,
            path,
        )
        return {"headers": header_map, "cookies": cookie_map}

    @staticmethod
    def _has_sapisid(cookies: Optional[Dict[str, str]]) -> bool:
        if not cookies:
            return False
        return any(
            key in cookies
            for key in ("SAPISID", "__Secure-3PAPISID", "__Secure-3PSID")
        )

    @staticmethod
    def _summarize_auth(
        headers: Optional[Dict[str, str]],
        cookies: Optional[Dict[str, str]],
    ) -> Tuple[str, bool, list]:
        preview = headers.get("Authorization") if headers else None
        if preview:
            if preview.startswith("SAPISIDHASH"):
                label = "SAPISIDHASH"
            else:
                label = preview.split(" ", 1)[0]
        else:
            label = "none"

        cookie_keys = sorted(cookies.keys()) if cookies else []
        sapisid_present = FileHandler._has_sapisid(cookies)
        return label, sapisid_present, cookie_keys

    def open(self, path: str, video_id: str) -> int:
        """Open a file and return a file handle.

        Args:
            path: The file path
            video_id: YouTube video ID for the file

        Returns:
            File handle
        """
        self.logger.debug(f"Opening {path} with video_id {video_id}")

        cache_path = self.cache_dir / "audio" / f"{video_id}.m4a"
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        with self.file_handle_lock:
            fh = self.next_fh
            self.next_fh += 1
            self.open_files[fh] = {
                "cache_path": str(cache_path),
                "video_id": video_id,
                "stream_url": "cached" if self._check_cached_audio(video_id) else None,
                "headers": None,
                "cookies": None,
                "status": "ready",
                "error": None,
                "path": path,
                "initialized_event": threading.Event(),
            }
            self.open_files[fh]["initialized_event"].set()
            self.path_to_fh[path] = fh

        return fh

    def _stream_content(
        self,
        stream_url: str,
        offset: int,
        size: int,
        auth_headers: Optional[dict] = None,
        cookies: Optional[dict] = None,
        retries: int = 3,
    ) -> bytes:
        """Stream content directly from URL when file is not yet cached.

        Args:
            stream_url: URL to stream from
            offset: Byte offset to start from
            size: Number of bytes to read
            retries: Number of retry attempts

        Returns:
            Requested bytes
        """
        buffer_size = 32768  # 32KB buffer (increased from 16KB)
        # Request more than needed to ensure smooth playback
        prefetch_size = buffer_size * 4

        # Calculate end byte (inclusive) according to HTTP range spec
        end_byte = offset + size + prefetch_size - 1
        header_source = dict(auth_headers) if auth_headers else {}
        base_headers, cookies = ensure_headers_and_cookies(header_source, cookies)

        for attempt in range(retries):
            try:
                request_headers = dict(base_headers)
                request_headers["Range"] = f"bytes={offset}-{end_byte}"
                self.logger.debug(f"Streaming with range: {offset}-{end_byte}")
                request_kwargs = {
                    "headers": request_headers,
                    "stream": True,
                    "timeout": 30,
                }
                if cookies:
                    request_kwargs["cookies"] = cookies

                with requests.get(stream_url, **request_kwargs) as response:
                    if response.status_code not in (200, 206):
                        raise OSError(
                            errno.EIO, f"Stream failed: HTTP {response.status_code}"
                        )

                    # Collect all data chunks
                    data = b""
                    for chunk in response.iter_content(chunk_size=buffer_size):
                        data += chunk
                        # Once we have enough data, we can return
                        if len(data) >= size:
                            break

                    # Return exactly what was requested
                    return data[:size]

            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Stream attempt {attempt + 1} failed: {e}")
                if attempt == retries - 1:
                    raise OSError(
                        errno.EIO, f"Failed to stream after {retries} attempts: {e}"
                    )
                # Exponential backoff before retry
                time.sleep(2**attempt)
            except Exception as e:
                # Non-request related exceptions
                self.logger.error(f"Unexpected streaming error: {e}")
                raise OSError(errno.EIO, f"Streaming error: {str(e)}")

        # Should not reach here but just in case
        raise OSError(errno.EIO, "Failed to stream content after all retries")

    def read(self, path: str, size: int, offset: int, fh: int) -> bytes:
        """Read data from a file, fetching stream URL on-demand if needed.

        Args:
            path: The file path
            size: Number of bytes to read
            offset: Offset to start reading from
            fh: File handle

        Returns:
            The requested bytes
        """
        if fh not in self.open_files:
            raise OSError(errno.EBADF, "Bad file descriptor")

        file_info = self.open_files[fh]
        cache_path = Path(file_info["cache_path"])
        video_id = file_info["video_id"]

        # If there was an error, raise it
        if file_info["status"] == "error":
            raise OSError(errno.EIO, file_info.get("error", "Unknown error"))

        # First try to read from the cache - fully downloaded files
        if cache_path.exists():
            progress = self.downloader.get_progress(video_id)
            if progress and progress["status"] == "complete":
                # Fully downloaded file
                with cache_path.open("rb") as f:
                    f.seek(offset)
                    return f.read(size)
            elif (
                progress
                and progress["status"] == "downloading"
                and progress["progress"] >= offset + size
            ):
                # Partially downloaded file with enough data
                with cache_path.open("rb") as f:
                    f.seek(offset)
                    return f.read(size)

        # If we still lack a stream URL (and it's not cached), fetch it on-demand
        if not file_info["stream_url"] or file_info["stream_url"] == "cached":
            if file_info["stream_url"] == "cached":
                # If marked as cached, we can read directly from the file
                with cache_path.open("rb") as f:
                    f.seek(offset)
                    return f.read(size)

            # Otherwise, proceed with fetching the stream URL
            self.logger.debug(f"Fetching stream URL on-demand for {video_id}")

            # Use ThreadPoolExecutor-based async extraction
            try:
                # Check if we already have a future for this video_id
                if video_id in self.futures:
                    future = self.futures[video_id]
                else:
                    # Submit new extraction task
                    future = self.yt_dlp_utils.extract_stream_url_async(
                        video_id, self.browser
                    )
                    self.futures[video_id] = future

                # Wait for result with timeout
                result = future.result(timeout=30)

                # Clean up future reference
                if video_id in self.futures:
                    del self.futures[video_id]

                if result["status"] == "error":
                    error_msg = result["error"]
                    self.logger.error(
                        f"Error fetching stream URL for {video_id}: {error_msg}"
                    )
                    with self.file_handle_lock:
                        file_info["status"] = "error"
                        file_info["error"] = error_msg
                    raise OSError(errno.EIO, error_msg)

                stream_url = result["stream_url"]
                self.logger.debug(
                    "yt-dlp stream url for %s: %s...",
                    video_id,
                    stream_url[:60],
                )
                cookies = result.get("cookies")
                if isinstance(cookies, dict):
                    cookies = dict(cookies)
                elif isinstance(cookies, (list, tuple)):
                    candidate: Dict[str, str] = {}
                    for item in cookies:
                        if hasattr(item, "name") and hasattr(item, "value"):
                            candidate[str(item.name)] = str(item.value)
                        elif isinstance(item, dict):
                            name = item.get("name") or item.get("key")
                            value = item.get("value")
                            if name and value is not None:
                                candidate[str(name)] = str(value)
                    cookies = candidate or None
                raw_headers = dict(result.get("http_headers") or {})
                if raw_headers:
                    self.logger.debug(
                        "yt-dlp headers for %s: %s",
                        video_id,
                        list(raw_headers.keys()),
                    )
                if "Cookie" in raw_headers or "cookie" in raw_headers:
                    cookie_preview = raw_headers.get("Cookie") or raw_headers.get(
                        "cookie"
                    )
                    self.logger.debug(
                        "yt-dlp cookie header for %s: %s",
                        video_id,
                        cookie_preview[:120] if cookie_preview else "empty",
                    )
                auth_headers, cookies = ensure_headers_and_cookies(
                    dict(raw_headers), cookies
                )
                if (
                    (not auth_headers or "Authorization" not in auth_headers)
                    or not self._has_sapisid(cookies)
                ) and self.browser_auth:
                    self.logger.debug(
                        "Falling back to auth file headers for %s", video_id
                    )
                    base_headers = dict(self.browser_auth["headers"])
                    base_cookies = (
                        dict(self.browser_auth["cookies"])
                        if self.browser_auth.get("cookies")
                        else None
                    )
                    fallback_headers, fallback_cookies = ensure_headers_and_cookies(
                        base_headers, base_cookies
                    )
                    auth_headers = fallback_headers
                    cookies = fallback_cookies
                auth_header_label, sapisid_present, cookie_keys = self._summarize_auth(
                    auth_headers, cookies
                )
                self.logger.debug(
                    "Prepared auth for %s: header=%s sapisid=%s cookies=%s",
                    video_id,
                    auth_header_label,
                    sapisid_present,
                    cookie_keys[:10],
                )
                with self.file_handle_lock:
                    file_info["stream_url"] = stream_url
                    # Persist the fully-prepared headers so subsequent reads
                    # reuse the computed authentication metadata (e.g.
                    # SAPISIDHASH).  Storing the raw yt-dlp headers caused
                    # follow-up streaming requests to miss the Authorization
                    # header which in turn triggered HTTP 403 responses.
                    file_info["headers"] = auth_headers
                    file_info["cookies"] = cookies

                # Extract and cache duration if available in the result
                if "duration" in result and self.cache:
                    duration = result["duration"]
                    self.logger.debug(f"Got duration for {video_id}: {duration}s")
                    # Use batch operation even for single duration
                    self.cache.set_durations_batch({video_id: duration})

                # Always start download in background
                self.downloader.download_file(
                    video_id, stream_url, path, headers=auth_headers, cookies=cookies
                )
            except Exception as e:
                error_msg = str(e)
                self.logger.error(
                    f"Error getting stream URL for {video_id}: {error_msg}"
                )
                with self.file_handle_lock:
                    file_info["status"] = "error"
                    file_info["error"] = error_msg

                # Clean up future reference in case of error
                if video_id in self.futures:
                    del self.futures[video_id]

                raise OSError(errno.EIO, error_msg)

        # If we are here, we need to stream directly from URL because:
        # 1. No cache file exists yet, or
        # 2. Download is in progress but hasn't reached the requested offset yet
        self.logger.debug(f"Streaming from URL for {video_id} at offset {offset}")
        return self._stream_content(
            file_info["stream_url"],
            offset,
            size,
            auth_headers=file_info.get("headers"),
            cookies=file_info.get("cookies"),
        )

    def release(self, path: str, fh: int) -> int:
        """Release (close) a file handle but allow downloads to continue.

        Args:
            path: The file path
            fh: File handle

        Returns:
            0 on success
        """
        self.logger.debug(f"Releasing file handle {fh} for {path}")
        with self.file_handle_lock:
            if fh not in self.open_files:
                return 0

            video_id = self.open_files[fh].get("video_id")

            # Don't stop the download - let it continue in the background
            # This ensures files are fully downloaded even after the handle is closed

            # Just remove the file handle from our tracking
            del self.open_files[fh]

            # Remove path_to_fh entry for this path
            if path in self.path_to_fh and self.path_to_fh[path] == fh:
                del self.path_to_fh[path]

        return 0

    def _check_cached_audio(self, video_id):
        """Check if an audio file is already cached completely.

        Args:
            video_id: YouTube video ID

        Returns:
            True if file is cached completely, False otherwise
        """
        cache_path = Path(self.cache_dir) / "audio" / f"{video_id}.m4a"
        status_path = Path(self.cache_dir) / "audio" / f"{video_id}.status"

        # First check for status file (most reliable)
        if status_path.exists():
            try:
                with open(status_path, "r") as f:
                    status = f.read().strip()
                if status == "complete":
                    self.logger.debug(
                        f"Found status file indicating {video_id} is complete"
                    )
                    return True
            except Exception as e:
                self.logger.debug(f"Error reading status file for {video_id}: {e}")

        # Fall back to checking if the file exists and has content
        if cache_path.exists() and cache_path.stat().st_size > 0:
            self.logger.debug(
                f"Found existing audio file for {video_id}, marking as complete"
            )
            return True

        return False
