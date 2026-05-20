#!/usr/bin/env python3

import errno
import logging
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

import requests

from ytmusicfs.downloader import Downloader
from ytmusicfs.http_utils import ensure_headers_and_cookies
from ytmusicfs.yt_dlp_utils import YTDLPUtils


class FileHandler:
    """Handles file operations for the YouTube Music filesystem."""

    CACHE_START_BYTES = 512 * 1024
    UNAVAILABLE_ERRORS = (
        "Video unavailable",
        "This video is not available",
    )

    def __init__(
        self,
        thread_manager: Any,  # ThreadManager
        cache_dir: Path,
        cache: Any,  # CacheManager
        logger: logging.Logger,
        update_file_size_callback: Callable[[str, int], None],
        yt_dlp_utils: YTDLPUtils,
        browser: Optional[str] = None,
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
        """
        self.cache_dir = cache_dir
        self.browser = browser
        self.cache = cache
        self.logger = logger
        self.update_file_size_callback = update_file_size_callback
        self.thread_manager = thread_manager
        self.yt_dlp_utils = yt_dlp_utils

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

    @staticmethod
    def _has_sapisid(cookies: Optional[Dict[str, str]]) -> bool:
        if not cookies:
            return False
        return any(
            key in cookies for key in ("SAPISID", "__Secure-3PAPISID", "__Secure-3PSID")
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
                "bytes_read": 0,
                "cache_started": False,
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
                    ) from e
                # Exponential backoff before retry
                time.sleep(2**attempt)
            except Exception as e:
                # Non-request related exceptions
                self.logger.error(f"Unexpected streaming error: {e}")
                raise OSError(errno.EIO, f"Streaming error: {str(e)}") from e

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
            error_msg = file_info.get("error", "Unknown error")
            raise OSError(self._stream_error_errno(error_msg), error_msg)

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
            elif self._check_cached_audio(video_id):
                with self.file_handle_lock:
                    file_info["stream_url"] = "cached"
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
                    with self.file_handle_lock:
                        file_info["status"] = "error"
                        file_info["error"] = error_msg
                    raise OSError(self._stream_error_errno(error_msg), error_msg)

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

            except Exception as e:
                error_msg = e.strerror if isinstance(e, OSError) else str(e)
                if isinstance(e, OSError):
                    error_code = e.errno or self._stream_error_errno(error_msg)
                    log_message = f"Stream unavailable for {video_id}: {error_msg}"
                else:
                    error_code = self._stream_error_errno(error_msg)
                    log_message = (
                        f"Error getting stream URL for {video_id}: {error_msg}"
                    )

                if error_code == errno.ENOENT:
                    self.logger.warning(log_message)
                else:
                    self.logger.error(log_message)

                with self.file_handle_lock:
                    file_info["status"] = "error"
                    file_info["error"] = error_msg

                # Clean up future reference in case of error
                if video_id in self.futures:
                    del self.futures[video_id]

                raise OSError(error_code, error_msg) from e

        # If we are here, we need to stream directly from URL because:
        # 1. No cache file exists yet, or
        # 2. Download is in progress but hasn't reached the requested offset yet
        self.logger.debug(f"Streaming from URL for {video_id} at offset {offset}")
        data = self._stream_content(
            file_info["stream_url"],
            offset,
            size,
            auth_headers=file_info.get("headers"),
            cookies=file_info.get("cookies"),
        )
        self._maybe_start_cache_download(path, file_info, len(data))
        return data

    def _maybe_start_cache_download(
        self, path: str, file_info: Dict[str, Any], bytes_read: int
    ) -> None:
        if bytes_read <= 0 or file_info.get("stream_url") == "cached":
            return

        file_info["bytes_read"] = file_info.get("bytes_read", 0) + bytes_read
        if file_info.get("cache_started"):
            return
        if file_info["bytes_read"] < self.CACHE_START_BYTES:
            return

        video_id = file_info["video_id"]
        self.downloader.download_file(
            video_id,
            file_info["stream_url"],
            path,
            headers=file_info.get("headers"),
            cookies=file_info.get("cookies"),
        )
        file_info["cache_started"] = True

    @classmethod
    def _stream_error_errno(cls, error_msg: str) -> int:
        if any(marker in error_msg for marker in cls.UNAVAILABLE_ERRORS):
            return errno.ENOENT
        return errno.EIO

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
