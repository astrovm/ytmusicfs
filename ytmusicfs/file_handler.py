#!/usr/bin/env python3

from pathlib import Path
from typing import Optional, Any, Callable
from ytmusicfs.downloader import Downloader
from ytmusicfs.yt_dlp_utils import extract_stream_url_process
import errno
import logging
import multiprocessing
import requests
import threading
import time


class FileHandler:
    """Handles file operations for the YouTube Music filesystem."""

    def __init__(
        self,
        cache_dir: Path,
        cache: Any,  # CacheManager
        logger: logging.Logger,
        update_file_size_callback: Callable[[str, int], None],
        browser: Optional[str] = None,
    ):
        """Initialize the FileHandler.

        Args:
            cache_dir: Directory for persistent cache
            cache: CacheManager instance for caching
            logger: Logger instance to use
            update_file_size_callback: Callback to update file size in filesystem cache
            browser: Browser to use for cookies (e.g., 'chrome', 'firefox')
        """
        self.cache_dir = cache_dir
        self.browser = browser
        self.cache = cache
        self.logger = logger
        self.update_file_size_callback = update_file_size_callback

        # File handling state
        self.open_files = {}  # {fh: {'stream_url': str, 'video_id': str, ...}}
        self.next_fh = 1  # Next file handle to assign
        self.path_to_fh = {}  # Store path to file handle mapping
        self.file_handle_lock = threading.RLock()  # Lock for file handle operations

        # Initialize Downloader
        self.downloader = Downloader(cache_dir, logger, update_file_size_callback)

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
                "status": "ready",
                "error": None,
                "path": path,
                "initialized_event": threading.Event(),
            }
            self.open_files[fh]["initialized_event"].set()
            self.path_to_fh[path] = fh

        return fh

    def _extract_stream_url(self, video_id: str, browser: str, queue):
        """Extract stream URL from YouTube Music using yt-dlp.

        Args:
            video_id: YouTube video ID
            browser: Browser to use for cookies
            queue: Queue to communicate results back to main process
        """
        extract_stream_url_process(video_id, browser, queue)

    def _stream_content(
        self, stream_url: str, offset: int, size: int, retries: int = 3
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
        headers = {"Range": f"bytes={offset}-{end_byte}"}

        for attempt in range(retries):
            try:
                self.logger.debug(f"Streaming with range: {offset}-{end_byte}")
                with requests.get(
                    stream_url, headers=headers, stream=True, timeout=30
                ) as response:
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

        # If we don't have the stream URL yet (and it's not a cached file), fetch it on-demand
        if not file_info["stream_url"] or file_info["stream_url"] == "cached":
            if file_info["stream_url"] == "cached":
                # If marked as cached, we can read directly from the file
                with cache_path.open("rb") as f:
                    f.seek(offset)
                    return f.read(size)

            # Otherwise, proceed with fetching the stream URL
            self.logger.debug(f"Fetching stream URL on-demand for {video_id}")

            queue = multiprocessing.Queue()
            process = multiprocessing.Process(
                target=self._extract_stream_url, args=(video_id, self.browser, queue)
            )
            process.daemon = True
            process.start()

            try:
                result = queue.get(timeout=30)
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
                with self.file_handle_lock:
                    file_info["stream_url"] = stream_url

                # Extract and cache duration if available in the result
                if "duration" in result and self.cache:
                    duration = result["duration"]
                    self.logger.debug(f"Got duration for {video_id}: {duration}s")
                    # Use batch operation even for single duration
                    self.cache.set_durations_batch({video_id: duration})

                # Always start download in background
                self.downloader.download_file(video_id, stream_url, path)
            except Exception as e:
                error_msg = str(e)
                self.logger.error(
                    f"Error getting stream URL for {video_id}: {error_msg}"
                )
                with self.file_handle_lock:
                    file_info["status"] = "error"
                    file_info["error"] = error_msg
                raise OSError(errno.EIO, error_msg)

        # If we are here, we need to stream directly from URL because:
        # 1. No cache file exists yet, or
        # 2. Download is in progress but hasn't reached the requested offset yet
        self.logger.debug(f"Streaming from URL for {video_id} at offset {offset}")
        return self._stream_content(file_info["stream_url"], offset, size)

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
