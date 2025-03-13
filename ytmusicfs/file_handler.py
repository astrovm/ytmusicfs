#!/usr/bin/env python3

from pathlib import Path
from typing import Optional, Any, Callable
from yt_dlp import YoutubeDL
from ytmusicfs.downloader import Downloader
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
        browser: Optional[str],
        cache: Any,  # CacheManager
        logger: logging.Logger,
        update_file_size_callback: Callable[[str, int], None],
    ):
        """Initialize the FileHandler.

        Args:
            cache_dir: Directory for persistent cache
            browser: Browser to use for cookies (e.g., 'chrome', 'firefox')
            cache: CacheManager instance for caching
            logger: Logger instance to use
            update_file_size_callback: Callback to update file size in filesystem cache
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

    def open(self, path: str, video_id: str, thread_pool) -> int:
        """Open a file and return a file handle.

        Args:
            path: The file path
            video_id: YouTube video ID for the file
            thread_pool: ThreadPoolExecutor for background tasks

        Returns:
            File handle
        """
        self.logger.debug(f"open: {path} with video_id {video_id}")
        cache_path = self.cache_dir / "audio" / f"{video_id}.m4a"
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        with self.file_handle_lock:
            fh = self.next_fh
            self.next_fh += 1
            self.open_files[fh] = {
                "cache_path": str(cache_path),
                "video_id": video_id,
                "stream_url": None,  # Will be populated in background
                "status": "initializing",  # Track status of file preparation
                "error": None,  # Store any errors that occur
                "path": path,  # Store path for reference
                "initialized_event": threading.Event(),  # Event to signal when initialization is complete
            }
            self.path_to_fh[path] = fh
            self.logger.debug(f"Assigned file handle {fh} to {path}")

        if self._check_cached_audio(video_id):
            self.logger.debug(f"Found complete cached audio for {video_id}")
            with self.file_handle_lock:
                self.open_files[fh]["status"] = "ready"
                self.open_files[fh]["initialized_event"].set()
            return fh

        thread_pool.submit(self._prepare_file_in_background, fh, video_id, path)
        return fh

    def _prepare_file_in_background(self, fh: int, video_id: str, path: str) -> None:
        """Prepare a file in the background by fetching its stream URL and starting download.

        Args:
            fh: File handle
            video_id: YouTube video ID
            path: File path
        """
        try:
            file_info = self.open_files.get(fh)
            if not file_info:
                self.logger.error(f"File handle {fh} no longer exists")
                return

            queue = multiprocessing.Queue()
            process = multiprocessing.Process(
                target=self._extract_stream_url, args=(video_id, self.browser, queue)
            )
            process.daemon = True
            process.start()

            stream_url = None
            try:
                result = queue.get(timeout=30)
                if result["status"] == "error":
                    raise Exception(result["error"])
                stream_url = result["stream_url"]

                head_response = requests.head(stream_url, timeout=10)
                if (
                    head_response.status_code == 200
                    and "content-length" in head_response.headers
                ):
                    actual_size = int(head_response.headers["content-length"])
                    self.logger.debug(
                        f"Got actual file size for {video_id}: {actual_size} bytes"
                    )
                    self.update_file_size_callback(path, actual_size)
            except Exception as e:
                self.logger.error(f"Error getting stream URL for {video_id}: {str(e)}")
                with self.file_handle_lock:
                    if fh in self.open_files:
                        self.open_files[fh]["status"] = "error"
                        self.open_files[fh]["error"] = str(e)
                        self.open_files[fh]["initialized_event"].set()
                return

            with self.file_handle_lock:
                if fh in self.open_files:
                    self.open_files[fh]["stream_url"] = stream_url
                    self.open_files[fh]["status"] = "ready"
                    self.open_files[fh]["initialized_event"].set()

            self.downloader.download_file(video_id, stream_url, path)
        except Exception as e:
            self.logger.error(f"Error preparing file {video_id}: {str(e)}")
            with self.file_handle_lock:
                if fh in self.open_files:
                    self.open_files[fh]["status"] = "error"
                    self.open_files[fh]["error"] = str(e)
                    self.open_files[fh]["initialized_event"].set()

    def _extract_stream_url(self, video_id: str, browser: str, queue):
        """Extract stream URL from YouTube Music using yt-dlp.

        Args:
            video_id: YouTube video ID
            browser: Browser to use for cookies
            queue: Queue to communicate results back to main process
        """
        try:
            ydl_opts = {
                "format": "141/bestaudio[ext=m4a]",
                "extractor_args": {
                    "youtube": {
                        "formats": ["missing_pot"],
                    },
                },
            }

            if browser:
                ydl_opts["cookiesfrombrowser"] = (browser,)

            url = f"https://music.youtube.com/watch?v={video_id}"

            with YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                stream_url = info["url"]
                queue.put({"status": "success", "stream_url": stream_url})
        except Exception as e:
            queue.put({"status": "error", "error": str(e)})

    def _stream_content(
        self, stream_url: str, offset: int, size: int, retries: int = 3
    ) -> bytes:
        """Stream content with buffering and retries."""
        buffer_size = 16384  # 16KB buffer
        prefetch_size = buffer_size * 2
        headers = {"Range": f"bytes={offset}-{offset + size + prefetch_size - 1}"}

        for attempt in range(retries):
            try:
                with requests.get(
                    stream_url, headers=headers, stream=True, timeout=10
                ) as response:
                    if response.status_code not in (200, 206):
                        raise OSError(
                            errno.EIO, f"Stream failed: {response.status_code}"
                        )
                    data = b""
                    for chunk in response.iter_content(chunk_size=buffer_size):
                        data += chunk
                        if len(data) >= size:
                            break
                    return data[:size] if len(data) > size else data
            except Exception as e:
                self.logger.warning(f"Stream attempt {attempt + 1} failed: {e}")
                if attempt == retries - 1:
                    raise OSError(
                        errno.EIO, f"Failed to stream after {retries} attempts: {e}"
                    )
                time.sleep(2**attempt)  # Exponential backoff

    def read(self, path: str, size: int, offset: int, fh: int) -> bytes:
        """Read data that might be partially downloaded.

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

        if file_info["status"] == "error":
            raise OSError(errno.EIO, file_info.get("error", "Unknown error"))

        if file_info["status"] == "initializing":
            file_info["initialized_event"].wait(timeout=10)
            if file_info["status"] == "error":
                raise OSError(errno.EIO, file_info.get("error", "Unknown error"))
            elif file_info["status"] == "initializing":
                raise OSError(errno.EIO, "Timeout waiting for initialization")

        progress = self.downloader.get_progress(video_id)
        if progress and progress["status"] == "complete" and cache_path.exists():
            with cache_path.open("rb") as f:
                f.seek(offset)
                return f.read(size)
        elif (
            progress
            and progress["status"] == "downloading"
            and progress["progress"] >= offset + size
        ):
            with cache_path.open("rb") as f:
                f.seek(offset)
                return f.read(size)

        stream_url = file_info["stream_url"]
        if not stream_url:
            raise OSError(errno.EIO, "Stream URL unavailable")

        return self._stream_content(stream_url, offset, size)

    def release(self, path: str, fh: int) -> int:
        """Release (close) a file handle and stop any ongoing download.

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
            del self.open_files[fh]
            # Remove path_to_fh entry for this path
            if path in self.path_to_fh and self.path_to_fh[path] == fh:
                del self.path_to_fh[path]

            # Stop the download if it's ongoing
            if video_id:
                self.logger.debug(f"Stopping download for {video_id}")
                # Stop the download using the download manager
                self.downloader.stop_download(video_id)
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
