#!/usr/bin/env python3

from pathlib import Path
from typing import Optional, Any, Callable
from yt_dlp import YoutubeDL
import errno
import logging
import multiprocessing
import os
import requests
import threading


class DownloadManager:
    def __init__(
        self,
        cache_dir: Path,
        logger: logging.Logger,
        update_file_size_callback: Callable[[str, int], None],
    ):
        self.cache_dir = cache_dir
        self.logger = logger
        self.update_file_size_callback = update_file_size_callback
        self.download_progress = {}
        self.download_processes = {}
        self.download_queues = {}
        self.lock = threading.RLock()

    def start_download(self, video_id: str, stream_url: str, path: str) -> None:
        """Start or resume a background download."""
        cache_path = self.cache_dir / "audio" / f"{video_id}.m4a"
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        def download_task(v_id, s_url, c_path, queue):
            try:
                headers = {}
                downloaded = 0
                if c_path.exists():
                    downloaded = c_path.stat().st_size
                    headers["Range"] = f"bytes={downloaded}-"

                with requests.get(
                    s_url, headers=headers, stream=True, timeout=10
                ) as response:
                    total_size = (
                        int(response.headers.get("content-length", 0)) + downloaded
                        if not headers
                        else downloaded
                    )
                    if total_size:
                        self.update_file_size_callback(path, total_size)

                    with c_path.open("ab") as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if (
                                queue.qsize()
                                and queue.get_nowait().get("type") == "stop"
                            ):
                                self.logger.debug(f"Download stopped for {v_id}")
                                return
                            f.write(chunk)
                            downloaded += len(chunk)
                            queue.put(
                                {
                                    "type": "progress",
                                    "video_id": v_id,
                                    "progress": downloaded,
                                }
                            )

                    queue.put({"type": "complete", "video_id": v_id})
                    with (c_path.parent / f"{v_id}.status").open("w") as sf:
                        sf.write("complete")
            except Exception as e:
                queue.put({"type": "error", "video_id": v_id, "error": str(e)})

        with self.lock:
            if video_id not in self.download_progress or self.download_progress[
                video_id
            ] not in ["complete", "interrupted"]:
                self.download_progress[video_id] = (
                    0 if not cache_path.exists() else cache_path.stat().st_size
                )
                queue = multiprocessing.Queue()
                process = multiprocessing.Process(
                    target=download_task, args=(video_id, stream_url, cache_path, queue)
                )
                process.daemon = True
                process.start()
                self.download_processes[video_id] = process
                self.download_queues[video_id] = queue

                # Start monitoring thread
                monitor_thread = threading.Thread(
                    target=self._monitor_progress,
                    args=(video_id, process, queue),
                    daemon=True,
                )
                monitor_thread.start()

    def _monitor_progress(self, video_id: str, process, queue):
        while process.is_alive():
            try:
                msg = queue.get(timeout=1)
                if msg["type"] == "progress":
                    self.download_progress[video_id] = msg["progress"]
                elif msg["type"] == "complete":
                    self.download_progress[video_id] = "complete"
                    break
                elif msg["type"] == "error":
                    self.download_progress[video_id] = "failed"
                    self.logger.error(f"Download failed for {video_id}: {msg['error']}")
                    break
            except multiprocessing.queues.Empty:
                continue
        with self.lock:
            self.download_processes.pop(video_id, None)
            self.download_queues.pop(video_id, None)

    def stop_download(self, video_id: str) -> None:
        with self.lock:
            self.download_progress[video_id] = "interrupted"
            if video_id in self.download_processes:
                process = self.download_processes[video_id]
                if process.is_alive():
                    self.download_queues[video_id].put({"type": "stop"})
                    process.terminate()


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
            browser: Browser to use for cookies (e.g., 'chrome', 'firefox', 'brave')
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
        self.open_files = {}  # Store file handles: {handle: {'stream_url': ...}}
        self.next_fh = 1  # Next file handle to assign
        self.path_to_fh = {}  # Store path to file handle mapping

        # Thread-related objects
        self.file_handle_lock = threading.RLock()  # Lock for file handle operations

        # Initialize the download manager
        self.download_manager = DownloadManager(
            cache_dir, logger, update_file_size_callback
        )

        # Track download threads - needed for interrupting downloads (for backward compatibility)
        self.download_threads = {}

        # For easier access to download manager's properties
        self.download_progress = self.download_manager.download_progress
        self.download_processes = self.download_manager.download_processes
        self.download_queues = self.download_manager.download_queues

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

        # Check if the audio file is already cached completely
        cache_path = Path(self.cache_dir / "audio" / f"{video_id}.m4a")
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        # Set up the file handle with initial information
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

        # Check if the audio file is already cached completely
        if self._check_cached_audio(video_id):
            self.logger.debug(f"Found complete cached audio for {video_id}")
            with self.file_handle_lock:
                self.open_files[fh]["status"] = "ready"
                self.open_files[fh]["initialized_event"].set()
            return fh

        # Start a background task to prepare the stream URL and file size
        thread_pool.submit(self._prepare_file_in_background, fh, video_id, path)

        # Return file handle immediately without waiting for background task
        return fh

    def _prepare_file_in_background(self, fh: int, video_id: str, path: str) -> None:
        """Prepare a file in the background by fetching its stream URL and starting download.

        Args:
            fh: File handle
            video_id: YouTube video ID
            path: File path
        """
        try:
            # Get file info from handle
            file_info = self.open_files.get(fh)
            if not file_info:
                self.logger.error(f"File handle {fh} no longer exists")
                return

            cache_path = file_info["cache_path"]

            # Create a queue for communication between processes
            result_queue = multiprocessing.Queue()

            # Define the process function to run yt-dlp
            def extract_stream_url_process(video_id, browser, result_queue):
                try:
                    # Use yt-dlp to get the audio stream URL
                    ydl_opts = {
                        "format": "141/bestaudio[ext=m4a]",
                        "extractor_args": {
                            "youtube": {
                                "formats": ["missing_pot"],
                            },
                        },
                    }

                    # Add browser cookies if a browser is specified
                    if browser:
                        ydl_opts["cookiesfrombrowser"] = (browser,)

                    url = f"https://music.youtube.com/watch?v={video_id}"

                    with YoutubeDL(ydl_opts) as ydl:
                        info = ydl.extract_info(url, download=False)
                        stream_url = info["url"]
                        result_queue.put(
                            {"status": "success", "stream_url": stream_url}
                        )
                except Exception as e:
                    result_queue.put({"status": "error", "error": str(e)})

            # Start the process
            process = multiprocessing.Process(
                target=extract_stream_url_process,
                args=(video_id, self.browser, result_queue),
            )
            process.daemon = True
            process.start()

            # Store the process and queue for potential termination
            self.download_processes[video_id] = process
            self.download_queues[video_id] = result_queue

            # Wait for the result with a timeout
            try:
                # Set a timeout to prevent hanging indefinitely
                result = result_queue.get(timeout=30)

                # Process is done, clean up
                if video_id in self.download_processes:
                    del self.download_processes[video_id]
                if video_id in self.download_queues:
                    del self.download_queues[video_id]

                if result["status"] == "error":
                    raise Exception(result["error"])

                stream_url = result["stream_url"]

                if not stream_url:
                    raise Exception("No suitable audio stream found")

                self.logger.debug(f"Successfully got stream URL for {video_id}")

                # Get the actual file size using a HEAD request
                actual_size = None
                try:
                    head_response = requests.head(stream_url, timeout=10)
                    if (
                        head_response.status_code == 200
                        and "content-length" in head_response.headers
                    ):
                        actual_size = int(head_response.headers["content-length"])
                        self.logger.debug(
                            f"Got actual file size for {video_id}: {actual_size} bytes"
                        )
                        # Update the file size cache
                        self.update_file_size_callback(path, actual_size)
                    else:
                        self.logger.warning(
                            f"Couldn't get file size from HEAD request for {video_id}"
                        )
                except Exception as e:
                    self.logger.warning(
                        f"Error getting file size for {video_id}: {str(e)}"
                    )
                    # Continue even if we can't get the file size

                # Update the file handle with the stream URL and status
                with self.file_handle_lock:
                    if fh in self.open_files:
                        self.open_files[fh]["stream_url"] = stream_url
                        self.open_files[fh]["status"] = "ready"
                        self.open_files[fh]["initialized_event"].set()

                # Initialize download status and start the download only if not already interrupted
                if video_id not in self.download_progress or self.download_progress[
                    video_id
                ] not in ["interrupted", "complete"]:
                    self.download_progress[video_id] = 0
                    self.download_manager.start_download(video_id, stream_url, path)

            except multiprocessing.queues.Empty:
                # Timeout occurred
                self.logger.error(f"Timeout while extracting stream URL for {video_id}")
                # Clean up the process
                if video_id in self.download_processes:
                    process = self.download_processes[video_id]
                    if process.is_alive():
                        process.terminate()
                    del self.download_processes[video_id]
                if video_id in self.download_queues:
                    del self.download_queues[video_id]

                # Update file handle with error
                with self.file_handle_lock:
                    if fh in self.open_files:
                        self.open_files[fh]["status"] = "error"
                        self.open_files[fh][
                            "error"
                        ] = "Timeout while extracting stream URL"
                        self.open_files[fh]["initialized_event"].set()

        except Exception as e:
            self.logger.error(f"Error preparing file {video_id}: {str(e)}")
            # Update file handle with error
            with self.file_handle_lock:
                if fh in self.open_files:
                    self.open_files[fh]["status"] = "error"
                    self.open_files[fh]["error"] = str(e)
                    self.open_files[fh]["initialized_event"].set()

            # Clean up any processes that might be running
            if video_id in self.download_processes:
                process = self.download_processes[video_id]
                if process.is_alive():
                    process.terminate()
                del self.download_processes[video_id]
            if video_id in self.download_queues:
                del self.download_queues[video_id]

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
        cache_path = file_info["cache_path"]
        video_id = file_info["video_id"]
        status = file_info.get("status")

        # Check for errors first
        if status == "error":
            error_msg = file_info.get("error", "Unknown error preparing file")
            self.logger.error(f"Error reading {path}: {error_msg}")
            raise OSError(errno.EIO, error_msg)

        # If still initializing, wait for initialization to complete with periodic checks
        if status == "initializing":
            # Wait up to 5 seconds total, checking every 0.1 seconds
            max_wait_time = 5.0  # seconds
            check_interval = 0.1  # seconds
            wait_iterations = int(max_wait_time / check_interval)

            for i in range(wait_iterations):
                # Check if initialization is complete
                if file_info["initialized_event"].wait(check_interval):
                    # Initialization completed
                    break

                stream_url = file_info.get("stream_url")
                if stream_url:
                    # Don't stream if download was interrupted
                    if self.download_progress.get(video_id) == "interrupted":
                        self.logger.debug(
                            f"Download interrupted for {path}, stopping read"
                        )
                        raise OSError(errno.EIO, "File access interrupted")

                    self.logger.debug(
                        f"Streaming directly during initialization: {path}"
                    )
                    return self._stream_content(stream_url, offset, size)

            # Check status after waiting
            if file_info.get("status") == "error":
                error_msg = file_info.get("error", "Unknown error preparing file")
                self.logger.error(f"Error after waiting: {error_msg}")
                raise OSError(errno.EIO, error_msg)
            elif file_info.get("status") == "initializing":
                self.logger.error(f"Timeout waiting for file initialization: {path}")
                raise OSError(errno.EIO, "Timeout waiting for file initialization")

        # Now check download status - use video_id to get download progress
        download_status = self.download_progress.get(video_id)

        # If download was interrupted, stop the read
        if download_status == "interrupted":
            self.logger.debug(f"Download interrupted for {path}, stopping read")
            raise OSError(errno.EIO, "File access interrupted")

        # If download is complete, read from cache
        if download_status == "complete":
            with open(cache_path, "rb") as f:
                f.seek(offset)
                return f.read(size)

        # If we have enough of the file downloaded, read from cache
        if isinstance(download_status, int) and download_status > offset + size:
            with open(cache_path, "rb") as f:
                f.seek(offset)
                return f.read(size)

        # Get the stream URL (should be available by now)
        stream_url = file_info["stream_url"]
        if not stream_url:
            self.logger.error(f"Stream URL not available for {path}")
            raise OSError(errno.EIO, "Stream URL not available")

        # Otherwise, stream directly - no waiting
        self.logger.debug(
            f"Requested range not cached yet, streaming directly: offset={offset}, size={size}"
        )
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
            if fh in self.open_files:
                video_id = self.open_files[fh].get("video_id")
                del self.open_files[fh]
                # Remove path_to_fh entry for this path
                if path in self.path_to_fh and self.path_to_fh[path] == fh:
                    del self.path_to_fh[path]

                # Stop the download if it's ongoing
                if video_id:
                    self.logger.debug(f"Stopping download for {video_id}")
                    # Stop the download using the download manager
                    self.download_manager.stop_download(video_id)

                    # For compatibility with existing code
                    if video_id in self.download_threads:
                        del self.download_threads[video_id]
        return 0

    def _stream_content(self, stream_url, offset, size):
        """Stream content directly from URL (fallback if download is too slow).

        Args:
            stream_url: URL to stream from
            offset: Byte offset to start from
            size: Number of bytes to read

        Returns:
            The requested bytes
        """
        headers = {"Range": f"bytes={offset}-{offset + size - 1}"}
        response = requests.get(stream_url, headers=headers, stream=False)

        if response.status_code in (200, 206):
            return response.content
        else:
            self.logger.error(f"Failed to stream: {response.status_code}")
            raise OSError(errno.EIO, "Failed to read stream")

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
                    self.download_progress[video_id] = "complete"
                    return True
            except Exception as e:
                self.logger.debug(f"Error reading status file for {video_id}: {e}")

        # Fall back to checking if the file exists and has content
        if cache_path.exists() and cache_path.stat().st_size > 0:
            self.logger.debug(
                f"Found existing audio file for {video_id}, marking as complete"
            )
            self.download_progress[video_id] = "complete"

            # If file exists but status doesn't, create the status file
            if not status_path.exists():
                try:
                    with open(status_path, "w") as f:
                        f.write("complete")
                except Exception as e:
                    self.logger.debug(
                        f"Failed to create status file for {video_id}: {e}"
                    )

            return True

        return False
