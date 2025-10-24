#!/usr/bin/env python3

from pathlib import Path
from typing import Callable, Optional, Any
import logging
import os
import requests
import tempfile
import time

from ytmusicfs.http_utils import ensure_headers_and_cookies


class Downloader:
    """Manages downloading of audio files with resumability and progress tracking."""

    def __init__(
        self,
        thread_manager: Any,  # ThreadManager (required)
        cache_dir: Path,
        logger: logging.Logger,
        update_file_size_callback: Callable[[str, int], None],
    ):
        """Initialize the Downloader.

        Args:
            thread_manager: ThreadManager instance for thread synchronization.
            cache_dir: Directory to store downloaded files.
            logger: Logger instance for logging.
            update_file_size_callback: Function to update file size in filesystem cache.
        """
        self.cache_dir = cache_dir
        self.logger = logger
        self.update_file_size_callback = update_file_size_callback
        self.thread_manager = thread_manager

        self.active_downloads = (
            {}
        )  # video_id: {'progress': int, 'total': int, 'status': str}

        # Use ThreadManager for lock creation
        self.lock = thread_manager.create_lock()
        self.logger.debug("Using ThreadManager for lock creation in Downloader")

    def download_file(
        self,
        video_id: str,
        stream_url: str,
        path: str,
        headers: Optional[dict] = None,
        cookies: Optional[dict] = None,
        retries: int = 3,
        chunk_size: int = 8192,
    ) -> bool:
        """Download a file using an on-demand stream URL.

        Args:
            video_id: YouTube video ID.
            stream_url: URL to download from (not cached).
            path: Filesystem path for size updates.
            headers: Authentication headers required for the request.
            cookies: Cookies required for the request.
            retries: Number of retry attempts.
            chunk_size: Size of chunks to download.

        Returns:
            True if download succeeds, False otherwise.
        """
        audio_path = self.cache_dir / "audio" / f"{video_id}.m4a"
        audio_path.parent.mkdir(parents=True, exist_ok=True)
        status_path = audio_path.parent / f"{video_id}.status"

        # Check if download is already complete with valid file
        if self._is_download_complete(video_id):
            self.logger.debug(f"Download already complete for {video_id}")
            return True

        # Check if there's already an active download for this video
        with self.lock:
            if (
                video_id in self.active_downloads
                and self.active_downloads[video_id]["status"] == "downloading"
            ):
                self.logger.debug(f"Download already in progress for {video_id}")
                return True

        # Start download as a background task using ThreadManager
        self.logger.debug(f"Submitting download task for {video_id} to ThreadManager")
        self.thread_manager.submit_task(
            "io",
            self._download_task,
            video_id,
            stream_url,
            path,
            headers,
            cookies,
            retries,
            chunk_size,
        )
        return True

    def _download_task(
        self,
        video_id: str,
        stream_url: str,
        path: str,
        headers: Optional[dict] = None,
        cookies: Optional[dict] = None,
        retries: int = 3,
        chunk_size: int = 8192,
    ) -> bool:
        """Internal download task that can be run in a thread.

        Args:
            video_id: YouTube video ID.
            stream_url: URL to download from (not cached).
            path: Filesystem path for size updates.
            headers: Authentication headers required for the request.
            cookies: Cookies required for the request.
            retries: Number of retry attempts.
            chunk_size: Size of chunks to download.

        Returns:
            True if download succeeds, False otherwise.
        """
        audio_path = self.cache_dir / "audio" / f"{video_id}.m4a"
        status_path = audio_path.parent / f"{video_id}.status"

        # Create a temporary file for the download
        temp_file = tempfile.NamedTemporaryFile(
            delete=False, dir=audio_path.parent, suffix=".tmp"
        )
        temp_path = Path(temp_file.name)

        # Mark as in-progress before starting download
        with self.lock:
            self.active_downloads[video_id] = {
                "progress": 0,
                "total": 0,
                "status": "starting",
            }

        with status_path.open("w") as sf:
            sf.write("downloading")

        # Check existing file size for potential resume
        downloaded = audio_path.stat().st_size if audio_path.exists() else 0

        base_headers, cookies_data = ensure_headers_and_cookies(headers, cookies)

        for attempt in range(retries):
            try:
                # Add range header if resuming download
                request_headers = dict(base_headers)
                if downloaded:
                    request_headers["Range"] = f"bytes={downloaded}-"

                # Verify the stream URL is still valid
                head_kwargs = {"headers": request_headers, "timeout": 10}
                if cookies_data:
                    head_kwargs["cookies"] = cookies_data

                head_response = requests.head(stream_url, **head_kwargs)
                if head_response.status_code not in (200, 206):
                    raise Exception(
                        f"Stream URL check failed: HTTP {head_response.status_code}"
                    )

                # If resuming, copy existing content to temp file
                if downloaded > 0 and audio_path.exists():
                    with audio_path.open("rb") as src:
                        with temp_file:
                            temp_file.write(src.read())

                # Get the expected total size
                expected_total = (
                    int(head_response.headers.get("content-length", 0)) + downloaded
                )
                self.update_file_size_callback(path, expected_total)

                with self.lock:
                    self.active_downloads[video_id].update(
                        {
                            "total": expected_total,
                            "status": "downloading",
                            "progress": downloaded,
                        }
                    )

                # Download the file
                get_headers = dict(request_headers)
                get_kwargs = {
                    "headers": get_headers,
                    "stream": True,
                    "timeout": 30,
                }
                if cookies_data:
                    get_kwargs["cookies"] = cookies_data

                with requests.get(stream_url, **get_kwargs) as response:
                    if response.status_code not in (200, 206):
                        raise Exception(f"HTTP {response.status_code}")

                    with open(temp_path, "ab") as f:
                        for chunk in response.iter_content(chunk_size=chunk_size):
                            # Check if download is marked for stopping
                            with self.lock:
                                if (
                                    video_id in self.active_downloads
                                    and self.active_downloads[video_id].get(
                                        "stop_requested", False
                                    )
                                ):
                                    self.logger.info(
                                        f"Download of {video_id} was explicitly stopped"
                                    )
                                    raise Exception("Download stopped by request")

                            f.write(chunk)
                            downloaded += len(chunk)

                            with self.lock:
                                self.active_downloads[video_id]["progress"] = downloaded

                            # Periodically update status file (but not on every chunk to reduce I/O)
                            if downloaded % (chunk_size * 50) == 0:
                                with status_path.open("w") as sf:
                                    sf.write("downloading")

                # Verify the download is complete
                if temp_path.stat().st_size < expected_total:
                    raise Exception(
                        f"Incomplete download: got {temp_path.stat().st_size} bytes, expected {expected_total}"
                    )

                # Validate the file format
                if not self._validate_file_format(temp_path):
                    raise Exception("Invalid file format")

                # Replace the old file with the new one
                temp_path.replace(audio_path)

                # Mark as complete in status file first (most important for recovery)
                with status_path.open("w") as sf:
                    sf.write("complete")

                with self.lock:
                    self.active_downloads[video_id]["status"] = "complete"

                self.logger.info(
                    f"Download completed for {video_id} ({downloaded} bytes)"
                )
                return True

            except Exception as e:
                self.logger.warning(
                    f"Download attempt {attempt + 1} failed for {video_id}: {e}"
                )
                try:
                    if temp_file and os.path.exists(temp_file.name):
                        os.unlink(temp_file.name)
                except Exception as cleanup_error:
                    self.logger.warning(
                        f"Failed to clean up temp file: {cleanup_error}"
                    )

                if attempt == retries - 1:
                    with self.lock:
                        self.active_downloads[video_id]["status"] = "failed"
                    with status_path.open("w") as sf:
                        sf.write("failed")
                    return False

                # Only sleep between retries if this wasn't an explicit stop
                with self.lock:
                    if video_id in self.active_downloads and self.active_downloads[
                        video_id
                    ].get("stop_requested", False):
                        self.logger.debug(
                            "Not retrying download that was explicitly stopped"
                        )
                        return False

                time.sleep(2**attempt)  # Exponential backoff

        return False

    def _is_download_complete(self, video_id: str) -> bool:
        """Check if download is already complete with a valid file.

        Args:
            video_id: YouTube video ID

        Returns:
            True if download is complete and file is valid
        """
        audio_path = self.cache_dir / "audio" / f"{video_id}.m4a"
        status_path = self.cache_dir / "audio" / f"{video_id}.status"

        # First, check the status file
        if status_path.exists():
            try:
                with status_path.open("r") as f:
                    status = f.read().strip()
                if (
                    status == "complete"
                    and audio_path.exists()
                    and self._validate_file_format(audio_path)
                ):
                    # We have a complete status and the file exists and is valid
                    with self.lock:
                        self.active_downloads[video_id] = {
                            "status": "complete",
                            "progress": audio_path.stat().st_size,
                            "total": audio_path.stat().st_size,
                        }
                    return True
            except Exception as e:
                self.logger.warning(f"Error checking status for {video_id}: {e}")

        # If status check doesn't confirm completion, do a more thorough check
        if audio_path.exists() and audio_path.stat().st_size > 0:
            if self._validate_file_format(audio_path):
                # File exists and passes validation, mark as complete
                with status_path.open("w") as sf:
                    sf.write("complete")
                with self.lock:
                    self.active_downloads[video_id] = {
                        "status": "complete",
                        "progress": audio_path.stat().st_size,
                        "total": audio_path.stat().st_size,
                    }
                return True

        return False

    def _validate_file_format(self, file_path: Path) -> bool:
        """Basic validation to check if file appears to be a valid m4a file.

        Args:
            file_path: Path to the file to validate

        Returns:
            True if the file passes basic validation, False otherwise
        """
        try:
            # Check if file exists and has a valid size
            if not file_path.exists() or file_path.stat().st_size < 100:
                return False

            # Basic m4a validation - check for ftyp header
            with open(file_path, "rb") as f:
                # Read the first 12 bytes
                header = f.read(12)
                # Check for 'ftyp' at position 4
                if len(header) >= 8 and header[4:8] == b"ftyp":
                    return True

                # If not at the beginning, seek to 0 and try again
                # (some files have metadata before the ftyp box)
                f.seek(0)
                # Read a larger chunk to search for the ftyp marker
                larger_chunk = f.read(4096)
                if b"ftyp" in larger_chunk:
                    return True

            return False
        except Exception as e:
            self.logger.warning(f"File validation error: {e}")
            return False

    def get_progress(self, video_id: str) -> Optional[dict]:
        """Get download progress for a video.

        Args:
            video_id: YouTube video ID.

        Returns:
            Dict with 'progress', 'total', and 'status', or None if not downloading.
        """
        with self.lock:
            return self.active_downloads.get(video_id)

    def stop_download(self, video_id: str) -> None:
        """Request a download to stop gracefully.

        Args:
            video_id: YouTube video ID.
        """
        with self.lock:
            if video_id in self.active_downloads:
                # Use a flag instead of immediately changing status - let the download thread handle it
                self.active_downloads[video_id]["stop_requested"] = True
                self.logger.debug(f"Requested stop of download for {video_id}")

                # Only update status file if download is not complete
                if self.active_downloads[video_id]["status"] != "complete":
                    status_path = self.cache_dir / "audio" / f"{video_id}.status"
                    try:
                        with status_path.open("w") as sf:
                            sf.write("interrupted")
                    except Exception as e:
                        self.logger.warning(
                            f"Failed to update status file for {video_id}: {e}"
                        )
            else:
                self.logger.debug(f"No active download found for {video_id}")
