#!/usr/bin/env python3

from pathlib import Path
from typing import Callable, Optional
import logging
import os
import requests
import tempfile
import threading
import time


class Downloader:
    """Manages downloading of audio files with resumability and progress tracking."""

    def __init__(
        self,
        cache_dir: Path,
        logger: logging.Logger,
        update_file_size_callback: Callable[[str, int], None],
    ):
        """Initialize the Downloader.

        Args:
            cache_dir: Directory to store downloaded files.
            logger: Logger instance for logging.
            update_file_size_callback: Function to update file size in filesystem cache.
        """
        self.cache_dir = cache_dir
        self.logger = logger
        self.update_file_size_callback = update_file_size_callback
        self.active_downloads = (
            {}
        )  # video_id: {'progress': int, 'total': int, 'status': str}
        self.lock = threading.Lock()

    def download_file(
        self,
        video_id: str,
        stream_url: str,
        path: str,
        retries: int = 3,
        chunk_size: int = 8192,
    ) -> bool:
        """Download a file with resumability and retry logic.

        Args:
            video_id: YouTube video ID.
            stream_url: URL to download from.
            path: Filesystem path for size updates.
            retries: Number of retry attempts.
            chunk_size: Size of chunks to download.

        Returns:
            True if download succeeds, False otherwise.
        """
        audio_path = self.cache_dir / "audio" / f"{video_id}.m4a"
        audio_path.parent.mkdir(parents=True, exist_ok=True)
        status_path = audio_path.parent / f"{video_id}.status"

        # Create a temporary file for the download
        temp_file = None

        # Check current status
        current_status = self._get_file_status(video_id)

        with self.lock:
            # Only return early if status is already complete and file is valid
            if (
                video_id in self.active_downloads
                and self.active_downloads[video_id]["status"] == "complete"
                and current_status == "complete"
                and audio_path.exists()
                and self._validate_file_format(audio_path)
            ):
                self.logger.debug(f"Download already complete for {video_id}")
                return True

            # Initialize or update download status
            self.active_downloads[video_id] = {
                "progress": 0,
                "total": 0,
                "status": "starting",
            }

        # Mark as in-progress before starting download
        with status_path.open("w") as sf:
            sf.write("downloading")

        # Check existing file size for resuming
        # Don't assume we can resume an interrupted download - start fresh
        if current_status == "interrupted" or current_status == "failed":
            self.logger.info(
                f"Previous download was {current_status}, starting fresh for {video_id}"
            )
            # Remove any existing partial file to start clean
            if audio_path.exists():
                try:
                    audio_path.unlink()
                    self.logger.debug(f"Removed partial file for {video_id}")
                except Exception as e:
                    self.logger.warning(f"Failed to remove partial file: {e}")
            downloaded = 0
        else:
            downloaded = audio_path.stat().st_size if audio_path.exists() else 0

        for attempt in range(retries):
            try:
                headers = {"Range": f"bytes={downloaded}-"} if downloaded else {}

                # First, check if the URL is still valid
                head_response = requests.head(stream_url, headers=headers, timeout=10)
                if head_response.status_code not in (200, 206):
                    raise Exception(
                        f"Stream URL check failed: HTTP {head_response.status_code}"
                    )

                # Create a temporary file for safe download
                temp_file = tempfile.NamedTemporaryFile(
                    delete=False, dir=audio_path.parent, suffix=".tmp"
                )
                temp_path = Path(temp_file.name)

                # If resuming, copy existing content to temp file
                if downloaded > 0 and audio_path.exists():
                    with audio_path.open("rb") as src:
                        with temp_file:
                            temp_file.write(src.read())

                # Start the actual download
                with requests.get(
                    stream_url, headers=headers, stream=True, timeout=10
                ) as response:
                    if response.status_code not in (200, 206):
                        raise Exception(f"HTTP {response.status_code}")

                    # Get the expected total size
                    content_length = int(response.headers.get("content-length", 0))
                    total_size = content_length + downloaded

                    # Update file size in filesystem
                    self.update_file_size_callback(path, total_size)

                    with self.lock:
                        self.active_downloads[video_id].update(
                            {"total": total_size, "status": "downloading"}
                        )

                    # Open the temp file for appending
                    with open(temp_path, "ab") as f:
                        for chunk in response.iter_content(chunk_size=chunk_size):
                            f.write(chunk)
                            downloaded += len(chunk)
                            with self.lock:
                                self.active_downloads[video_id]["progress"] = downloaded
                                # Always keep status file in sync with memory state
                                try:
                                    with status_path.open("w") as sf:
                                        sf.write("downloading")
                                except Exception:
                                    pass  # Just continue if we can't update status

                # Verify the download is complete by checking file size
                actual_size = temp_path.stat().st_size
                expected_size = total_size

                if actual_size < expected_size:
                    self.logger.warning(
                        f"Download for {video_id} is incomplete: {actual_size}/{expected_size} bytes"
                    )
                    raise Exception("Incomplete download: file size mismatch")

                # Verify the file has basic m4a headers (simple validation)
                if not self._validate_file_format(temp_path):
                    raise Exception(
                        "Invalid file format: does not appear to be a valid m4a file"
                    )

                # If validation passed, move the temp file to the final location
                temp_path.replace(audio_path)

                # Mark as complete only after successful validation and move
                with status_path.open("w") as sf:
                    sf.write("complete")

                with self.lock:
                    self.active_downloads[video_id]["status"] = "complete"

                self.logger.info(f"Download completed and validated for {video_id}")
                return True

            except Exception as e:
                self.logger.warning(
                    f"Download attempt {attempt + 1} failed for {video_id}: {e}"
                )
                # Clean up temp file if it exists
                if temp_file is not None:
                    try:
                        os.unlink(temp_file.name)
                    except Exception:
                        pass

                if attempt == retries - 1:
                    with self.lock:
                        self.active_downloads[video_id]["status"] = "failed"
                    # Update status file to reflect the failure
                    with status_path.open("w") as sf:
                        sf.write("failed")
                    return False

                time.sleep(2**attempt)  # Exponential backoff

    def _get_file_status(self, video_id: str) -> str:
        """Get the current status of a file from its status file.

        Args:
            video_id: YouTube video ID

        Returns:
            Current status as a string: 'complete', 'downloading', 'interrupted', 'failed', or 'unknown'
        """
        status_path = self.cache_dir / "audio" / f"{video_id}.status"

        if not status_path.exists():
            return "unknown"

        try:
            with status_path.open("r") as f:
                status = f.read().strip()
            return status
        except Exception as e:
            self.logger.warning(f"Error reading status file: {e}")
            return "unknown"

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
        """Stop a download by marking it interrupted.

        Args:
            video_id: YouTube video ID.
        """
        with self.lock:
            if video_id in self.active_downloads:
                previous_status = self.active_downloads[video_id]["status"]

                # Only mark as interrupted if it's not already complete
                if previous_status != "complete":
                    self.active_downloads[video_id]["status"] = "interrupted"
                    self.logger.debug(f"Download interrupted for {video_id}")

                    # Update the status file to indicate interruption - only if not complete
                    status_path = self.cache_dir / "audio" / f"{video_id}.status"
                    try:
                        # Check file status first to avoid overwriting 'complete'
                        current_status = self._get_file_status(video_id)
                        if current_status != "complete":
                            with status_path.open("w") as sf:
                                sf.write("interrupted")
                    except Exception as e:
                        self.logger.warning(
                            f"Failed to update status file for {video_id}: {e}"
                        )
                else:
                    self.logger.debug(
                        f"Not interrupting completed download for {video_id}"
                    )
