#!/usr/bin/env python3

import logging
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import requests

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
        format_id: str,
        headers: dict[str, Any] | None = None,
        cookies: dict[str, Any] | None = None,
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

        # Check if download is already complete with valid file
        if self._is_download_complete(video_id, format_id):
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
            format_id,
            headers,
            cookies,
            retries,
            chunk_size,
        )
        return True

    def download_file_now(
        self,
        video_id: str,
        stream_url: str,
        path: str,
        format_id: str,
        headers: dict[str, Any] | None = None,
        cookies: dict[str, Any] | None = None,
        retries: int = 3,
        chunk_size: int = 8192,
    ) -> bool:
        """Download a file in the current worker."""
        if self._is_download_complete(video_id, format_id):
            self.logger.debug(f"Download already complete for {video_id}")
            return True

        with self.lock:
            active = self.active_downloads.get(video_id)
            if active and active.get("status") in {"starting", "downloading"}:
                self.logger.debug(f"Download already in progress for {video_id}")
                return True

        return self._download_task(
            video_id,
            stream_url,
            path,
            format_id,
            headers,
            cookies,
            retries,
            chunk_size,
        )

    def _download_task(
        self,
        video_id: str,
        stream_url: str,
        path: str,
        format_id: str,
        headers: dict[str, Any] | None = None,
        cookies: dict[str, Any] | None = None,
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

        # Mark as in-progress before starting download
        cached_format = self._cached_status_format(status_path)
        status_text = status_path.read_text().strip() if status_path.exists() else ""

        # Don't replace a complete higher-quality file with a lower-quality one
        if (
            cached_format
            and status_text.startswith("complete:")
            and self._format_quality(format_id) <= self._format_quality(cached_format)
            and audio_path.exists()
            and self._validate_file_format(audio_path)
        ):
            self.logger.info(
                "Keeping cached format %s for %s, skipping lower-quality %s",
                cached_format,
                video_id,
                format_id,
            )
            with self.lock:
                self.active_downloads[video_id] = {
                    "status": "complete",
                    "progress": audio_path.stat().st_size,
                    "total": audio_path.stat().st_size,
                }
            return True

        if cached_format not in (None, format_id):
            audio_path.unlink(missing_ok=True)
        downloaded = audio_path.stat().st_size if audio_path.exists() else 0
        with self.lock:
            self.active_downloads[video_id] = {
                "progress": downloaded,
                "total": 0,
                "status": "starting",
            }

        with status_path.open("w") as sf:
            sf.write(f"downloading:{format_id}")

        base_headers, cookies_data = ensure_headers_and_cookies(headers, cookies)

        for attempt in range(retries):
            try:
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

                if downloaded and head_response.status_code == 200:
                    audio_path.unlink(missing_ok=True)
                    downloaded = 0
                    request_headers.pop("Range", None)

                # Get the expected total file size
                expected_size = (
                    int(head_response.headers.get("content-length", 0)) + downloaded
                )
                self.update_file_size_callback(path, expected_size)

                with self.lock:
                    self.active_downloads[video_id].update(
                        {
                            "total": expected_size,
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

                    with audio_path.open("ab") as f:
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
                                    sf.write(f"downloading:{format_id}")

                # Verify the download is complete
                if audio_path.stat().st_size < expected_size:
                    raise Exception(
                        f"Incomplete download: got {audio_path.stat().st_size} bytes, expected {expected_size}"
                    )

                # Validate the file format
                if not self._validate_file_format(audio_path):
                    raise Exception("Invalid file format")

                # Mark as complete in status file first (most important for recovery)
                with status_path.open("w") as sf:
                    sf.write(f"complete:{format_id}")

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
                if attempt == retries - 1:
                    with self.lock:
                        self.active_downloads[video_id]["status"] = "failed"
                    with status_path.open("w") as sf:
                        sf.write(f"failed:{format_id}")
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

    @staticmethod
    def _cached_status_format(status_path: Path) -> str | None:
        try:
            status = status_path.read_text().strip()
        except OSError:
            return None
        for prefix in ("complete:", "downloading:", "failed:"):
            if status.startswith(prefix):
                return status.removeprefix(prefix)
        return None

    @staticmethod
    def _format_quality(format_id: str) -> int:
        return {"141": 3, "140": 2, "139": 1}.get(format_id, 0)

    def _is_download_complete(self, video_id: str, format_id: str) -> bool:
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
                    status == f"complete:{format_id}"
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
        if (
            audio_path.exists()
            and audio_path.stat().st_size > 0
            and self._validate_file_format(audio_path)
        ):
            # File exists and passes validation, mark as complete
            self.logger.debug(
                "Ignoring unmarked cached audio for %s while checking format %s",
                video_id,
                format_id,
            )

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

    def get_progress(self, video_id: str) -> dict[str, Any] | None:
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
