#!/usr/bin/env python3

import errno
import time
from typing import TYPE_CHECKING

import requests

from ytmusicfs.http_utils import ensure_headers_and_cookies
from ytmusicfs.models import DownloadProgress, DownloadRequest, DownloadStatus
from ytmusicfs.retry import RetryPolicy

if TYPE_CHECKING:
    from pathlib import Path

    from ytmusicfs.dependencies import DownloaderDependencies


class Downloader:
    """Manages downloading of audio files with resumability and progress tracking."""

    def __init__(self, dependencies: DownloaderDependencies) -> None:
        self.cache_dir = dependencies.cache_dir
        self.logger = dependencies.logger
        self.update_file_size_callback = dependencies.update_file_size
        self.thread_manager = dependencies.thread_manager
        self.active_downloads: dict[str, DownloadProgress] = {}
        self.lock = dependencies.thread_manager.create_lock()
        self.logger.debug("Using ThreadManager for lock creation in Downloader")

    def download_file(self, request: DownloadRequest) -> bool:
        """Schedule an audio download unless it is complete or already active."""
        audio_path = self._audio_path(request.video_id)
        audio_path.parent.mkdir(parents=True, exist_ok=True)
        if self._is_download_complete(request.video_id, request.format_id):
            self.logger.debug("Download already complete for %s", request.video_id)
            return True

        with self.lock:
            active = self.active_downloads.get(request.video_id)
            if active and active.get("status") == DownloadStatus.DOWNLOADING:
                self.logger.debug(
                    "Download already in progress for %s", request.video_id
                )
                return True

        self.logger.debug("Submitting download task for %s", request.video_id)
        self.thread_manager.submit_task("io", self._download_task, request)
        return True

    def download_file_now(self, request: DownloadRequest) -> bool:
        """Download a file in the current worker."""
        if self._is_download_complete(request.video_id, request.format_id):
            self.logger.debug("Download already complete for %s", request.video_id)
            return True

        with self.lock:
            active = self.active_downloads.get(request.video_id)
            if active and active.get("status") in {
                DownloadStatus.STARTING,
                DownloadStatus.DOWNLOADING,
            }:
                self.logger.debug(
                    "Download already in progress for %s", request.video_id
                )
                return True

        return self._download_task(request)

    def _download_task(self, request: DownloadRequest) -> bool:
        audio_path = self._audio_path(request.video_id)
        status_path = self._status_path(request.video_id)
        cached_format = self._cached_status_format(status_path)
        status_text = status_path.read_text().strip() if status_path.exists() else ""
        if self._keep_existing_download(
            request, audio_path, cached_format, status_text
        ):
            return True

        if cached_format not in (None, request.format_id):
            audio_path.unlink(missing_ok=True)
        downloaded = audio_path.stat().st_size if audio_path.exists() else 0
        self._set_progress(
            request.video_id,
            status=DownloadStatus.STARTING,
            progress=downloaded,
            total=0,
        )
        status_path.write_text(f"downloading:{request.format_id}")
        base_headers, cookies = ensure_headers_and_cookies(
            request.headers, request.cookies
        )

        policy = RetryPolicy(request.retries, exponential=True)
        for attempt in policy:
            try:
                request_headers, downloaded, expected_size = self._probe_stream(
                    request, audio_path, base_headers, cookies, downloaded
                )
                downloaded = self._write_stream(
                    request,
                    request_headers,
                    cookies,
                    downloaded,
                )
                self._validate_download(audio_path, expected_size)
                self._mark_complete(request, status_path, downloaded)
                return True
            except Exception as error:
                self.logger.warning(
                    "Download attempt %s failed for %s: %s",
                    attempt.number,
                    request.video_id,
                    error,
                )
                if attempt.is_last:
                    self._mark_failed(request, status_path)
                    return False
                if self._stop_requested(request.video_id):
                    self.logger.debug("Not retrying an explicitly stopped download")
                    return False
                time.sleep(attempt.delay)
        return False

    def _audio_path(self, video_id: str) -> Path:
        return self.cache_dir / "audio" / f"{video_id}.m4a"

    def _status_path(self, video_id: str) -> Path:
        return self.cache_dir / "audio" / f"{video_id}.status"

    def _keep_existing_download(
        self,
        request: DownloadRequest,
        audio_path: Path,
        cached_format: str | None,
        status_text: str,
    ) -> bool:
        if not (
            cached_format
            and status_text.startswith("complete:")
            and self._format_quality(request.format_id)
            <= self._format_quality(cached_format)
            and audio_path.exists()
            and self._validate_file_format(audio_path)
        ):
            return False

        size = audio_path.stat().st_size
        self.logger.info(
            "Keeping cached format %s for %s, skipping lower-quality %s",
            cached_format,
            request.video_id,
            request.format_id,
        )
        self._set_progress(
            request.video_id,
            status=DownloadStatus.COMPLETE,
            progress=size,
            total=size,
        )
        return True

    def _set_progress(
        self,
        video_id: str,
        *,
        status: DownloadStatus,
        progress: int,
        total: int,
    ) -> None:
        with self.lock:
            self.active_downloads[video_id] = {
                "status": status,
                "progress": progress,
                "total": total,
            }

    def _probe_stream(
        self,
        request: DownloadRequest,
        audio_path: Path,
        base_headers: dict[str, str],
        cookies: dict[str, str] | None,
        downloaded: int,
    ) -> tuple[dict[str, str], int, int]:
        request_headers = dict(base_headers)
        if downloaded:
            request_headers["Range"] = f"bytes={downloaded}-"

        response = requests.head(
            request.stream_url,
            headers=request_headers,
            cookies=cookies,
            timeout=10,
        )
        if response.status_code not in (200, 206):
            raise OSError(
                errno.EIO, f"Stream URL check failed: HTTP {response.status_code}"
            )

        if downloaded and response.status_code == 200:
            audio_path.unlink(missing_ok=True)
            downloaded = 0
            request_headers.pop("Range", None)

        expected_size = int(response.headers.get("content-length", 0)) + downloaded
        self.update_file_size_callback(request.path, expected_size)
        self._set_progress(
            request.video_id,
            status=DownloadStatus.DOWNLOADING,
            progress=downloaded,
            total=expected_size,
        )
        return request_headers, downloaded, expected_size

    def _write_stream(
        self,
        request: DownloadRequest,
        request_headers: dict[str, str],
        cookies: dict[str, str] | None,
        downloaded: int,
    ) -> int:
        audio_path = self._audio_path(request.video_id)
        status_path = self._status_path(request.video_id)
        with requests.get(
            request.stream_url,
            headers=request_headers,
            cookies=cookies,
            stream=True,
            timeout=30,
        ) as response:
            if response.status_code not in (200, 206):
                raise OSError(errno.EIO, f"HTTP {response.status_code}")

            with audio_path.open("ab") as audio_file:
                for chunk in response.iter_content(chunk_size=request.chunk_size):
                    if self._stop_requested(request.video_id):
                        self.logger.info(
                            "Download of %s was explicitly stopped", request.video_id
                        )
                        raise OSError(errno.ECANCELED, "Download stopped by request")
                    audio_file.write(chunk)
                    downloaded += len(chunk)
                    with self.lock:
                        self.active_downloads[request.video_id]["progress"] = downloaded
                    if downloaded % (request.chunk_size * 50) == 0:
                        status_path.write_text(f"downloading:{request.format_id}")
        return downloaded

    def _validate_download(self, audio_path: Path, expected_size: int) -> None:
        actual_size = audio_path.stat().st_size
        if actual_size < expected_size:
            raise OSError(
                errno.EIO,
                f"Incomplete download: got {actual_size} bytes, expected {expected_size}",
            )
        if not self._validate_file_format(audio_path):
            raise OSError(errno.EIO, "Invalid file format")

    def _mark_complete(
        self, request: DownloadRequest, status_path: Path, downloaded: int
    ) -> None:
        status_path.write_text(f"complete:{request.format_id}")
        with self.lock:
            self.active_downloads[request.video_id]["status"] = DownloadStatus.COMPLETE
        self.logger.info(
            "Download completed for %s (%s bytes)", request.video_id, downloaded
        )

    def _mark_failed(self, request: DownloadRequest, status_path: Path) -> None:
        with self.lock:
            self.active_downloads[request.video_id]["status"] = DownloadStatus.FAILED
        status_path.write_text(f"failed:{request.format_id}")

    def _stop_requested(self, video_id: str) -> bool:
        with self.lock:
            active = self.active_downloads.get(video_id)
            return bool(active and active.get("stop_requested"))

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

        if status_path.exists():
            try:
                with status_path.open("r") as f:
                    status = f.read().strip()
                if (
                    status == f"complete:{format_id}"
                    and audio_path.exists()
                    and self._validate_file_format(audio_path)
                ):
                    with self.lock:
                        self.active_downloads[video_id] = {
                            "status": "complete",
                            "progress": audio_path.stat().st_size,
                            "total": audio_path.stat().st_size,
                        }
                    return True
            except Exception as e:
                self.logger.warning(f"Error checking status for {video_id}: {e}")

        if (
            audio_path.exists()
            and audio_path.stat().st_size > 0
            and self._validate_file_format(audio_path)
        ):
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
            if not file_path.exists() or file_path.stat().st_size < 100:
                return False

            with open(file_path, "rb") as f:
                header = f.read(12)
                if len(header) >= 8 and header[4:8] == b"ftyp":
                    return True

                # Some M4A files place metadata before the ftyp box.
                f.seek(0)
                larger_chunk = f.read(4096)
                if b"ftyp" in larger_chunk:
                    return True

            return False
        except Exception as e:
            self.logger.warning(f"File validation error: {e}")
            return False

    def get_progress(self, video_id: str) -> DownloadProgress | None:
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
                # The worker owns the final state transition.
                self.active_downloads[video_id]["stop_requested"] = True
                self.logger.debug(f"Requested stop of download for {video_id}")

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
