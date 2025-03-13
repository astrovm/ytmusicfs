#!/usr/bin/env python3

import requests
from pathlib import Path
from typing import Callable, Optional
import logging
import time
import threading


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

        with self.lock:
            if (
                video_id in self.active_downloads
                and self.active_downloads[video_id]["status"] == "complete"
            ):
                self.logger.debug(f"Download already complete for {video_id}")
                return True
            self.active_downloads[video_id] = {
                "progress": 0,
                "total": 0,
                "status": "starting",
            }

        # Check existing file size for resuming
        downloaded = audio_path.stat().st_size if audio_path.exists() else 0

        for attempt in range(retries):
            try:
                headers = {"Range": f"bytes={downloaded}-"} if downloaded else {}
                with requests.get(
                    stream_url, headers=headers, stream=True, timeout=10
                ) as response:
                    if response.status_code not in (200, 206):
                        raise Exception(f"HTTP {response.status_code}")
                    total_size = (
                        int(response.headers.get("content-length", 0)) + downloaded
                    )
                    self.update_file_size_callback(path, total_size)

                    with self.lock:
                        self.active_downloads[video_id].update(
                            {"total": total_size, "status": "downloading"}
                        )

                    with audio_path.open("ab") as f:
                        for chunk in response.iter_content(chunk_size=chunk_size):
                            f.write(chunk)
                            downloaded += len(chunk)
                            with self.lock:
                                self.active_downloads[video_id]["progress"] = downloaded

                    # Mark as complete
                    with status_path.open("w") as sf:
                        sf.write("complete")
                    with self.lock:
                        self.active_downloads[video_id]["status"] = "complete"
                    self.logger.info(f"Download completed for {video_id}")
                    return True

            except Exception as e:
                self.logger.warning(
                    f"Download attempt {attempt + 1} failed for {video_id}: {e}"
                )
                if attempt == retries - 1:
                    with self.lock:
                        self.active_downloads[video_id]["status"] = "failed"
                    return False
                time.sleep(2**attempt)  # Exponential backoff

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
                self.active_downloads[video_id]["status"] = "interrupted"
                self.logger.debug(f"Download interrupted for {video_id}")
