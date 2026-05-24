#!/usr/bin/env python3

import logging
import shutil
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

from ytmusicfs.downloader import Downloader


class TestDownloaderCookieMerging(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir)
        self.thread_manager = Mock()
        self.thread_manager.create_lock.return_value = threading.Lock()
        self.thread_manager.submit_task = Mock()
        self.logger = logging.getLogger("test")
        self.update_callback = Mock()
        self.downloader = Downloader(
            thread_manager=self.thread_manager,
            cache_dir=self.cache_dir,
            logger=self.logger,
            update_file_size_callback=self.update_callback,
        )
        (self.cache_dir / "audio").mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir)

    @patch("ytmusicfs.downloader.requests.get")
    @patch("ytmusicfs.downloader.requests.head")
    def test_download_task_merges_cookie_header(self, mock_head, mock_get):
        video_id = "abc123"
        stream_url = "https://example.com/audio.m4a"
        path = "/playlists/test/song.m4a"

        chunk = b"\x00\x00\x00\x18ftypm4a " + (b"\x00" * 90)

        head_response = MagicMock()
        head_response.status_code = 206
        head_response.headers = {"content-length": str(len(chunk))}
        mock_head.return_value = head_response

        get_response = MagicMock()
        get_response.status_code = 200
        get_response.iter_content.return_value = [chunk]

        mock_context = MagicMock()
        mock_context.__enter__.return_value = get_response
        mock_context.__exit__.return_value = None
        mock_get.return_value = mock_context

        result = self.downloader._download_task(
            video_id=video_id,
            stream_url=stream_url,
            path=path,
            format_id="141",
            headers={
                "User-Agent": "UnitTest",
                "Cookie": "SID=headerSid; HSID=headerHsid",
            },
            cookies={"SID": "mappingSid", "CONSENT": "YES+"},
            retries=1,
            chunk_size=len(chunk),
        )

        self.assertTrue(result)

        head_kwargs = mock_head.call_args.kwargs
        self.assertNotIn("Cookie", head_kwargs["headers"])
        self.assertEqual(
            head_kwargs["cookies"],
            {"SID": "mappingSid", "HSID": "headerHsid", "CONSENT": "YES+"},
        )

        get_kwargs = mock_get.call_args.kwargs
        self.assertNotIn("Cookie", get_kwargs["headers"])
        self.assertEqual(
            get_kwargs["cookies"],
            {"SID": "mappingSid", "HSID": "headerHsid", "CONSENT": "YES+"},
        )

        audio_path = self.cache_dir / "audio" / f"{video_id}.m4a"
        self.assertTrue(audio_path.exists())
        with audio_path.open("rb") as f:
            self.assertTrue(f.read().startswith(b"\x00\x00\x00\x18ftyp"))

    def test_download_file_now_runs_in_current_worker(self):
        with patch.object(self.downloader, "_download_task", return_value=True) as task:
            result = self.downloader.download_file_now(
                "abc123",
                "https://example.com/audio.m4a",
                "/liked_songs/song.m4a",
                "141",
            )

        self.assertTrue(result)
        self.thread_manager.submit_task.assert_not_called()
        task.assert_called_once()

    @patch("ytmusicfs.downloader.requests.get")
    @patch("ytmusicfs.downloader.requests.head")
    def test_download_task_resumes_existing_progressive_cache(
        self, mock_head, mock_get
    ):
        video_id = "abc123"
        stream_url = "https://example.com/audio.m4a"
        path = "/playlists/test/song.m4a"
        audio_path = self.cache_dir / "audio" / f"{video_id}.m4a"
        prefix = b"\x00\x00\x00\x18ftypm4a " + (b"\x00" * 90)
        suffix = b"tail"
        audio_path.write_bytes(prefix)

        head_response = MagicMock()
        head_response.status_code = 206
        head_response.headers = {"content-length": str(len(suffix))}
        mock_head.return_value = head_response

        get_response = MagicMock()
        get_response.status_code = 206
        get_response.iter_content.return_value = [suffix]
        mock_context = MagicMock()
        mock_context.__enter__.return_value = get_response
        mock_context.__exit__.return_value = None
        mock_get.return_value = mock_context

        result = self.downloader._download_task(
            video_id=video_id,
            stream_url=stream_url,
            path=path,
            format_id="141",
            retries=1,
            chunk_size=len(suffix),
        )

        self.assertTrue(result)
        self.assertEqual(audio_path.read_bytes(), prefix + suffix)
        self.assertEqual(
            mock_head.call_args.kwargs["headers"]["Range"],
            f"bytes={len(prefix)}-",
        )
        self.assertEqual(
            mock_get.call_args.kwargs["headers"]["Range"],
            f"bytes={len(prefix)}-",
        )

    @patch("ytmusicfs.downloader.requests.get")
    @patch("ytmusicfs.downloader.requests.head")
    def test_download_task_replaces_cache_from_different_format(
        self, mock_head, mock_get
    ):
        video_id = "abc123"
        stream_url = "https://example.com/audio.m4a"
        path = "/playlists/test/song.m4a"
        audio_path = self.cache_dir / "audio" / f"{video_id}.m4a"
        status_path = self.cache_dir / "audio" / f"{video_id}.status"
        old_data = b"\x00\x00\x00\x18ftypm4a " + (b"\x00" * 90)
        new_data = b"\x00\x00\x00\x18ftypm4a " + (b"\x01" * 90)
        audio_path.write_bytes(old_data)
        status_path.write_text("complete:140")

        head_response = MagicMock()
        head_response.status_code = 200
        head_response.headers = {"content-length": str(len(new_data))}
        mock_head.return_value = head_response

        get_response = MagicMock()
        get_response.status_code = 200
        get_response.iter_content.return_value = [new_data]
        mock_context = MagicMock()
        mock_context.__enter__.return_value = get_response
        mock_context.__exit__.return_value = None
        mock_get.return_value = mock_context

        result = self.downloader._download_task(
            video_id=video_id,
            stream_url=stream_url,
            path=path,
            format_id="141",
            retries=1,
            chunk_size=len(new_data),
        )

        self.assertTrue(result)
        self.assertEqual(audio_path.read_bytes(), new_data)
        self.assertNotIn("Range", mock_head.call_args.kwargs["headers"])
        self.assertNotIn("Range", mock_get.call_args.kwargs["headers"])


if __name__ == "__main__":
    unittest.main()
