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


if __name__ == "__main__":
    unittest.main()
