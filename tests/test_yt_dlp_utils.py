#!/usr/bin/env python3

import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from ytmusicfs.yt_dlp_utils import YTDLPUtils


class TestYTDLPUtils(unittest.TestCase):
    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_stream_extraction_enables_ejs_runtime(self, mock_youtube_dl):
        info = {
            "url": "https://example.com/audio.m4a",
            "http_headers": {"User-Agent": "UnitTest"},
            "format_id": "141",
        }

        ydl = MagicMock()
        ydl.extract_info.return_value = info
        mock_youtube_dl.return_value.__enter__.return_value = ydl

        result = YTDLPUtils().extract_stream_url("abc123", browser="brave")

        self.assertEqual(result["stream_url"], "https://example.com/audio.m4a")
        opts = mock_youtube_dl.call_args.args[0]
        self.assertEqual(opts["format"], "141/140/bestaudio[ext=m4a]")
        self.assertEqual(opts["cookiesfrombrowser"], ("brave",))
        self.assertEqual(
            opts["extractor_args"], {"youtube": {"formats": ["missing_pot"]}}
        )
        self.assertIn("node", opts["js_runtimes"])
        self.assertIn("deno", opts["js_runtimes"])

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_reuses_cached_browser_cookie_file(self, mock_youtube_dl):
        first_info = {
            "url": "https://example.com/one.m4a",
            "http_headers": {},
        }
        second_info = {
            "url": "https://example.com/two.m4a",
            "http_headers": {},
        }

        first_ydl = MagicMock()
        first_ydl.extract_info.return_value = first_info
        second_ydl = MagicMock()
        second_ydl.extract_info.return_value = second_info
        mock_youtube_dl.return_value.__enter__.side_effect = [first_ydl, second_ydl]

        utils = YTDLPUtils()
        utils.extract_stream_url("one", browser="brave")
        utils.extract_stream_url("two", browser="brave")

        first_opts = mock_youtube_dl.call_args_list[0].args[0]
        second_opts = mock_youtube_dl.call_args_list[1].args[0]
        self.assertEqual(first_opts["cookiesfrombrowser"], ("brave",))
        self.assertNotIn("cookiesfrombrowser", second_opts)
        self.assertIn("cookiefile", second_opts)
        first_ydl.cookiejar.save.assert_called_once()

        cookie_file = second_opts["cookiefile"]
        self.assertTrue(utils._browser_cookie_files)
        utils.cleanup()
        self.assertFalse(utils._browser_cookie_files)
        self.assertFalse(Path(cookie_file).exists())

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_extract_browser_cookies_filters_youtube_domains(self, mock_youtube_dl):
        ydl = mock_youtube_dl.return_value.__enter__.return_value
        ydl.cookiejar = [
            SimpleNamespace(name="SAPISID", value="abc", domain=".youtube.com"),
            SimpleNamespace(name="SID", value="sid", domain=".music.youtube.com"),
            SimpleNamespace(name="OTHER", value="nope", domain="example.com"),
            SimpleNamespace(name="EMPTY", value=None, domain=".youtube.com"),
        ]

        cookies = YTDLPUtils().extract_browser_cookies("brave")

        self.assertEqual(cookies, {"SAPISID": "abc", "SID": "sid"})
        opts = mock_youtube_dl.call_args.args[0]
        self.assertEqual(opts["cookiesfrombrowser"], ("brave",))


if __name__ == "__main__":
    unittest.main()
