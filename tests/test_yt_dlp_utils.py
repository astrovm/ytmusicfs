#!/usr/bin/env python3

import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from ytmusicfs.yt_dlp_utils import YTDLPUtils


class TestYTDLPUtils(unittest.TestCase):
    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_playlist_extraction_retries_known_partial_results(self, mock_youtube_dl):
        first_ydl = MagicMock()
        first_ydl.extract_info.return_value = {
            "entries": [{"id": "one"}],
            "playlist_count": 10,
        }
        second_ydl = MagicMock()
        second_ydl.extract_info.return_value = {
            "entries": [{"id": str(index)} for index in range(10)],
            "playlist_count": 10,
        }
        mock_youtube_dl.return_value.__enter__.side_effect = [first_ydl, second_ydl]

        utils = YTDLPUtils()
        result = utils.extract_playlist_content("LM", 10000, "brave")

        self.assertEqual(len(result), 10)
        self.assertEqual(mock_youtube_dl.call_count, 2)
        self.assertEqual(utils.get_last_playlist_total_count("LM"), 10)
        opts = mock_youtube_dl.call_args.args[0]
        self.assertEqual(opts["playlist_items"], "1-10000")

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_playlist_extraction_returns_best_partial_result(self, mock_youtube_dl):
        results = [
            {"entries": [{"id": "one"}], "playlist_count": 10},
            {
                "entries": [{"id": str(index)} for index in range(3)],
                "playlist_count": 10,
            },
            {"entries": [{"id": "one"}], "playlist_count": 10},
            {
                "entries": [{"id": str(index)} for index in range(2)],
                "playlist_count": 10,
            },
        ]
        contexts = []
        for result in results:
            ydl = MagicMock()
            ydl.extract_info.return_value = result
            contexts.append(ydl)
        mock_youtube_dl.return_value.__enter__.side_effect = contexts

        result = YTDLPUtils().extract_playlist_content("LM", 10000, "brave")

        self.assertEqual([entry["id"] for entry in result], ["0", "1", "2"])
        self.assertEqual(mock_youtube_dl.call_count, 4)

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
            "format_id": "141",
        }
        second_info = {
            "url": "https://example.com/two.m4a",
            "http_headers": {},
            "format_id": "141",
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
    def test_retries_lower_quality_first_stream_with_cached_cookies(
        self, mock_youtube_dl
    ):
        first_info = {
            "url": "https://example.com/low.m4a",
            "http_headers": {},
            "format_id": "140",
        }
        retry_info = {
            "url": "https://example.com/high.m4a",
            "http_headers": {},
            "format_id": "141",
        }

        first_ydl = MagicMock()
        first_ydl.extract_info.return_value = first_info
        retry_ydl = MagicMock()
        retry_ydl.extract_info.return_value = retry_info
        mock_youtube_dl.return_value.__enter__.side_effect = [first_ydl, retry_ydl]

        utils = YTDLPUtils()
        result = utils.extract_stream_url("abc123", browser="brave")

        self.assertEqual(result["stream_url"], "https://example.com/high.m4a")
        self.assertEqual(result["format_id"], "141")
        self.assertEqual(mock_youtube_dl.call_count, 2)

        first_opts = mock_youtube_dl.call_args_list[0].args[0]
        retry_opts = mock_youtube_dl.call_args_list[1].args[0]
        self.assertEqual(first_opts["cookiesfrombrowser"], ("brave",))
        self.assertNotIn("cookiesfrombrowser", retry_opts)
        self.assertIn("cookiefile", retry_opts)

        utils.cleanup()

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_does_not_retry_when_first_stream_is_preferred(self, mock_youtube_dl):
        info = {
            "url": "https://example.com/high.m4a",
            "http_headers": {},
            "format_id": "141",
        }

        ydl = MagicMock()
        ydl.extract_info.return_value = info
        mock_youtube_dl.return_value.__enter__.return_value = ydl

        result = YTDLPUtils().extract_stream_url("abc123", browser="brave")

        self.assertEqual(result["stream_url"], "https://example.com/high.m4a")
        self.assertEqual(result["format_id"], "141")
        self.assertEqual(mock_youtube_dl.call_count, 1)

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_retry_failure_returns_first_valid_stream(self, mock_youtube_dl):
        first_info = {
            "url": "https://example.com/low.m4a",
            "http_headers": {},
            "format_id": "140",
        }

        first_ydl = MagicMock()
        first_ydl.extract_info.return_value = first_info
        retry_ydl = MagicMock()
        retry_ydl.extract_info.side_effect = RuntimeError("blocked")
        mock_youtube_dl.return_value.__enter__.side_effect = [first_ydl, retry_ydl]

        utils = YTDLPUtils()
        result = utils.extract_stream_url("abc123", browser="brave")

        self.assertEqual(result["stream_url"], "https://example.com/low.m4a")
        self.assertEqual(result["format_id"], "140")
        self.assertEqual(mock_youtube_dl.call_count, 2)

        utils.cleanup()

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_does_not_retry_when_cookiejar_cannot_be_cached(self, mock_youtube_dl):
        info = {
            "url": "https://example.com/low.m4a",
            "http_headers": {},
            "format_id": "140",
        }

        ydl = MagicMock()
        ydl.cookiejar = None
        ydl.extract_info.return_value = info
        mock_youtube_dl.return_value.__enter__.return_value = ydl

        result = YTDLPUtils().extract_stream_url("abc123", browser="brave")

        self.assertEqual(result["stream_url"], "https://example.com/low.m4a")
        self.assertEqual(result["format_id"], "140")
        self.assertEqual(mock_youtube_dl.call_count, 1)

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_stream_extraction_requires_browser_auth(self, mock_youtube_dl):
        with self.assertRaisesRegex(ValueError, "Browser auth is required"):
            YTDLPUtils().extract_stream_url("abc123", browser="")

        mock_youtube_dl.assert_not_called()

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

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_extract_browser_cookies_requires_browser_auth(self, mock_youtube_dl):
        with self.assertRaisesRegex(ValueError, "Browser auth is required"):
            YTDLPUtils().extract_browser_cookies("")

        mock_youtube_dl.assert_not_called()


if __name__ == "__main__":
    unittest.main()
