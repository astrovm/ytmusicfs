#!/usr/bin/env python3

import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from ytmusicfs.yt_dlp_utils import YTDLPUtils


class TestYTDLPUtils(unittest.TestCase):
    def _ydl(self, result=None, cookies=None):
        ydl = MagicMock()
        if result is not None:
            ydl.extract_info.return_value = result
        default_cookies = [
            SimpleNamespace(domain=".youtube.com", name="SAPISID", value="abc"),
            SimpleNamespace(domain=".youtube.com", name="APISID", value="def"),
        ]
        ydl.cookiejar = FakeCookieJar(
            cookies if cookies is not None else default_cookies
        )
        return ydl

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
        contexts = [self._ydl()]
        contexts.extend(self._ydl(result) for result in results)
        mock_youtube_dl.return_value.__enter__.side_effect = contexts

        result = YTDLPUtils().extract_playlist_content("LM", 10000, "brave")

        self.assertEqual([entry["id"] for entry in result], ["0", "1", "2"])
        self.assertEqual(mock_youtube_dl.call_count, 5)

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_stream_extraction_enables_ejs_runtime(self, mock_youtube_dl):
        info = {
            "url": "https://example.com/audio.m4a",
            "http_headers": {"User-Agent": "UnitTest"},
            "format_id": "141",
        }

        mock_youtube_dl.return_value.__enter__.side_effect = [
            self._ydl(),
            self._ydl(info),
        ]

        result = YTDLPUtils().extract_stream_url("abc123", browser="brave")

        self.assertEqual(result["stream_url"], "https://example.com/audio.m4a")
        opts = mock_youtube_dl.call_args.args[0]
        self.assertEqual(opts["format"], "141/140/bestaudio[ext=m4a]")
        self.assertNotIn("cookiesfrombrowser", opts)
        self.assertIn("cookiefile", opts)
        self.assertEqual(
            opts["extractor_args"], {"youtube": {"formats": ["missing_pot"]}}
        )
        self.assertIn("node", opts["js_runtimes"])
        # Only assert deno if it's available in this environment
        # (js_runtimes now dynamically detects available runtimes)

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

        warmup_ydl = self._ydl()
        first_ydl = self._ydl(first_info)
        second_ydl = self._ydl(second_info)
        mock_youtube_dl.return_value.__enter__.side_effect = [
            warmup_ydl,
            first_ydl,
            second_ydl,
        ]

        utils = YTDLPUtils()
        utils.extract_stream_url("one", browser="brave")
        utils.extract_stream_url("two", browser="brave")

        first_opts = mock_youtube_dl.call_args_list[1].args[0]
        second_opts = mock_youtube_dl.call_args_list[2].args[0]
        self.assertNotIn("cookiesfrombrowser", first_opts)
        self.assertIn("cookiefile", first_opts)
        self.assertNotIn("cookiesfrombrowser", second_opts)
        self.assertIn("cookiefile", second_opts)

        cookie_file = second_opts["cookiefile"]
        self.assertTrue(utils._browser_cookie_files)
        utils.cleanup()
        self.assertFalse(utils._browser_cookie_files)
        self.assertFalse(Path(cookie_file).exists())

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_retries_non_preferred_stream_with_browser_cookie_file(
        self, mock_youtube_dl
    ):
        """Non-141 streams must be retried with the reusable browser cookie file."""
        first_info = {
            "url": "https://example.com/low.m4a",
            "http_headers": {},
            "format_id": "140",
        }
        second_info = {
            "url": "https://example.com/high.m4a",
            "http_headers": {},
            "format_id": "141",
        }

        mock_youtube_dl.return_value.__enter__.side_effect = [
            self._ydl(),
            self._ydl(first_info),
            self._ydl(second_info),
        ]

        utils = YTDLPUtils()
        result = utils.extract_stream_url("abc123", browser="brave")

        self.assertEqual(result["stream_url"], "https://example.com/high.m4a")
        self.assertEqual(result["format_id"], "141")
        self.assertEqual(mock_youtube_dl.call_count, 3)

        first_opts = mock_youtube_dl.call_args_list[1].args[0]
        retry_opts = mock_youtube_dl.call_args_list[2].args[0]
        self.assertNotIn("cookiesfrombrowser", first_opts)
        self.assertIn("cookiefile", first_opts)
        self.assertNotIn("cookiesfrombrowser", retry_opts)
        self.assertEqual(retry_opts["cookiefile"], first_opts["cookiefile"])

        utils.cleanup()

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_does_not_retry_when_first_stream_is_preferred(self, mock_youtube_dl):
        info = {
            "url": "https://example.com/high.m4a",
            "http_headers": {},
            "format_id": "141",
        }

        mock_youtube_dl.return_value.__enter__.side_effect = [
            self._ydl(),
            self._ydl(info),
        ]

        result = YTDLPUtils().extract_stream_url("abc123", browser="brave")

        self.assertEqual(result["stream_url"], "https://example.com/high.m4a")
        self.assertEqual(result["format_id"], "141")
        self.assertEqual(mock_youtube_dl.call_count, 2)

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_retries_transient_stream_format_failure(self, mock_youtube_dl):
        info = {
            "url": "https://example.com/high.m4a",
            "http_headers": {},
            "format_id": "141",
        }

        first_ydl = self._ydl()
        first_ydl.extract_info.side_effect = RuntimeError(
            "Requested format is not available"
        )
        mock_youtube_dl.return_value.__enter__.side_effect = [
            self._ydl(),
            first_ydl,
            self._ydl(info),
        ]

        result = YTDLPUtils().extract_stream_url("abc123", browser="brave")

        self.assertEqual(result["stream_url"], "https://example.com/high.m4a")
        self.assertEqual(mock_youtube_dl.call_count, 3)

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_does_not_retry_unavailable_stream(self, mock_youtube_dl):
        ydl = self._ydl()
        ydl.extract_info.side_effect = RuntimeError("Video unavailable")
        mock_youtube_dl.return_value.__enter__.side_effect = [self._ydl(), ydl]

        with self.assertRaisesRegex(RuntimeError, "Video unavailable"):
            YTDLPUtils().extract_stream_url("abc123", browser="brave")

        self.assertEqual(mock_youtube_dl.call_count, 2)

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_returns_first_stream_when_quality_retry_does_not_upgrade(
        self, mock_youtube_dl
    ):
        first_info = {
            "url": "https://example.com/low.m4a",
            "http_headers": {},
            "format_id": "140",
        }
        second_info = {
            "url": "https://example.com/low-retry.m4a",
            "http_headers": {},
            "format_id": "140",
        }

        mock_youtube_dl.return_value.__enter__.side_effect = [
            self._ydl(),
            self._ydl(first_info),
            self._ydl(second_info),
        ]

        utils = YTDLPUtils()
        result = utils.extract_stream_url("abc123", browser="brave")

        self.assertEqual(result["stream_url"], "https://example.com/low.m4a")
        self.assertEqual(result["format_id"], "140")
        self.assertEqual(mock_youtube_dl.call_count, 3)

        utils.cleanup()

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_retries_even_when_post_extraction_cookiejar_cannot_be_cached(
        self, mock_youtube_dl
    ):
        info = {
            "url": "https://example.com/low.m4a",
            "http_headers": {},
            "format_id": "140",
        }

        ydl = self._ydl(info)
        ydl.cookiejar = None
        mock_youtube_dl.return_value.__enter__.side_effect = [
            self._ydl(),
            ydl,
            self._ydl(info),
        ]

        result = YTDLPUtils().extract_stream_url("abc123", browser="brave")

        self.assertEqual(result["stream_url"], "https://example.com/low.m4a")
        self.assertEqual(result["format_id"], "140")
        self.assertEqual(mock_youtube_dl.call_count, 3)

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_stream_extraction_requires_browser_auth(self, mock_youtube_dl):
        with self.assertRaisesRegex(ValueError, "Browser auth is required"):
            YTDLPUtils().extract_stream_url("abc123", browser="")

        mock_youtube_dl.assert_not_called()

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_extract_browser_cookies_filters_youtube_domains(self, mock_youtube_dl):
        ydl = self._ydl(
            cookies=[
                SimpleNamespace(name="SAPISID", value="abc", domain=".youtube.com"),
                SimpleNamespace(name="SID", value="sid", domain=".music.youtube.com"),
                SimpleNamespace(name="OTHER", value="nope", domain="example.com"),
                SimpleNamespace(name="EMPTY", value=None, domain=".youtube.com"),
            ]
        )
        mock_youtube_dl.return_value.__enter__.return_value = ydl

        cookies = YTDLPUtils().extract_browser_cookies("brave")

        self.assertEqual(cookies, {"SAPISID": "abc", "SID": "sid"})
        opts = mock_youtube_dl.call_args.args[0]
        self.assertEqual(opts["cookiesfrombrowser"], ("brave",))

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_extract_browser_cookies_requires_browser_auth(self, mock_youtube_dl):
        with self.assertRaisesRegex(ValueError, "Browser auth is required"):
            YTDLPUtils().extract_browser_cookies("")

        mock_youtube_dl.assert_not_called()


class FakeCookieJar(list):
    def save(self, filename, ignore_discard=True, ignore_expires=True):
        with open(filename, "w", encoding="utf-8") as cookie_file:
            cookie_file.write("# Netscape HTTP Cookie File\n")
            for cookie in self:
                value = getattr(cookie, "value", None)
                if value is None:
                    continue
                domain = getattr(cookie, "domain", "")
                include_subdomains = "TRUE" if str(domain).startswith(".") else "FALSE"
                cookie_file.write(
                    "\t".join(
                        [
                            str(domain),
                            include_subdomains,
                            "/",
                            "FALSE",
                            "0",
                            str(cookie.name),
                            str(value),
                        ]
                    )
                    + "\n"
                )


if __name__ == "__main__":
    unittest.main()
