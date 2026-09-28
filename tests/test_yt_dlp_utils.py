#!/usr/bin/env python3

import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

from ytmusicfs.yt_dlp_utils import MAX_UNPRODUCTIVE_QUALITY_RETRIES, YTDLPUtils


class TestYTDLPUtils(unittest.TestCase):
    def _utils(self, **kwargs) -> YTDLPUtils:
        utils = YTDLPUtils(**kwargs)
        self.addCleanup(utils.cleanup)
        return utils

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

        utils = self._utils()
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

        result = self._utils().extract_playlist_content("LM", 10000, "brave")

        self.assertEqual([entry["id"] for entry in result], ["0", "1", "2"])
        self.assertEqual(mock_youtube_dl.call_count, 5)

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_playlist_extraction_returns_empty_when_every_attempt_is_empty(
        self, mock_youtube_dl
    ):
        empty = {"entries": [], "playlist_count": 10}
        contexts = [self._ydl()]
        contexts.extend(self._ydl(empty) for _ in range(4))
        mock_youtube_dl.return_value.__enter__.side_effect = contexts

        utils = self._utils()
        result = utils.extract_playlist_content("LM", 10000, "brave")

        self.assertEqual(result, [])
        self.assertEqual(mock_youtube_dl.call_count, 5)
        # The reported total survives even though no attempt returned tracks.
        self.assertEqual(utils.get_last_playlist_total_count("LM"), 10)

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

        result = self._utils().extract_stream_url("abc123", browser="brave")

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

        utils = self._utils()
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

        utils = self._utils()
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

        result = self._utils().extract_stream_url("abc123", browser="brave")

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

        result = self._utils().extract_stream_url("abc123", browser="brave")

        self.assertEqual(result["stream_url"], "https://example.com/high.m4a")
        self.assertEqual(mock_youtube_dl.call_count, 3)

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_does_not_retry_unavailable_stream(self, mock_youtube_dl):
        ydl = self._ydl()
        ydl.extract_info.side_effect = RuntimeError("Video unavailable")
        mock_youtube_dl.return_value.__enter__.side_effect = [self._ydl(), ydl]

        with self.assertRaisesRegex(RuntimeError, "Video unavailable"):
            self._utils().extract_stream_url("abc123", browser="brave")

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

        utils = self._utils()
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

        result = self._utils().extract_stream_url("abc123", browser="brave")

        self.assertEqual(result["stream_url"], "https://example.com/low.m4a")
        self.assertEqual(result["format_id"], "140")
        self.assertEqual(mock_youtube_dl.call_count, 3)

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_stream_extraction_requires_browser_auth(self, mock_youtube_dl):
        with self.assertRaisesRegex(ValueError, "Browser auth is required"):
            self._utils().extract_stream_url("abc123", browser="")

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

        cookies = self._utils().extract_browser_cookies("brave")

        self.assertEqual(cookies, {"SAPISID": "abc", "SID": "sid"})
        opts = mock_youtube_dl.call_args.args[0]
        self.assertEqual(opts["cookiesfrombrowser"], ("brave",))

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_extract_browser_cookies_requires_browser_auth(self, mock_youtube_dl):
        with self.assertRaisesRegex(ValueError, "Browser auth is required"):
            self._utils().extract_browser_cookies("")

        mock_youtube_dl.assert_not_called()

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_extract_browser_cookies_returns_empty_when_cookie_file_unreadable(
        self, mock_youtube_dl
    ):
        ydl = self._ydl()
        ydl.cookiejar = None  # nothing gets saved, leaving an empty file
        mock_youtube_dl.return_value.__enter__.return_value = ydl
        utils = self._utils()

        with self.assertLogs("YTDLPUtils", level="WARNING"):
            cookies = utils.extract_browser_cookies("brave")

        self.assertEqual(cookies, {})

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_ensure_browser_cookiefile_refreshes_stale_file_in_place(
        self, mock_youtube_dl
    ):
        mock_youtube_dl.return_value.__enter__.side_effect = [
            self._ydl(),
            self._ydl(),
        ]
        utils = self._utils()

        first = utils.ensure_browser_cookiefile("brave")
        self.assertEqual(utils.ensure_browser_cookiefile("brave"), first)
        self.assertEqual(mock_youtube_dl.call_count, 1)

        utils._browser_cookie_file_times["brave"] = 0.0
        second = utils.ensure_browser_cookiefile("brave")

        self.assertEqual(second, first)
        self.assertEqual(mock_youtube_dl.call_count, 2)

    def test_ensure_browser_cookiefile_requires_browser(self):
        with self.assertRaisesRegex(ValueError, "Browser auth is required"):
            self._utils().ensure_browser_cookiefile("")

    def test_has_auth_cookies_false_for_missing_file(self):
        self.assertFalse(YTDLPUtils._has_auth_cookies("/nonexistent/cookies.txt"))

    def test_cleanup_ignores_unlink_errors(self):
        utils = self._utils()
        utils._browser_cookie_files["brave"] = "/tmp/does-not-matter.cookies"

        with patch.object(Path, "unlink", side_effect=OSError("busy")):
            utils.cleanup()

        self.assertEqual(utils._browser_cookie_files, {})

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_playlist_extraction_returns_best_tracks_on_error(self, mock_youtube_dl):
        failing = self._ydl()
        failing.extract_info.side_effect = RuntimeError("boom")
        mock_youtube_dl.return_value.__enter__.side_effect = [
            self._ydl(),
            self._ydl({"entries": [{"id": "a"}], "playlist_count": 10}),
            failing,
        ]

        with self.assertLogs("YTDLPUtils", level="WARNING"):
            result = self._utils().extract_playlist_content("PL1", 100, "brave")

        self.assertEqual(result, [{"id": "a"}])

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_playlist_extraction_returns_empty_when_result_has_no_entries(
        self, mock_youtube_dl
    ):
        mock_youtube_dl.return_value.__enter__.side_effect = [
            self._ydl(),
            self._ydl({"title": "no entries"}),
        ]

        with self.assertLogs("YTDLPUtils", level="WARNING"):
            result = self._utils().extract_playlist_content("PL1", 100, "brave")

        self.assertEqual(result, [])

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_playlist_extraction_filters_entries_and_applies_limit(
        self, mock_youtube_dl
    ):
        mock_youtube_dl.return_value.__enter__.side_effect = [
            self._ydl(),
            self._ydl({"entries": [None, {"id": "a"}, {"id": "b"}, {"id": "c"}]}),
        ]
        utils = self._utils()

        result = utils.extract_playlist_content("PL1", 2, "brave")

        self.assertEqual(result, [{"id": "a"}, {"id": "b"}])
        self.assertIsNone(utils.get_last_playlist_total_count("PL1"))

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_playlist_extraction_treats_non_list_entries_as_empty(
        self, mock_youtube_dl
    ):
        mock_youtube_dl.return_value.__enter__.side_effect = [
            self._ydl(),
            self._ydl({"entries": iter([{"id": "a"}]), "playlist_count": "x"}),
        ]

        result = self._utils().extract_playlist_content("PL1", 10, "brave")

        self.assertEqual(result, [])

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_playlist_extraction_resolves_album_to_olak_playlist(self, mock_youtube_dl):
        redirect_ydl = self._ydl(
            {
                "_type": "url",
                "url": "https://music.youtube.com/playlist?list=OLAK5uy_abc-1",
            }
        )
        playlist_ydl = self._ydl({"entries": [{"id": "t1"}], "playlist_count": 1})
        mock_youtube_dl.return_value.__enter__.side_effect = [
            self._ydl(),
            redirect_ydl,
            playlist_ydl,
        ]

        result = self._utils().extract_playlist_content("MPREb_album", 10, "brave")

        self.assertEqual(result, [{"id": "t1"}])
        redirect_ydl.extract_info.assert_called_once_with(
            "https://music.youtube.com/browse/MPREb_album",
            download=False,
            process=False,
        )
        playlist_ydl.extract_info.assert_called_once_with(
            "https://music.youtube.com/playlist?list=OLAK5uy_abc-1", download=False
        )

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_album_url_falls_back_to_browse_url(self, mock_youtube_dl):
        browse_url = "https://music.youtube.com/browse/MPREb_album"
        failing = self._ydl()
        failing.extract_info.side_effect = RuntimeError("redirect failed")
        cases = [
            ("error", failing),
            ("not a redirect", self._ydl({"_type": "playlist"})),
            ("non-string url", self._ydl({"_type": "url", "url": 5})),
            ("url without OLAK list", self._ydl({"_type": "url", "url": "x?y=1"})),
            ("empty info", self._ydl({})),
        ]
        mock_youtube_dl.return_value.__enter__.side_effect = [self._ydl()] + [
            ydl for _, ydl in cases
        ]
        utils = self._utils()

        for label, _ in cases:
            with self.subTest(label):
                self.assertEqual(
                    utils._playlist_url("MPREb_album", "brave"), browse_url
                )

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_stream_extraction_raises_when_info_is_missing(self, mock_youtube_dl):
        empty = self._ydl()
        empty.extract_info.return_value = None
        mock_youtube_dl.return_value.__enter__.side_effect = [self._ydl(), empty]

        with self.assertRaisesRegex(RuntimeError, "Failed to extract stream URL"):
            self._utils().extract_stream_url("abc123", browser="brave")

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_stream_extraction_raises_after_last_transient_failure(
        self, mock_youtube_dl
    ):
        def failing():
            ydl = self._ydl()
            ydl.extract_info.side_effect = RuntimeError(
                "Requested format is not available"
            )
            return ydl

        mock_youtube_dl.return_value.__enter__.side_effect = [self._ydl()] + [
            failing() for _ in range(3)
        ]

        with (
            self.assertLogs("YTDLPUtils", level="WARNING"),
            self.assertRaisesRegex(RuntimeError, "Requested format"),
        ):
            self._utils().extract_stream_url("abc123", browser="brave")

        self.assertEqual(mock_youtube_dl.call_count, 4)

    @patch("ytmusicfs.yt_dlp_utils.STREAM_EXTRACTION_ATTEMPTS", 0)
    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_stream_extraction_without_attempts_raises(self, mock_youtube_dl):
        warmup = self._ydl()
        mock_youtube_dl.return_value.__enter__.side_effect = [warmup]

        with self.assertRaisesRegex(RuntimeError, "Failed to extract stream URL"):
            self._utils().extract_stream_url("abc123", browser="brave")

        # Only the browser cookie refresh ran; no watch page was extracted.
        self.assertEqual(mock_youtube_dl.call_count, 1)
        warmup.extract_info.assert_not_called()

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_quality_retry_failure_returns_first_stream(self, mock_youtube_dl):
        low = {"url": "https://example.com/low.m4a", "format_id": "140"}
        failing = self._ydl()
        failing.extract_info.side_effect = RuntimeError("network")
        mock_youtube_dl.return_value.__enter__.side_effect = [
            self._ydl(),
            self._ydl(low),
            failing,
        ]
        utils = self._utils()

        with self.assertLogs("YTDLPUtils", level="WARNING"):
            result = utils.extract_stream_url("abc123", browser="brave")

        self.assertEqual(result["stream_url"], "https://example.com/low.m4a")
        self.assertEqual(utils._unproductive_quality_retries, 1)

    def test_retry_with_cached_cookies_skips_when_cookie_file_missing(self):
        utils = self._utils()

        self.assertIsNone(utils._retry_stream_url_with_cached_cookies("v", "brave"))

        utils._browser_cookie_files["brave"] = "/nonexistent/cookies.txt"
        self.assertIsNone(utils._retry_stream_url_with_cached_cookies("v", "brave"))

    def test_stream_result_from_info_normalizes_fields(self):
        utils = self._utils()
        cookie_objects = [SimpleNamespace(name="A", value="1")]
        cases = [
            (
                "cookie objects",
                {"url": "u", "cookies": cookie_objects, "duration": 12.7},
                {
                    "stream_url": "u",
                    "http_headers": {},
                    "cookies": {"A": "1"},
                    "duration": 12,
                },
            ),
            (
                "cookie dict",
                {"url": "u", "cookies": {"A": "1"}, "format_id": 140},
                {
                    "stream_url": "u",
                    "http_headers": {},
                    "cookies": {"A": "1"},
                    "format_id": "140",
                },
            ),
            (
                "cookie dict list",
                {
                    "url": "u",
                    "cookies": [{"name": "A", "value": "1"}, {"name": "B"}, "x"],
                },
                {"stream_url": "u", "http_headers": {}, "cookies": {"A": "1"}},
            ),
            (
                "unhashable cookie name",
                {"url": "u", "cookies": [{"name": ["bad"], "value": "1"}]},
                {"stream_url": "u", "http_headers": {}},
            ),
            (
                "unsupported cookie type",
                {"url": "u", "cookies": "A=1", "duration": "long"},
                {"stream_url": "u", "http_headers": {}},
            ),
            (
                "none values",
                {"url": "u", "http_headers": None, "format_id": None, "duration": None},
                {"stream_url": "u", "http_headers": {}},
            ),
        ]

        for label, info, expected in cases:
            with self.subTest(label):
                self.assertEqual(utils._stream_result_from_info(info), expected)

    def test_extract_stream_url_async_requires_thread_manager(self):
        with self.assertRaisesRegex(RuntimeError, "ThreadManager not set"):
            self._utils().extract_stream_url_async("v", "brave")

    def test_extract_stream_url_async_submits_worker(self):
        thread_manager = Mock()
        utils = self._utils(thread_manager=thread_manager)

        future = utils.extract_stream_url_async("v", "brave")

        self.assertIs(future, thread_manager.submit_task.return_value)
        thread_manager.submit_task.assert_called_once_with(
            "extraction", utils._extract_stream_url_worker, "v", "brave"
        )

    def test_extract_stream_url_worker_wraps_success(self):
        utils = self._utils()
        utils.extract_stream_url = Mock(return_value={"stream_url": "u"})

        self.assertEqual(
            utils._extract_stream_url_worker("v", "brave"),
            {"status": "success", "stream_url": "u"},
        )

    def test_extract_stream_url_worker_reports_errors(self):
        utils = self._utils()
        for message, level in (
            ("Video unavailable", "WARNING"),
            ("socket closed", "ERROR"),
        ):
            with self.subTest(message):
                utils.extract_stream_url = Mock(side_effect=RuntimeError(message))
                with self.assertLogs("YTDLPUtils", level=level) as logs:
                    result = utils._extract_stream_url_worker("v", "brave")
                self.assertEqual(result, {"status": "error", "error": message})
                self.assertEqual(logs.records[-1].levelname, level)


class TestYTDLPUtilsQualityRetry(unittest.TestCase):
    @staticmethod
    def _ydl(result=None):
        ydl = MagicMock()
        if result is not None:
            ydl.extract_info.return_value = result
        ydl.cookiejar = MagicMock()
        ydl.cookiejar.__iter__.return_value = iter(
            [
                SimpleNamespace(domain=".youtube.com", name="SAPISID", value="a"),
                SimpleNamespace(domain=".youtube.com", name="APISID", value="b"),
            ]
        )
        return ydl

    def _utils(self) -> YTDLPUtils:
        utils = YTDLPUtils()
        self.addCleanup(utils.cleanup)
        utils._has_auth_cookies = Mock(return_value=True)
        return utils

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_stops_retrying_when_account_never_gets_preferred_format(self, ydl_class):
        low = {"url": "https://example.com/low", "http_headers": {}, "format_id": "140"}
        ydl_class.return_value.__enter__.side_effect = lambda: self._ydl(low)
        utils = self._utils()

        for index in range(MAX_UNPRODUCTIVE_QUALITY_RETRIES + 2):
            utils.extract_stream_url(f"v{index}", browser="brave")

        # One cookie refresh, one extraction per track, and one retry for
        # each of the first MAX_UNPRODUCTIVE_QUALITY_RETRIES tracks.
        expected = 1 + (MAX_UNPRODUCTIVE_QUALITY_RETRIES + 2)
        expected += MAX_UNPRODUCTIVE_QUALITY_RETRIES
        self.assertEqual(ydl_class.call_count, expected)

    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_keeps_retrying_once_preferred_format_was_seen(self, ydl_class):
        high = {"url": "https://example.com/hi", "http_headers": {}, "format_id": "141"}
        low = {"url": "https://example.com/low", "http_headers": {}, "format_id": "140"}
        utils = self._utils()
        utils._unproductive_quality_retries = MAX_UNPRODUCTIVE_QUALITY_RETRIES
        ydl_class.return_value.__enter__.side_effect = [
            self._ydl(),
            self._ydl(high),
            self._ydl(low),
            self._ydl(high),
        ]

        utils.extract_stream_url("first", browser="brave")
        result = utils.extract_stream_url("second", browser="brave")

        self.assertEqual(result["format_id"], "141")
        self.assertEqual(ydl_class.call_count, 4)


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
