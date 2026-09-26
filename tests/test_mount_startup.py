#!/usr/bin/env python3

import logging
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

from ytmusicfs.content_fetcher import ContentFetcher
from ytmusicfs.dependencies import ContentFetcherDependencies
from ytmusicfs.filesystem import YouTubeMusicFS
from ytmusicfs.yt_dlp_utils import MAX_UNPRODUCTIVE_QUALITY_RETRIES, YTDLPUtils

SAVED_REGISTRY = [
    {"name": "liked_songs", "id": "LM", "type": "liked_songs", "path": "/liked_songs"},
    {"name": "Mix", "id": "PL1", "type": "playlist", "path": "/playlists/Mix"},
]


def build_fetcher(cache: Mock, client: Mock) -> ContentFetcher:
    processor = Mock()
    processor.sanitize_filename.side_effect = lambda name: name
    return ContentFetcher(
        ContentFetcherDependencies(
            client=client,
            processor=processor,
            cache=cache,
            logger=logging.getLogger("test"),
            yt_dlp=Mock(),
            browser="brave",
        )
    )


class TestSavedRegistryStartup(unittest.TestCase):
    def setUp(self):
        self.cache = Mock()
        self.cache.get_refresh_metadata.return_value = (None, None)
        self.cache.get_directory_listing_with_attrs.return_value = None
        self.client = Mock()
        self.client.get_library_playlists.return_value = [
            {"title": "Mix", "playlistId": "PL1"},
            {"title": "New", "playlistId": "PL2"},
        ]
        self.client.get_library_albums.return_value = []

    def test_saved_registry_skips_network_at_startup(self):
        self.cache.get.return_value = SAVED_REGISTRY

        fetcher = build_fetcher(self.cache, self.client)

        self.assertTrue(fetcher.registry_loaded_from_cache)
        self.assertEqual(fetcher.get_playlist_id_from_name("Mix", "playlist"), "PL1")
        self.client.get_library_playlists.assert_not_called()

    def test_first_mount_without_saved_registry_fetches(self):
        self.cache.get.return_value = None

        fetcher = build_fetcher(self.cache, self.client)

        self.assertFalse(fetcher.registry_loaded_from_cache)
        self.client.get_library_playlists.assert_called_once()
        self.assertEqual(fetcher.get_playlist_id_from_name("New", "playlist"), "PL2")

    def test_refresh_library_roots_republishes_root_listings(self):
        self.cache.get.return_value = SAVED_REGISTRY
        fetcher = build_fetcher(self.cache, self.client)
        published: dict[str, list[str]] = {}
        fetcher.cache_directory_callback = lambda path, tracks: published.update(
            {path: [track["filename"] for track in tracks]}
        )

        fetcher.refresh_library_roots()

        self.client.get_library_playlists.assert_called_once()
        self.assertEqual(published["/playlists"], ["Mix", "New"])
        self.assertEqual(fetcher.get_playlist_id_from_name("New", "playlist"), "PL2")

    def test_mount_init_refreshes_roots_only_for_saved_registry(self):
        for loaded_from_cache, expected_calls in ((True, 1), (False, 0)):
            fs = object.__new__(YouTubeMusicFS)
            fs.fetcher = Mock(registry_loaded_from_cache=loaded_from_cache)
            fs.logger = logging.getLogger("test")
            fs._check_repair_notifications = Mock()
            fs._automatic_refresh_after_mount = Mock()
            fs._poll_repair_notifications = Mock()
            done = threading.Event()
            fs.fetcher.refresh_library_roots.side_effect = lambda _d=done: _d.set()

            fs.init("/")

            if expected_calls:
                self.assertTrue(done.wait(2))
            self.assertEqual(
                fs.fetcher.refresh_library_roots.call_count, expected_calls
            )


class TestQualityRetryCutoff(unittest.TestCase):
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


class TestRecentResultCacheBound(unittest.TestCase):
    def test_recent_results_are_bounded(self):
        with (
            patch("ytmusicfs.filesystem.ThreadManager") as thread_manager,
            patch("ytmusicfs.filesystem.YTDLPUtils"),
            patch("ytmusicfs.filesystem.YTMusicAuthAdapter"),
            patch("ytmusicfs.filesystem.YouTubeMusicClient"),
            patch("ytmusicfs.filesystem.TrackProcessor"),
            patch("ytmusicfs.filesystem.CacheManager") as cache,
            patch("ytmusicfs.filesystem.ContentFetcher"),
            patch("ytmusicfs.filesystem.PathRouter"),
            patch("ytmusicfs.filesystem.FileHandler"),
            patch("ytmusicfs.filesystem.MetadataManager"),
        ):
            thread_manager.return_value.create_lock.side_effect = threading.RLock
            cache.return_value.get_directory_listing_with_attrs.return_value = None
            fs = YouTubeMusicFS(cache_dir="/tmp/ytmusicfs-test", browser="brave")

        for index in range(fs.RECENT_RESULT_CACHE_SIZE + 100):
            fs._store_readdir_result(f"readdir:/p{index}", [".", ".."])
        self.assertEqual(len(fs.last_access_results), fs.RECENT_RESULT_CACHE_SIZE)


class TestRawFileInfoOpen(unittest.TestCase):
    def _fs(self, real_size):
        fs = object.__new__(YouTubeMusicFS)
        fs._open_handle = Mock(return_value=7)
        fs._get_real_file_size = Mock(return_value=real_size)
        fs.file_handler = Mock()
        fs.file_handler.read.return_value = b"data"
        fs.file_handler.release.return_value = 0
        fs.logger = logging.getLogger("test")
        fs._record_stat = Mock()
        fs._record_elapsed = Mock()
        return fs

    def test_unknown_size_uses_direct_io(self):
        fs = self._fs(real_size=None)
        info = SimpleNamespace(flags=0, fh=0, direct_io=0)

        self.assertEqual(fs.open("/liked_songs/a.m4a", info), 0)

        self.assertEqual(info.fh, 7)
        self.assertEqual(info.direct_io, 1)
        self.assertEqual(fs.read("/liked_songs/a.m4a", 4, 0, info), b"data")
        fs.file_handler.read.assert_called_once_with("/liked_songs/a.m4a", 4, 0, 7)
        fs.release("/liked_songs/a.m4a", info)
        fs.file_handler.release.assert_called_once_with("/liked_songs/a.m4a", 7)

    def test_known_size_keeps_page_cache(self):
        fs = self._fs(real_size=1234)
        info = SimpleNamespace(flags=0, fh=0, direct_io=0)

        fs.open("/liked_songs/a.m4a", info)

        self.assertEqual(info.direct_io, 0)

    def test_plain_flags_still_return_handle(self):
        self.assertEqual(self._fs(real_size=None).open("/liked_songs/a.m4a", 0), 7)


if __name__ == "__main__":
    unittest.main()
