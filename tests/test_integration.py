#!/usr/bin/env python3

import errno
import logging
import shutil
import stat
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from fuse import FuseOSError

from ytmusicfs.cache import CacheManager
from ytmusicfs.content_fetcher import ContentFetcher
from ytmusicfs.file_handler import FileHandler
from ytmusicfs.filesystem import YouTubeMusicFS
from ytmusicfs.thread_manager import ThreadManager

PLAYLIST_PATH = "/playlists/My Playlist"
PROCESSED_TRACKS = [
    {"filename": "Song 1.m4a", "videoId": "vid123", "duration_seconds": 225},
    {"filename": "Song 2.m4a", "videoId": "vid456", "duration_seconds": 260},
]


class TestYouTubeMusicFSIntegration(unittest.TestCase):
    """YouTubeMusicFS wired to a real CacheManager, MetadataManager and ThreadManager.

    Network-facing collaborators (client, fetcher, file handler, yt-dlp) are mocked.
    """

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.temp_dir, ignore_errors=True)
        self.cache_dir = Path(self.temp_dir)
        self.logger = logging.getLogger("test")

        self.thread_manager = ThreadManager(logger=self.logger)
        self.cache = CacheManager(
            cache_dir=self.cache_dir,
            thread_manager=self.thread_manager,
            cache_timeout=3600,
            logger=self.logger,
        )
        self.content_fetcher = Mock(spec=ContentFetcher)
        self.content_fetcher.registry_loaded_from_cache = False
        self.content_fetcher.get_playlist_entry_from_path.return_value = None
        self.file_handler = Mock(spec=FileHandler)
        self.router = Mock()
        self.router.validate_path.return_value = True

        with (
            patch(
                "ytmusicfs.filesystem.ThreadManager", return_value=self.thread_manager
            ),
            patch("ytmusicfs.filesystem.YTMusicAuthAdapter"),
            patch("ytmusicfs.filesystem.YouTubeMusicClient"),
            patch("ytmusicfs.filesystem.YTDLPUtils"),
            patch("ytmusicfs.filesystem.TrackProcessor"),
            patch("ytmusicfs.filesystem.CacheManager", return_value=self.cache),
            patch("ytmusicfs.filesystem.PathRouter", return_value=self.router),
            patch(
                "ytmusicfs.filesystem.ContentFetcher", return_value=self.content_fetcher
            ),
            patch("ytmusicfs.filesystem.FileHandler", return_value=self.file_handler),
        ):
            self.fs = YouTubeMusicFS(cache_dir=str(self.cache_dir), browser="brave")
        # Let the idle pre-cache worker run immediately instead of polling.
        self.fs.PRECACHE_IDLE_SECONDS = 0

    def tearDown(self):
        self.thread_manager.shutdown(wait=True, timeout=5.0)
        self.cache.close()

    def test_playlist_listing_is_served_from_memory_then_from_sqlite(self):
        self.fs._cache_directory_listing_with_attrs(PLAYLIST_PATH, PROCESSED_TRACKS)

        self.assertEqual(
            self.fs.readdir(PLAYLIST_PATH),
            [".", "..", "Song 1.m4a", "Song 2.m4a"],
        )
        song_attrs = self.fs.getattr(f"{PLAYLIST_PATH}/Song 1.m4a")
        self.assertTrue(stat.S_ISREG(song_attrs["st_mode"]))
        self.assertEqual(
            song_attrs["st_size"], 225 * YouTubeMusicFS.ESTIMATED_BYTES_PER_SECOND
        )

        # Drop the in-memory copy; the persisted listing must still resolve.
        self.fs._clear_hot_metadata()
        self.fs.last_access_results.clear()
        self.assertEqual(
            self.fs.readdir(PLAYLIST_PATH),
            [".", "..", "Song 1.m4a", "Song 2.m4a"],
        )
        self.assertEqual(
            self.fs.metadata_manager.get_video_id(f"{PLAYLIST_PATH}/Song 2.m4a"),
            "vid456",
        )
        self.router.route.assert_not_called()

    def test_unavailable_track_is_hidden_from_listing_and_getattr(self):
        self.fs._cache_directory_listing_with_attrs(PLAYLIST_PATH, PROCESSED_TRACKS)
        self.cache.mark_unavailable_track(
            "vid123", f"{PLAYLIST_PATH}/Song 1.m4a", "Video unavailable"
        )

        self.assertEqual(self.fs.readdir(PLAYLIST_PATH), [".", "..", "Song 2.m4a"])
        with self.assertRaises(FuseOSError) as cm:
            self.fs.getattr(f"{PLAYLIST_PATH}/Song 1.m4a")
        self.assertEqual(cm.exception.errno, errno.ENOENT)

    def test_open_and_read_stream_through_file_handler_with_hot_video_id(self):
        self.fs._cache_directory_listing_with_attrs(PLAYLIST_PATH, PROCESSED_TRACKS)
        self.file_handler.open.return_value = 42
        self.file_handler.read.return_value = b"audio"
        path = f"{PLAYLIST_PATH}/Song 1.m4a"

        fh = self.fs.open(path, 0)
        data = self.fs.read(path, 5, 0, fh)

        self.assertEqual(data, b"audio")
        self.file_handler.open.assert_called_once_with(path, "vid123")
        self.file_handler.read.assert_called_once_with(path, 5, 0, 42)

    def test_read_maps_stream_network_error_to_its_errno(self):
        self.file_handler.read.side_effect = OSError(errno.ECONNRESET, "reset")

        with self.assertRaises(FuseOSError) as cm:
            self.fs.read(f"{PLAYLIST_PATH}/Song 1.m4a", 1024, 0, 42)

        self.assertEqual(cm.exception.errno, errno.ECONNRESET)

    def test_concurrent_readdir_and_getattr_return_consistent_results(self):
        self.fs._cache_directory_listing_with_attrs(PLAYLIST_PATH, PROCESSED_TRACKS)
        errors: list[BaseException] = []
        listings: list[list[str]] = []
        sizes: list[int] = []

        def worker():
            try:
                for _ in range(20):
                    listings.append(self.fs.readdir(PLAYLIST_PATH))
                    sizes.append(
                        self.fs.getattr(f"{PLAYLIST_PATH}/Song 2.m4a")["st_size"]
                    )
            except BaseException as exc:  # pragma: no cover - reported below
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])
        self.assertEqual(len(listings), 160)
        self.assertTrue(
            all(entry == [".", "..", "Song 1.m4a", "Song 2.m4a"] for entry in listings)
        )
        self.assertEqual(set(sizes), {260 * YouTubeMusicFS.ESTIMATED_BYTES_PER_SECOND})

    def test_refresh_trigger_clears_cached_metadata_and_schedules_refresh(self):
        self.fs._cache_directory_listing_with_attrs(PLAYLIST_PATH, PROCESSED_TRACKS)
        refresh_done = threading.Event()
        self.fs._automatic_refresh_after_mount = refresh_done.set
        self.cache.record_cache_trigger("refresh")

        self.fs._check_repair_notifications()

        self.assertTrue(refresh_done.wait(2))
        self.assertIsNone(self.cache.get_pending_cache_trigger())
        self.assertEqual(self.fs.hot_paths, {})
        self.assertIsNone(self.cache.get_directory_listing_with_attrs(PLAYLIST_PATH))

    def test_repair_trigger_invalidates_repaired_hot_path(self):
        self.fs._cache_directory_listing_with_attrs(PLAYLIST_PATH, PROCESSED_TRACKS)
        path = f"{PLAYLIST_PATH}/Song 1.m4a"
        self.cache.record_repair_trigger(
            [{"old_video_id": "vid123", "path": path, "new_video_id": "vid999"}]
        )

        self.fs._check_repair_notifications()

        self.assertNotIn(path, self.fs.hot_paths)
        self.assertIn(f"{PLAYLIST_PATH}/Song 2.m4a", self.fs.hot_paths)
        self.assertIsNone(self.cache.get_pending_repair_trigger())


if __name__ == "__main__":
    unittest.main()
