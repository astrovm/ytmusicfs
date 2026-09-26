#!/usr/bin/env python3

import logging
import os
import shutil
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from ytmusicfs.cache import CacheManager
from ytmusicfs.dependencies import FileHandlerDependencies
from ytmusicfs.file_handler import FileHandler
from ytmusicfs.filesystem import YouTubeMusicFS


class TestCacheHotPaths(unittest.TestCase):
    """Hot-path cache behavior against a real SQLite database."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        thread_manager = Mock()
        thread_manager.create_lock.side_effect = threading.RLock
        self.cache = CacheManager(
            thread_manager=thread_manager,
            cache_dir=self.temp_dir,
            logger=logging.getLogger("test"),
        )

    def tearDown(self):
        self.cache.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _row_count(self) -> int:
        return int(
            self.cache.conn.execute("SELECT COUNT(*) FROM cache_entries").fetchone()[0]
        )

    def test_mark_valid_skips_known_paths(self):
        self.cache.mark_valid("/playlists/Mix/a.m4a", is_directory=False)
        with patch.object(self.cache, "_record_write") as record_write:
            self.cache.mark_valid("/playlists/Mix/a.m4a", is_directory=False)
            self.cache.mark_valid("/playlists/Mix/a.m4a")
        record_write.assert_not_called()

    def test_mark_valid_persists_type_changes(self):
        self.cache.mark_valid("/playlists/Mix", is_directory=False)
        self.cache.mark_valid("/playlists/Mix", is_directory=True)
        self.assertEqual(self.cache.get_entry_type("/playlists/Mix"), "directory")
        keys = {
            row[0] for row in self.cache.conn.execute("SELECT key FROM cache_entries")
        }
        self.assertIn(self.cache.path_to_key("valid_dir:/playlists/Mix"), keys)
        self.assertNotIn(self.cache.path_to_key("exact_path:/playlists/Mix"), keys)

    def test_get_many_reads_hot_and_database_entries(self):
        self.cache.set("filesize:/a", 1)
        self.cache.set("filesize:/b", 2)
        self.cache.hotcache.clear()
        self.cache.set("filesize:/c", 3)

        values = self.cache.get_many(["filesize:/a", "filesize:/b", "filesize:/c", "x"])

        self.assertEqual(values, {"filesize:/a": 1, "filesize:/b": 2, "filesize:/c": 3})

    def test_update_file_attrs_merges_and_writes_only_listing(self):
        listing = {
            "a.m4a": {"st_mode": 0o100644, "st_size": 10, "videoId": "vid_a"},
            "b.m4a": {"st_mode": 0o100644, "st_size": 20, "videoId": "vid_b"},
        }
        self.cache.set_directory_listing_with_attrs("/liked_songs", listing)
        rows_before = self._row_count()

        with patch.object(self.cache, "set_batch") as set_batch:
            self.cache.update_file_attrs_in_parent_dir(
                "/liked_songs/a.m4a", {"st_size": 999}
            )

        set_batch.assert_not_called()
        self.assertEqual(self._row_count(), rows_before)
        attrs = self.cache.get_file_attrs_from_parent_dir("/liked_songs/a.m4a")
        self.assertEqual(attrs["st_size"], 999)
        self.assertEqual(attrs["videoId"], "vid_a")
        self.cache.directory_listings_cache.clear()
        self.cache.hotcache.clear()
        persisted = self.cache.get_directory_listing_with_attrs("/liked_songs")
        self.assertEqual(persisted["a.m4a"]["st_size"], 999)
        self.assertEqual(persisted["a.m4a"]["videoId"], "vid_a")


class TestStreamingHotPaths(unittest.TestCase):
    """Read-ahead and cached playback behavior."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        thread_manager = Mock()
        thread_manager.create_lock.side_effect = threading.RLock
        cache = Mock()
        cache.get_unavailable_track.return_value = None
        self.handler = FileHandler(
            FileHandlerDependencies(
                thread_manager=thread_manager,
                cache_dir=Path(self.temp_dir),
                cache=cache,
                logger=logging.getLogger("test"),
                update_file_size=Mock(),
                yt_dlp=Mock(),
                browser="brave",
            )
        )
        self.handler.downloader = Mock()
        self.handler.downloader.get_progress.return_value = None

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _open_streaming(self, path: str) -> int:
        fh = self.handler.open(path, "vid")
        self.handler.open_files[fh]["stream_url"] = "https://example.com/a.m4a"
        self.handler.open_files[fh]["format_id"] = "141"
        return fh

    def test_sequential_reads_are_served_from_readahead(self):
        path = "/liked_songs/a.m4a"
        fh = self._open_streaming(path)
        audio = bytes(range(256)) * 16384

        def fake_stream(request):
            return audio[request.offset : request.offset + request.size]

        with patch.object(
            self.handler, "_stream_content", side_effect=fake_stream
        ) as stream:
            chunks = [
                self.handler.read(path, 131072, offset, fh)
                for offset in range(0, 1024 * 1024, 131072)
            ]

        self.assertEqual(b"".join(chunks), audio[: 1024 * 1024])
        sizes = [c.args[0].size for c in stream.call_args_list]
        self.assertEqual(sizes, [256 * 1024, 512 * 1024, 1024 * 1024])

    def test_seek_resets_readahead_window(self):
        path = "/liked_songs/a.m4a"
        fh = self._open_streaming(path)
        with patch.object(
            self.handler,
            "_stream_content",
            side_effect=lambda request: b"x" * request.size,
        ) as stream:
            self.handler.read(path, 4096, 0, fh)
            self.handler.read(path, 4096, 256 * 1024, fh)
            self.handler.read(path, 4096, 3 * 1024 * 1024, fh)

        sizes = [c.args[0].size for c in stream.call_args_list]
        self.assertEqual(sizes, [256 * 1024, 512 * 1024, 256 * 1024])

    def test_readahead_serves_short_reads_at_eof(self):
        path = "/liked_songs/a.m4a"
        fh = self._open_streaming(path)
        with patch.object(
            self.handler, "_stream_content", return_value=b"tail"
        ) as stream:
            self.assertEqual(self.handler.read(path, 4096, 0, fh), b"tail")
            self.assertEqual(self.handler.read(path, 4096, 4, fh), b"")
        stream.assert_called_once()

    def test_complete_cached_audio_reuses_one_descriptor(self):
        audio_dir = Path(self.temp_dir) / "audio"
        audio_dir.mkdir(parents=True, exist_ok=True)
        (audio_dir / "vid.m4a").write_bytes(b"0123456789")
        (audio_dir / "vid.status").write_text("complete:141")
        path = "/liked_songs/a.m4a"
        fh = self.handler.open(path, "vid")

        with patch.object(
            self.handler,
            "_cached_audio_format",
            wraps=self.handler._cached_audio_format,
        ) as status_check:
            self.assertEqual(self.handler.read(path, 4, 0, fh), b"0123")
            self.assertEqual(self.handler.read(path, 4, 4, fh), b"4567")
        self.assertEqual(status_check.call_count, 1)

        fd = self.handler.open_files[fh]["local_fd"]
        self.handler.release(path, fh)
        with self.assertRaises(OSError):
            os.fstat(fd)

    def test_range_index_scans_directory_once(self):
        path = "/liked_songs/a.m4a"
        fh = self._open_streaming(path)
        self.handler.open_files[fh]["stream_url"] = None
        range_dir = self.handler._range_cache_dir("vid", "141")
        range_dir.mkdir(parents=True)
        (range_dir / "0-8.part").write_bytes(b"abcdefgh")

        with patch.object(Path, "glob", wraps=range_dir.glob) as glob:
            for offset in range(4):
                self.handler.open_files[fh].pop("readahead", None)
                self.assertEqual(
                    self.handler.read(path, 2, offset, fh),
                    b"abcdefgh"[offset : offset + 2],
                )
        self.assertEqual(glob.call_count, 1)


class TestFilesystemSizeLookups(unittest.TestCase):
    """Size bookkeeping that runs on every getattr and streamed range."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.fs = object.__new__(YouTubeMusicFS)
        self.fs.cache = Mock()
        self.fs.cache.cache_dir = self.temp_dir
        self.fs.hot_metadata_lock = threading.RLock()
        self.fs.last_access_lock = threading.RLock()
        self.fs.hot_attrs_by_path = {}
        self.fs.last_access_results = {}
        self.fs.complete_audio_sizes = {}
        self.fs.reported_file_sizes = {}

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_update_file_size_persists_each_size_once(self):
        for _ in range(5):
            self.fs._update_file_size("/liked_songs/a.m4a", 1234)
        self.fs._update_file_size("/liked_songs/a.m4a", 5678)

        self.assertEqual(self.fs.cache.set.call_count, 2)
        self.assertEqual(self.fs.cache.update_file_attrs_in_parent_dir.call_count, 2)

    def test_complete_audio_size_is_memoized_until_status_changes(self):
        audio_dir = Path(self.temp_dir) / "audio"
        audio_dir.mkdir()
        (audio_dir / "vid.m4a").write_bytes(b"x" * 10)
        status = audio_dir / "vid.status"
        status.write_text("downloading:141")
        os.utime(status, ns=(1, 1))
        self.assertIsNone(self.fs._complete_cached_audio_size("vid"))

        status.write_text("complete:141")
        os.utime(status, ns=(2, 2))
        with patch.object(Path, "read_text", wraps=status.read_text) as read_text:
            self.assertEqual(self.fs._complete_cached_audio_size("vid"), 10)
            self.assertEqual(self.fs._complete_cached_audio_size("vid"), 10)
        self.assertEqual(read_text.call_count, 1)
        self.assertIsNone(self.fs._complete_cached_audio_size("missing"))


if __name__ == "__main__":
    unittest.main()
