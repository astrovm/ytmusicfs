#!/usr/bin/env python3

import errno
import logging
import os
import shutil
import stat
import tempfile
import threading
import time
import unittest
from concurrent.futures import Future
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

from fuse import FuseOSError

from ytmusicfs.filesystem import HotPath, YouTubeMusicFS, mount_ytmusicfs


class YouTubeMusicFSTestCase(unittest.TestCase):
    """Base fixture: a YouTubeMusicFS whose collaborators are all mocks."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.temp_dir, ignore_errors=True)
        with (
            patch("ytmusicfs.filesystem.ThreadManager") as mock_thread_manager,
            patch("ytmusicfs.filesystem.YTDLPUtils") as mock_yt_dlp_utils,
            patch("ytmusicfs.filesystem.YTMusicAuthAdapter"),
            patch("ytmusicfs.filesystem.YouTubeMusicClient") as mock_client,
            patch("ytmusicfs.filesystem.TrackProcessor"),
            patch("ytmusicfs.filesystem.CacheManager") as mock_cache,
            patch("ytmusicfs.filesystem.ContentFetcher") as mock_fetcher,
            patch("ytmusicfs.filesystem.PathRouter") as mock_router,
            patch("ytmusicfs.filesystem.FileHandler") as mock_file_handler,
            patch("ytmusicfs.filesystem.MetadataManager") as mock_metadata,
        ):
            self.mock_thread_manager = mock_thread_manager.return_value
            self.mock_thread_manager.create_lock.side_effect = threading.RLock
            self.mock_thread_manager.is_shutdown.return_value = False
            self.mock_yt_dlp_utils = mock_yt_dlp_utils.return_value
            self.mock_client = mock_client.return_value
            self.mock_cache = mock_cache.return_value
            self.mock_cache.cache_dir = self.temp_dir
            self.mock_cache.get.return_value = None
            self.mock_cache.get_many.return_value = {}
            self.mock_cache.get_entry_type.return_value = None
            self.mock_cache.get_duration.return_value = None
            self.mock_cache.get_file_attrs_from_parent_dir.return_value = None
            self.mock_cache.get_directory_listing_with_attrs.return_value = None
            self.mock_cache.is_track_unavailable.return_value = False
            self.mock_cache.is_path_unavailable.return_value = False
            self.mock_cache.is_no_replacement.return_value = False
            self.mock_cache.get_unavailable_video_ids.return_value = set()
            self.mock_fetcher = mock_fetcher.return_value
            self.mock_router = mock_router.return_value
            self.mock_file_handler = mock_file_handler.return_value
            self.mock_file_handler.get_recent_handles.return_value = []
            self.mock_metadata = mock_metadata.return_value
            self.mock_metadata.get_video_id.return_value = "video0"

            self.fs = YouTubeMusicFS(cache_dir=self.temp_dir, browser="brave")
        self.fs.logger = logging.getLogger("test")

    def write_cached_audio(self, video_id, data, status="complete:141"):
        audio_dir = Path(self.temp_dir) / "audio"
        audio_dir.mkdir(parents=True, exist_ok=True)
        (audio_dir / f"{video_id}.m4a").write_bytes(data)
        (audio_dir / f"{video_id}.status").write_text(status)


class TestYouTubeMusicFS(YouTubeMusicFSTestCase):
    """Directory listing, attributes and the status file."""

    def test_readdir_root_lists_library_and_metadata_dirs(self):
        result = self.fs.readdir("/", None)

        self.assertEqual(
            sorted(result),
            sorted([".", "..", "playlists", "liked_songs", "albums", ".ytmusicfs"]),
        )

    def test_hot_readdir_does_not_route_or_call_external_clients(self):
        path = "/playlists/Mix"
        self.fs.hot_dir_entries[path] = ["song.m4a"]
        self.fs.hot_paths[f"{path}/song.m4a"] = HotPath(
            attrs={"videoId": "abc123"}, video_id="abc123"
        )
        self.mock_router.reset_mock()
        self.mock_fetcher.reset_mock()
        self.mock_client.reset_mock()
        self.mock_yt_dlp_utils.reset_mock()

        result = self.fs.readdir(path, None)

        self.assertEqual(result, [".", "..", "song.m4a"])
        self.mock_router.route.assert_not_called()
        self.mock_fetcher.fetch_playlist_content.assert_not_called()
        self.assertEqual(self.mock_client.method_calls, [])
        self.assertEqual(self.mock_yt_dlp_utils.method_calls, [])

    def test_hot_getattr_does_not_route_or_call_external_clients(self):
        path = "/playlists/Mix"
        self.fs.hot_paths[path] = HotPath(
            attrs={
                "st_mode": stat.S_IFDIR | 0o555,
                "st_nlink": 2,
                "st_size": 4096,
            }
        )
        self.mock_router.reset_mock()
        self.mock_fetcher.reset_mock()
        self.mock_client.reset_mock()
        self.mock_yt_dlp_utils.reset_mock()

        attrs = self.fs.getattr(path, None)

        self.assertEqual(attrs["st_size"], 4096)
        self.mock_router.validate_path.assert_not_called()
        self.mock_router.route.assert_not_called()
        self.assertEqual(self.mock_fetcher.method_calls, [])
        self.assertEqual(self.mock_client.method_calls, [])
        self.assertEqual(self.mock_yt_dlp_utils.method_calls, [])

    def test_status_file_is_listed_readable_and_reports_state(self):
        result = self.fs.readdir("/.ytmusicfs", None)
        attrs = self.fs.getattr("/.ytmusicfs/status.json")
        content = self.fs.read("/.ytmusicfs/status.json", 4096, 0, 0)

        self.assertEqual(result, [".", "..", "status.json"])
        self.assertEqual(attrs["st_mode"], stat.S_IFREG | 0o444)
        self.assertIn(b'"browser": "brave"', content)
        self.assertIn(b'"recent_handles"', content)
        self.assertIn(b'"profiler"', content)
        self.assertIn(b'"refresh"', content)
        self.assertIn(b'"stats"', content)

    def test_status_file_read_honours_offset_and_size(self):
        full = self.fs.read("/.ytmusicfs/status.json", 1 << 16, 0, 0)

        self.assertEqual(self.fs.read("/.ytmusicfs/status.json", 5, 3, 0), full[3:8])

    def test_status_counts_filesystem_operations(self):
        self.fs.readdir("/", None)
        self.fs.getattr("/", None)
        self.fs.open("/.ytmusicfs/status.json", os.O_RDONLY)
        self.fs.read("/.ytmusicfs/status.json", 4096, 0, 0)

        status = self.fs.read("/.ytmusicfs/status.json", 4096, 0, 0)

        self.assertIn(b'"readdir": 1', status)
        self.assertIn(b'"getattr": 1', status)
        self.assertIn(b'"open": 0', status)
        self.assertIn(b'"read": 0', status)

    def test_status_profiler_summarizes_hot_hit_rates(self):
        self.fs.stats.update(
            {
                "getattr_hot_hits": 3,
                "getattr_fallbacks": 1,
                "readdir_hot_hits": 1,
                "readdir_fallbacks": 1,
                "video_id_hot_hits": 4,
                "video_id_fallbacks": 0,
            }
        )

        status = self.fs.read("/.ytmusicfs/status.json", 4096, 0, 0)

        self.assertIn(b'"getattr_hot_hit_rate": 0.75', status)
        self.assertIn(b'"readdir_hot_hit_rate": 0.5', status)
        self.assertIn(b'"video_id_hot_hit_rate": 1.0', status)

    def test_status_average_is_zero_without_numeric_count_and_total(self):
        average = YouTubeMusicFS._average_ms

        self.assertEqual(
            average({"read": 4, "read_total_ms": 10}, "read_total_ms", "read"), 2.5
        )
        self.assertEqual(
            average({"read": 0, "read_total_ms": 10}, "read_total_ms", "read"), 0.0
        )
        self.assertEqual(
            average({"read": 2, "read_total_ms": "x"}, "read_total_ms", "read"), 0.0
        )

    def test_status_file_size_stays_stable_while_reading(self):
        attrs = self.fs.getattr("/.ytmusicfs/status.json")
        content = self.fs.read("/.ytmusicfs/status.json", attrs["st_size"], 0, 0)

        self.assertEqual(len(content), attrs["st_size"])

    def test_readdir_playlists_root_lists_registry_entries(self):
        self.mock_fetcher.readdir_playlist_by_type.return_value = [
            ".",
            "..",
            "my_playlist",
            "workout_mix",
        ]

        result = self.fs.readdir("/playlists", None)

        self.assertEqual(result, [".", "..", "my_playlist", "workout_mix"])
        self.mock_fetcher.readdir_playlist_by_type.assert_called_once_with(
            "playlist", "/playlists"
        )

    def test_readdir_uncached_playlist_routes_through_router(self):
        playlist_path = "/playlists/my_playlist"
        self.mock_router.validate_path.return_value = True
        self.mock_router.route.return_value = [".", "..", "song1.m4a", "song2.m4a"]

        result = self.fs.readdir(playlist_path, None)

        self.assertEqual(result, [".", "..", "song1.m4a", "song2.m4a"])
        self.mock_router.route.assert_called_once_with(playlist_path)

    def test_readdir_routed_listing_marks_children_valid(self):
        playlist_path = "/playlists/my_playlist"
        self.mock_router.validate_path.return_value = True
        self.mock_router.route.return_value = [".", "..", "a.m4a"]

        self.fs.readdir(playlist_path, None)

        self.mock_cache.mark_valid.assert_any_call(playlist_path, is_directory=True)
        self.mock_cache.set.assert_any_call(f"valid_files:{playlist_path}", ["a.m4a"])
        self.mock_thread_manager.submit_task.assert_any_call(
            "io",
            self.mock_cache.set_batch,
            {f"path_valid:{playlist_path}/a.m4a": True},
        )

    def test_readdir_empty_routed_listing_is_not_cached(self):
        playlist_path = "/playlists/empty"
        self.mock_router.validate_path.return_value = True
        self.mock_router.route.return_value = [".", ".."]
        self.mock_cache.mark_valid.reset_mock()

        self.assertEqual(self.fs.readdir(playlist_path, None), [".", ".."])

        self.mock_cache.mark_valid.assert_not_called()
        self.mock_cache.set.assert_not_called()

    def test_readdir_rejected_path_returns_empty_listing(self):
        self.mock_router.validate_path.return_value = False

        self.assertEqual(self.fs.readdir("/playlists/nope", None), [".", ".."])
        self.mock_router.route.assert_not_called()

    def test_readdir_router_error_returns_empty_listing(self):
        self.mock_router.validate_path.return_value = True
        self.mock_router.route.side_effect = RuntimeError("boom")

        self.assertEqual(self.fs.readdir("/playlists/broken", None), [".", ".."])

    def test_readdir_of_file_or_hidden_path_returns_empty_listing(self):
        self.mock_cache.get_entry_type.side_effect = lambda path: (
            "file" if path.endswith(".m4a") else None
        )

        self.assertEqual(self.fs.readdir("/liked_songs/a.m4a", None), [".", ".."])
        self.assertEqual(self.fs.readdir("/playlists/.hidden", None), [".", ".."])
        self.mock_router.validate_path.assert_not_called()
        self.mock_router.route.assert_not_called()

    def test_readdir_uses_persisted_listing_and_publishes_it_hot(self):
        directory = "/playlists/my_playlist"
        self.mock_cache.get.side_effect = lambda key: (
            {"a.m4a": {"videoId": "va", "st_size": 1}}
            if key == f"{directory}_listing_with_attrs"
            else None
        )

        self.assertEqual(self.fs.readdir(directory, None), [".", "..", "a.m4a"])

        self.assertEqual(self.fs.hot_dir_entries[directory], ["a.m4a"])
        self.assertEqual(self.fs.hot_paths[f"{directory}/a.m4a"].video_id, "va")
        self.assertEqual(self.fs.stats["readdir_fallbacks"], 1)

    def test_readdir_filters_unavailable_tracks_from_cached_listing(self):
        directory = "/playlists/my_playlist"
        self.mock_cache.get.return_value = {
            "good.m4a": {"videoId": "good"},
            "bad.m4a": {"videoId": "bad"},
        }
        self.mock_cache.get_unavailable_video_ids.return_value = {"bad"}

        result = self.fs.readdir(directory, None)

        self.assertEqual(result, [".", "..", "good.m4a"])
        self.mock_cache.is_track_unavailable.assert_not_called()

    def test_prime_hot_metadata_loads_cached_root_listings(self):
        self.mock_cache.get_directory_listing_with_attrs.side_effect = lambda path: (
            {".": {}, "a.m4a": {"videoId": "va", "st_size": 7}}
            if path == "/liked_songs"
            else None
        )

        self.fs._prime_hot_metadata_from_cache()

        self.assertEqual(self.fs.hot_dir_entries, {"/liked_songs": ["a.m4a"]})
        self.assertEqual(self.fs.hot_paths["/liked_songs/a.m4a"].video_id, "va")
        self.assertEqual(self.fs.readdir("/liked_songs"), [".", "..", "a.m4a"])

    def test_dynamic_routes_fetch_content_by_registry_id(self):
        routes = {
            args[0]: args[1]
            for args, _ in self.mock_router.register_dynamic.call_args_list
        }
        static_routes = {
            args[0]: args[1] for args, _ in self.mock_router.register.call_args_list
        }
        self.mock_fetcher.get_playlist_id_from_name.side_effect = (
            lambda name, kind: f"{kind}:{name}"
        )
        self.mock_fetcher.fetch_playlist_content.return_value = ["a.m4a"]
        self.mock_fetcher.readdir_playlist_by_type.return_value = [".", "..", "X"]

        self.assertEqual(
            routes["/playlists/*"]("/playlists/Mix", "Mix"), [".", "..", "a.m4a"]
        )
        self.assertEqual(routes["/albums/*"]("/albums/LP", "LP"), [".", "..", "a.m4a"])
        self.assertIn("playlists", static_routes["/"]())
        for root, kind in YouTubeMusicFS.LIBRARY_ROOTS.items():
            self.assertEqual(static_routes[root](), [".", "..", "X"])
            self.mock_fetcher.readdir_playlist_by_type.assert_called_with(kind, root)
        self.mock_fetcher.fetch_playlist_content.assert_has_calls(
            [
                call("playlist:Mix", "/playlists/Mix"),
                call("album:LP", "/albums/LP"),
            ]
        )

    def test_getattr_playlist_directory_does_not_rewrite_cached_attrs(self):
        self.mock_router.validate_path.return_value = True

        attrs = self.fs.getattr("/playlists/my_playlist")

        self.assertEqual(attrs["st_mode"], stat.S_IFDIR | 0o555)
        self.assertEqual(attrs["st_nlink"], 2)
        self.assertEqual(attrs["st_size"], 4096)
        self.mock_router.validate_path.assert_called_once_with("/playlists/my_playlist")
        self.mock_cache.update_file_attrs_in_parent_dir.assert_not_called()
        self.assertNotIn(
            call("/playlists/my_playlist", is_directory=True),
            self.mock_cache.mark_valid.mock_calls,
        )

    def test_getattr_unknown_playlist_directory_raises_enoent(self):
        self.mock_router.validate_path.return_value = False

        with self.assertRaises(FuseOSError) as cm:
            self.fs.getattr("/playlists/missing")

        self.assertEqual(cm.exception.errno, errno.ENOENT)

    def test_getattr_root_returns_directory_attrs(self):
        current_time = time.time()
        with patch("time.time", return_value=current_time):
            attrs = self.fs.getattr("/", None)

        self.assertTrue(stat.S_ISDIR(attrs["st_mode"]))
        self.assertEqual(attrs["st_nlink"], 2)
        self.assertTrue(attrs["st_mode"] & stat.S_IRUSR)
        self.assertTrue(attrs["st_mode"] & stat.S_IXUSR)
        self.assertEqual(attrs["st_size"], 4096)
        self.assertEqual(attrs["st_ctime"], current_time)
        self.assertEqual(attrs["st_mtime"], current_time)
        self.assertEqual(attrs["st_atime"], current_time)
        self.mock_cache.mark_valid.assert_any_call("/", is_directory=True)

    def test_getattr_unknown_path_raises_enoent(self):
        self.mock_router.validate_path.return_value = False

        with self.assertRaises(FuseOSError) as context:
            self.fs.getattr("/nonexistent", None)

        self.assertEqual(context.exception.args[0], errno.ENOENT)

    def test_getattr_unexpected_error_raises_enoent(self):
        self.mock_router.validate_path.side_effect = RuntimeError("boom")

        with self.assertRaises(FuseOSError) as cm:
            self.fs.getattr("/liked_songs/sub", None)

        self.assertEqual(cm.exception.errno, errno.ENOENT)

    def test_getattr_routed_directory_is_marked_valid_and_cached(self):
        self.mock_router.validate_path.return_value = True

        attrs = self.fs.getattr("/liked_songs/sub", None)

        self.assertTrue(stat.S_ISDIR(attrs["st_mode"]))
        self.mock_cache.mark_valid.assert_any_call(
            "/liked_songs/sub", is_directory=True
        )
        self.mock_cache.update_file_attrs_in_parent_dir.assert_called_once_with(
            "/liked_songs/sub", attrs
        )

    def test_getattr_cached_file_returns_parent_listing_attrs(self):
        file_path = "/playlists/my_playlist/song.m4a"
        self.mock_cache.get_file_attrs_from_parent_dir.return_value = {
            "st_mode": stat.S_IFREG | 0o444,
            "st_nlink": 1,
            "st_size": 1024 * 1024,
        }

        attrs = self.fs.getattr(file_path, None)

        self.assertTrue(stat.S_ISREG(attrs["st_mode"]))
        self.assertEqual(attrs["st_nlink"], 1)
        self.assertEqual(attrs["st_size"], 1024 * 1024)
        self.assertEqual(self.fs.stats["getattr_fallbacks"], 1)

    def test_getattr_audio_resolves_video_id_from_hot_path(self):
        file_path = "/liked_songs/song.m4a"
        self.fs.hot_paths[file_path] = HotPath(video_id="hot1")
        self.mock_cache.get_file_attrs_from_parent_dir.return_value = {
            "st_mode": stat.S_IFREG | 0o444,
            "st_size": 10,
        }

        self.fs.getattr(file_path, None)

        self.mock_cache.is_track_unavailable.assert_called_with("hot1")
        self.mock_metadata.get_video_id.assert_not_called()
        self.assertGreaterEqual(self.fs.stats["video_id_hot_hits"], 1)

    def test_getattr_uncached_audio_uses_duration_estimate(self):
        file_path = "/liked_songs/song.m4a"
        self.mock_router.validate_path.return_value = True
        self.mock_metadata.get_video_id.return_value = "abc123"
        self.mock_cache.get_duration.return_value = 180

        attrs = self.fs.getattr(file_path, None)

        self.mock_metadata.get_video_id.assert_called_with(file_path)
        self.assertEqual(attrs["st_size"], 180 * self.fs.ESTIMATED_BYTES_PER_SECOND)
        self.mock_cache.mark_valid.assert_any_call(file_path, is_directory=False)

    def test_getattr_uncached_audio_without_duration_uses_minimum_size(self):
        self.mock_router.validate_path.return_value = True

        attrs = self.fs.getattr("/liked_songs/song.m4a", None)

        self.assertEqual(attrs["st_size"], self.fs.MIN_AUDIO_SIZE)

    def test_getattr_rejects_unavailable_audio_before_cooldown_cache(self):
        file_path = "/liked_songs/song.m4a"
        self.fs.last_access_results[f"getattr:{file_path}"] = {
            "st_mode": stat.S_IFREG | 0o644,
            "st_size": 123,
        }
        self.fs.last_access_time[f"getattr:{file_path}"] = time.time()
        self.mock_metadata.get_video_id.return_value = "abc123"
        self.mock_cache.is_track_unavailable.return_value = True

        with self.assertRaises(FuseOSError) as cm:
            self.fs.getattr(file_path, None)

        self.assertEqual(cm.exception.errno, errno.ENOENT)

    def test_getattr_rejects_unavailable_path_before_stale_attr_cache(self):
        file_path = "/liked_songs/song.m4a"
        self.mock_cache.is_path_unavailable.return_value = True

        with self.assertRaises(FuseOSError) as cm:
            self.fs.getattr(file_path, None)

        self.assertEqual(cm.exception.errno, errno.ENOENT)
        self.mock_cache.get_file_attrs_from_parent_dir.assert_not_called()

    def test_getattr_audio_uses_cached_real_size(self):
        file_path = "/liked_songs/song.m4a"
        self.mock_cache.get.return_value = 12345
        self.mock_router.validate_path.return_value = True

        attrs = self.fs.getattr(file_path, None)

        self.assertEqual(attrs["st_size"], 12345)

    def test_getattr_audio_prefers_complete_cached_audio_size(self):
        file_path = "/liked_songs/song.m4a"
        video_id = "complete999"
        self.write_cached_audio(video_id, b"a" * 200)
        self.mock_cache.get_file_attrs_from_parent_dir.return_value = {
            "st_mode": stat.S_IFREG | 0o444,
            "st_nlink": 1,
            "st_size": 100,
        }
        self.mock_cache.get.return_value = 100
        self.mock_metadata.get_video_id.return_value = video_id

        attrs = self.fs.getattr(file_path, None)

        self.assertEqual(attrs["st_size"], 200)

    def test_slow_getattr_is_logged(self):
        self.fs.logger = Mock()

        self.fs._log_slow_getattr("/a", time.time() - 1, "cache")
        self.fs._log_slow_getattr("/b", time.time(), "cache")

        self.fs.logger.info.assert_called_once()

    def test_update_file_size_updates_hot_attrs_and_getattr_cache(self):
        file_path = "/liked_songs/song.m4a"
        self.fs.hot_paths[file_path] = HotPath(
            attrs={"st_size": 100, "videoId": "abc123"}, video_id="abc123"
        )
        self.fs.last_access_results[f"getattr:{file_path}"] = {"st_size": 100}

        self.fs._update_file_size(file_path, 200)

        self.assertEqual(self.fs.hot_paths[file_path].attrs["st_size"], 200)
        self.assertNotIn(f"getattr:{file_path}", self.fs.last_access_results)

    def test_cached_listing_uses_duration_estimate(self):
        self.fs._cache_directory_listing_with_attrs(
            "/liked_songs",
            [
                {
                    "filename": "song.m4a",
                    "videoId": "abc123",
                    "duration_seconds": 9999,
                }
            ],
        )

        listing = self.mock_cache.set_directory_listing_with_attrs.call_args.args[1]
        self.assertEqual(
            listing["song.m4a"]["st_size"],
            9999 * self.fs.ESTIMATED_BYTES_PER_SECOND,
        )

    def test_cached_listing_does_not_reuse_old_parent_size_estimate(self):
        self.mock_cache.get_file_attrs_from_parent_dir.return_value = {
            "st_size": self.fs.MIN_AUDIO_SIZE
        }

        self.fs._cache_directory_listing_with_attrs(
            "/liked_songs",
            [
                {
                    "filename": "song.m4a",
                    "videoId": "abc123",
                    "duration_seconds": 180,
                }
            ],
        )

        listing = self.mock_cache.set_directory_listing_with_attrs.call_args.args[1]
        self.assertEqual(
            listing["song.m4a"]["st_size"],
            180 * self.fs.ESTIMATED_BYTES_PER_SECOND,
        )

    def test_cached_listing_sizes_prefer_complete_audio_then_known_size(self):
        self.write_cached_audio("done", b"x" * 42)
        self.mock_cache.get_many.return_value = {
            "filesize:/liked_songs/known.m4a": 777,
        }

        self.fs._cache_directory_listing_with_attrs(
            "/liked_songs",
            [
                {"filename": "done.m4a", "videoId": "done", "duration_seconds": 60},
                {"filename": "known.m4a", "videoId": "known", "duration_seconds": 60},
                {"filename": "bare.m4a"},
            ],
        )

        listing = self.mock_cache.set_directory_listing_with_attrs.call_args.args[1]
        self.assertEqual(listing["done.m4a"]["st_size"], 42)
        self.assertEqual(listing["known.m4a"]["st_size"], 777)
        self.assertEqual(listing["bare.m4a"]["st_size"], self.fs.MIN_AUDIO_SIZE)

    def test_cached_listing_skips_unavailable_and_nameless_tracks(self):
        self.mock_cache.get_unavailable_video_ids.return_value = {"dead"}

        self.fs._cache_directory_listing_with_attrs(
            "/albums",
            [
                {"filename": "dead.m4a", "videoId": "dead"},
                {"videoId": "noname"},
                {"filename": "LP", "is_directory": True, "browseId": "MPRE1"},
            ],
        )

        listing = self.mock_cache.set_directory_listing_with_attrs.call_args.args[1]
        self.assertEqual(list(listing), ["LP"])
        self.assertTrue(stat.S_ISDIR(listing["LP"]["st_mode"]))
        self.assertEqual(listing["LP"]["browseId"], "MPRE1")
        self.mock_cache.set.assert_called_once_with("valid_files:/albums", ["LP"])
        self.assertEqual(self.fs.hot_dir_entries["/albums"], ["LP"])
        self.assertIsNone(self.fs.hot_paths["/albums/LP"].video_id)

    def test_mkdir_and_rmdir_are_not_permitted(self):
        for operation in (
            lambda: self.fs.mkdir("/playlists/new", 0o755),
            lambda: self.fs.rmdir("/playlists/old"),
        ):
            with self.assertRaises(OSError) as cm:
                operation()
            self.assertEqual(cm.exception.errno, errno.EPERM)

    def test_destroy_releases_resources_even_when_each_step_fails(self):
        self.mock_yt_dlp_utils.cleanup.side_effect = RuntimeError("a")
        self.mock_thread_manager.shutdown.side_effect = RuntimeError("b")
        self.mock_cache.close.side_effect = RuntimeError("c")

        self.fs.destroy("/mnt")

        self.mock_yt_dlp_utils.cleanup.assert_called_once_with()
        self.mock_thread_manager.shutdown.assert_called_once_with(
            wait=True, timeout=10.0
        )
        self.mock_cache.close.assert_called_once_with()

    def test_destroy_skips_missing_components(self):
        self.fs.yt_dlp_utils = None
        self.fs.thread_manager = None
        self.fs.cache = None

        self.fs.destroy("/mnt")

    def test_mount_uses_short_kernel_metadata_ttls(self):
        attr_timeout = YouTubeMusicFS.FUSE_ATTR_TIMEOUT
        entry_timeout = YouTubeMusicFS.FUSE_ENTRY_TIMEOUT
        negative_timeout = YouTubeMusicFS.FUSE_NEGATIVE_TIMEOUT
        with (
            patch("ytmusicfs.filesystem.FUSE") as mock_fuse,
            patch("ytmusicfs.filesystem.YouTubeMusicFS") as mock_fs_class,
        ):
            mount_ytmusicfs("/tmp/ytmusic", cache_dir="/tmp/cache", browser="brave")

        mock_fs_class.assert_called_once_with(cache_dir="/tmp/cache", browser="brave")
        kwargs = mock_fuse.call_args.kwargs
        self.assertEqual(kwargs["attr_timeout"], attr_timeout)
        self.assertEqual(kwargs["entry_timeout"], entry_timeout)
        self.assertEqual(kwargs["negative_timeout"], negative_timeout)
        self.assertTrue(kwargs["raw_fi"])


class TestYouTubeMusicFSUnlistedPaths(YouTubeMusicFSTestCase):
    """Lookups of paths whose folder has not been listed since mounting."""

    PATH = "/playlists/Mix/Artist - Song.m4a"

    def test_resolve_video_id_lists_unlisted_folder_then_retries(self):
        self.mock_metadata.get_video_id.side_effect = [
            OSError(errno.ENOENT, "missing"),
            "vid123",
        ]

        with patch.object(self.fs, "readdir") as readdir:
            self.assertEqual(self.fs._resolve_video_id(self.PATH), "vid123")

        readdir.assert_called_once_with("/playlists/Mix")

    def test_resolve_video_id_raises_when_folder_was_already_listed(self):
        self.mock_cache.get_directory_listing_with_attrs.return_value = {}
        self.mock_metadata.get_video_id.side_effect = OSError(errno.ENOENT, "")

        with (
            patch.object(self.fs, "readdir") as readdir,
            self.assertRaises(OSError),
        ):
            self.fs._resolve_video_id(self.PATH)

        readdir.assert_not_called()

    def test_resolve_video_id_raises_when_folder_cannot_be_listed(self):
        self.mock_metadata.get_video_id.side_effect = OSError(errno.ENOENT, "")

        with (
            patch.object(self.fs, "readdir", side_effect=FuseOSError(errno.ENOENT)),
            self.assertRaises(OSError),
        ):
            self.fs._resolve_video_id(self.PATH)

    def test_getattr_unknown_playlist_lists_playlists_before_rejecting(self):
        self.mock_router.validate_path.return_value = False

        with (
            patch.object(self.fs, "readdir") as readdir,
            self.assertRaises(FuseOSError) as context,
        ):
            self.fs.getattr("/playlists/Missing")

        readdir.assert_called_once_with("/playlists")
        self.assertEqual(context.exception.errno, errno.ENOENT)


class TestYouTubeMusicFSOpen(YouTubeMusicFSTestCase):
    """open, read and release, including fusepy's raw_fi file-info mode."""

    def test_open_passes_resolved_video_id_to_file_handler(self):
        file_path = "/playlists/my_playlist/song.m4a"
        self.mock_metadata.get_video_id.return_value = "dQw4w9WgXcQ"
        self.mock_router.validate_path.return_value = True
        self.mock_file_handler.open.return_value = 42

        file_handle = self.fs.open(file_path, os.O_RDONLY)

        self.assertEqual(file_handle, 42)
        self.mock_file_handler.open.assert_called_once_with(file_path, "dQw4w9WgXcQ")
        self.mock_cache.mark_valid.assert_any_call(file_path, is_directory=False)

    def test_open_uses_hot_video_id_without_validating_path(self):
        path = "/liked_songs/a.m4a"
        self.fs.hot_paths[path] = HotPath(video_id="hot1")
        self.mock_file_handler.open.return_value = 5

        self.assertEqual(self.fs.open(path, os.O_RDONLY), 5)

        self.mock_file_handler.open.assert_called_once_with(path, "hot1")
        self.mock_router.validate_path.assert_not_called()

    def test_open_rejects_unavailable_hot_video_id(self):
        path = "/liked_songs/a.m4a"
        self.fs.hot_paths[path] = HotPath(video_id="hot1")
        self.mock_cache.is_track_unavailable.return_value = True

        with self.assertRaises(FuseOSError) as cm:
            self.fs.open(path, os.O_RDONLY)

        self.assertEqual(cm.exception.errno, errno.ENOENT)
        self.mock_file_handler.open.assert_not_called()

    def test_open_rejects_unavailable_path_before_stale_valid_cache(self):
        file_path = "/liked_songs/song.m4a"
        self.mock_cache.is_path_unavailable.return_value = True

        with self.assertRaises(FuseOSError) as cm:
            self.fs.open(file_path, os.O_RDONLY)

        self.assertEqual(cm.exception.errno, errno.ENOENT)
        self.mock_router.validate_path.assert_not_called()
        self.mock_file_handler.open.assert_not_called()

    def test_open_directory_raises_eisdir(self):
        self.mock_cache.get_entry_type.return_value = "directory"

        with self.assertRaises(FuseOSError) as cm:
            self.fs.open("/playlists/Mix", os.O_RDONLY)

        self.assertEqual(cm.exception.errno, errno.EISDIR)

    def test_open_unknown_path_raises_enoent(self):
        self.mock_router.validate_path.return_value = False

        with self.assertRaises(FuseOSError) as cm:
            self.fs.open("/liked_songs/missing.m4a", os.O_RDONLY)

        self.assertEqual(cm.exception.errno, errno.ENOENT)
        self.mock_file_handler.open.assert_not_called()

    def test_open_without_resolvable_video_id_raises_enoent(self):
        self.mock_cache.get_entry_type.return_value = "file"
        for lookup in (
            {"side_effect": OSError(errno.ENOENT, "missing")},
            {"return_value": ""},
        ):
            with self.subTest(lookup=lookup):
                self.mock_metadata.get_video_id.configure_mock(
                    **{"side_effect": None, **lookup}
                )
                with self.assertRaises(FuseOSError) as cm:
                    self.fs.open("/liked_songs/a.m4a", os.O_RDONLY)
                self.assertEqual(cm.exception.errno, errno.ENOENT)
        self.mock_file_handler.open.assert_not_called()

    def test_open_unexpected_error_raises_enoent(self):
        self.mock_cache.get_entry_type.return_value = "file"
        self.mock_file_handler.open.side_effect = RuntimeError("boom")

        with self.assertRaises(FuseOSError) as cm:
            self.fs.open("/liked_songs/a.m4a", os.O_RDONLY)

        self.assertEqual(cm.exception.errno, errno.ENOENT)

    def test_open_raw_file_info_with_unknown_size_uses_direct_io(self):
        path = "/liked_songs/a.m4a"
        self.fs.hot_paths[path] = HotPath(video_id="v1")
        self.mock_file_handler.open.return_value = 7
        self.mock_file_handler.read.return_value = b"data"
        self.mock_file_handler.release.return_value = 0
        info = SimpleNamespace(flags=0, fh=0, direct_io=0)

        self.assertEqual(self.fs.open(path, info), 0)

        self.assertEqual(info.fh, 7)
        self.assertEqual(info.direct_io, 1)
        self.assertEqual(self.fs.read(path, 4, 0, info), b"data")
        self.mock_file_handler.read.assert_called_once_with(path, 4, 0, 7)
        self.fs.release(path, info)
        self.mock_file_handler.release.assert_called_once_with(path, 7)

    def test_open_raw_file_info_with_known_size_keeps_page_cache(self):
        path = "/liked_songs/a.m4a"
        self.fs.hot_paths[path] = HotPath(video_id="v1")
        self.mock_cache.get.side_effect = lambda key: (
            1234 if key == f"filesize:{path}" else None
        )
        info = SimpleNamespace(flags=0, fh=0, direct_io=0)

        self.fs.open(path, info)

        self.assertEqual(info.direct_io, 0)

    def test_open_raw_file_info_for_status_file_uses_direct_io(self):
        info = SimpleNamespace(flags=0, fh=9, direct_io=0)

        self.assertEqual(self.fs.open(YouTubeMusicFS.STATUS_FILE, info), 0)

        self.assertEqual(info.fh, 0)
        self.assertEqual(info.direct_io, 1)

    def test_open_plain_flags_return_handle(self):
        self.fs.hot_paths["/liked_songs/a.m4a"] = HotPath(video_id="v1")
        self.mock_file_handler.open.return_value = 7

        self.assertEqual(self.fs.open("/liked_songs/a.m4a", 0), 7)

    def test_read_delegates_to_file_handler(self):
        file_path = "/playlists/my_playlist/song.m4a"
        mock_data = b"test data" * 100
        self.mock_file_handler.read.return_value = mock_data

        data = self.fs.read(file_path, 1024, 0, 42)

        self.assertEqual(data, mock_data)
        self.mock_file_handler.read.assert_called_once_with(file_path, 1024, 0, 42)
        self.assertEqual(self.fs.stats["read"], 1)

    def test_read_preserves_os_error_code(self):
        self.mock_file_handler.read.side_effect = OSError(errno.ENOENT, "missing")

        with self.assertRaises(FuseOSError) as context:
            self.fs.read("/playlists/my_playlist/song.m4a", 1024, 0, 42)

        self.assertEqual(context.exception.args[0], errno.ENOENT)

    def test_read_os_error_without_errno_becomes_eio(self):
        self.mock_file_handler.read.side_effect = OSError("no code")

        with self.assertRaises(FuseOSError) as cm:
            self.fs.read("/liked_songs/a.m4a", 1024, 0, 42)

        self.assertEqual(cm.exception.errno, errno.EIO)

    def test_read_passes_fuse_errors_through(self):
        self.mock_file_handler.read.side_effect = FuseOSError(errno.EACCES)

        with self.assertRaises(FuseOSError) as cm:
            self.fs.read("/liked_songs/a.m4a", 1024, 0, 42)

        self.assertEqual(cm.exception.errno, errno.EACCES)

    def test_read_unexpected_error_becomes_eio(self):
        self.mock_file_handler.read.side_effect = ValueError("bad")

        with self.assertRaises(FuseOSError) as cm:
            self.fs.read("/liked_songs/a.m4a", 1024, 0, 42)

        self.assertEqual(cm.exception.errno, errno.EIO)

    def test_read_failure_logs_once_per_cooldown(self):
        self.mock_file_handler.read.side_effect = OSError(errno.ENOENT, "missing")
        self.fs.logger = Mock()

        for _ in range(2):
            with self.assertRaises(FuseOSError):
                self.fs.read("/playlists/my_playlist/song.m4a", 1024, 0, 42)

        self.fs.logger.warning.assert_called_once()

    def test_release_delegates_to_file_handler(self):
        file_path = "/playlists/my_playlist/song.m4a"
        self.mock_file_handler.release.return_value = 0

        self.assertEqual(self.fs.release(file_path, 42), 0)
        self.mock_file_handler.release.assert_called_once_with(file_path, 42)

    def test_release_error_still_reports_success(self):
        self.mock_file_handler.release.side_effect = RuntimeError("boom")

        self.assertEqual(self.fs.release("/liked_songs/a.m4a", 42), 0)


class TestYouTubeMusicFSRecentResults(YouTubeMusicFSTestCase):
    """Short-lived memo of readdir/getattr results used to absorb FUSE bursts."""

    def test_recent_results_are_bounded(self):
        for index in range(self.fs.RECENT_RESULT_CACHE_SIZE + 100):
            self.fs._store_readdir_result(f"readdir:/p{index}", [".", ".."])

        self.assertEqual(
            len(self.fs.last_access_results), self.fs.RECENT_RESULT_CACHE_SIZE
        )

    def test_readdir_reuses_routed_result_within_cooldown(self):
        self.mock_router.validate_path.return_value = True
        self.mock_router.route.return_value = [".", "..", "a.m4a"]

        first = self.fs.readdir("/playlists/Mix", None)
        second = self.fs.readdir("/playlists/Mix", None)

        self.assertEqual(first, second)
        self.mock_router.route.assert_called_once_with("/playlists/Mix")

    def test_readdir_routes_again_after_cooldown(self):
        self.fs.request_cooldown = 0
        self.mock_router.validate_path.return_value = True
        self.mock_router.route.return_value = [".", "..", "a.m4a"]

        self.fs.readdir("/playlists/Mix", None)
        self.fs.readdir("/playlists/Mix", None)

        self.assertEqual(self.mock_router.route.call_count, 2)

    def test_getattr_reuses_result_within_cooldown(self):
        self.mock_cache.get_file_attrs_from_parent_dir.return_value = {
            "st_mode": stat.S_IFREG | 0o444,
            "st_size": 10,
        }

        first = self.fs.getattr("/liked_songs/a.m4a")
        second = self.fs.getattr("/liked_songs/a.m4a")

        self.assertEqual(first, second)
        self.mock_cache.get_file_attrs_from_parent_dir.assert_called_once()


class TestYouTubeMusicFSSizeLookups(unittest.TestCase):
    """Size bookkeeping that runs on every getattr and streamed range."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.fs = object.__new__(YouTubeMusicFS)
        self.fs.cache = Mock()
        self.fs.cache.cache_dir = self.temp_dir
        self.fs.cache.get.return_value = None
        self.fs.cache.get_file_attrs_from_parent_dir.return_value = None
        self.fs.metadata_manager = Mock()
        self.fs.metadata_manager.get_video_id.side_effect = OSError(errno.ENOENT, "")
        self.fs.hot_metadata_lock = threading.RLock()
        self.fs.last_access_lock = threading.RLock()
        self.fs.hot_paths = {}
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

    def test_complete_audio_size_is_none_when_audio_file_is_missing(self):
        audio_dir = Path(self.temp_dir) / "audio"
        audio_dir.mkdir()
        (audio_dir / "vid.status").write_text("complete:141")

        self.assertIsNone(self.fs._complete_cached_audio_size("vid"))

    def test_real_file_size_falls_back_to_metadata_video_id(self):
        audio_dir = Path(self.temp_dir) / "audio"
        audio_dir.mkdir()
        (audio_dir / "meta.m4a").write_bytes(b"x" * 3)
        (audio_dir / "meta.status").write_text("complete:141")
        self.fs.metadata_manager.get_video_id.side_effect = None
        self.fs.metadata_manager.get_video_id.return_value = "meta"

        self.assertEqual(self.fs._get_real_file_size("/liked_songs/a.m4a"), 3)

    def test_real_file_size_uses_persisted_size_or_none(self):
        self.assertIsNone(self.fs._get_real_file_size("/liked_songs/a.m4a"))

        self.fs.cache.get.return_value = 99
        self.assertEqual(self.fs._get_real_file_size("/liked_songs/a.m4a"), 99)
        self.fs.cache.get.assert_called_with("filesize:/liked_songs/a.m4a")

    def test_advertised_size_prefers_real_size_then_listing_size(self):
        path = "/liked_songs/a.m4a"
        self.assertIsNone(self.fs._get_advertised_file_size(path))

        self.fs.cache.get_file_attrs_from_parent_dir.return_value = {"st_mode": 1}
        self.assertIsNone(self.fs._get_advertised_file_size(path))

        self.fs.cache.get_file_attrs_from_parent_dir.return_value = {"st_size": 50}
        self.assertEqual(self.fs._get_advertised_file_size(path), 50)

        self.fs.cache.get.return_value = 60
        self.assertEqual(self.fs._get_advertised_file_size(path), 60)

    def test_update_file_size_without_hot_entry_only_persists(self):
        self.fs._update_file_size("/liked_songs/cold.m4a", 10)

        self.assertEqual(self.fs.hot_paths, {})
        self.fs.cache.set.assert_called_once_with("filesize:/liked_songs/cold.m4a", 10)


class TestYouTubeMusicFSPrecache(YouTubeMusicFSTestCase):
    """Idle pre-caching of the first tracks of each listed directory."""

    def publish_tracks(self, dir_path, count, prefix="song"):
        names = [f"{prefix}{index}.m4a" for index in range(count)]
        for index, name in enumerate(names):
            self.fs.hot_paths[f"{dir_path}/{name}"] = HotPath(
                video_id=f"{prefix}-v{index}"
            )
        return names

    def test_readdir_schedules_idle_precache_for_audio_entries(self):
        playlist_path = "/playlists/my_playlist"
        self.fs.hot_paths[f"{playlist_path}/song1.m4a"] = HotPath(video_id="video1")
        self.fs.hot_paths[f"{playlist_path}/song2.m4a"] = HotPath(video_id="video2")
        self.mock_router.validate_path.return_value = True
        self.mock_router.route.return_value = [".", "..", "song1.m4a", "song2.m4a"]

        self.fs.readdir(playlist_path, None)

        self.assertEqual(
            list(self.fs.precache_queue),
            [
                (f"{playlist_path}/song1.m4a", "video1"),
                (f"{playlist_path}/song2.m4a", "video2"),
            ],
        )
        self.mock_thread_manager.submit_task.assert_any_call(
            "io", self.fs._run_precache_worker
        )

    def test_precache_queues_at_most_tracks_per_directory(self):
        names = self.publish_tracks("/liked_songs", 12)

        self.fs._schedule_precache_for_entries("/liked_songs", names)

        self.assertEqual(
            len(self.fs.precache_queue), self.fs.PRECACHE_TRACKS_PER_DIRECTORY
        )

    def test_precache_skips_non_audio_unknown_unavailable_and_queued_entries(self):
        dir_path = "/liked_songs"
        names = self.publish_tracks(dir_path, 3)
        self.mock_cache.get_unavailable_video_ids.return_value = {"song-v1"}
        self.fs.precache_queued_paths.add(f"{dir_path}/song2.m4a")
        self.fs.precache_worker_running = True

        self.fs._schedule_precache_for_entries(
            dir_path, [".", "..", "cover.jpg", "unknown.m4a", *names]
        )

        self.assertEqual(
            list(self.fs.precache_queue), [(f"{dir_path}/song0.m4a", "song-v0")]
        )
        self.mock_thread_manager.submit_task.assert_not_called()

    def test_precache_without_candidates_does_not_start_worker(self):
        self.fs._schedule_precache_for_entries("/liked_songs", [".", "..", "x.m4a"])

        self.assertEqual(len(self.fs.precache_queue), 0)
        self.mock_thread_manager.submit_task.assert_not_called()

    def test_precache_respects_max_queue_depth(self):
        self.fs.precache_queue.extend(
            ("/old", "v") for _ in range(self.fs.PRECACHE_MAX_QUEUE_DEPTH)
        )
        self.fs.precache_worker_running = True
        names = self.publish_tracks("/liked_songs", 2)

        self.fs._schedule_precache_for_entries("/liked_songs", names)

        self.assertEqual(len(self.fs.precache_queue), self.fs.PRECACHE_MAX_QUEUE_DEPTH)
        self.assertEqual(self.fs.precache_queued_paths, set())

    def test_precache_worker_records_each_outcome_and_stops_when_empty(self):
        self.fs.last_fs_activity = 0
        self.fs.precache_worker_running = True
        self.fs.precache_queue.extend(
            [("/a", "dead"), ("/b", "ok"), ("/c", "cached"), ("/d", "err")]
        )
        self.mock_cache.is_track_unavailable.side_effect = lambda vid: vid == "dead"
        outcomes = {"ok": True, "cached": False}

        def precache(_path, video_id):
            if video_id == "err":
                raise RuntimeError("network")
            return outcomes[video_id]

        self.mock_file_handler.precache.side_effect = precache

        self.fs._run_precache_worker()

        self.assertEqual(self.fs.stats["precache_started"], 3)
        self.assertEqual(self.fs.stats["precache_completed"], 1)
        self.assertEqual(self.fs.stats["precache_skipped"], 2)
        self.assertEqual(self.fs.stats["precache_failed"], 1)
        self.assertFalse(self.fs.precache_worker_running)
        self.mock_thread_manager.submit_task.assert_not_called()

    def test_precache_worker_does_not_count_its_own_stats_as_activity(self):
        self.fs.last_fs_activity = 0
        self.fs.precache_queue.extend([("/a", "v1"), ("/b", "v2")])
        self.mock_file_handler.precache.return_value = True

        with patch("ytmusicfs.filesystem.time.sleep") as sleep:
            self.fs._run_precache_worker()

        sleep.assert_not_called()
        self.assertEqual(self.fs.last_fs_activity, 0)
        self.assertEqual(self.fs.stats["precache_completed"], 2)

    def test_precache_worker_waits_for_filesystem_to_go_idle(self):
        self.fs.last_fs_activity = time.time()
        self.fs.precache_queue.append(("/a", "v"))
        self.mock_file_handler.precache.return_value = True

        def idle(_seconds):
            self.fs.last_fs_activity = 0

        with patch("ytmusicfs.filesystem.time.sleep", side_effect=idle) as sleep:
            self.fs._run_precache_worker()

        sleep.assert_called_once_with(0.25)
        self.assertEqual(self.fs.stats["precache_completed"], 1)

    def test_precache_worker_drops_queue_on_shutdown(self):
        self.fs.last_fs_activity = time.time()
        self.fs.precache_worker_running = True
        self.fs.precache_queue.extend([("/a", "v1"), ("/b", "v2")])
        self.fs.precache_queued_paths.update({"/a", "/b"})
        self.mock_thread_manager.is_shutdown.return_value = True

        with patch("ytmusicfs.filesystem.time.sleep") as sleep:
            self.fs._run_precache_worker()

        sleep.assert_not_called()
        self.mock_file_handler.precache.assert_not_called()
        self.assertEqual(len(self.fs.precache_queue), 0)
        self.assertEqual(self.fs.precache_queued_paths, set())
        self.assertFalse(self.fs.precache_worker_running)
        self.mock_thread_manager.submit_task.assert_not_called()

    def test_precache_worker_resubmits_itself_when_interrupted_with_work_left(self):
        self.fs.last_fs_activity = 0
        self.fs.precache_queue.extend([("/a", "v1"), ("/b", "v2")])
        self.mock_cache.is_track_unavailable.side_effect = RuntimeError("db closed")

        with self.assertRaises(RuntimeError):
            self.fs._run_precache_worker()

        self.mock_thread_manager.submit_task.assert_called_once_with(
            "io", self.fs._run_precache_worker
        )


class TestYouTubeMusicFSBackgroundWork(YouTubeMusicFSTestCase):
    """Post-mount refresh, playlist prefetch, trigger polling and auto-repair."""

    def run_tasks_inline(self):
        def submit_task(_pool, fn, *args):
            future = Future()
            future.set_result(fn(*args))
            return future

        self.mock_thread_manager.submit_task.side_effect = submit_task

    def refresh_state(self):
        with self.fs.refresh_state_lock:
            return dict(self.fs.refresh_state)

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

    def test_refresh_library_roots_logs_failures(self):
        self.mock_fetcher.refresh_library_roots.side_effect = RuntimeError("offline")
        self.fs.logger = Mock()

        self.fs._refresh_library_roots()

        self.fs.logger.warning.assert_called_once()

    def test_automatic_refresh_prefetches_after_liked_songs(self):
        self.fs.last_fs_activity = time.time() - 20

        with (
            patch.object(self.fs, "_sleep_refresh_delay", return_value=True),
            patch.object(self.fs, "_prefetch_playlist_album_contents") as mock_prefetch,
        ):
            self.fs._automatic_refresh_after_mount()

        self.mock_fetcher.refresh_liked_songs_automatic.assert_called_once_with()
        mock_prefetch.assert_called_once_with()
        state = self.refresh_state()
        self.assertEqual(state["last_result"], "ok")
        self.assertFalse(state["running"])

    def test_automatic_refresh_stops_when_cancelled_before_start(self):
        with patch.object(self.fs, "_sleep_refresh_delay", return_value=False):
            self.fs._automatic_refresh_after_mount()

        self.mock_fetcher.refresh_liked_songs_automatic.assert_not_called()
        self.assertEqual(self.refresh_state()["phase"], "scheduled")

    def test_automatic_refresh_backs_off_while_active_then_can_be_cancelled(self):
        self.fs.last_fs_activity = time.time() + 60

        with patch.object(
            self.fs, "_sleep_refresh_delay", side_effect=[True, True, False]
        ):
            self.fs._automatic_refresh_after_mount()

        self.assertEqual(self.refresh_state()["backoffs"], 2)
        self.mock_fetcher.refresh_liked_songs_automatic.assert_not_called()

    def test_automatic_refresh_records_failure_and_skips_prefetch(self):
        self.fs.last_fs_activity = 0
        self.mock_fetcher.refresh_liked_songs_automatic.side_effect = RuntimeError(
            "quota"
        )

        with (
            patch.object(self.fs, "_sleep_refresh_delay", return_value=True),
            patch.object(self.fs, "_prefetch_playlist_album_contents") as prefetch,
        ):
            self.fs._automatic_refresh_after_mount()

        prefetch.assert_not_called()
        state = self.refresh_state()
        self.assertEqual(state["last_result"], "failed: quota")
        self.assertEqual(state["phase"], "idle")
        self.assertFalse(state["running"])

    def test_sleep_refresh_delay_sleeps_each_second_until_shutdown(self):
        with patch("ytmusicfs.filesystem.time.sleep") as sleep:
            self.assertTrue(self.fs._sleep_refresh_delay(3))
            self.assertEqual(sleep.call_count, 3)

            self.mock_thread_manager.is_shutdown.side_effect = [False, True]
            self.assertFalse(self.fs._sleep_refresh_delay(3))

        self.assertEqual(sleep.call_count, 4)
        self.assertEqual(
            self.refresh_state()["last_result"], "cancelled during shutdown"
        )

    def test_playlist_prefetch_skips_cached_and_fetches_uncached_entries(self):
        self.fs.last_fs_activity = time.time() - 20
        self.mock_fetcher.PLAYLIST_REGISTRY = [
            {
                "name": "cached",
                "id": "PL_CACHED",
                "type": "playlist",
                "path": "/playlists/cached",
            },
            {
                "name": "uncached",
                "id": "PL_UNCACHED",
                "type": "playlist",
                "path": "/playlists/uncached",
            },
            {
                "name": "album",
                "id": "MPREb_123",
                "type": "album",
                "path": "/albums/album",
            },
            {
                "name": "liked_songs",
                "id": "LM",
                "type": "liked_songs",
                "path": "/liked_songs",
            },
        ]
        self.mock_cache.get.side_effect = lambda key: (
            [{"filename": "song.m4a"}] if key == "/playlists/cached_processed" else []
        )

        self.fs._prefetch_playlist_album_contents()

        self.mock_fetcher.fetch_playlist_content.assert_any_call(
            "PL_UNCACHED", "/playlists/uncached", force_refresh=False
        )
        self.mock_fetcher.fetch_playlist_content.assert_any_call(
            "MPREb_123", "/albums/album", force_refresh=False
        )
        self.assertEqual(self.mock_fetcher.fetch_playlist_content.call_count, 2)
        prefetch = self.refresh_state()["playlist_prefetch"]
        self.assertEqual(prefetch["queued"], 3)
        self.assertEqual(prefetch["skipped"], 1)
        self.assertEqual(prefetch["completed"], 2)
        self.assertEqual(prefetch["failed"], 0)

    def test_playlist_prefetch_counts_failures_and_continues(self):
        self.fs.last_fs_activity = 0
        self.mock_fetcher.PLAYLIST_REGISTRY = [
            {"name": "a", "id": "PL_A", "type": "playlist", "path": "/playlists/a"},
            {"name": "b", "id": "PL_B", "type": "playlist", "path": "/playlists/b"},
        ]
        self.mock_fetcher.fetch_playlist_content.side_effect = [
            RuntimeError("boom"),
            ["x.m4a"],
        ]

        self.fs._prefetch_playlist_album_contents()

        prefetch = self.refresh_state()["playlist_prefetch"]
        self.assertEqual(prefetch["failed"], 1)
        self.assertEqual(prefetch["completed"], 1)
        self.assertIsNone(prefetch["current"])

    def test_playlist_prefetch_with_no_entries_leaves_phase_unchanged(self):
        self.mock_fetcher.PLAYLIST_REGISTRY = [
            {"name": "liked_songs", "type": "liked_songs", "path": "/liked_songs"}
        ]

        self.fs._prefetch_playlist_album_contents()

        state = self.refresh_state()
        self.assertEqual(state["phase"], "idle")
        self.assertEqual(state["playlist_prefetch"]["queued"], 0)

    def test_playlist_prefetch_stops_on_shutdown(self):
        self.mock_fetcher.PLAYLIST_REGISTRY = [
            {"name": "a", "id": "PL_A", "type": "playlist", "path": "/playlists/a"}
        ]
        self.mock_thread_manager.is_shutdown.return_value = True

        self.fs._prefetch_playlist_album_contents()

        self.mock_fetcher.fetch_playlist_content.assert_not_called()
        self.assertEqual(
            self.refresh_state()["last_result"], "cancelled during shutdown"
        )

    def test_playlist_prefetch_stops_when_idle_wait_is_cancelled(self):
        self.mock_fetcher.PLAYLIST_REGISTRY = [
            {"name": "a", "id": "PL_A", "type": "playlist", "path": "/playlists/a"}
        ]

        with patch.object(
            self.fs, "_wait_for_playlist_prefetch_idle", return_value=False
        ):
            self.fs._prefetch_playlist_album_contents()

        self.mock_fetcher.fetch_playlist_content.assert_not_called()

    def test_playlist_prefetch_backs_off_while_filesystem_is_active(self):
        self.fs.last_fs_activity = time.time()

        with patch.object(self.fs, "_sleep_refresh_delay", return_value=False):
            result = self.fs._wait_for_playlist_prefetch_idle()

        self.assertFalse(result)
        self.assertGreater(self.refresh_state()["backoffs"], 0)

    def test_playlist_prefetch_resumes_once_filesystem_goes_idle(self):
        self.fs.last_fs_activity = time.time()

        def sleep(_seconds):
            self.fs.last_fs_activity = 0
            return True

        with patch.object(self.fs, "_sleep_refresh_delay", side_effect=sleep):
            self.assertTrue(self.fs._wait_for_playlist_prefetch_idle())

        self.assertEqual(self.refresh_state()["backoffs"], 1)

    def test_check_repair_notifications_handles_refresh(self):
        self.mock_cache.get_pending_cache_trigger.return_value = "refresh"
        self.mock_cache.get_pending_repair_trigger.return_value = None
        self.fs.hot_paths["/liked_songs/a.m4a"] = HotPath(video_id="v")

        self.fs._check_repair_notifications()

        self.mock_cache.clear_metadata.assert_called_once()
        self.mock_cache.clear_cache_trigger.assert_called_once_with("refresh")
        self.mock_thread_manager.submit_task.assert_called_once_with(
            "api", self.fs._automatic_refresh_after_mount
        )
        self.assertEqual(self.fs.hot_paths, {})

    def test_check_repair_notifications_handles_clear(self):
        self.mock_cache.get_pending_cache_trigger.return_value = "clear"
        self.mock_cache.get_pending_repair_trigger.return_value = None

        self.fs._check_repair_notifications()

        self.mock_cache.clear_all.assert_called_once()
        self.mock_cache.clear_cache_trigger.assert_called_once_with("clear")
        self.mock_thread_manager.submit_task.assert_called_once()

    def test_check_repair_notifications_prefers_clear_over_refresh(self):
        self.mock_cache.get_pending_cache_trigger.return_value = "clear"

        self.fs._check_repair_notifications()

        self.mock_cache.clear_all.assert_called_once()
        self.mock_cache.clear_metadata.assert_not_called()

    def test_check_repair_notifications_falls_back_to_repair(self):
        path = "/liked_songs/song.m4a"
        repairs = [{"old_video_id": "old1", "path": path}, {"old_video_id": "x"}]
        self.mock_cache.get_pending_cache_trigger.return_value = None
        self.mock_cache.get_pending_repair_trigger.return_value = {"repairs": repairs}
        self.fs.hot_paths[path] = HotPath(video_id="old1")
        self.fs.hot_paths["/liked_songs/other.m4a"] = HotPath(video_id="o")

        self.fs._check_repair_notifications()

        self.mock_cache.invalidate_repaired_paths.assert_called_once_with(repairs)
        self.mock_cache.clear_repair_trigger.assert_called_once()
        self.assertEqual(list(self.fs.hot_paths), ["/liked_songs/other.m4a"])

    def test_check_repair_notifications_clears_empty_repair_trigger(self):
        self.mock_cache.get_pending_cache_trigger.return_value = None
        self.mock_cache.get_pending_repair_trigger.return_value = {"repairs": []}

        self.fs._check_repair_notifications()

        self.mock_cache.invalidate_repaired_paths.assert_not_called()
        self.mock_cache.clear_repair_trigger.assert_called_once_with()

    def test_check_repair_notifications_without_triggers_changes_nothing(self):
        self.mock_cache.get_pending_cache_trigger.return_value = None
        self.mock_cache.get_pending_repair_trigger.return_value = None

        self.fs._check_repair_notifications()

        self.mock_cache.clear_repair_trigger.assert_not_called()
        self.mock_thread_manager.submit_task.assert_not_called()

    def test_check_repair_notifications_swallows_trigger_errors(self):
        self.mock_cache.get_pending_cache_trigger.side_effect = OSError("disk")
        self.fs.logger = Mock()

        self.fs._check_repair_notifications()

        self.fs.logger.warning.assert_called_once()

    def test_poll_repair_notifications_checks_until_shutdown(self):
        # One poll with a full five-second wait, then shutdown mid-wait.
        self.mock_thread_manager.is_shutdown.side_effect = [False] * 7 + [True]
        self.fs.logger = Mock()

        with (
            patch.object(
                self.fs,
                "_check_repair_notifications",
                side_effect=RuntimeError("boom"),
            ) as check,
            patch("ytmusicfs.filesystem.time.sleep") as sleep,
        ):
            self.fs._poll_repair_notifications()

        self.assertEqual(check.call_count, 2)
        self.assertEqual(sleep.call_count, 5)
        self.assertEqual(self.fs.logger.warning.call_count, 2)

    def test_poll_repair_notifications_exits_immediately_after_shutdown(self):
        self.mock_thread_manager.is_shutdown.return_value = True

        with patch.object(self.fs, "_check_repair_notifications") as check:
            self.fs._poll_repair_notifications()

        check.assert_not_called()

    @patch("ytmusicfs.filesystem.LikedSongsRepairer")
    def test_auto_repair_supports_playlist_paths_locally(self, mock_repairer_class):
        path = "/playlists/Mix/Artist - Song.m4a"
        repair = Mock()
        repair.old_video_id = "old"
        repair.new_video_id = "new"
        repair.path = path
        repair.old_track = {"videoId": "old"}
        repair.replacement = {"videoId": "new"}

        repairer = mock_repairer_class.return_value
        repairer._plan_one.return_value = repair
        self.run_tasks_inline()
        self.fs.hot_paths[path] = HotPath(attrs={"videoId": "old"}, video_id="old")

        result = self.fs._auto_repair_on_stream_unavailable("old", path)

        self.assertEqual(result, "new")
        mock_repairer_class.assert_called_once()
        dependencies = mock_repairer_class.call_args.args[0]
        self.assertFalse(dependencies.sync_account)
        repairer._replace_cached_liked_track.assert_called_once_with(
            "old", path, repair.old_track, repair.replacement
        )
        self.mock_cache.clear_unavailable_track.assert_called_once_with("old", path)
        self.assertEqual(self.fs.hot_paths[path].video_id, "new")
        self.assertEqual(self.fs.hot_paths[path].attrs["videoId"], "new")

    @patch("ytmusicfs.filesystem.LikedSongsRepairer")
    def test_auto_repair_without_replacement_returns_none(self, mock_repairer_class):
        mock_repairer_class.return_value._plan_one.return_value = None
        self.run_tasks_inline()

        result = self.fs._auto_repair_on_stream_unavailable("old", "/albums/LP/a.m4a")

        self.assertIsNone(result)
        self.mock_cache.record_repair_trigger.assert_not_called()

    def test_auto_repair_skips_paths_outside_library_and_known_dead_tracks(self):
        self.assertIsNone(
            self.fs._auto_repair_on_stream_unavailable("v", "/.ytmusicfs/x.m4a")
        )
        self.mock_cache.is_no_replacement.return_value = True
        self.assertIsNone(
            self.fs._auto_repair_on_stream_unavailable("v", "/liked_songs/a.m4a")
        )

        self.mock_thread_manager.submit_task.assert_not_called()

    def test_auto_repair_gives_up_after_timeout(self):
        future = Mock()
        future.result.side_effect = TimeoutError
        self.mock_thread_manager.submit_task.return_value = future

        self.assertIsNone(
            self.fs._auto_repair_on_stream_unavailable("v", "/liked_songs/a.m4a")
        )
        future.result.assert_called_once_with(timeout=10.0)

    def test_replace_hot_video_id_creates_entry_for_unlisted_path(self):
        self.fs._replace_hot_video_id("/liked_songs/new.m4a", "v2")

        entry = self.fs.hot_paths["/liked_songs/new.m4a"]
        self.assertEqual(entry.video_id, "v2")
        self.assertIsNone(entry.attrs)


if __name__ == "__main__":
    unittest.main()
