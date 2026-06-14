#!/usr/bin/env python3

import errno
import logging
import os
import stat
import threading
import time
import unittest
from concurrent.futures import Future
from pathlib import Path
from unittest.mock import Mock, call, patch

from fuse import FuseOSError

# Import the class to test
from ytmusicfs.filesystem import YouTubeMusicFS, mount_ytmusicfs


class TestYouTubeMusicFS(unittest.TestCase):
    """Test case for YouTubeMusicFS class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Mock all dependencies using patch
        with (
            patch("ytmusicfs.filesystem.ThreadManager") as mock_thread_manager,
            patch("ytmusicfs.filesystem.YTDLPUtils") as mock_yt_dlp_utils,
            patch("ytmusicfs.filesystem.YTMusicAuthAdapter") as mock_auth_adapter,
            patch("ytmusicfs.filesystem.YouTubeMusicClient") as mock_client,
            patch("ytmusicfs.filesystem.TrackProcessor") as mock_processor,
            patch("ytmusicfs.filesystem.CacheManager") as mock_cache,
            patch("ytmusicfs.filesystem.ContentFetcher") as mock_fetcher,
            patch("ytmusicfs.filesystem.PathRouter") as mock_router,
            patch("ytmusicfs.filesystem.FileHandler") as mock_file_handler,
            patch("ytmusicfs.filesystem.MetadataManager") as mock_metadata,
        ):

            # Set up the mocks
            self.mock_thread_manager = mock_thread_manager.return_value
            self.mock_yt_dlp_utils = mock_yt_dlp_utils.return_value
            self.mock_auth_adapter = mock_auth_adapter.return_value
            self.mock_client = mock_client.return_value
            self.mock_processor = mock_processor.return_value
            self.mock_cache = mock_cache.return_value
            self.mock_cache.cache_dir = "/tmp/cache_test"
            self.mock_cache.is_track_unavailable.return_value = False
            self.mock_cache.is_path_unavailable.return_value = False
            self.mock_cache.get_unavailable_video_ids.return_value = set()
            self.mock_fetcher = mock_fetcher.return_value
            self.mock_router = mock_router.return_value
            self.mock_file_handler = mock_file_handler.return_value
            self.mock_file_handler.get_recent_handles.return_value = []
            self.mock_metadata = mock_metadata.return_value

            # Create the instance to test
            self.fs = YouTubeMusicFS(
                cache_dir="/tmp/cache_test",
                browser="brave",
            )

            # Make sure the internal attributes are set to our mocks
            self.fs.thread_manager = self.mock_thread_manager
            self.fs.yt_dlp_utils = self.mock_yt_dlp_utils
            self.fs.oauth_adapter = self.mock_auth_adapter
            self.fs.client = self.mock_client
            self.fs.processor = self.mock_processor
            self.fs.cache = self.mock_cache
            self.fs.fetcher = self.mock_fetcher
            self.fs.router = self.mock_router
            self.fs.file_handler = self.mock_file_handler
            self.fs.metadata = self.mock_metadata
            self.fs.logger = logging.getLogger("test")

            # Create a lock for last_access_lock
            self.fs.last_access_lock = self.mock_thread_manager.create_lock.return_value
            # Initialize the last_access dictionaries
            self.fs.last_access_time = {}
            self.fs.last_access_results = {}
            self.fs.hot_metadata_lock = (
                self.mock_thread_manager.create_lock.return_value
            )

    def test_readdir_root(self):
        """Test reading the contents of the root directory."""
        # Call the method
        result = self.fs.readdir("/", None)

        # Verify expected results
        self.assertEqual(len(result), 6)  # ".", "..", 3 library dirs, metadata dir
        self.assertIn(".", result)
        self.assertIn("..", result)
        self.assertIn("playlists", result)
        self.assertIn("liked_songs", result)
        self.assertIn("albums", result)
        self.assertIn(".ytmusicfs", result)

    def test_hot_readdir_does_not_route_or_call_external_clients(self):
        path = "/playlists/Mix"
        self.fs.hot_dir_entries[path] = ["song.m4a"]
        self.fs.hot_attrs_by_path[f"{path}/song.m4a"] = {"videoId": "abc123"}
        self.mock_cache.get_unavailable_video_ids.return_value = set()
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
        self.fs.hot_attrs_by_path[path] = {
            "st_mode": stat.S_IFDIR | 0o555,
            "st_nlink": 2,
            "st_size": 4096,
        }
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

    def test_metadata_status_file(self):
        """Test reading the virtual status file."""
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

    def test_status_file_size_stays_stable_while_reading(self):
        attrs = self.fs.getattr("/.ytmusicfs/status.json")
        content = self.fs.read("/.ytmusicfs/status.json", attrs["st_size"], 0, 0)

        self.assertEqual(len(content), attrs["st_size"])

    def test_readdir_playlists(self):
        """Test reading the contents of the playlists directory."""
        # Configure mock
        self.mock_fetcher.readdir_playlist_by_type.return_value = [
            ".",
            "..",
            "my_playlist",
            "workout_mix",
        ]

        # Call the method
        result = self.fs.readdir("/playlists", None)

        # Verify expected results
        self.assertEqual(len(result), 4)
        self.assertIn(".", result)
        self.assertIn("..", result)
        self.assertIn("my_playlist", result)
        self.assertIn("workout_mix", result)

        # Verify mock was called correctly
        self.mock_fetcher.readdir_playlist_by_type.assert_called_once_with(
            "playlist", "/playlists"
        )

    def test_readdir_playlist_contents(self):
        """Test reading the contents of a specific playlist."""
        # Configure mocks
        playlist_path = "/playlists/my_playlist"

        # Mock the cache get method to return None initially for the cache key
        # This is to simulate the first priority check that looks for a cached directory listing
        self.mock_cache.get.return_value = None

        # Mock the get_directory_listing_with_attrs to also return None initially
        # This makes the code fall through to the router
        self.mock_cache.get_directory_listing_with_attrs.return_value = None

        # Set the router.validate_path to return True
        self.mock_router.validate_path.return_value = True

        # Mock the router to return these items
        self.mock_router.route.return_value = [".", "..", "song1.m4a", "song2.m4a"]

        # Call the method
        result = self.fs.readdir(playlist_path, None)

        # Verify expected results
        self.assertEqual(len(result), 4)
        self.assertIn(".", result)
        self.assertIn("..", result)
        self.assertIn("song1.m4a", result)
        self.assertIn("song2.m4a", result)

        # Verify router was called
        self.mock_router.route.assert_called_once_with(playlist_path)

    def test_getattr_playlist_directory_does_not_rewrite_cached_attrs(self):
        self.mock_cache.get_file_attrs_from_parent_dir.return_value = None
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

    def test_readdir_schedules_idle_precache_for_audio_entries(self):
        playlist_path = "/playlists/my_playlist"
        self.fs.precache_lock = threading.RLock()
        self.fs.hot_video_ids_by_path[f"{playlist_path}/song1.m4a"] = "video1"
        self.fs.hot_video_ids_by_path[f"{playlist_path}/song2.m4a"] = "video2"
        self.mock_cache.get.return_value = None
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

    def test_automatic_refresh_prefetches_after_liked_songs(self):
        self.fs.last_fs_activity = time.time() - 20
        self.mock_thread_manager.is_shutdown.return_value = False

        with (
            patch.object(self.fs, "_sleep_refresh_delay", return_value=True),
            patch.object(self.fs, "_prefetch_playlist_album_contents") as mock_prefetch,
        ):
            self.fs._automatic_refresh_after_mount()

        self.mock_fetcher.refresh_liked_songs_automatic.assert_called_once_with()
        mock_prefetch.assert_called_once_with()

    def test_playlist_prefetch_skips_cached_and_fetches_uncached_entries(self):
        self.fs.last_fs_activity = time.time() - 20
        self.mock_thread_manager.is_shutdown.return_value = False
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
        with self.fs.refresh_state_lock:
            prefetch = self.fs.refresh_state["playlist_prefetch"]
        self.assertEqual(prefetch["queued"], 3)
        self.assertEqual(prefetch["skipped"], 1)
        self.assertEqual(prefetch["completed"], 2)
        self.assertEqual(prefetch["failed"], 0)

    def test_playlist_prefetch_backs_off_while_filesystem_is_active(self):
        self.fs.last_fs_activity = time.time()
        self.mock_thread_manager.is_shutdown.return_value = False

        with patch.object(self.fs, "_sleep_refresh_delay", return_value=False):
            result = self.fs._wait_for_playlist_prefetch_idle()

        self.assertFalse(result)
        with self.fs.refresh_state_lock:
            self.assertGreater(self.fs.refresh_state["backoffs"], 0)

    def test_getattr_root(self):
        """Test getting attributes of the root directory."""
        # Configure the cache mock to recognize the path
        self.mock_cache.is_valid_path.return_value = True
        self.mock_cache.is_directory.return_value = True

        # Configure current time for consistent tests
        current_time = time.time()

        # Configure a mock timestamp - we'll need this to get consistent results
        with patch("time.time", return_value=current_time):
            # Call the method
            attrs = self.fs.getattr("/", None)

            # Verify root directory attributes
            self.assertTrue(stat.S_ISDIR(attrs["st_mode"]))  # Is a directory
            self.assertEqual(attrs["st_nlink"], 2)  # Standard for directories
            self.assertTrue(attrs["st_mode"] & stat.S_IRUSR)  # Readable
            self.assertTrue(attrs["st_mode"] & stat.S_IXUSR)  # Executable

            # Directory size should match actual implementation
            # Real directories typically use 4096 bytes (even if empty)
            # The implementation might use 0 or 4096 bytes for directory size
            self.assertEqual(attrs["st_size"], 4096)  # Most common directory size

            # Verify timestamps
            self.assertAlmostEqual(attrs["st_ctime"], current_time, delta=1)
            self.assertAlmostEqual(attrs["st_mtime"], current_time, delta=1)
            self.assertAlmostEqual(attrs["st_atime"], current_time, delta=1)

    def test_getattr_nonexistent_path(self):
        """Test getting attributes of a nonexistent path."""
        # Configure mock to report path as invalid
        self.mock_cache.is_valid_path.return_value = False

        # In the actual implementation, the method uses router.validate_path if cache.is_valid_path is False
        # So we need to also mock the router.validate_path to return False
        self.mock_router.validate_path.return_value = False

        # And mock any other methods that might prevent the exception from being raised
        self.mock_cache.get_file_attrs_from_parent_dir.return_value = None
        self.mock_cache.get_entry_type.return_value = None

        # Create a mock for the last_access_lock to avoid issues
        self.fs.last_access_lock = self.mock_thread_manager.create_lock.return_value

        # Initialize the last_access dictionaries
        self.fs.last_access_time = {}
        self.fs.last_access_results = {}

        # The method should raise a FuseOSError with ENOENT code
        with self.assertRaises(FuseOSError) as context:
            self.fs.getattr("/nonexistent", None)

        # Verify the error code is ENOENT
        self.assertEqual(context.exception.args[0], errno.ENOENT)

    def test_getattr_file(self):
        """Test getting attributes of a file."""
        # Configure mocks
        file_path = "/playlists/my_playlist/song.m4a"
        self.mock_cache.is_valid_path.return_value = True
        self.mock_cache.is_directory.return_value = False

        # Mock file attributes
        mock_attrs = {
            "st_mode": stat.S_IFREG | 0o444,  # Regular file with read permission
            "st_nlink": 1,
            "st_size": 1024 * 1024,  # 1MB file
            "st_ctime": time.time() - 3600,  # Created 1 hour ago
            "st_mtime": time.time() - 1800,  # Modified 30 minutes ago
            "st_atime": time.time() - 300,  # Accessed 5 minutes ago
        }
        self.mock_cache.get_file_attrs_from_parent_dir.return_value = mock_attrs

        # Call the method
        attrs = self.fs.getattr(file_path, None)

        # Verify file attributes
        self.assertTrue(stat.S_ISREG(attrs["st_mode"]))  # Is a regular file
        self.assertEqual(attrs["st_nlink"], 1)  # Standard for files
        self.assertEqual(attrs["st_size"], 1024 * 1024)  # File size is 1MB
        self.assertTrue(attrs["st_mode"] & stat.S_IRUSR)  # Readable

    def test_getattr_uncached_audio_uses_duration_estimate(self):
        file_path = "/liked_songs/song.m4a"
        self.mock_cache.get_file_attrs_from_parent_dir.return_value = None
        self.mock_cache.get.return_value = None
        self.mock_router.validate_path.return_value = True
        self.mock_metadata.get_video_id.return_value = "abc123"
        self.mock_cache.get_duration.return_value = 180

        attrs = self.fs.getattr(file_path, None)

        self.mock_metadata.get_video_id.assert_called_once_with(file_path)
        self.assertEqual(attrs["st_size"], 180 * self.fs.ESTIMATED_BYTES_PER_SECOND)

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
        self.mock_cache.get_file_attrs_from_parent_dir.return_value = None
        self.mock_cache.get.return_value = 12345
        self.mock_router.validate_path.return_value = True

        attrs = self.fs.getattr(file_path, None)

        self.assertEqual(attrs["st_size"], 12345)

    def test_getattr_audio_prefers_complete_cached_audio_size(self):
        file_path = "/liked_songs/song.m4a"
        video_id = "complete999"
        audio_dir = Path(self.mock_cache.cache_dir) / "audio"
        audio_dir.mkdir(parents=True, exist_ok=True)
        (audio_dir / f"{video_id}.m4a").write_bytes(b"a" * 200)
        (audio_dir / f"{video_id}.status").write_text("complete:141")
        self.mock_cache.get_file_attrs_from_parent_dir.return_value = {
            "st_mode": stat.S_IFREG | 0o444,
            "st_nlink": 1,
            "st_size": 100,
        }
        self.mock_cache.get.return_value = 100
        self.mock_metadata.get_video_id.return_value = video_id

        attrs = self.fs.getattr(file_path, None)

        self.assertEqual(attrs["st_size"], 200)

    def test_update_file_size_updates_hot_attrs_and_getattr_cache(self):
        file_path = "/liked_songs/song.m4a"
        self.fs.hot_attrs_by_path[file_path] = {"st_size": 100, "videoId": "abc123"}
        self.fs.last_access_results[f"getattr:{file_path}"] = {"st_size": 100}

        self.fs._update_file_size(file_path, 200)

        self.assertEqual(self.fs.hot_attrs_by_path[file_path]["st_size"], 200)
        self.assertNotIn(f"getattr:{file_path}", self.fs.last_access_results)

    def test_cached_listing_uses_duration_estimate(self):
        self.mock_cache.get.return_value = None

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
        self.mock_cache.get.return_value = None
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

    def test_open_file(self):
        """Test opening a file."""
        # Configure mocks
        file_path = "/playlists/my_playlist/song.m4a"
        video_id = "dQw4w9WgXcQ"  # Example YouTube video ID

        # Mock fetcher behavior to extract video ID
        self.fs._get_video_id = Mock(return_value=video_id)

        # Mock file handler
        self.mock_file_handler.open.return_value = 42  # Mock file handle

        # Call the method
        file_handle = self.fs.open(file_path, os.O_RDONLY)

        # Verify file was opened correctly
        self.assertEqual(file_handle, 42)
        self.mock_file_handler.open.assert_called_once_with(file_path, video_id)

    def test_open_rejects_unavailable_path_before_stale_valid_cache(self):
        file_path = "/liked_songs/song.m4a"
        self.mock_cache.is_path_unavailable.return_value = True

        with self.assertRaises(FuseOSError) as cm:
            self.fs.open(file_path, os.O_RDONLY)

        self.assertEqual(cm.exception.errno, errno.ENOENT)
        self.mock_router.validate_path.assert_not_called()
        self.mock_file_handler.open.assert_not_called()

    def test_read_file(self):
        """Test reading content from a file."""
        # Configure mocks
        file_path = "/playlists/my_playlist/song.m4a"
        file_handle = 42
        size = 1024
        offset = 0

        # Mock file handler
        mock_data = b"test data" * 100  # About 900 bytes
        self.mock_file_handler.read.return_value = mock_data

        # Call the method
        data = self.fs.read(file_path, size, offset, file_handle)

        # Verify file was read correctly
        self.assertEqual(data, mock_data)
        self.mock_file_handler.read.assert_called_once_with(
            file_path, size, offset, file_handle
        )

    def test_read_preserves_os_error_code(self):
        """Filesystem read should pass through file-handler errno."""

        file_path = "/playlists/my_playlist/song.m4a"
        file_handle = 42
        self.mock_file_handler.read.side_effect = OSError(errno.ENOENT, "missing")

        with self.assertRaises(FuseOSError) as context:
            self.fs.read(file_path, 1024, 0, file_handle)

        self.assertEqual(context.exception.args[0], errno.ENOENT)

    def test_read_failure_logs_once_per_cooldown(self):
        file_path = "/playlists/my_playlist/song.m4a"
        file_handle = 42
        self.mock_file_handler.read.side_effect = OSError(errno.ENOENT, "missing")
        self.fs.logger = Mock()

        for _ in range(2):
            with self.assertRaises(FuseOSError):
                self.fs.read(file_path, 1024, 0, file_handle)

        self.fs.logger.warning.assert_called_once()

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

    def test_release_file(self):
        """Test releasing (closing) a file."""
        # Configure mocks
        file_path = "/playlists/my_playlist/song.m4a"
        file_handle = 42

        # Mock return value to avoid errors
        self.mock_file_handler.release.return_value = 0

        # Call the method
        result = self.fs.release(file_path, file_handle)

        # Verify file was released correctly
        self.assertEqual(result, 0)  # Should return 0 on success
        self.mock_file_handler.release.assert_called_once_with(file_path, file_handle)

    def test_check_repair_notifications_handles_refresh(self):
        """A refresh trigger should trigger metadata clear and background refresh."""
        self.mock_cache.get_pending_cache_trigger.return_value = "refresh"
        self.mock_cache.get_pending_repair_trigger.return_value = None

        self.fs._check_repair_notifications()

        self.mock_cache.clear_metadata.assert_called_once()
        self.mock_cache.clear_cache_trigger.assert_called_once_with("refresh")
        self.mock_thread_manager.submit_task.assert_called_once()

    def test_check_repair_notifications_handles_clear(self):
        """A clear trigger should trigger full cache clear and background refresh."""
        self.mock_cache.get_pending_cache_trigger.return_value = "clear"
        self.mock_cache.get_pending_repair_trigger.return_value = None

        self.fs._check_repair_notifications()

        self.mock_cache.clear_all.assert_called_once()
        self.mock_cache.clear_cache_trigger.assert_called_once_with("clear")
        self.mock_thread_manager.submit_task.assert_called_once()

    def test_check_repair_notifications_prefers_clear_over_refresh(self):
        """If a clear trigger exists, it takes priority."""
        self.mock_cache.get_pending_cache_trigger.return_value = "clear"

        self.fs._check_repair_notifications()

        self.mock_cache.clear_all.assert_called_once()
        self.mock_cache.clear_metadata.assert_not_called()

    def test_check_repair_notifications_falls_back_to_repair(self):
        """A repair trigger should invalidate repaired paths."""
        self.mock_cache.get_pending_cache_trigger.return_value = None
        self.mock_cache.get_pending_repair_trigger.return_value = {
            "repairs": [{"old_video_id": "old1", "path": "/liked_songs/song.m4a"}]
        }

        self.fs._check_repair_notifications()

        self.mock_cache.invalidate_repaired_paths.assert_called_once()
        self.mock_cache.clear_repair_trigger.assert_called_once()

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

        def submit_task(_name, fn):
            future = Future()
            future.set_result(fn())
            return future

        self.mock_thread_manager.submit_task.side_effect = submit_task
        self.mock_cache.is_no_replacement.return_value = False
        self.fs.hot_video_ids_by_path[path] = "old"
        self.fs.hot_attrs_by_path[path] = {"videoId": "old"}

        result = self.fs._auto_repair_on_stream_unavailable("old", path)

        self.assertEqual(result, "new")
        mock_repairer_class.assert_called_once()
        dependencies = mock_repairer_class.call_args.args[0]
        self.assertFalse(dependencies.sync_account)
        repairer._replace_cached_liked_track.assert_called_once_with(
            "old", path, repair.old_track, repair.replacement
        )
        self.mock_cache.clear_unavailable_track.assert_called_once_with("old", path)
        self.assertEqual(self.fs.hot_video_ids_by_path[path], "new")
        self.assertEqual(self.fs.hot_attrs_by_path[path]["videoId"], "new")


if __name__ == "__main__":
    unittest.main()
