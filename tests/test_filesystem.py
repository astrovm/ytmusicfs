#!/usr/bin/env python3

import errno
import logging
import os
import stat
import time
import unittest
from unittest.mock import Mock, patch

from fuse import FuseOSError

# Import the class to test
from ytmusicfs.filesystem import YouTubeMusicFS


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
            self.mock_cache.get_unavailable_video_ids.return_value = set()
            self.mock_fetcher = mock_fetcher.return_value
            self.mock_router = mock_router.return_value
            self.mock_file_handler = mock_file_handler.return_value
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

    def test_metadata_status_file(self):
        """Test reading the virtual status file."""
        result = self.fs.readdir("/.ytmusicfs", None)
        attrs = self.fs.getattr("/.ytmusicfs/status.json")
        content = self.fs.read("/.ytmusicfs/status.json", 4096, 0, 0)

        self.assertEqual(result, [".", "..", "status.json"])
        self.assertEqual(attrs["st_mode"], stat.S_IFREG | 0o444)
        self.assertIn(b'"browser": "brave"', content)
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

    def test_getattr_audio_uses_cached_real_size(self):
        file_path = "/liked_songs/song.m4a"
        self.mock_cache.get_file_attrs_from_parent_dir.return_value = None
        self.mock_cache.get.return_value = 12345
        self.mock_router.validate_path.return_value = True

        attrs = self.fs.getattr(file_path, None)

        self.assertEqual(attrs["st_size"], 12345)

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


if __name__ == "__main__":
    unittest.main()
