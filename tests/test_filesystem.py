#!/usr/bin/env python3

import unittest
from unittest.mock import Mock, patch, MagicMock, ANY
import errno
import logging
import os
import stat
import time
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
            patch("ytmusicfs.filesystem.YTMusicOAuthAdapter") as mock_oauth_adapter,
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
            self.mock_oauth_adapter = mock_oauth_adapter.return_value
            self.mock_client = mock_client.return_value
            self.mock_processor = mock_processor.return_value
            self.mock_cache = mock_cache.return_value
            self.mock_fetcher = mock_fetcher.return_value
            self.mock_router = mock_router.return_value
            self.mock_file_handler = mock_file_handler.return_value
            self.mock_metadata = mock_metadata.return_value

            # Create the instance to test
            self.fs = YouTubeMusicFS(
                auth_file="dummy_auth.json",
                client_id="dummy_id",
                client_secret="dummy_secret",
                credentials_file="dummy_credentials.json",
                cache_dir="/tmp/cache_test",
            )

            # Make sure the internal attributes are set to our mocks
            self.fs.thread_manager = self.mock_thread_manager
            self.fs.yt_dlp_utils = self.mock_yt_dlp_utils
            self.fs.oauth_adapter = self.mock_oauth_adapter
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
        self.assertEqual(len(result), 5)  # ".", "..", and 3 default directories
        self.assertIn(".", result)
        self.assertIn("..", result)
        self.assertIn("playlists", result)
        self.assertIn("liked_songs", result)
        self.assertIn("albums", result)

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

        # Mock the cache behavior for directory listing
        cache_listing = {
            ".": {"is_dir": True},
            "..": {"is_dir": True},
            "song1.m4a": {"is_dir": False},
            "song2.m4a": {"is_dir": False},
        }

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
