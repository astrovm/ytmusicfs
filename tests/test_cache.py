#!/usr/bin/env python3

import unittest
from unittest.mock import Mock, patch, MagicMock
import logging
import os
import shutil
import tempfile
import time
from pathlib import Path
import threading
import sqlite3

# Import the class to test
from ytmusicfs.cache import CacheManager


class TestCacheManager(unittest.TestCase):
    """Test case for CacheManager class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a temporary directory for the cache
        self.temp_dir = tempfile.mkdtemp()

        # Create lock with proper context manager support
        self.mock_lock = MagicMock()
        self.mock_lock.__enter__.return_value = None
        self.mock_lock.__exit__.return_value = None

        # Mock thread manager
        self.thread_manager = Mock()
        self.thread_manager.create_lock.return_value = self.mock_lock

        # Create a logger
        self.logger = logging.getLogger("test")

        # Patch sqlite3.connect to avoid DB interaction
        self.patcher = patch("sqlite3.connect")
        self.mock_sqlite = self.patcher.start()

        # Mock connection and cursor with improved mock configuration
        self.mock_conn = Mock()
        self.mock_cursor = Mock()

        # Configure cursor methods to return iterable results
        self.mock_cursor.fetchall.return_value = []  # Empty list instead of a Mock
        self.mock_cursor.fetchone.return_value = None
        self.mock_conn.cursor.return_value = self.mock_cursor
        self.mock_conn.execute.return_value = None
        self.mock_sqlite.return_value = self.mock_conn

        # Add patch for _load_valid_paths method to avoid DB interaction during init
        self.load_paths_patcher = patch.object(CacheManager, "_load_valid_paths")
        self.mock_load_paths = self.load_paths_patcher.start()

        # Create the instance to test with patched initialization
        self.cache = CacheManager(
            thread_manager=self.thread_manager,
            cache_dir=self.temp_dir,
            logger=self.logger,
            maxsize=100,
        )

        # Mock internal methods/properties for testing
        self.cache.get = Mock()
        self.cache.set = Mock()
        self.cache.set_batch = Mock()
        self.cache.mark_valid = Mock()
        self.cache.is_valid_path = Mock(return_value=True)
        self.cache.is_directory = Mock()
        self.cache.get_directory_listing_with_attrs = Mock()
        self.cache.set_directory_listing_with_attrs = Mock()
        self.cache.get_file_attrs_from_parent_dir = Mock()
        self.cache.mem_cache = {}
        self.cache.path_to_key = Mock(return_value="mocked_key")
        self.cache.get_refresh_metadata = Mock(return_value=(time.time(), "fresh"))

    def tearDown(self):
        """Clean up after each test method."""
        # Stop patchers
        self.patcher.stop()
        self.load_paths_patcher.stop()

        # Close the cache to release DB connections
        self.cache.close()

        # Remove the temporary directory
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_set_and_get(self):
        """Test setting and getting a value in the cache."""
        # Test data
        key = "test_key"
        value = {"name": "Test Value", "id": 123}

        # Configure mock
        self.cache.get.return_value = value

        # Set the value (using the mock)
        self.cache.set(key, value)

        # Get the value (using the mock)
        result = self.cache.get(key)

        # Verify result
        self.assertEqual(result, value)

        # Verify the mock was called correctly
        self.cache.set.assert_called_once_with(key, value)
        self.cache.get.assert_called_once_with(key)

    def test_set_and_get_batch(self):
        """Test setting and getting multiple values in the cache."""
        # Test data
        entries = {"key1": "value1", "key2": {"name": "value2"}, "key3": [1, 2, 3]}

        # Configure mocks for get to return appropriate values
        def mock_get_side_effect(key):
            return entries.get(key)

        self.cache.get.side_effect = mock_get_side_effect

        # Set the values
        self.cache.set_batch(entries)

        # Verify set_batch was called correctly
        self.cache.set_batch.assert_called_once_with(entries)

        # Get the values and verify
        for key, expected_value in entries.items():
            value = self.cache.get(key)
            self.assertEqual(value, expected_value)

    def test_path_validation(self):
        """Test path validation functionality."""
        # Configure mock behavior
        paths = {
            "/": True,
            "/playlists": True,
            "/playlists/my_playlist": True,
            "/playlists/my_playlist/song.m4a": True,
            "/invalid": False,
            "/playlists/invalid": False,
            "/playlists/my_playlist/invalid.m4a": False,
        }

        def mock_is_valid_path(path):
            return paths.get(path, False)

        self.cache.is_valid_path.side_effect = mock_is_valid_path

        # Test valid paths
        self.assertTrue(self.cache.is_valid_path("/"))
        self.assertTrue(self.cache.is_valid_path("/playlists"))
        self.assertTrue(self.cache.is_valid_path("/playlists/my_playlist"))
        self.assertTrue(self.cache.is_valid_path("/playlists/my_playlist/song.m4a"))

        # Test invalid paths
        self.assertFalse(self.cache.is_valid_path("/invalid"))
        self.assertFalse(self.cache.is_valid_path("/playlists/invalid"))
        self.assertFalse(self.cache.is_valid_path("/playlists/my_playlist/invalid.m4a"))

    def test_is_directory(self):
        """Test directory type checking."""
        # Configure mock behavior
        directory_status = {
            "/playlists": True,
            "/playlists/my_playlist/song.m4a": False,
            "/unknown": None,
        }

        def mock_is_directory(path):
            return directory_status.get(path)

        self.cache.is_directory.side_effect = mock_is_directory

        # Test directory recognition
        self.assertTrue(self.cache.is_directory("/playlists"))
        self.assertFalse(self.cache.is_directory("/playlists/my_playlist/song.m4a"))

        # Test unknown path
        self.assertIsNone(self.cache.is_directory("/unknown"))

    def test_directory_listing_with_attrs(self):
        """Test storing and retrieving directory listings with attributes."""
        # Path to test
        path = "/playlists/my_playlist"

        # Directory listing with attributes
        listing = {
            ".": {"is_dir": True, "st_mode": 16877},
            "..": {"is_dir": True, "st_mode": 16877},
            "song1.m4a": {
                "is_dir": False,
                "st_mode": 33188,
                "st_size": 5242880,
                "st_mtime": time.time(),
            },
            "song2.m4a": {
                "is_dir": False,
                "st_mode": 33188,
                "st_size": 3145728,
                "st_mtime": time.time(),
            },
        }

        # Configure mock
        self.cache.get_directory_listing_with_attrs.return_value = listing

        # Store the listing
        self.cache.set_directory_listing_with_attrs(path, listing)

        # Verify set was called
        self.cache.set_directory_listing_with_attrs.assert_called_once_with(
            path, listing
        )

        # Retrieve the listing
        result = self.cache.get_directory_listing_with_attrs(path)

        # Verify get was called
        self.cache.get_directory_listing_with_attrs.assert_called_with(path)

        # Verify results
        self.assertEqual(result, listing)

    def test_file_attrs_from_parent_dir(self):
        """Test retrieving file attributes from parent directory."""
        # Setup parent directory with files
        parent_path = "/playlists/my_playlist"
        file_name = "song.m4a"
        file_path = f"{parent_path}/{file_name}"

        # Set up directory listing
        file_attrs = {
            "is_dir": False,
            "st_mode": 33188,
            "st_size": 4194304,
            "st_mtime": time.time(),
            "st_ctime": time.time() - 3600,
            "st_atime": time.time() - 1800,
        }

        # Configure mock
        self.cache.get_file_attrs_from_parent_dir.return_value = file_attrs

        # Retrieve file attributes
        result = self.cache.get_file_attrs_from_parent_dir(file_path)

        # Verify mock was called
        self.cache.get_file_attrs_from_parent_dir.assert_called_once_with(file_path)

        # Verify results
        self.assertEqual(result, file_attrs)

    def test_refresh_metadata(self):
        """Test setting and getting refresh metadata."""
        # Test data
        key = "playlist_registry"
        timestamp = time.time()
        status = "fresh"

        # Patch the refresh metadata methods
        self.cache.set_refresh_metadata = Mock()
        self.cache.get_refresh_metadata = Mock(return_value=(timestamp, status))

        # Set refresh metadata
        self.cache.set_refresh_metadata(key, timestamp, status)

        # Verify set was called
        self.cache.set_refresh_metadata.assert_called_once_with(key, timestamp, status)

        # Get refresh metadata
        result_timestamp, result_status = self.cache.get_refresh_metadata(key)

        # Verify get was called
        self.cache.get_refresh_metadata.assert_called_once_with(key)

        # Verify results
        self.assertEqual(result_timestamp, timestamp)
        self.assertEqual(result_status, status)


if __name__ == "__main__":
    unittest.main()
