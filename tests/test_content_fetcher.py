#!/usr/bin/env python3

import unittest
from unittest.mock import Mock, patch, MagicMock
import logging
import time
from typing import Dict, List, Any

# Import the class to test
from ytmusicfs.content_fetcher import ContentFetcher


class TestContentFetcher(unittest.TestCase):
    """Test case for ContentFetcher class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create patches for initialization
        self.patcher_initialize = patch.object(
            ContentFetcher, "_initialize_playlist_registry"
        )
        self.mock_initialize = self.patcher_initialize.start()

        # Mock dependencies
        self.client = Mock()
        self.processor = Mock()
        self.cache = Mock()
        self.logger = logging.getLogger("test")
        self.yt_dlp_utils = Mock()

        # Configure mock behavior for common calls
        self.processor.sanitize_filename.side_effect = lambda x: x.lower().replace(
            " ", "_"
        )

        # Configure the cache mock to return proper values
        self.cache.get_refresh_metadata.return_value = (
            time.time() - 7200,
            "stale",
        )  # Return as stale by default
        self.cache.get_directory_listing_with_attrs.return_value = (
            {}
        )  # Empty directory listing

        # Create the instance to test
        self.fetcher = ContentFetcher(
            client=self.client,
            processor=self.processor,
            cache=self.cache,
            logger=self.logger,
            yt_dlp_utils=self.yt_dlp_utils,
        )

        # Reset mocks for clean state in each test
        self.client.reset_mock()
        self.processor.reset_mock()
        self.cache.reset_mock()
        self.yt_dlp_utils.reset_mock()
        self.mock_initialize.reset_mock()

    def tearDown(self):
        """Clean up after each test."""
        self.patcher_initialize.stop()

    def test_initialize_playlist_registry(self):
        """Test initialization of playlist registry with mocked API responses."""
        # Re-enable the original method for this test
        self.patcher_initialize.stop()

        # Configure mocks
        self.client.get_library_playlists.return_value = [
            {"title": "My Playlist", "playlistId": "PL123"},
            {"title": "Podcast Playlist", "playlistId": "SE"},
        ]
        self.client.get_library_albums.return_value = [
            {"title": "My Album", "browseId": "MPREb_456"}
        ]

        # Configure cache refresh metadata
        self.cache.get_refresh_metadata.return_value = (
            time.time() - 7200,
            "stale",
        )  # Old timestamp (2 hours ago), stale status

        # Clear existing registry for clean test
        self.fetcher.PLAYLIST_REGISTRY = []

        # Call the method directly
        self.fetcher._initialize_playlist_registry()

        # Verify registry content
        self.assertEqual(
            len(self.fetcher.PLAYLIST_REGISTRY), 3
        )  # Liked + playlist + album

        self.client.get_library_playlists.assert_called_once_with(limit=1000)
        self.client.get_library_albums.assert_called_once_with(limit=1000)

        # Check for liked songs entry
        liked_entry = next(
            (p for p in self.fetcher.PLAYLIST_REGISTRY if p["type"] == "liked_songs"),
            None,
        )
        self.assertIsNotNone(liked_entry)
        self.assertEqual(liked_entry["id"], "LM")
        self.assertEqual(liked_entry["path"], "/liked_songs")

        # Check for playlist entry
        playlist_entry = next(
            (p for p in self.fetcher.PLAYLIST_REGISTRY if p["type"] == "playlist"), None
        )
        self.assertIsNotNone(playlist_entry)
        self.assertEqual(playlist_entry["id"], "PL123")
        self.assertEqual(playlist_entry["name"], "my_playlist")

        # Check for album entry
        album_entry = next(
            (p for p in self.fetcher.PLAYLIST_REGISTRY if p["type"] == "album"), None
        )
        self.assertIsNotNone(album_entry)
        self.assertEqual(album_entry["id"], "MPREb_456")
        self.assertEqual(album_entry["name"], "my_album")

        # Ensure podcast playlist was skipped
        skipped_entry = next(
            (
                p
                for p in self.fetcher.PLAYLIST_REGISTRY
                if p.get("id") == "SE"
            ),
            None,
        )
        self.assertIsNone(skipped_entry)

        # Verify cache was updated
        self.cache.set_refresh_metadata.assert_called_once()

        # Restart the patch for other tests
        self.patcher_initialize = patch.object(
            ContentFetcher, "_initialize_playlist_registry"
        )
        self.mock_initialize = self.patcher_initialize.start()

    def test_fetch_playlist_content(self):
        """Test fetching playlist content with mocked yt-dlp responses."""
        # Configure the cache mock to return stale data
        self.cache.get_refresh_metadata.return_value = (
            time.time() - 7200,
            "stale",
        )  # Old timestamp (2 hours ago), stale status

        # Configure mocks
        self.yt_dlp_utils.extract_playlist_content.return_value = [
            {"id": "vid1", "title": "Song 1", "uploader": "Artist 1", "duration": 180},
            {"id": "vid2", "title": "Song 2", "uploader": "Artist 2", "duration": 240},
        ]

        self.processor.extract_track_info.side_effect = [
            {
                "title": "Song 1",
                "artist": "Artist 1",
                "videoId": "vid1",
                "duration_seconds": 180,
            },
            {
                "title": "Song 2",
                "artist": "Artist 2",
                "videoId": "vid2",
                "duration_seconds": 240,
            },
        ]

        # Cache module requires a side effect to avoid modifying real files
        self.cache.set.return_value = None

        # Call the method
        result = self.fetcher.fetch_playlist_content(
            "PL123", "/playlists/test", limit=2
        )

        # Verify expected calls
        self.yt_dlp_utils.extract_playlist_content.assert_called_once()
        self.assertEqual(self.processor.extract_track_info.call_count, 2)

        # Verify results
        self.assertEqual(len(result), 2)
        self.assertIn("song_1.m4a", result[0].lower())
        self.assertIn("song_2.m4a", result[1].lower())

        # Verify cache operations
        self.assertEqual(self.cache.set.call_count, 1)  # Cache the result

    def test_readdir_playlist_by_type(self):
        """Test listing directory contents for a playlist type."""
        # Configure mock state
        self.fetcher.PLAYLIST_REGISTRY = [
            {
                "name": "liked_songs",
                "id": "LM",
                "type": "liked_songs",
                "path": "/liked_songs",
            },
            {
                "name": "my_playlist",
                "id": "PL123",
                "type": "playlist",
                "path": "/playlists/my_playlist",
            },
            {
                "name": "another_playlist",
                "id": "PL456",
                "type": "playlist",
                "path": "/playlists/another_playlist",
            },
            {
                "name": "my_album",
                "id": "MPREb_456",
                "type": "album",
                "path": "/albums/my_album",
            },
        ]

        # Configure cache mock to return directory listing
        # When there is a cached listing, the method returns those entries
        self.cache.get_directory_listing_with_attrs.return_value = {
            ".": {"is_dir": True},
            "..": {"is_dir": True},
            "my_playlist": {"is_dir": True},
            "another_playlist": {"is_dir": True},
        }

        # Test playlists directory
        result = self.fetcher.readdir_playlist_by_type("playlist", "/playlists")

        # The result will contain at least ".", "..", and should include the playlists
        self.assertIn(".", result)
        self.assertIn("..", result)
        self.assertIn("my_playlist", result)
        self.assertIn("another_playlist", result)

        # Configure cache mock for albums
        self.cache.get_directory_listing_with_attrs.return_value = {
            ".": {"is_dir": True},
            "..": {"is_dir": True},
            "my_album": {"is_dir": True},
        }

        # Test albums directory
        result = self.fetcher.readdir_playlist_by_type("album", "/albums")

        # The result will contain at least ".", "..", and should include the album
        self.assertIn(".", result)
        self.assertIn("..", result)
        self.assertIn("my_album", result)

    def test_get_playlist_id_from_name(self):
        """Test retrieving playlist ID from its name."""
        # Configure mock state
        self.fetcher.PLAYLIST_REGISTRY = [
            {
                "name": "liked_songs",
                "id": "LM",
                "type": "liked_songs",
                "path": "/liked_songs",
            },
            {
                "name": "my_playlist",
                "id": "PL123",
                "type": "playlist",
                "path": "/playlists/my_playlist",
            },
            {
                "name": "my_album",
                "id": "MPREb_456",
                "type": "album",
                "path": "/albums/my_album",
            },
        ]

        # Test finding by name
        playlist_id = self.fetcher.get_playlist_id_from_name("my_playlist")
        self.assertEqual(playlist_id, "PL123")

        # Test finding by name with type filter
        playlist_id = self.fetcher.get_playlist_id_from_name(
            "my_album", type_filter="album"
        )
        self.assertEqual(playlist_id, "MPREb_456")

        # Test not finding
        playlist_id = self.fetcher.get_playlist_id_from_name("nonexistent")
        self.assertIsNone(playlist_id)

        # Test type filter excludes results
        playlist_id = self.fetcher.get_playlist_id_from_name(
            "my_playlist", type_filter="album"
        )
        self.assertIsNone(playlist_id)


if __name__ == "__main__":
    unittest.main()
