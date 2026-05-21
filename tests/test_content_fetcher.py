#!/usr/bin/env python3

import logging
import time
import unittest
from unittest.mock import ANY, Mock, patch

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
        self.cache.is_track_unavailable.return_value = False
        self.cache.get_unavailable_video_ids.return_value = set()

        # Create the instance to test
        self.fetcher = ContentFetcher(
            client=self.client,
            processor=self.processor,
            cache=self.cache,
            logger=self.logger,
            yt_dlp_utils=self.yt_dlp_utils,
            browser="brave",
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
            {"title": "My Playlist", "playlistId": "PL123"}
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

        # Verify cache was updated
        self.cache.set_refresh_metadata.assert_called_once()

        # Restart the patch for other tests
        self.patcher_initialize = patch.object(
            ContentFetcher, "_initialize_playlist_registry"
        )
        self.mock_initialize = self.patcher_initialize.start()

    def test_initialize_playlist_registry_rebuilds_empty_registry_when_recent(self):
        """A fresh timestamp must not skip rebuilding an empty in-memory registry."""
        self.patcher_initialize.stop()

        self.client.get_library_playlists.return_value = [
            {"title": "My Playlist", "playlistId": "PL123"}
        ]
        self.client.get_library_albums.return_value = [
            {"title": "My Album", "browseId": "MPREb_456"}
        ]
        self.cache.get_refresh_metadata.return_value = (time.time(), "fresh")
        self.fetcher.PLAYLIST_REGISTRY = []

        self.fetcher._initialize_playlist_registry()

        self.assertEqual(
            self.fetcher.get_playlist_id_from_name(
                "my_playlist", type_filter="playlist"
            ),
            "PL123",
        )
        self.assertEqual(
            self.fetcher.get_playlist_id_from_name("my_album", type_filter="album"),
            "MPREb_456",
        )

        self.patcher_initialize = patch.object(
            ContentFetcher, "_initialize_playlist_registry"
        )
        self.mock_initialize = self.patcher_initialize.start()

    def test_initialize_playlist_registry_skips_entries_without_ids(self):
        """Malformed library entries should not poison route lookups with None IDs."""
        self.patcher_initialize.stop()

        self.client.get_library_playlists.return_value = [
            {"title": "Broken Playlist"},
            {"title": "Good Playlist", "playlistId": "PL123"},
        ]
        self.client.get_library_albums.return_value = [
            {"title": "Broken Album"},
            {"title": "Good Album", "browseId": "MPREb_456"},
        ]
        self.cache.get_refresh_metadata.return_value = (
            time.time() - 7200,
            "stale",
        )
        self.fetcher.PLAYLIST_REGISTRY = []

        self.fetcher._initialize_playlist_registry()

        self.assertIsNone(
            self.fetcher.get_playlist_id_from_name(
                "broken_playlist", type_filter="playlist"
            )
        )
        self.assertIsNone(
            self.fetcher.get_playlist_id_from_name("broken_album", type_filter="album")
        )
        self.assertEqual(
            self.fetcher.get_playlist_id_from_name(
                "good_playlist", type_filter="playlist"
            ),
            "PL123",
        )
        self.assertEqual(
            self.fetcher.get_playlist_id_from_name("good_album", type_filter="album"),
            "MPREb_456",
        )

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

    def test_fetch_playlist_content_missing_id_uses_cache(self):
        """Missing route IDs should degrade to cached entries instead of crashing."""
        cached_tracks = [{"filename": "cached_song.m4a", "videoId": "vid1"}]
        self.cache.get.return_value = cached_tracks

        result = self.fetcher.fetch_playlist_content(None, "/albums/broken", limit=2)

        self.assertEqual(result, ["cached_song.m4a"])
        self.yt_dlp_utils.extract_playlist_content.assert_not_called()

    def test_liked_songs_use_yt_dlp_with_total_count_guard(self):
        self.cache.get_refresh_metadata.return_value = (time.time() - 7200, "stale")
        self.yt_dlp_utils.extract_playlist_content.return_value = [
            {"id": "vid1", "title": "Song 1", "uploader": "Artist 1", "duration": 180}
        ]
        self.yt_dlp_utils.get_last_playlist_total_count.return_value = 1
        self.processor.extract_track_info.return_value = {
            "title": "Song 1",
            "artist": "Artist 1",
            "videoId": "vid1",
            "duration_seconds": 180,
        }

        result = self.fetcher.fetch_playlist_content("LM", "/liked_songs", limit=10000)

        self.assertEqual(result, ["artist_1_-_song_1.m4a"])
        self.client.get_liked_songs.assert_not_called()
        self.yt_dlp_utils.extract_playlist_content.assert_called_once_with(
            "LM", 10000, "brave"
        )

    def test_known_partial_fetch_stays_stale(self):
        self.cache.get_refresh_metadata.return_value = (time.time() - 7200, "stale")
        self.yt_dlp_utils.extract_playlist_content.return_value = [
            {"id": "vid1", "title": "Song 1", "uploader": "Artist 1", "duration": 180}
        ]
        self.yt_dlp_utils.get_last_playlist_total_count.return_value = 10
        self.processor.extract_track_info.return_value = {
            "title": "Song 1",
            "artist": "Artist 1",
            "videoId": "vid1",
            "duration_seconds": 180,
        }

        result = self.fetcher.fetch_playlist_content(
            "PL123", "/playlists/test", limit=10000
        )

        self.assertEqual(result, ["artist_1_-_song_1.m4a"])
        self.cache.set_refresh_metadata.assert_any_call(
            "/playlists/test_processed", ANY, "stale"
        )

    def test_fetch_below_retry_complete_ratio_stays_stale(self):
        fetched_tracks = [
            {
                "id": f"vid{i}",
                "title": f"Song {i}",
                "uploader": "Artist",
                "duration": 180,
            }
            for i in range(9)
        ]
        processed_tracks = [
            {
                "title": f"Song {i}",
                "artist": "Artist",
                "videoId": f"vid{i}",
                "duration_seconds": 180,
            }
            for i in range(9)
        ]
        self.cache.get_refresh_metadata.return_value = (time.time() - 7200, "stale")
        self.yt_dlp_utils.extract_playlist_content.return_value = fetched_tracks
        self.yt_dlp_utils.get_last_playlist_total_count.return_value = 10
        self.processor.extract_track_info.side_effect = processed_tracks

        self.fetcher.fetch_playlist_content("PL123", "/playlists/test", limit=10000)

        self.cache.set_refresh_metadata.assert_any_call(
            "/playlists/test_processed", ANY, "stale"
        )

    def test_known_partial_fetch_does_not_replace_larger_cache(self):
        cached_tracks = [
            {"filename": f"cached_{i}.m4a", "videoId": f"cached_{i}"} for i in range(5)
        ]
        self.cache.get.return_value = cached_tracks
        self.cache.get_refresh_metadata.return_value = (time.time() - 7200, "stale")
        self.yt_dlp_utils.extract_playlist_content.return_value = [
            {"id": "vid1", "title": "Song 1", "uploader": "Artist 1", "duration": 180}
        ]
        self.yt_dlp_utils.get_last_playlist_total_count.return_value = 10
        self.processor.extract_track_info.return_value = {
            "title": "Song 1",
            "artist": "Artist 1",
            "videoId": "vid1",
            "duration_seconds": 180,
        }

        result = self.fetcher.fetch_playlist_content(
            "PL123", "/playlists/test", limit=10000
        )

        self.assertEqual(result, [track["filename"] for track in cached_tracks])
        self.cache.set.assert_not_called()

    def test_known_partial_fetch_merges_with_smaller_cache(self):
        cached_tracks = [
            {"filename": "cached_1.m4a", "videoId": "cached_1"},
            {"filename": "old_dup.m4a", "videoId": "vid1"},
        ]
        fetched_tracks = [
            {"id": "vid1", "title": "Song 1", "uploader": "Artist 1", "duration": 180},
            {"id": "vid2", "title": "Song 2", "uploader": "Artist 2", "duration": 180},
            {"id": "vid3", "title": "Song 3", "uploader": "Artist 3", "duration": 180},
        ]
        processed_tracks = [
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
                "duration_seconds": 180,
            },
            {
                "title": "Song 3",
                "artist": "Artist 3",
                "videoId": "vid3",
                "duration_seconds": 180,
            },
        ]
        self.cache.get.return_value = cached_tracks
        self.cache.get_refresh_metadata.return_value = (time.time() - 7200, "stale")
        self.yt_dlp_utils.extract_playlist_content.return_value = fetched_tracks
        self.yt_dlp_utils.get_last_playlist_total_count.return_value = 10
        self.processor.extract_track_info.side_effect = processed_tracks

        result = self.fetcher.fetch_playlist_content(
            "PL123", "/playlists/test", limit=10000
        )

        self.assertEqual(
            result,
            [
                "artist_1_-_song_1.m4a",
                "artist_2_-_song_2.m4a",
                "artist_3_-_song_3.m4a",
                "cached_1.m4a",
            ],
        )
        cached_value = self.cache.set.call_args.args[1]
        self.assertEqual(
            [track["videoId"] for track in cached_value],
            [
                "vid1",
                "vid2",
                "vid3",
                "cached_1",
            ],
        )

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

    def test_readdir_playlist_by_type_filters_unavailable_tracks(self):
        self.cache.get_directory_listing_with_attrs.return_value = {
            "good.m4a": {"videoId": "good"},
            "bad.m4a": {"videoId": "bad"},
        }
        self.cache.get_unavailable_video_ids.return_value = {"bad"}

        # Use "playlist" type because cached listings are still served for
        # playlist/album roots; /liked_songs bypasses the stale listing cache
        # and goes through refresh_content instead.
        result = self.fetcher.readdir_playlist_by_type("playlist", "/playlists")

        self.assertEqual(result, [".", "..", "good.m4a"])
        self.cache.is_track_unavailable.assert_not_called()

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
