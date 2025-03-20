#!/usr/bin/env python3

import unittest
from unittest.mock import Mock, patch, MagicMock, call
import logging
import os
import stat
import time
import threading
import tempfile
import shutil
from fuse import FuseOSError
from pathlib import Path
import errno

# Import the classes to test
from ytmusicfs.filesystem import YouTubeMusicFS
from ytmusicfs.cache import CacheManager
from ytmusicfs.thread_manager import ThreadManager
from ytmusicfs.file_handler import FileHandler
from ytmusicfs.content_fetcher import ContentFetcher


class TestYTMusicFSIntegration(unittest.TestCase):
    """Integration tests for YTMusicFS with real component interactions."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create test logger
        self.logger = logging.getLogger("test")
        self.logger.setLevel(logging.DEBUG)

        # Create temporary directory for cache
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir)

        # Mock the high-level components that would require external services
        with (
            patch("ytmusicfs.oauth_adapter.YTMusicOAuthAdapter") as mock_oauth_adapter,
            patch("ytmusicfs.client.YouTubeMusicClient") as mock_client,
            patch("ytmusicfs.yt_dlp_utils.YTDLPUtils") as mock_yt_dlp_utils,
        ):
            # Set up the mocks for external services
            self.mock_oauth_adapter = mock_oauth_adapter.return_value
            self.mock_client = mock_client.return_value
            self.mock_yt_dlp_utils = mock_yt_dlp_utils.return_value

            # Configure mock client responses
            self.mock_client.get_library_playlists.return_value = [
                {"title": "Test Playlist", "playlistId": "PL123"}
            ]
            self.mock_client.get_library_albums.return_value = [
                {"title": "Test Album", "browseId": "MPREb_456"}
            ]

            # Configure mock yt_dlp_utils responses
            self.mock_yt_dlp_utils.extract_playlist_content.return_value = [
                {
                    "id": "video123",
                    "title": "Test Song",
                    "uploader": "Test Artist",
                    "duration": 180,
                }
            ]
            self.mock_yt_dlp_utils.extract_stream_url_async.return_value = MagicMock()
            self.mock_yt_dlp_utils.extract_stream_url_async.return_value.result.return_value = {
                "status": "success",
                "stream_url": "https://example.com/stream.m4a",
                "duration": 180,
            }

            # Initialize thread manager (real implementation)
            self.thread_manager = ThreadManager(logger=self.logger)

            # Initialize the filesystem with real cache manager
            self.fs = YouTubeMusicFS(
                auth_file="dummy_auth.json",
                client_id="dummy_id",
                client_secret="dummy_secret",
                cache_dir=str(self.cache_dir),
            )

            # Keep reference to real components
            self.cache = self.fs.cache
            self.fetcher = self.fs.fetcher
            self.router = self.fs.router
            self.file_handler = self.fs.file_handler

            # Replace external service components with mocks
            self.fs.client = self.mock_client
            self.fs.yt_dlp_utils = self.mock_yt_dlp_utils
            self.fs.oauth_adapter = self.mock_oauth_adapter
            self.fetcher.client = self.mock_client
            self.fetcher.yt_dlp_utils = self.mock_yt_dlp_utils
            self.file_handler.yt_dlp_utils = self.mock_yt_dlp_utils

            # Initialize playlist registry in fetcher
            self.fetcher._initialize_playlist_registry(force_refresh=True)

    def tearDown(self):
        """Clean up after each test."""
        # Shutdown thread manager
        self.thread_manager.shutdown(wait=True)

        # Close the cache
        self.cache.close()

        # Remove temporary directory
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_playlist_navigation_workflow(self):
        """Test a realistic workflow: navigate to playlist and get file attributes."""
        # Step 1: Get root directory contents
        root_contents = self.fs.readdir("/", None)
        self.assertIn("playlists", root_contents)

        # Step 2: Get playlists directory contents
        playlists_contents = self.fs.readdir("/playlists", None)
        self.assertIn("test_playlist", playlists_contents)

        # Step 3: Get attributes of the playlist directory
        playlist_path = "/playlists/test_playlist"
        playlist_attrs = self.fs.getattr(playlist_path, None)
        self.assertTrue(stat.S_ISDIR(playlist_attrs["st_mode"]))

        # Step 4: List the contents of the playlist
        playlist_contents = self.fs.readdir(playlist_path, None)
        self.assertGreater(len(playlist_contents), 2)  # At least ".", ".." and a song

        # Step 5: Get attributes of a song file
        song_filename = None
        for entry in playlist_contents:
            if entry not in [".", ".."] and entry.endswith(".m4a"):
                song_filename = entry
                break

        self.assertIsNotNone(song_filename, "No song found in playlist")
        song_path = f"{playlist_path}/{song_filename}"
        song_attrs = self.fs.getattr(song_path, None)
        self.assertTrue(stat.S_ISREG(song_attrs["st_mode"]))  # Regular file

        # Verify cache contains expected entries
        self.assertTrue(self.cache.is_valid_path(playlist_path))
        self.assertTrue(self.cache.is_valid_path(song_path))

    def test_file_streaming_with_network_error(self):
        """Test file streaming workflow with a network error and retry."""
        # Prepare a file path and video ID
        file_path = "/playlists/test_playlist/test_artist_-_test_song.m4a"
        video_id = "video123"

        # Configure mock for video ID extraction
        self.fs.metadata_manager.get_video_id = Mock(return_value=video_id)

        # Configure requests.get to fail on first call and succeed on second
        mock_response_fail = MagicMock()
        mock_response_fail.status_code = 503
        mock_response_fail.__enter__.return_value = mock_response_fail

        mock_response_success = MagicMock()
        mock_response_success.status_code = 200
        mock_response_success.content = b"test_audio_data" * 100
        mock_response_success.__enter__.return_value = mock_response_success
        mock_response_success.iter_content.return_value = [
            mock_response_success.content
        ]

        # Step 1: Open the file
        with patch.object(self.file_handler, "_check_cached_audio", return_value=False):
            with patch(
                "ytmusicfs.file_handler.requests.get",
                side_effect=[mock_response_fail, mock_response_success],
            ):
                with patch("ytmusicfs.file_handler.time.sleep") as mock_sleep:
                    # Step 1: Open the file
                    file_handle = self.fs.open(file_path, os.O_RDONLY)

                    # Step 2: Read from the file, triggering the stream URL extraction
                    data = self.fs.read(file_path, 1024, 0, file_handle)

                    # Step 3: Release the file
                    self.fs.release(file_path, file_handle)

        # Verify the expected behavior
        self.assertEqual(len(data), len(mock_response_success.content))
        mock_sleep.assert_called_once()  # Should have performed a retry with sleep

    def test_concurrent_access(self):
        """Test concurrent access to the filesystem from multiple threads."""
        # Set up the test data
        playlists_path = "/playlists"
        playlist_path = "/playlists/test_playlist"

        # Create threads to access the filesystem concurrently
        success_count = [0]
        error_count = [0]
        lock = threading.Lock()

        def worker(worker_id):
            try:
                # Each thread will perform a sequence of operations
                if worker_id % 3 == 0:
                    # Thread Type 1: List directories
                    root_contents = self.fs.readdir("/", None)
                    playlists_contents = self.fs.readdir(playlists_path, None)
                    if "test_playlist" in playlists_contents:
                        playlist_contents = self.fs.readdir(playlist_path, None)
                elif worker_id % 3 == 1:
                    # Thread Type 2: Get attributes
                    root_attrs = self.fs.getattr("/", None)
                    playlists_attrs = self.fs.getattr(playlists_path, None)
                    playlist_attrs = self.fs.getattr(playlist_path, None)
                else:
                    # Thread Type 3: Validate paths
                    self.fs.router.validate_path("/")
                    self.fs.router.validate_path(playlists_path)
                    self.fs.router.validate_path(playlist_path)

                with lock:
                    success_count[0] += 1
            except Exception as e:
                with lock:
                    error_count[0] += 1
                    print(f"Thread {worker_id} error: {str(e)}")

        # Create and start threads
        threads = []
        for i in range(10):  # 10 concurrent workers
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Verify all threads completed successfully
        self.assertEqual(success_count[0], 10)
        self.assertEqual(error_count[0], 0)

    def test_cache_refresh_workflow(self):
        """Test the workflow of refreshing cached content."""
        # Get initial playlist data
        playlist_path = "/playlists/test_playlist"
        initial_playlist_contents = self.fs.readdir(playlist_path, None)

        # Change the mock data for the playlist content
        new_track = {
            "id": "video456",
            "title": "New Song",
            "uploader": "New Artist",
            "duration": 240,
        }
        self.mock_yt_dlp_utils.extract_playlist_content.return_value = [
            {
                "id": "video123",
                "title": "Test Song",
                "uploader": "Test Artist",
                "duration": 180,
            },
            new_track,
        ]

        # Force a refresh by setting stale metadata
        cache_key = f"{playlist_path}_processed"
        self.cache.set_refresh_metadata(cache_key, time.time() - 7200, "stale")

        # Get the refreshed playlist contents
        refreshed_playlist_contents = self.fs.readdir(playlist_path, None)

        # Verify the new track was added
        self.assertGreater(
            len(refreshed_playlist_contents), len(initial_playlist_contents)
        )
        new_song_filename = "new_artist_-_new_song.m4a"
        self.assertIn(new_song_filename, refreshed_playlist_contents)

        # Check that the track is now in the cache
        song_path = f"{playlist_path}/{new_song_filename}"
        self.assertTrue(self.cache.is_valid_path(song_path))
        song_attrs = self.fs.getattr(song_path, None)
        self.assertTrue(stat.S_ISREG(song_attrs["st_mode"]))


if __name__ == "__main__":
    unittest.main()
