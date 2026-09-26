#!/usr/bin/env python3

import errno
import logging
import os
import shutil
import tempfile
import threading
import time
import unittest
from concurrent.futures import Future
from pathlib import Path
from unittest.mock import MagicMock, Mock, call, patch

import requests

from ytmusicfs.dependencies import FileHandlerDependencies
from ytmusicfs.file_handler import FileHandler
from ytmusicfs.models import StreamRequest


class TestFileHandler(unittest.TestCase):
    """Test case for FileHandler class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a temporary directory for testing
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir)

        # Mock dependencies
        self.thread_manager = Mock()
        self.thread_manager.create_lock.return_value = threading.Lock()

        self.cache = Mock()
        self.cache.get_unavailable_track.return_value = None
        self.cache.is_track_unavailable.return_value = False
        self.logger = logging.getLogger("test")
        self.update_file_size_callback = Mock()
        self.yt_dlp_utils = Mock()

        self.file_handler = FileHandler(
            FileHandlerDependencies(
                thread_manager=self.thread_manager,
                cache_dir=self.cache_dir,
                cache=self.cache,
                logger=self.logger,
                update_file_size=self.update_file_size_callback,
                yt_dlp=self.yt_dlp_utils,
                browser="brave",
            )
        )

        # Initialize class attributes
        self.file_handler.next_fh = 1
        self.file_handler.downloader = Mock()
        self.file_handler.futures = {}

        # Patch _check_cached_audio method to return False by default
        self.original_check_cached = self.file_handler._check_cached_audio
        self.original_cached_audio_format = self.file_handler._cached_audio_format
        self.file_handler._check_cached_audio = Mock(return_value=False)
        self.file_handler._cached_audio_format = Mock(return_value=None)

    def tearDown(self):
        """Clean up after each test method."""
        # Restore original methods
        self.file_handler._check_cached_audio = self.original_check_cached
        self.file_handler._cached_audio_format = self.original_cached_audio_format

        # Ensure all file handles are released
        self.file_handler.open_files = {}
        self.file_handler.path_to_fh = {}

        # Clean up temporary directory
        shutil.rmtree(self.temp_dir)

    def test_open_registers_new_handle_with_ready_state(self):
        """Test opening a file for streaming."""
        # Test data
        path = "/playlists/my_playlist/song.m4a"
        video_id = "dQw4w9WgXcQ"  # Example YouTube video ID
        expected_stream_url = None  # Initially None, stream URL fetched on demand

        # Call the method
        file_handle = self.file_handler.open(path, video_id)

        # Verify file handle was assigned
        self.assertEqual(file_handle, 1)  # First handle should be 1

        # Verify file handle was registered
        self.assertIn(file_handle, self.file_handler.open_files)
        self.assertEqual(
            self.file_handler.open_files[file_handle]["video_id"], video_id
        )
        self.assertEqual(
            self.file_handler.open_files[file_handle]["stream_url"], expected_stream_url
        )
        self.assertIsNone(self.file_handler.open_files[file_handle]["headers"])
        self.assertIsNone(self.file_handler.open_files[file_handle]["cookies"])
        self.assertEqual(self.file_handler.path_to_fh[path], file_handle)

        # Verify other required fields are present
        self.assertEqual(self.file_handler.open_files[file_handle]["status"], "ready")
        self.assertIsNone(self.file_handler.open_files[file_handle]["error"])
        self.assertTrue(
            isinstance(
                self.file_handler.open_files[file_handle]["initialized_event"],
                threading.Event,
            )
        )
        self.assertTrue(
            self.file_handler.open_files[file_handle]["initialized_event"].is_set()
        )

    def test_open_allocates_new_handle_when_path_already_open(self):
        """Test opening a file for a path that already has a handle, but expecting a new handle.

        Note: The implementation doesn't reuse handles even if the path exists.
        """
        # Set up mock data
        path = "/playlists/my_playlist/song.m4a"
        video_id = "dQw4w9WgXcQ"
        existing_file_handle = 42
        cache_path = os.path.join(self.temp_dir, "audio", f"{video_id}.m4a")

        # Create an event for the initialized_event field
        initialized_event = threading.Event()
        initialized_event.set()

        # Pre-register the file handle
        self.file_handler.path_to_fh[path] = existing_file_handle
        self.file_handler.open_files[existing_file_handle] = {
            "video_id": video_id,
            "stream_url": None,
            "offset": 0,
            "cache_path": cache_path,
            "headers": None,
            "cookies": None,
            "status": "ready",
            "error": None,
            "path": path,
            "initialized_event": initialized_event,
        }

        # Set the next_fh to a known value
        self.file_handler.next_fh = 99

        # Call the method
        file_handle = self.file_handler.open(path, video_id)

        # Verify we get a new file handle (the implementation doesn't actually reuse handles)
        self.assertEqual(file_handle, 99)  # Should get the new handle
        self.assertEqual(
            self.file_handler.next_fh, 100
        )  # Handle counter should increment

        # Verify the new file handle has correct info
        self.assertIn(file_handle, self.file_handler.open_files)
        self.assertEqual(
            self.file_handler.open_files[file_handle]["video_id"], video_id
        )
        self.assertEqual(self.file_handler.path_to_fh[path], file_handle)

    def test_open_rejects_cached_unavailable_video(self):
        path = "/playlists/my_playlist/song.m4a"
        video_id = "OFuzv2fm2PY"
        self.cache.get_unavailable_track.return_value = {
            "videoId": video_id,
            "path": path,
            "reason": "Video unavailable",
            "timestamp": time.time(),
        }

        with self.assertRaises(OSError) as context:
            self.file_handler.open(path, video_id)

        self.assertEqual(context.exception.errno, errno.ENOENT)

    def test_open_allows_unavailable_video_when_audio_is_cached(self):
        path = "/playlists/my_playlist/song.m4a"
        video_id = "OFuzv2fm2PY"
        self.cache.get_unavailable_track.return_value = {
            "videoId": video_id,
            "path": path,
            "reason": "Video unavailable",
            "timestamp": time.time(),
        }
        self.file_handler._cached_audio_format.return_value = "140"

        fh = self.file_handler.open(path, video_id)

        self.assertEqual(self.file_handler.open_files[fh]["stream_url"], "cached")
        self.assertEqual(self.file_handler.open_files[fh]["format_id"], "140")

    @patch("ytmusicfs.file_handler.http_get")
    def test_read_streams_initial_readahead_window(self, mock_requests_get):
        """Test reading content from a file."""
        # Set up mock data
        path = "/playlists/my_playlist/song.m4a"
        file_handle = 1
        video_id = "dQw4w9WgXcQ"
        stream_url = "https://example.com/stream.m4a"
        size = 1024
        offset = 0
        cache_path = os.path.join(self.temp_dir, "audio", f"{video_id}.m4a")

        # Mock response data
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"test_audio_data" * 64  # 960 bytes

        # Make the response mock support context manager protocol
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_response
        mock_context.__exit__.return_value = None
        mock_requests_get.return_value = mock_context

        # Mock response to support iter_content
        mock_response.iter_content.return_value = [mock_response.content]

        # Create an initialized event
        initialized_event = threading.Event()
        initialized_event.set()

        # Pre-register the file handle with all required fields
        self.file_handler.open_files[file_handle] = {
            "video_id": video_id,
            "stream_url": stream_url,
            "format_id": "141",
            "offset": 0,
            "cache_path": cache_path,
            "headers": None,
            "cookies": None,
            "status": "ready",
            "error": None,
            "path": path,
            "initialized_event": initialized_event,
        }

        # Mock the downloader progress to indicate file is not yet downloaded
        self.file_handler.downloader.get_progress.return_value = {
            "status": "downloading",
            "progress": 0,
        }

        # Patch Path.exists to return False so it tries to stream instead of reading from cache
        with (
            patch("pathlib.Path.exists", return_value=False),
            patch.object(
                self.file_handler, "_stream_content", return_value=mock_response.content
            ),
        ):
            data = self.file_handler.read(path, size, offset, file_handle)

            # Verify correct data was returned
            self.assertEqual(data, mock_response.content)

            # Verify _stream_content was called with the right arguments
            self.file_handler._stream_content.assert_called_once_with(
                StreamRequest(
                    url=stream_url,
                    offset=offset,
                    size=FileHandler.READAHEAD_INITIAL_BYTES,
                    path=path,
                )
            )

    def test_release_reports_recent_read_ranges(self):
        path = "/playlists/my_playlist/song.m4a"
        video_id = "abc123"
        fh = self.file_handler.open(path, video_id)
        self.file_handler.open_files[fh]["stream_url"] = "https://example.com/audio.m4a"

        with patch.object(self.file_handler, "_stream_content", return_value=b"audio"):
            self.file_handler.read(path, size=1024, offset=0, fh=fh)
            self.file_handler.read(path, size=2048, offset=4096, fh=fh)

        self.file_handler.release(path, fh)

        self.assertEqual(
            self.file_handler.get_recent_handles()[-1]["read_ranges"],
            [[0, 1024], [4096, 2048]],
        )
        self.assertEqual(
            self.file_handler.get_recent_handles()[-1]["requested_bytes"], 3072
        )

    def test_read_persists_sanitized_headers(self):
        """Ensure prepared headers (with Authorization) are cached for reuse."""

        path = "/playlists/my_playlist/song.m4a"
        video_id = "dQw4w9WgXcQ"
        fh = self.file_handler.open(path, video_id)

        future = Future()
        future.set_result(
            {
                "status": "success",
                "stream_url": "https://example.com/stream.m4a",
                "format_id": "141",
                "http_headers": {"Cookie": "SAPISID=abc123"},
                "cookies": {"SAPISID": "abc123", "foo": "bar"},
            }
        )
        self.file_handler.yt_dlp_utils.extract_stream_url_async.return_value = future
        with patch.object(
            self.file_handler, "_stream_content", return_value=b"payload"
        ) as mock_stream:
            data = self.file_handler.read(path, size=1024, offset=0, fh=fh)

        self.assertEqual(data, b"payload")
        file_info = self.file_handler.open_files[fh]
        self.assertIn("Authorization", file_info["headers"])
        self.assertEqual(file_info["cookies"], {"SAPISID": "abc123", "foo": "bar"})
        mock_stream.assert_called_once_with(
            StreamRequest(
                url="https://example.com/stream.m4a",
                offset=0,
                size=FileHandler.READAHEAD_INITIAL_BYTES,
                path=path,
                headers=file_info["headers"],
                cookies=file_info["cookies"],
            )
        )
        self.file_handler.downloader.download_file.assert_not_called()

    def test_read_starts_cache_download_after_playback_threshold(self):
        """Playback-sized reads should cache the full song after probing phase."""

        path = "/playlists/my_playlist/song.m4a"
        video_id = "abc123"
        fh = self.file_handler.open(path, video_id)

        future = Future()
        future.set_result(
            {
                "status": "success",
                "stream_url": "https://example.com/audio.m4a",
                "format_id": "141",
                "http_headers": {"User-Agent": "UnitTest"},
                "cookies": {"CONSENT": "YES+"},
            }
        )
        self.yt_dlp_utils.extract_stream_url_async.return_value = future

        first_read = b"a" * (FileHandler.CACHE_START_BYTES - 1)
        second_read = b"b"

        with patch.object(
            self.file_handler,
            "_stream_content",
            side_effect=[first_read, second_read],
        ) as mock_stream:
            self.assertEqual(
                self.file_handler.read(path, len(first_read), 0, fh), first_read
            )
            self.file_handler.downloader.download_file.assert_not_called()
            self.assertEqual(
                self.file_handler.read(path, 1, len(first_read), fh), second_read
            )

        file_info = self.file_handler.open_files[fh]
        self.assertEqual(mock_stream.call_count, 2)
        audio_path = self.cache_dir / "audio" / f"{video_id}.m4a"
        self.assertEqual(audio_path.read_bytes(), first_read + second_read)
        self.file_handler.downloader.download_file.assert_called_once()
        request = self.file_handler.downloader.download_file.call_args.args[0]
        self.assertEqual(request.video_id, video_id)
        self.assertEqual(request.stream_url, "https://example.com/audio.m4a")
        self.assertEqual(request.path, path)
        self.assertEqual(request.format_id, "141")
        self.assertEqual(request.headers, file_info["headers"])
        self.assertEqual(request.cookies, file_info["cookies"])

    def test_best_available_stream_writes_audio_and_range_cache(self):
        path = "/playlists/my_playlist/song.m4a"
        video_id = "abc123"
        fh = self.file_handler.open(path, video_id)

        future = Future()
        future.set_result(
            {
                "status": "success",
                "stream_url": "https://example.com/audio.m4a",
                "format_id": "141",
                "http_headers": {},
                "cookies": {},
            }
        )
        self.yt_dlp_utils.extract_stream_url_async.return_value = future

        with patch.object(
            self.file_handler,
            "_stream_content",
            return_value=b"a" * FileHandler.CACHE_START_BYTES,
        ):
            self.file_handler.read(path, FileHandler.CACHE_START_BYTES, 0, fh)

        self.file_handler.downloader.download_file.assert_called_once()
        self.assertTrue((self.cache_dir / "ranges" / "141" / video_id).exists())
        self.assertEqual(
            (self.cache_dir / "audio" / f"{video_id}.m4a").stat().st_size,
            FileHandler.CACHE_START_BYTES,
        )
        self.assertEqual(
            (self.cache_dir / "audio" / f"{video_id}.status").read_text(),
            "partial:141",
        )

    def test_read_uses_progressive_audio_cache_before_remote_stream(self):
        path = "/playlists/my_playlist/song.m4a"
        video_id = "abc123"
        fh = self.file_handler.open(path, video_id)
        cache_path = self.cache_dir / "audio" / f"{video_id}.m4a"
        cache_path.write_bytes(b"abcdef")
        cache_path.with_name(f"{video_id}.status").write_text("partial:141")
        self.file_handler.open_files[fh]["format_id"] = "141"

        data = self.file_handler.read(path, 3, 2, fh)

        self.assertEqual(data, b"cde")
        self.yt_dlp_utils.extract_stream_url_async.assert_not_called()

    def test_read_ignores_progressive_audio_cache_from_other_format(self):
        path = "/playlists/my_playlist/song.m4a"
        video_id = "abc123"
        fh = self.file_handler.open(path, video_id)
        cache_path = self.cache_dir / "audio" / f"{video_id}.m4a"
        cache_path.write_bytes(b"old-format")
        cache_path.with_name(f"{video_id}.status").write_text("partial:140")

        future = Future()
        future.set_result(
            {
                "status": "success",
                "stream_url": "https://example.com/audio.m4a",
                "format_id": "141",
                "http_headers": {},
                "cookies": {},
            }
        )
        self.yt_dlp_utils.extract_stream_url_async.return_value = future

        with patch.object(self.file_handler, "_stream_content", return_value=b"fresh"):
            data = self.file_handler.read(path, 5, 0, fh)

        self.assertEqual(data, b"fresh")

    def test_read_ignores_failed_progressive_audio_cache(self):
        path = "/playlists/my_playlist/song.m4a"
        video_id = "abc123"
        fh = self.file_handler.open(path, video_id)
        cache_path = self.cache_dir / "audio" / f"{video_id}.m4a"
        cache_path.write_bytes(b"bad-cache")
        cache_path.with_name(f"{video_id}.status").write_text("failed:141")

        future = Future()
        future.set_result(
            {
                "status": "success",
                "stream_url": "https://example.com/audio.m4a",
                "format_id": "141",
                "http_headers": {},
                "cookies": {},
            }
        )
        self.yt_dlp_utils.extract_stream_url_async.return_value = future

        with patch.object(
            self.file_handler, "_stream_content", return_value=b"fresh"
        ) as mock_stream:
            data = self.file_handler.read(path, 5, 0, fh)

        self.assertEqual(data, b"fresh")
        mock_stream.assert_called_once()

    def test_progressive_audio_cache_stops_writing_after_download_starts(self):
        path = "/playlists/my_playlist/song.m4a"
        video_id = "abc123"
        fh = self.file_handler.open(path, video_id)
        self.file_handler.open_files[fh]["stream_url"] = "https://example.com/audio.m4a"
        self.file_handler.open_files[fh]["format_id"] = "141"
        self.file_handler.open_files[fh]["cache_started"] = True
        cache_path = self.cache_dir / "audio" / f"{video_id}.m4a"
        cache_path.write_bytes(b"seed")

        with patch.object(self.file_handler, "_stream_content", return_value=b"next"):
            data = self.file_handler.read(path, 4, 4, fh)

        self.assertEqual(data, b"next")
        self.assertEqual(cache_path.read_bytes(), b"seed")

    def test_read_starts_cache_download_for_best_available_non_preferred_format(self):
        path = "/playlists/my_playlist/song.m4a"
        video_id = "abc123"
        fh = self.file_handler.open(path, video_id)

        future = Future()
        future.set_result(
            {
                "status": "success",
                "stream_url": "https://example.com/audio.m4a",
                "format_id": "140",
                "http_headers": {},
                "cookies": {},
            }
        )
        self.yt_dlp_utils.extract_stream_url_async.return_value = future

        with patch.object(
            self.file_handler,
            "_stream_content",
            return_value=b"a" * FileHandler.CACHE_START_BYTES,
        ):
            self.file_handler.read(path, FileHandler.CACHE_START_BYTES, 0, fh)

        self.file_handler.downloader.download_file.assert_called_once()
        request = self.file_handler.downloader.download_file.call_args.args[0]
        self.assertEqual(request.video_id, video_id)
        self.assertEqual(request.stream_url, "https://example.com/audio.m4a")
        self.assertEqual(request.path, path)
        self.assertEqual(request.format_id, "140")

    def test_precache_extracts_stream_and_downloads_now(self):
        path = "/playlists/my_playlist/song.m4a"
        video_id = "abc123"
        future = Future()
        future.set_result(
            {
                "status": "success",
                "stream_url": "https://example.com/audio.m4a",
                "format_id": "141",
                "http_headers": {"User-Agent": "UnitTest"},
                "cookies": {"CONSENT": "YES+"},
            }
        )
        self.yt_dlp_utils.extract_stream_url_async.return_value = future
        self.file_handler.downloader.download_file_now.return_value = True

        result = self.file_handler.precache(path, video_id)

        self.assertTrue(result)
        self.file_handler.downloader.download_file_now.assert_called_once()
        request = self.file_handler.downloader.download_file_now.call_args.args[0]
        self.assertEqual(request.video_id, video_id)
        self.assertEqual(request.stream_url, "https://example.com/audio.m4a")
        self.assertEqual(request.path, path)
        self.assertEqual(request.format_id, "141")
        self.assertEqual(request.headers["User-Agent"], "UnitTest")
        self.assertEqual(request.cookies, {"CONSENT": "YES+"})

    def test_precache_downloads_best_available_non_preferred_format(self):
        path = "/playlists/my_playlist/song.m4a"
        video_id = "abc123"
        future = Future()
        future.set_result(
            {
                "status": "success",
                "stream_url": "https://example.com/audio.m4a",
                "format_id": "140",
                "http_headers": {},
                "cookies": {},
            }
        )
        self.yt_dlp_utils.extract_stream_url_async.return_value = future

        result = self.file_handler.precache(path, video_id)

        self.assertTrue(result)
        self.file_handler.downloader.download_file_now.assert_called_once()
        request = self.file_handler.downloader.download_file_now.call_args.args[0]
        self.assertEqual(request.video_id, video_id)
        self.assertEqual(request.stream_url, "https://example.com/audio.m4a")
        self.assertEqual(request.path, path)
        self.assertEqual(request.format_id, "140")

    def test_cached_audio_accepts_any_complete_format_status(self):
        video_id = "abc123"
        audio_dir = self.cache_dir / "audio"
        audio_dir.mkdir(parents=True, exist_ok=True)
        (audio_dir / f"{video_id}.m4a").write_bytes(b"audio")
        status_path = audio_dir / f"{video_id}.status"

        status_path.write_text("complete")
        self.assertIsNone(self.original_cached_audio_format(video_id))

        status_path.write_text("complete:140")
        self.assertEqual(self.original_cached_audio_format(video_id), "140")

        status_path.write_text("complete:141")
        self.assertEqual(self.original_cached_audio_format(video_id), "141")

    def test_high_offset_uncached_read_skips_yt_dlp(self):
        path = "/playlists/my_playlist/song.m4a"
        video_id = "abc123"
        fh = self.file_handler.open(path, video_id)
        self.file_handler.record_stat_callback = Mock()
        self.file_handler.get_file_size_callback = Mock(return_value=2 * 1024 * 1024)

        result = self.file_handler.read(
            path,
            size=4096,
            offset=(2 * 1024 * 1024) - 4096,
            fh=fh,
        )

        self.assertEqual(result, b"")
        self.yt_dlp_utils.extract_stream_url_async.assert_not_called()
        self.file_handler.record_stat_callback.assert_called_once_with(
            "probe_eof_skips"
        )

    def test_high_offset_uncached_read_extracts_when_not_tail_probe(self):
        path = "/playlists/my_playlist/song.m4a"
        video_id = "abc123"
        fh = self.file_handler.open(path, video_id)
        self.file_handler.get_file_size_callback = Mock(return_value=8 * 1024 * 1024)

        future = Future()
        future.set_result(
            {
                "status": "success",
                "stream_url": "https://example.com/audio.m4a",
                "format_id": "141",
                "http_headers": {},
                "cookies": {},
            }
        )
        self.yt_dlp_utils.extract_stream_url_async.return_value = future

        with patch.object(
            self.file_handler, "_stream_content", return_value=b"payload"
        ):
            result = self.file_handler.read(
                path,
                size=4096,
                offset=2 * 1024 * 1024,
                fh=fh,
            )

        self.assertEqual(result, b"payload")
        self.yt_dlp_utils.extract_stream_url_async.assert_called_once_with(
            video_id, "brave"
        )

    def test_offset_zero_uncached_read_extracts_stream(self):
        path = "/playlists/my_playlist/song.m4a"
        video_id = "abc123"
        fh = self.file_handler.open(path, video_id)

        future = Future()
        future.set_result(
            {
                "status": "success",
                "stream_url": "https://example.com/audio.m4a",
                "format_id": "141",
                "http_headers": {},
                "cookies": {},
            }
        )
        self.yt_dlp_utils.extract_stream_url_async.return_value = future

        with patch.object(
            self.file_handler, "_stream_content", return_value=b"payload"
        ):
            result = self.file_handler.read(path, size=1024, offset=0, fh=fh)

        self.assertEqual(result, b"payload")
        self.yt_dlp_utils.extract_stream_url_async.assert_called_once_with(
            video_id, "brave"
        )

    def test_reopened_uncached_file_reuses_stream_info(self):
        path = "/playlists/my_playlist/song.m4a"
        video_id = "abc123"
        first_fh = self.file_handler.open(path, video_id)

        future = Future()
        future.set_result(
            {
                "status": "success",
                "stream_url": "https://example.com/audio.m4a",
                "format_id": "141",
                "http_headers": {"User-Agent": "UnitTest"},
                "cookies": {"CONSENT": "YES+"},
            }
        )
        self.yt_dlp_utils.extract_stream_url_async.return_value = future

        with patch.object(
            self.file_handler, "_stream_content", return_value=b"payload"
        ):
            self.assertEqual(
                self.file_handler.read(path, size=1024, offset=0, fh=first_fh),
                b"payload",
            )

        self.file_handler.release(path, first_fh)
        second_fh = self.file_handler.open(path, video_id)
        self.file_handler.record_stat_callback = Mock()

        with patch.object(
            self.file_handler, "_stream_content", return_value=b"again"
        ) as mock_stream:
            self.assertEqual(
                self.file_handler.read(path, size=1024, offset=0, fh=second_fh),
                b"again",
            )

        self.yt_dlp_utils.extract_stream_url_async.assert_called_once_with(
            video_id, "brave"
        )
        file_info = self.file_handler.open_files[second_fh]
        mock_stream.assert_called_once_with(
            StreamRequest(
                url="https://example.com/audio.m4a",
                offset=0,
                size=FileHandler.READAHEAD_INITIAL_BYTES,
                path=path,
                headers=file_info["headers"],
                cookies=file_info["cookies"],
            )
        )
        self.file_handler.record_stat_callback.assert_any_call("stream_info_cache_hits")

    def test_reopened_probe_read_uses_format_keyed_cache_before_stream_extraction(
        self,
    ):
        path = "/liked_songs/song.m4a"
        video_id = "abc123"
        first_fh = self.file_handler.open(path, video_id)
        self.file_handler.open_files[first_fh][
            "stream_url"
        ] = "https://example.com/audio.m4a"
        self.file_handler.open_files[first_fh]["format_id"] = "141"

        with patch.object(self.file_handler, "_stream_content", return_value=b"prefix"):
            self.assertEqual(
                self.file_handler.read(path, size=6, offset=0, fh=first_fh),
                b"prefix",
            )

        self.file_handler.release(path, first_fh)
        self.file_handler.stream_info_cache.clear()
        self.file_handler.record_stat_callback = Mock()
        second_fh = self.file_handler.open(path, video_id)

        self.assertEqual(
            self.file_handler.read(path, size=6, offset=0, fh=second_fh), b"prefix"
        )
        self.yt_dlp_utils.extract_stream_url_async.assert_not_called()
        self.file_handler.record_stat_callback.assert_called_once_with(
            "range_cache_hits"
        )

    def test_range_cache_serves_short_eof_read(self):
        path = "/liked_songs/song.m4a"
        video_id = "abc123"
        first_fh = self.file_handler.open(path, video_id)
        self.file_handler.open_files[first_fh][
            "stream_url"
        ] = "https://example.com/audio.m4a"
        self.file_handler.open_files[first_fh]["format_id"] = "141"

        with patch.object(self.file_handler, "_stream_content", return_value=b"end"):
            self.assertEqual(
                self.file_handler.read(path, size=6, offset=100, fh=first_fh), b"end"
            )

        self.file_handler.release(path, first_fh)
        self.file_handler.get_file_size_callback = Mock(return_value=103)
        second_fh = self.file_handler.open(path, video_id)

        self.assertEqual(
            self.file_handler.read(path, size=6, offset=100, fh=second_fh), b"end"
        )
        self.yt_dlp_utils.extract_stream_url_async.assert_not_called()

    def test_timeout_error_message_is_not_empty(self):
        path = "/playlists/my_playlist/song.m4a"
        video_id = "abc123"
        fh = self.file_handler.open(path, video_id)

        future = Mock()
        future.result.side_effect = TimeoutError()
        self.yt_dlp_utils.extract_stream_url_async.return_value = future

        with self.assertRaises(OSError) as context:
            self.file_handler.read(path, size=1024, offset=0, fh=fh)

        self.assertEqual(context.exception.errno, errno.EIO)
        self.assertEqual(context.exception.strerror, "TimeoutError")

    def test_read_maps_unavailable_video_to_not_found(self):
        """Unavailable YouTube videos should fail as missing files."""

        path = "/playlists/my_playlist/song.m4a"
        video_id = "OFuzv2fm2PY"
        fh = self.file_handler.open(path, video_id)

        future = Future()
        future.set_result(
            {
                "status": "error",
                "error": "ERROR: [youtube] OFuzv2fm2PY: Video unavailable. This video is not available",
            }
        )
        self.yt_dlp_utils.extract_stream_url_async.return_value = future

        with self.assertRaises(OSError) as context:
            self.file_handler.read(path, size=1024, offset=0, fh=fh)

        self.assertEqual(context.exception.errno, errno.ENOENT)
        self.cache.mark_unavailable_track.assert_called_once()
        self.file_handler.downloader.download_file.assert_not_called()

    def test_read_skips_yt_dlp_for_cached_unavailable_video(self):
        """Unavailable cache should make repeated probes cheap."""

        path = "/playlists/my_playlist/song.m4a"
        video_id = "OFuzv2fm2PY"
        fh = self.file_handler.open(path, video_id)
        self.cache.get_unavailable_track.return_value = {
            "videoId": video_id,
            "path": path,
            "reason": "Video unavailable",
            "timestamp": time.time(),
        }

        with self.assertRaises(OSError) as context:
            self.file_handler.read(path, size=1024, offset=0, fh=fh)

        self.assertEqual(context.exception.errno, errno.ENOENT)
        self.yt_dlp_utils.extract_stream_url_async.assert_not_called()
        self.file_handler.downloader.download_file.assert_not_called()

    @patch("ytmusicfs.file_handler.http_get")
    def test_read_at_offset_streams_from_that_offset(self, mock_requests_get):
        """Test reading content from a file with an offset."""
        # Set up mock data
        path = "/playlists/my_playlist/song.m4a"
        file_handle = 1
        video_id = "dQw4w9WgXcQ"
        stream_url = "https://example.com/stream.m4a"
        size = 1024
        offset = 2048  # Start partway through the file
        cache_path = os.path.join(self.temp_dir, "audio", f"{video_id}.m4a")

        # Mock response data
        mock_response = Mock()
        mock_response.status_code = 206  # Partial content
        mock_response.content = b"partial_audio_data" * 64  # About 1088 bytes

        # Make the response mock support context manager protocol
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_response
        mock_context.__exit__.return_value = None
        mock_requests_get.return_value = mock_context

        # Mock response to support iter_content
        mock_response.iter_content.return_value = [mock_response.content]

        # Create an initialized event
        initialized_event = threading.Event()
        initialized_event.set()

        # Pre-register the file handle with all required fields
        self.file_handler.open_files[file_handle] = {
            "video_id": video_id,
            "stream_url": stream_url,
            "format_id": "141",
            "offset": 0,
            "cache_path": cache_path,
            "headers": None,
            "cookies": None,
            "status": "ready",
            "error": None,
            "path": path,
            "initialized_event": initialized_event,
        }

        # Mock the downloader progress to indicate file is not yet downloaded
        self.file_handler.downloader.get_progress.return_value = {
            "status": "downloading",
            "progress": 0,
        }

        # Patch Path.exists to return False so it tries to stream instead of reading from cache
        with (
            patch("pathlib.Path.exists", return_value=False),
            patch.object(
                self.file_handler, "_stream_content", return_value=mock_response.content
            ),
        ):
            data = self.file_handler.read(path, size, offset, file_handle)

            # Verify correct data was returned
            self.assertEqual(data, mock_response.content[:size])

            # Verify _stream_content was called with the right arguments
            self.file_handler._stream_content.assert_called_once_with(
                StreamRequest(
                    url=stream_url,
                    offset=offset,
                    size=FileHandler.READAHEAD_INITIAL_BYTES,
                    path=path,
                )
            )

    def test_read_sanitizes_headers_and_cookies(self):
        """Stream metadata from yt-dlp should be normalised before use."""

        path = "/playlists/my_playlist/song.m4a"
        video_id = "abc123"
        fh = self.file_handler.open(path, video_id)

        future = Future()
        future.set_result(
            {
                "status": "success",
                "stream_url": "https://example.com/audio.m4a",
                "format_id": "141",
                "http_headers": {
                    "User-Agent": "UnitTest",
                    "Host": "music.youtube.com",
                    "X-Goog-AuthUser": 0,
                    "X-YouTube-Identity-Token": None,
                },
                "cookies": {"CONSENT": "YES+", "BAD": None},
            }
        )
        self.yt_dlp_utils.extract_stream_url_async.return_value = future

        with patch.object(
            self.file_handler, "_stream_content", return_value=b"payload"
        ) as mock_stream:
            result = self.file_handler.read(path, size=1024, offset=0, fh=fh)

        self.assertEqual(result, b"payload")

        request = mock_stream.call_args.args[0]
        headers = request.headers
        cookies = request.cookies

        self.assertNotIn("Host", headers)
        self.assertEqual(headers["User-Agent"], "UnitTest")
        self.assertEqual(headers["X-Goog-AuthUser"], "0")
        self.assertNotIn("X-YouTube-Identity-Token", headers)

        self.assertEqual(cookies, {"CONSENT": "YES+"})

        self.file_handler.downloader.download_file.assert_not_called()

    @patch("ytmusicfs.file_handler.http_get")
    def test_stream_content_preserves_lowercase_user_agent(self, mock_requests_get):
        """Lowercase user-agent headers must not be replaced by defaults."""

        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.iter_content.return_value = [b"a" * 32]

        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_response
        mock_context.__exit__.return_value = None
        mock_requests_get.return_value = mock_context

        data = self.file_handler._stream_content(
            StreamRequest(
                url="https://example.com/audio.m4a",
                offset=0,
                size=16,
                headers={"user-agent": "Real UA"},
            )
        )

        self.assertEqual(data, b"a" * 16)

        mock_requests_get.assert_called_once()
        sent_headers = mock_requests_get.call_args.kwargs["headers"]

        self.assertIn("user-agent", sent_headers)
        self.assertEqual(sent_headers["user-agent"], "Real UA")
        self.assertNotIn("User-Agent", sent_headers)

    @patch("ytmusicfs.file_handler.http_get")
    def test_stream_content_treats_416_as_eof(self, mock_requests_get):
        mock_response = Mock()
        mock_response.status_code = 416

        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_response
        mock_context.__exit__.return_value = None
        mock_requests_get.return_value = mock_context
        self.file_handler.record_stat_callback = Mock()

        data = self.file_handler._stream_content(
            StreamRequest(
                url="https://example.com/audio.m4a",
                offset=FileHandler.PROBE_EOF_OFFSET,
                size=4096,
                path="/liked_songs/song.m4a",
                retries=1,
            )
        )

        self.assertEqual(data, b"")
        self.file_handler.record_stat_callback.assert_called_once_with("range_416_eof")

    @patch("ytmusicfs.file_handler.http_get")
    def test_stream_content_updates_size_from_content_range(self, mock_requests_get):
        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.headers = {"Content-Range": "bytes 0-4095/12345"}
        mock_response.iter_content.return_value = [b"a" * 4096]

        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_response
        mock_context.__exit__.return_value = None
        mock_requests_get.return_value = mock_context

        data = self.file_handler._stream_content(
            StreamRequest(
                url="https://example.com/audio.m4a",
                offset=0,
                size=4096,
                path="/liked_songs/song.m4a",
                retries=1,
            )
        )

        self.assertEqual(data, b"a" * 4096)
        self.update_file_size_callback.assert_called_once_with(
            "/liked_songs/song.m4a", 12345
        )

    def test_release_removes_handle_and_path_mapping(self):
        """Test releasing (closing) a file handle."""
        # Set up mock data
        path = "/playlists/my_playlist/song.m4a"
        file_handle = 1
        video_id = "dQw4w9WgXcQ"
        stream_url = "https://example.com/stream.m4a"
        cache_path = os.path.join(self.temp_dir, "audio", f"{video_id}.m4a")

        # Create an initialized event
        initialized_event = threading.Event()
        initialized_event.set()

        # Pre-register the file handle
        self.file_handler.path_to_fh[path] = file_handle
        self.file_handler.open_files[file_handle] = {
            "video_id": video_id,
            "stream_url": stream_url,
            "offset": 1024,
            "cache_path": cache_path,
            "headers": None,
            "cookies": None,
            "status": "ready",
            "error": None,
            "path": path,
            "initialized_event": initialized_event,
        }

        # Call the method
        result = self.file_handler.release(path, file_handle)

        # Verify correct result (should be 0 for success)
        self.assertEqual(result, 0)

        # Verify file handle was removed
        self.assertNotIn(file_handle, self.file_handler.open_files)
        self.assertNotIn(path, self.file_handler.path_to_fh)

    @staticmethod
    def _stream_response(status_code, chunks=()):
        response = MagicMock()
        response.status_code = status_code
        response.headers = {}
        response.iter_content.return_value = list(chunks)
        context = MagicMock()
        context.__enter__.return_value = response
        context.__exit__.return_value = None
        return context

    @patch("ytmusicfs.file_handler.http_get")
    @patch("ytmusicfs.file_handler.time.sleep")
    def test_stream_content_retries_request_errors_then_succeeds(
        self, mock_sleep, mock_http_get
    ):
        mock_http_get.side_effect = [
            requests.exceptions.ConnectionError("reset"),
            self._stream_response(206, [b"audio-bytes"]),
        ]

        data = self.file_handler._stream_content(
            StreamRequest("https://example.com/stream.m4a", 0, 5, retries=2)
        )

        self.assertEqual(data, b"audio")
        self.assertEqual(mock_http_get.call_count, 2)
        mock_sleep.assert_called_once_with(1.0)

    @patch("ytmusicfs.file_handler.http_get")
    @patch("ytmusicfs.file_handler.time.sleep")
    def test_stream_content_raises_eio_after_retries_exhausted(
        self, mock_sleep, mock_http_get
    ):
        mock_http_get.side_effect = requests.exceptions.Timeout("slow")

        with self.assertRaises(OSError) as context:
            self.file_handler._stream_content(
                StreamRequest("https://example.com/stream.m4a", 0, 1024, retries=3)
            )

        self.assertEqual(context.exception.errno, errno.EIO)
        self.assertIn("after 3 attempts", str(context.exception))
        self.assertEqual(mock_http_get.call_count, 3)
        mock_sleep.assert_has_calls([call(1.0), call(2.0)])

    @patch("ytmusicfs.file_handler.http_get")
    @patch("ytmusicfs.file_handler.time.sleep")
    def test_stream_content_does_not_retry_http_error_status(
        self, mock_sleep, mock_http_get
    ):
        mock_http_get.return_value = self._stream_response(503)

        with self.assertRaises(OSError) as context:
            self.file_handler._stream_content(
                StreamRequest("https://example.com/stream.m4a", 0, 1024, retries=3)
            )

        self.assertEqual(context.exception.errno, errno.EIO)
        self.assertIn("HTTP 503", str(context.exception))
        mock_http_get.assert_called_once()
        mock_sleep.assert_not_called()

    def test_read_response_bytes_stops_iterating_once_size_is_reached(self):
        consumed = []

        def chunks(chunk_size):
            for chunk in (b"ab", b"cd", b"ef"):
                consumed.append(chunk)
                yield chunk

        response = Mock()
        response.iter_content.side_effect = chunks

        self.assertEqual(FileHandler._read_response_bytes(response, 3, 2), b"abc")
        self.assertEqual(consumed, [b"ab", b"cd"])

    def test_read_response_bytes_returns_short_data_when_stream_ends(self):
        response = Mock()
        response.iter_content.return_value = [b"ab"]

        self.assertEqual(FileHandler._read_response_bytes(response, 10, 2), b"ab")

    @patch("ytmusicfs.file_handler.http_get")
    def test_stream_content_merges_cookie_header(self, mock_requests_get):
        """Cookies present only in the header should be preserved for streaming."""

        stream_url = "https://example.com/audio.m4a"
        offset = 0
        size = 4096
        chunk = b"a" * (size + 100)

        mock_response = MagicMock()
        mock_response.status_code = 206
        mock_response.iter_content.return_value = [chunk]

        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_response
        mock_context.__exit__.return_value = None
        mock_requests_get.return_value = mock_context

        data = self.file_handler._stream_content(
            StreamRequest(
                url=stream_url,
                offset=offset,
                size=size,
                headers={
                    "Cookie": "SID=headerSid; HSID=headerHsid",
                    "User-Agent": "UnitTest",
                },
                cookies={"SID": "mappingSid", "CONSENT": "YES+"},
                retries=1,
            )
        )

        self.assertEqual(data, chunk[:size])

        called_kwargs = mock_requests_get.call_args.kwargs
        self.assertNotIn("Cookie", called_kwargs["headers"])
        self.assertEqual(called_kwargs["headers"]["User-Agent"], "UnitTest")
        self.assertEqual(
            called_kwargs["cookies"],
            {"SID": "mappingSid", "HSID": "headerHsid", "CONSENT": "YES+"},
        )

    def test_read_serves_complete_cached_file_from_disk(self):
        """Test reading content from a completely cached file."""
        # Set up mock data
        path = "/playlists/my_playlist/song.m4a"
        file_handle = 1
        video_id = "dQw4w9WgXcQ"
        size = 1024
        offset = 0
        cache_path = os.path.join(self.temp_dir, "audio", f"{video_id}.m4a")

        # Create a test file with content in the cache directory
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        test_content = b"cached_audio_data" * 100
        with open(cache_path, "wb") as f:
            f.write(test_content)

        # Create an initialized event
        initialized_event = threading.Event()
        initialized_event.set()

        # Pre-register the file handle with "cached" stream URL
        self.file_handler.open_files = {}  # Clear any existing mocks
        self.file_handler.open_files[file_handle] = {
            "video_id": video_id,
            "stream_url": "cached",  # This indicates a cached file
            "offset": 0,
            "cache_path": cache_path,
            "headers": None,
            "cookies": None,
            "status": "ready",
            "error": None,
            "path": path,
            "initialized_event": initialized_event,
        }
        self.file_handler.path_to_fh = {}
        self.file_handler.path_to_fh[path] = file_handle

        # Mock downloader.get_progress to return a complete status
        self.file_handler.downloader.get_progress.return_value = {
            "status": "complete",
            "progress": 100,
        }

        # Configure check_cached_audio to return True
        self.file_handler._check_cached_audio.return_value = True

        # Call the method
        data = self.file_handler.read(path, size, offset, file_handle)

        # Verify correct data from cache was returned
        self.assertEqual(data, test_content[:size])

        # Clean up the test file
        os.remove(cache_path)

    def test_read_switches_to_cache_when_download_completed(self):
        """An open streaming handle should use the cache once the download finishes."""
        path = "/playlists/my_playlist/song.m4a"
        file_handle = 1
        video_id = "dQw4w9WgXcQ"
        cache_path = os.path.join(self.temp_dir, "audio", f"{video_id}.m4a")
        test_content = b"cached audio data"

        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, "wb") as f:
            f.write(test_content)

        initialized_event = threading.Event()
        initialized_event.set()
        self.file_handler.open_files[file_handle] = {
            "video_id": video_id,
            "stream_url": "https://example.com/audio.m4a",
            "cache_path": cache_path,
            "headers": None,
            "cookies": None,
            "status": "ready",
            "error": None,
            "path": path,
            "initialized_event": initialized_event,
        }
        self.file_handler.downloader.get_progress.return_value = None
        self.file_handler._check_cached_audio.return_value = True

        data = self.file_handler.read(path, len(test_content), 0, file_handle)

        self.assertEqual(data, test_content)
        self.assertEqual(
            self.file_handler.open_files[file_handle]["stream_url"], "cached"
        )

    def test_read_auto_repair_retries_with_new_video_id(self):
        """Auto-repair should retry stream extraction with replacement video_id."""

        path = "/liked_songs/Artist - Song.m4a"
        video_id = "dead123"
        new_video_id = "new456"
        fh = self.file_handler.open(path, video_id)

        # First call fails, second succeeds
        error_future = Future()
        error_future.set_result(
            {
                "status": "error",
                "error": "ERROR: [youtube] dead123: Video unavailable",
            }
        )
        success_future = Future()
        success_future.set_result(
            {
                "status": "success",
                "stream_url": "https://example.com/new_stream.m4a",
                "format_id": "141",
            }
        )

        self.yt_dlp_utils.extract_stream_url_async.side_effect = [
            error_future,
            success_future,
        ]

        callback = Mock(return_value=new_video_id)
        self.file_handler.on_stream_unavailable = callback

        with patch.object(
            self.file_handler, "_stream_content", return_value=b"repaired_audio"
        ):
            data = self.file_handler.read(path, size=1024, offset=0, fh=fh)

        self.assertEqual(data, b"repaired_audio")
        callback.assert_called_once_with(video_id, path)
        self.assertEqual(self.file_handler.open_files[fh]["video_id"], new_video_id)

    def test_read_auto_repair_raises_when_callback_returns_none(self):
        """Auto-repair should raise ENOENT when callback returns no replacement."""

        path = "/liked_songs/Artist - Song.m4a"
        video_id = "dead123"
        fh = self.file_handler.open(path, video_id)

        future = Future()
        future.set_result(
            {
                "status": "error",
                "error": "ERROR: [youtube] dead123: Video unavailable",
            }
        )
        self.yt_dlp_utils.extract_stream_url_async.return_value = future

        callback = Mock(return_value=None)
        self.file_handler.on_stream_unavailable = callback

        with self.assertRaises(OSError) as context:
            self.file_handler.read(path, size=1024, offset=0, fh=fh)

        self.assertEqual(context.exception.errno, errno.ENOENT)

    def test_read_auto_repair_raises_when_retry_fails(self):
        """Auto-repair should raise when retry stream extraction also fails."""

        path = "/liked_songs/Artist - Song.m4a"
        video_id = "dead123"
        new_video_id = "new456"
        fh = self.file_handler.open(path, video_id)

        error_future = Future()
        error_future.set_result(
            {
                "status": "error",
                "error": "ERROR: [youtube] new456: Video unavailable",
            }
        )

        self.yt_dlp_utils.extract_stream_url_async.return_value = error_future

        callback = Mock(return_value=new_video_id)
        self.file_handler.on_stream_unavailable = callback

        with self.assertRaises(OSError) as context:
            self.file_handler.read(path, size=1024, offset=0, fh=fh)

        self.assertEqual(context.exception.errno, errno.ENOENT)


class _RealFileHandlerMixin:
    """Build a FileHandler whose cache checks run against a temp directory."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir)
        thread_manager = Mock()
        thread_manager.create_lock.side_effect = threading.RLock
        self.cache = Mock()
        self.cache.get_unavailable_track.return_value = None
        self.cache.is_track_unavailable.return_value = False
        self.yt_dlp = Mock()
        self.update_size = Mock()
        self.record_stat = Mock()
        self.file_sizes: dict[str, int] = {}
        self.handler = FileHandler(
            FileHandlerDependencies(
                thread_manager=thread_manager,
                cache_dir=self.cache_dir,
                cache=self.cache,
                logger=logging.getLogger("test"),
                update_file_size=self.update_size,
                yt_dlp=self.yt_dlp,
                browser="brave",
                record_stat=self.record_stat,
                get_file_size=self.file_sizes.get,
            )
        )
        self.handler.downloader = Mock()
        self.handler.downloader.get_progress.return_value = None
        self.audio_dir = self.cache_dir / "audio"
        self.audio_dir.mkdir(parents=True)

    def tearDown(self):
        for fh in list(self.handler.open_files):
            self.handler.release("", fh)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _stream_result(self, **overrides):
        result = {
            "status": "success",
            "stream_url": "https://example.com/new.m4a",
            "format_id": "141",
            "http_headers": {},
            "cookies": None,
        }
        result.update(overrides)
        future = Future()
        future.set_result(result)
        return future


class TestFileHandlerStreamInfo(_RealFileHandlerMixin, unittest.TestCase):
    """Stream extraction, auth preparation and precaching."""

    def test_summarize_auth_labels_authorization_scheme(self):
        self.assertEqual(
            FileHandler._summarize_auth({"Authorization": "Bearer abc"}, None),
            ("Bearer", False, []),
        )
        self.assertEqual(
            FileHandler._summarize_auth(
                {"Authorization": "SAPISIDHASH 1_x"}, {"SAPISID": "s", "A": "b"}
            ),
            ("SAPISIDHASH", True, ["A", "SAPISID"]),
        )
        self.assertEqual(FileHandler._summarize_auth(None, {}), ("none", False, []))

    def test_normalize_cookies_accepts_cookie_objects_and_dicts(self):
        jar_cookie = Mock()
        jar_cookie.name = "SID"
        jar_cookie.value = 123
        cookies = [
            jar_cookie,
            {"name": "HSID", "value": "h"},
            {"key": "CONSENT", "value": "YES+"},
            {"name": "EMPTY", "value": None},
            "ignored",
        ]

        self.assertEqual(
            FileHandler._normalize_cookies(cookies),
            {"SID": "123", "HSID": "h", "CONSENT": "YES+"},
        )

    def test_normalize_cookies_returns_none_for_unusable_input(self):
        self.assertIsNone(FileHandler._normalize_cookies("SID=abc"))
        self.assertIsNone(FileHandler._normalize_cookies([]))
        self.assertEqual(FileHandler._normalize_cookies({"A": "b"}), {"A": "b"})

    def test_get_stream_info_reuses_in_flight_future(self):
        self.handler.futures["vid"] = self._stream_result()

        result = self.handler._get_stream_info("vid")

        self.assertEqual(result["format_id"], "141")
        self.yt_dlp.extract_stream_url_async.assert_not_called()
        self.assertNotIn("vid", self.handler.futures)

    def test_get_stream_info_rejects_non_dict_result(self):
        future = Future()
        future.set_result("not a dict")
        self.yt_dlp.extract_stream_url_async.return_value = future

        with self.assertRaises(OSError) as context:
            self.handler._get_stream_info("vid")

        self.assertEqual(context.exception.errno, errno.EIO)
        self.record_stat.assert_called_with("stream_extractions")

    def test_apply_stream_info_refuses_fallback_after_preferred_format(self):
        file_info = {"video_id": "vid", "format_id": "141"}

        with self.assertRaises(OSError) as context:
            self.handler._apply_stream_info(
                file_info, {"stream_url": "https://x", "format_id": "140"}
            )

        self.assertEqual(context.exception.errno, errno.EIO)
        self.assertEqual(file_info["format_id"], "141")

    def test_extract_stream_stores_integer_duration(self):
        self.yt_dlp.extract_stream_url_async.return_value = self._stream_result(
            duration=215
        )
        file_info = {"video_id": "vid"}

        self.handler._extract_and_apply_stream("vid", file_info)

        self.cache.set_durations_batch.assert_called_once_with({"vid": 215})
        self.assertEqual(file_info["stream_url"], "https://example.com/new.m4a")

    def test_expired_stream_info_is_discarded(self):
        self.handler.stream_info_cache["vid"] = {
            "time": time.time() - FileHandler.STREAM_INFO_TTL - 1,
            "stream_url": "https://example.com/old.m4a",
        }

        self.assertFalse(self.handler._use_cached_stream_info({"video_id": "vid"}))
        self.assertNotIn("vid", self.handler.stream_info_cache)

    def test_mark_unavailable_ignores_transient_errors(self):
        self.handler._mark_unavailable_if_needed("vid", "/p.m4a", "HTTP Error 500")
        self.cache.mark_unavailable_track.assert_not_called()

        self.handler._mark_unavailable_if_needed("vid", "/p.m4a", "Video unavailable")
        self.cache.mark_unavailable_track.assert_called_once_with(
            "vid", "/p.m4a", "Video unavailable"
        )

    def test_precache_returns_true_for_complete_cached_audio(self):
        (self.audio_dir / "vid.m4a").write_bytes(b"data")
        (self.audio_dir / "vid.status").write_text("complete:140")

        self.assertTrue(self.handler.precache("/p.m4a", "vid"))
        self.yt_dlp.extract_stream_url_async.assert_not_called()
        self.handler.downloader.download_file_now.assert_not_called()

    def test_precache_skips_known_unavailable_track(self):
        self.cache.is_track_unavailable.return_value = True

        self.assertFalse(self.handler.precache("/p.m4a", "vid"))
        self.yt_dlp.extract_stream_url_async.assert_not_called()

    def test_precache_marks_unavailable_on_extraction_error(self):
        self.yt_dlp.extract_stream_url_async.return_value = self._stream_result(
            status="error", error="Video unavailable"
        )

        self.assertFalse(self.handler.precache("/p.m4a", "vid"))
        self.cache.mark_unavailable_track.assert_called_once_with(
            "vid", "/p.m4a", "Video unavailable"
        )
        self.handler.downloader.download_file_now.assert_not_called()

    def test_precache_uses_cached_stream_info(self):
        self.handler.stream_info_cache["vid"] = {
            "time": time.time(),
            "stream_url": "https://example.com/cached.m4a",
            "format_id": "140",
        }

        self.assertTrue(self.handler.precache("/p.m4a", "vid"))

        self.yt_dlp.extract_stream_url_async.assert_not_called()
        request = self.handler.downloader.download_file_now.call_args.args[0]
        self.assertEqual(request.stream_url, "https://example.com/cached.m4a")
        self.assertEqual(request.format_id, "140")

    def test_precache_returns_false_without_format(self):
        self.handler.stream_info_cache["vid"] = {
            "time": time.time(),
            "stream_url": "https://example.com/cached.m4a",
            "format_id": None,
        }

        self.assertFalse(self.handler.precache("/p.m4a", "vid"))
        self.handler.downloader.download_file_now.assert_not_called()


class TestFileHandlerCaches(_RealFileHandlerMixin, unittest.TestCase):
    """Local audio, range and progressive cache behavior."""

    def _open(self, format_id="141", stream_url="https://example.com/a.m4a"):
        fh = self.handler.open("/liked_songs/a.m4a", "vid")
        self.handler.open_files[fh]["format_id"] = format_id
        self.handler.open_files[fh]["stream_url"] = stream_url
        return fh

    def test_read_unknown_handle_raises_ebadf(self):
        with self.assertRaises(OSError) as context:
            self.handler.read("/a.m4a", 10, 0, 999)
        self.assertEqual(context.exception.errno, errno.EBADF)

    def test_read_reraises_stored_stream_error(self):
        fh = self._open()
        self.handler.open_files[fh]["status"] = "error"
        self.handler.open_files[fh]["error"] = "Video unavailable"

        with self.assertRaises(OSError) as context:
            self.handler.read("/liked_songs/a.m4a", 10, 0, fh)
        self.assertEqual(context.exception.errno, errno.ENOENT)

    def test_read_zero_bytes_returns_empty_without_streaming(self):
        fh = self._open()
        with patch.object(self.handler, "_stream_content") as stream:
            self.assertEqual(self.handler.read("/liked_songs/a.m4a", 0, 0, fh), b"")
        stream.assert_not_called()

    def test_read_uses_downloaded_bytes_reported_by_downloader(self):
        fh = self._open(format_id="141")
        (self.audio_dir / "vid.m4a").write_bytes(b"0123456789")
        (self.audio_dir / "vid.status").write_text("partial:140")
        self.handler.downloader.get_progress.return_value = {
            "status": "downloading",
            "progress": 8,
        }

        with patch.object(self.handler, "_stream_content") as stream:
            data = self.handler.read("/liked_songs/a.m4a", 4, 2, fh)

        self.assertEqual(data, b"2345")
        stream.assert_not_called()

    def test_download_has_range_requires_enough_progress(self):
        self.assertFalse(FileHandler._download_has_range(None, 0, 1))
        self.assertFalse(
            FileHandler._download_has_range(
                {"status": "downloading", "progress": 5}, 2, 4
            )
        )
        self.assertTrue(FileHandler._download_has_range({"status": "complete"}, 0, 9))

    def test_read_remote_requires_stream_url(self):
        fh = self._open(stream_url="cached")
        with self.assertRaises(OSError) as context:
            self.handler._read_remote(
                "/liked_songs/a.m4a", self.handler.open_files[fh], 0, 10
            )
        self.assertEqual(context.exception.errno, errno.EIO)

    def test_cached_range_drops_index_when_part_file_vanishes(self):
        fh = self._open(stream_url=None)
        range_dir = self.handler._range_cache_dir("vid", "141")
        range_dir.mkdir(parents=True)
        part = range_dir / "0-8.part"
        part.write_bytes(b"abcdefgh")
        file_info = self.handler.open_files[fh]
        self.assertEqual(
            self.handler._read_cached_range("/liked_songs/a.m4a", file_info, 0, 4),
            b"abcd",
        )

        part.unlink()

        self.assertIsNone(
            self.handler._read_cached_range("/liked_songs/a.m4a", file_info, 0, 4)
        )
        self.assertNotIn(range_dir, self.handler.range_index)

    def test_cached_range_serves_short_read_at_known_eof(self):
        fh = self._open(stream_url=None)
        self.file_sizes["/liked_songs/a.m4a"] = 8
        range_dir = self.handler._range_cache_dir("vid", "141")
        range_dir.mkdir(parents=True)
        (range_dir / "0-8.part").write_bytes(b"abcdefgh")

        data = self.handler._read_cached_range(
            "/liked_songs/a.m4a", self.handler.open_files[fh], 6, 10
        )

        self.assertEqual(data, b"gh")

    def test_cached_ranges_ignore_malformed_part_names(self):
        range_dir = self.handler._range_cache_dir("vid", "141")
        range_dir.mkdir(parents=True)
        (range_dir / "junk.part").write_bytes(b"x")
        (range_dir / "0-1.part").write_bytes(b"x")

        ranges = self.handler._cached_ranges(range_dir)

        self.assertEqual([(start, end) for start, end, _ in ranges], [(0, 1)])

    def test_cache_range_does_not_rewrite_existing_part(self):
        fh = self._open()
        file_info = self.handler.open_files[fh]
        self.handler._cache_range(file_info, 0, b"first")
        self.handler._cache_range(file_info, 0, b"other")

        part = self.handler._range_cache_dir("vid", "141") / "0-5.part"
        self.assertEqual(part.read_bytes(), b"first")
        self.assertEqual(self.record_stat.call_args_list, [call("range_cache_writes")])

    def test_read_available_audio_cache_edge_cases(self):
        cache_path = self.audio_dir / "vid.m4a"
        self.assertEqual(
            FileHandler._read_available_audio_cache(cache_path, "vid", "141", 0, 0),
            b"",
        )
        # No status and no audio file: stat fails.
        self.assertIsNone(
            FileHandler._read_available_audio_cache(cache_path, "vid", "141", 0, 4)
        )
        cache_path.write_bytes(b"abc")
        self.assertIsNone(
            FileHandler._read_available_audio_cache(cache_path, "vid", "141", 0, 4)
        )

    def test_read_cache_status_returns_none_when_unreadable(self):
        status_path = self.audio_dir / "vid.status"
        status_path.mkdir()
        self.assertIsNone(FileHandler._read_cache_status(status_path))

    def test_progressive_cache_ignores_empty_data(self):
        fh = self._open()
        self.handler._write_progressive_audio_cache(self.handler.open_files[fh], 0, b"")
        self.assertFalse((self.audio_dir / "vid.m4a").exists())

    def test_progressive_cache_write_errors_are_swallowed(self):
        fh = self._open()

        with patch.object(Path, "open", side_effect=OSError(errno.ENOSPC, "full")):
            self.handler._write_progressive_audio_cache(
                self.handler.open_files[fh], 0, b"data"
            )

        self.assertFalse((self.audio_dir / "vid.status").exists())

    def test_maybe_start_cache_download_ignores_empty_reads(self):
        fh = self._open()
        file_info = self.handler.open_files[fh]
        self.handler._maybe_start_cache_download("/a.m4a", file_info, 0)
        self.assertEqual(file_info["bytes_read"], 0)

    def test_maybe_start_cache_download_requires_format(self):
        fh = self._open(format_id=None)
        file_info = self.handler.open_files[fh]

        self.handler._maybe_start_cache_download(
            "/a.m4a", file_info, FileHandler.CACHE_START_BYTES
        )

        self.handler.downloader.download_file.assert_not_called()
        self.assertFalse(file_info["cache_started"])

    def test_read_ranges_are_capped_at_twelve(self):
        file_info = {}
        for offset in range(20):
            FileHandler._record_read_request(file_info, 1, offset)
        self.assertEqual(len(file_info["read_ranges"]), 12)
        self.assertEqual(file_info["read_calls"], 20)

    def test_uncached_probe_ignores_small_advertised_size(self):
        self.file_sizes["/a.m4a"] = FileHandler.PROBE_EOF_OFFSET
        self.assertFalse(
            self.handler._is_uncached_probe_read("/a.m4a", FileHandler.PROBE_EOF_OFFSET)
        )

    def test_update_size_uses_content_length_for_full_response(self):
        response = Mock(status_code=200, headers={"Content-Length": "4096"})
        self.handler._update_size_from_response("/a.m4a", response, 0)
        self.update_size.assert_called_once_with("/a.m4a", 4096)

    def test_update_size_ignores_unknown_length(self):
        response = Mock(status_code=200, headers={"Content-Length": "abc"})
        self.handler._update_size_from_response("/a.m4a", response, 0)
        response = Mock(status_code=206, headers={"Content-Range": "bytes 0-1/*"})
        self.handler._update_size_from_response("/a.m4a", response, 0)
        self.update_size.assert_not_called()

    def test_content_range_total_parses_only_numeric_totals(self):
        self.assertIsNone(FileHandler._content_range_total(None))
        self.assertIsNone(FileHandler._content_range_total("bytes 0-1"))
        self.assertIsNone(FileHandler._content_range_total("bytes 0-1/*"))
        self.assertIsNone(FileHandler._content_range_total("bytes 0-1/x"))
        self.assertEqual(FileHandler._content_range_total("bytes 0-1/99"), 99)

    def test_release_unknown_handle_is_noop(self):
        self.assertEqual(self.handler.release("/a.m4a", 12345), 0)
        self.assertEqual(self.handler.get_recent_handles(), [])

    def test_release_keeps_path_mapping_owned_by_newer_handle(self):
        first = self.handler.open("/a.m4a", "vid")
        second = self.handler.open("/a.m4a", "vid")

        self.handler.release("/a.m4a", first)

        self.assertEqual(self.handler.path_to_fh["/a.m4a"], second)
        self.assertEqual(self.handler.get_recent_handles()[0]["video_id"], "vid")

    def test_cached_audio_format_returns_none_when_status_unreadable(self):
        (self.audio_dir / "vid.status").mkdir()
        (self.audio_dir / "vid.m4a").write_bytes(b"data")

        self.assertIsNone(self.handler._cached_audio_format("vid"))


class TestFileHandlerStreaming(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
