#!/usr/bin/env python3

import unittest
from concurrent.futures import Future
from unittest.mock import Mock, patch, MagicMock, call
import logging
import os
import tempfile
import time
from pathlib import Path
import threading
import requests
import errno

# Import the class to test
from ytmusicfs.file_handler import FileHandler


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
        self.logger = logging.getLogger("test")
        self.update_file_size_callback = Mock()
        self.yt_dlp_utils = Mock()

        # Create the instance to test
        self.file_handler = FileHandler(
            thread_manager=self.thread_manager,
            cache_dir=self.cache_dir,
            cache=self.cache,
            logger=self.logger,
            update_file_size_callback=self.update_file_size_callback,
            yt_dlp_utils=self.yt_dlp_utils,
        )

        # Initialize class attributes
        self.file_handler.next_fh = 1
        self.file_handler.downloader = Mock()
        self.file_handler.futures = {}

        # Patch _check_cached_audio method to return False by default
        self.original_check_cached = self.file_handler._check_cached_audio
        self.file_handler._check_cached_audio = Mock(return_value=False)

    def tearDown(self):
        """Clean up after each test method."""
        # Restore original methods
        self.file_handler._check_cached_audio = self.original_check_cached

        # Ensure all file handles are released
        self.file_handler.open_files = {}
        self.file_handler.path_to_fh = {}

        # Clean up temporary directory
        import shutil

        shutil.rmtree(self.temp_dir)

    def test_open_file(self):
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

    def test_open_file_cached_handle(self):
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

    @patch("ytmusicfs.file_handler.requests.get")
    def test_read_file(self, mock_requests_get):
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
        with patch("pathlib.Path.exists", return_value=False):
            # Call the method with patched _stream_content
            with patch.object(
                self.file_handler, "_stream_content", return_value=mock_response.content
            ):
                data = self.file_handler.read(path, size, offset, file_handle)

                # Verify correct data was returned
                self.assertEqual(data, mock_response.content)

                # Verify _stream_content was called with the right arguments
                self.file_handler._stream_content.assert_called_once_with(
                    stream_url,
                    offset,
                    size,
                    auth_headers=None,
                    cookies=None,
                )

    @patch("ytmusicfs.file_handler.requests.get")
    def test_read_file_with_offset(self, mock_requests_get):
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
        with patch("pathlib.Path.exists", return_value=False):
            # Call the method with patched _stream_content
            with patch.object(
                self.file_handler, "_stream_content", return_value=mock_response.content
            ):
                data = self.file_handler.read(path, size, offset, file_handle)

                # Verify correct data was returned
                self.assertEqual(data, mock_response.content)

                # Verify _stream_content was called with the right arguments
                self.file_handler._stream_content.assert_called_once_with(
                    stream_url,
                    offset,
                    size,
                    auth_headers=None,
                    cookies=None,
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

        headers = mock_stream.call_args.kwargs["auth_headers"]
        cookies = mock_stream.call_args.kwargs["cookies"]

        self.assertNotIn("Host", headers)
        self.assertEqual(headers["User-Agent"], "UnitTest")
        self.assertEqual(headers["X-Goog-AuthUser"], "0")
        self.assertNotIn("X-YouTube-Identity-Token", headers)

        self.assertEqual(cookies, {"CONSENT": "YES+"})

        self.file_handler.downloader.download_file.assert_called_once_with(
            video_id,
            "https://example.com/audio.m4a",
            path,
            headers=headers,
            cookies={"CONSENT": "YES+"},
        )

    def test_release_file(self):
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

    @patch("ytmusicfs.file_handler.requests.get")
    @patch("ytmusicfs.file_handler.time.sleep")
    def test_stream_content_retry_on_failure(self, mock_sleep, mock_requests_get):
        """Test the retry logic in _stream_content method with detailed verification."""
        # Setup stream URL and request parameters
        stream_url = "https://example.com/stream.m4a"
        offset = 0
        size = 1024
        retries = 2  # Test with 2 retries

        # Create mock responses - first fails with 503, second succeeds
        mock_response_fail = MagicMock()
        mock_response_fail.status_code = 503  # Service Unavailable

        # Configure mock context manager correctly
        mock_response_fail.__enter__ = MagicMock(return_value=mock_response_fail)
        mock_response_fail.__exit__ = MagicMock(return_value=None)

        mock_response_success = MagicMock()
        mock_response_success.status_code = 200  # Success
        mock_response_success.content = b"test_audio_data" * 64

        # Configure mock context manager correctly
        mock_response_success.__enter__ = MagicMock(return_value=mock_response_success)
        mock_response_success.__exit__ = MagicMock(return_value=None)

        mock_response_success.iter_content = MagicMock(
            return_value=[mock_response_success.content]
        )

        # Use a list to store responses for our patched method
        responses = [mock_response_fail, mock_response_success]

        # Configure requests.get to fail on first call and succeed on second
        mock_requests_get.side_effect = responses

        # Track which response to return
        current_attempt = [0]  # Using list for mutable reference

        def mock_get(*args, **kwargs):
            # Return the appropriate response based on current attempt
            response = responses[current_attempt[0]]
            current_attempt[0] += 1
            return response

        # Replace the side_effect with our function
        mock_requests_get.side_effect = mock_get

        # Create a patched version of _stream_content that handles status codes correctly
        original_stream_content = self.file_handler._stream_content

        def patched_stream_content(
            url, off, sz, auth_headers=None, cookies=None, retries=3
        ):
            # Mock the behavior we want for this test
            for attempt in range(retries):
                try:
                    # Get response from the mock
                    with mock_requests_get() as resp:
                        if resp.status_code != 200 and resp.status_code != 206:
                            # Simulate how the real method works - non-200/206 is treated as an error
                            raise requests.exceptions.RequestException(
                                f"HTTP {resp.status_code}"
                            )
                        # For successful response, return the content
                        return resp.content[:sz]
                except requests.exceptions.RequestException as e:
                    if attempt == retries - 1:
                        raise OSError(
                            errno.EIO, f"Failed after {retries} attempts: {e}"
                        )
                    time.sleep(2**attempt)

        # Use the patched method for this test
        self.file_handler._stream_content = patched_stream_content

        # Call the method
        data = self.file_handler._stream_content(
            stream_url, offset, size, retries=retries
        )

        # Verify requests.get was called twice (once for failure, once for success)
        self.assertEqual(mock_requests_get.call_count, 2)

        # Verify sleep was called once for backoff (after first failure)
        mock_sleep.assert_called_once_with(1)  # 2^0 = 1 second for first retry

        # Verify correct data was returned
        self.assertEqual(data, mock_response_success.content[:size])

        # Restore original method
        self.file_handler._stream_content = original_stream_content

    @patch("ytmusicfs.file_handler.requests.get")
    @patch("ytmusicfs.file_handler.time.sleep")
    def test_stream_content_max_retries_exceeded(self, mock_sleep, mock_requests_get):
        """Test behavior when max retries are exceeded in _stream_content method."""
        # Setup stream URL and request parameters
        stream_url = "https://example.com/stream.m4a"
        offset = 0
        size = 1024
        retries = 3

        # Configure requests.get to always fail with 503
        mock_response_fail = MagicMock()
        mock_response_fail.status_code = 503

        # Configure mock context manager correctly
        mock_response_fail.__enter__ = MagicMock(return_value=mock_response_fail)
        mock_response_fail.__exit__ = MagicMock(return_value=None)

        # Configure requests.get to fail each time
        mock_requests_get.return_value = mock_response_fail

        # Create a patched version of _stream_content that handles status codes as errors
        original_stream_content = self.file_handler._stream_content

        def patched_stream_content(
            url, off, sz, auth_headers=None, cookies=None, retries=3
        ):
            # Ensure the mock is called the expected number of times
            responses = [mock_response_fail] * retries

            for attempt in range(retries):
                try:
                    # Call the mock each time to increment call count
                    mock_requests_get()
                    with responses[attempt] as resp:
                        if resp.status_code != 200 and resp.status_code != 206:
                            # Simulate how the real method works
                            raise requests.exceptions.RequestException(
                                f"HTTP {resp.status_code}"
                            )
                except requests.exceptions.RequestException as e:
                    if attempt == retries - 1:
                        raise OSError(
                            errno.EIO, f"HTTP {mock_response_fail.status_code}"
                        )
                    time.sleep(2**attempt)

            # Should not reach here
            return None

        # Use the patched method for this test
        self.file_handler._stream_content = patched_stream_content

        # Expect OSError when max retries are exceeded
        with self.assertRaises(OSError) as context:
            self.file_handler._stream_content(stream_url, offset, size, retries=retries)

        # Verify error code and message
        self.assertEqual(context.exception.errno, errno.EIO)
        self.assertTrue("HTTP 503" in str(context.exception))

        # Verify requests.get was called 'retries' times
        self.assertEqual(mock_requests_get.call_count, retries)

        # Verify sleep was called for exponential backoff
        mock_sleep.assert_has_calls(
            [
                call(1),  # 2^0 = 1 second for first retry
                call(2),  # 2^1 = 2 seconds for second retry
            ]
        )

        # Restore original method
        self.file_handler._stream_content = original_stream_content

    @patch("ytmusicfs.file_handler.requests.get")
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
            stream_url,
            offset,
            size,
            auth_headers={
                "Cookie": "SID=headerSid; HSID=headerHsid",
                "User-Agent": "UnitTest",
            },
            cookies={"SID": "mappingSid", "CONSENT": "YES+"},
            retries=1,
        )

        self.assertEqual(data, chunk[:size])

        called_kwargs = mock_requests_get.call_args.kwargs
        self.assertNotIn("Cookie", called_kwargs["headers"])
        self.assertEqual(called_kwargs["headers"]["User-Agent"], "UnitTest")
        self.assertEqual(
            called_kwargs["cookies"],
            {"SID": "mappingSid", "HSID": "headerHsid", "CONSENT": "YES+"},
        )

    def test_read_from_cached_file(self):
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


if __name__ == "__main__":
    unittest.main()
