#!/usr/bin/env python3

import logging
import shutil
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

from ytmusicfs.dependencies import DownloaderDependencies
from ytmusicfs.downloader import Downloader
from ytmusicfs.models import DownloadRequest, DownloadStatus


class TestDownloader(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir)
        self.thread_manager = Mock()
        self.thread_manager.create_lock.return_value = threading.Lock()
        self.thread_manager.submit_task = Mock()
        self.thread_manager.is_shutdown.return_value = False
        self.logger = logging.getLogger("test")
        self.update_callback = Mock()
        self.downloader = Downloader(
            DownloaderDependencies(
                thread_manager=self.thread_manager,
                cache_dir=self.cache_dir,
                logger=self.logger,
                update_file_size=self.update_callback,
            )
        )
        (self.cache_dir / "audio").mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir)

    @patch("ytmusicfs.downloader.http_get")
    @patch("ytmusicfs.downloader.http_head")
    def test_download_task_merges_cookie_header(self, mock_head, mock_get):
        video_id = "abc123"
        stream_url = "https://example.com/audio.m4a"
        path = "/playlists/test/song.m4a"

        chunk = b"\x00\x00\x00\x18ftypm4a " + (b"\x00" * 90)

        head_response = MagicMock()
        head_response.status_code = 206
        head_response.headers = {"content-length": str(len(chunk))}
        mock_head.return_value = head_response

        get_response = MagicMock()
        get_response.status_code = 200
        get_response.iter_content.return_value = [chunk]

        mock_context = MagicMock()
        mock_context.__enter__.return_value = get_response
        mock_context.__exit__.return_value = None
        mock_get.return_value = mock_context

        result = self.downloader._download_task(
            DownloadRequest(
                video_id=video_id,
                stream_url=stream_url,
                path=path,
                format_id="141",
                headers={
                    "User-Agent": "UnitTest",
                    "Cookie": "SID=headerSid; HSID=headerHsid",
                },
                cookies={"SID": "mappingSid", "CONSENT": "YES+"},
                retries=1,
                chunk_size=len(chunk),
            )
        )

        self.assertTrue(result)

        head_kwargs = mock_head.call_args.kwargs
        self.assertNotIn("Cookie", head_kwargs["headers"])
        self.assertEqual(
            head_kwargs["cookies"],
            {"SID": "mappingSid", "HSID": "headerHsid", "CONSENT": "YES+"},
        )

        get_kwargs = mock_get.call_args.kwargs
        self.assertNotIn("Cookie", get_kwargs["headers"])
        self.assertEqual(
            get_kwargs["cookies"],
            {"SID": "mappingSid", "HSID": "headerHsid", "CONSENT": "YES+"},
        )

        audio_path = self.cache_dir / "audio" / f"{video_id}.m4a"
        self.assertTrue(audio_path.exists())
        with audio_path.open("rb") as f:
            self.assertTrue(f.read().startswith(b"\x00\x00\x00\x18ftyp"))

    def test_download_file_now_runs_in_current_worker(self):
        with patch.object(self.downloader, "_download_task", return_value=True) as task:
            request = DownloadRequest(
                video_id="abc123",
                stream_url="https://example.com/audio.m4a",
                path="/liked_songs/song.m4a",
                format_id="141",
            )
            result = self.downloader.download_file_now(request)

        self.assertTrue(result)
        self.thread_manager.submit_task.assert_not_called()
        task.assert_called_once_with(request)

    @patch("ytmusicfs.downloader.http_get")
    @patch("ytmusicfs.downloader.http_head")
    def test_download_task_resumes_existing_progressive_cache(
        self, mock_head, mock_get
    ):
        video_id = "abc123"
        stream_url = "https://example.com/audio.m4a"
        path = "/playlists/test/song.m4a"
        audio_path = self.cache_dir / "audio" / f"{video_id}.m4a"
        prefix = b"\x00\x00\x00\x18ftypm4a " + (b"\x00" * 90)
        suffix = b"tail"
        audio_path.write_bytes(prefix)

        head_response = MagicMock()
        head_response.status_code = 206
        head_response.headers = {"content-length": str(len(suffix))}
        mock_head.return_value = head_response

        get_response = MagicMock()
        get_response.status_code = 206
        get_response.iter_content.return_value = [suffix]
        mock_context = MagicMock()
        mock_context.__enter__.return_value = get_response
        mock_context.__exit__.return_value = None
        mock_get.return_value = mock_context

        result = self.downloader._download_task(
            DownloadRequest(
                video_id=video_id,
                stream_url=stream_url,
                path=path,
                format_id="141",
                retries=1,
                chunk_size=len(suffix),
            )
        )

        self.assertTrue(result)
        self.assertEqual(audio_path.read_bytes(), prefix + suffix)
        self.assertEqual(
            mock_head.call_args.kwargs["headers"]["Range"],
            f"bytes={len(prefix)}-",
        )
        self.assertEqual(
            mock_get.call_args.kwargs["headers"]["Range"],
            f"bytes={len(prefix)}-",
        )

    @patch("ytmusicfs.downloader.http_get")
    @patch("ytmusicfs.downloader.http_head")
    def test_download_task_replaces_cache_from_different_format(
        self, mock_head, mock_get
    ):
        video_id = "abc123"
        stream_url = "https://example.com/audio.m4a"
        path = "/playlists/test/song.m4a"
        audio_path = self.cache_dir / "audio" / f"{video_id}.m4a"
        status_path = self.cache_dir / "audio" / f"{video_id}.status"
        old_data = b"\x00\x00\x00\x18ftypm4a " + (b"\x00" * 90)
        new_data = b"\x00\x00\x00\x18ftypm4a " + (b"\x01" * 90)
        audio_path.write_bytes(old_data)
        status_path.write_text("complete:140")

        head_response = MagicMock()
        head_response.status_code = 200
        head_response.headers = {"content-length": str(len(new_data))}
        mock_head.return_value = head_response

        get_response = MagicMock()
        get_response.status_code = 200
        get_response.iter_content.return_value = [new_data]
        mock_context = MagicMock()
        mock_context.__enter__.return_value = get_response
        mock_context.__exit__.return_value = None
        mock_get.return_value = mock_context

        result = self.downloader._download_task(
            DownloadRequest(
                video_id=video_id,
                stream_url=stream_url,
                path=path,
                format_id="141",
                retries=1,
                chunk_size=len(new_data),
            )
        )

        self.assertTrue(result)
        self.assertEqual(audio_path.read_bytes(), new_data)
        self.assertNotIn("Range", mock_head.call_args.kwargs["headers"])
        self.assertNotIn("Range", mock_get.call_args.kwargs["headers"])

    @patch("ytmusicfs.downloader.http_get")
    @patch("ytmusicfs.downloader.http_head")
    def test_download_task_keeps_partial_cache_after_failure(self, mock_head, mock_get):
        video_id = "abc123"
        stream_url = "https://example.com/audio.m4a"
        path = "/playlists/test/song.m4a"
        audio_path = self.cache_dir / "audio" / f"{video_id}.m4a"
        status_path = self.cache_dir / "audio" / f"{video_id}.status"
        partial = b"\x00\x00\x00\x18ftypm4a " + (b"\x00" * 90)
        audio_path.write_bytes(partial)
        status_path.write_text("partial:140")

        head_response = MagicMock()
        head_response.status_code = 503
        head_response.headers = {}
        mock_head.return_value = head_response

        result = self.downloader._download_task(
            DownloadRequest(
                video_id=video_id,
                stream_url=stream_url,
                path=path,
                format_id="140",
                retries=1,
            )
        )

        self.assertFalse(result)
        self.assertEqual(audio_path.read_bytes(), partial)
        self.assertEqual(status_path.read_text(), "failed:140")
        mock_get.assert_not_called()

    def test_download_task_skips_downgrade_from_141_to_140(self):
        video_id = "abc123"
        path = "/playlists/test/song.m4a"
        audio_path = self.cache_dir / "audio" / f"{video_id}.m4a"
        status_path = self.cache_dir / "audio" / f"{video_id}.status"
        data = b"\x00\x00\x00\x18ftypm4a " + (b"\x00" * 90)
        audio_path.write_bytes(data)
        status_path.write_text("complete:141")

        result = self.downloader._download_task(
            DownloadRequest(
                video_id=video_id,
                stream_url="https://example.com/audio.m4a",
                path=path,
                format_id="140",
                retries=1,
            )
        )

        self.assertTrue(result)
        self.assertEqual(audio_path.read_bytes(), data)
        self.thread_manager.submit_task.assert_not_called()

    def test_download_task_replaces_140_with_141(self):
        video_id = "abc123"
        path = "/playlists/test/song.m4a"
        audio_path = self.cache_dir / "audio" / f"{video_id}.m4a"
        status_path = self.cache_dir / "audio" / f"{video_id}.status"
        old_data = b"\x00\x00\x00\x18ftypm4a " + (b"\x00" * 90)
        new_data = b"\x00\x00\x00\x18ftypm4a " + (b"\x01" * 90)
        audio_path.write_bytes(old_data)
        status_path.write_text("complete:140")

        with (
            patch.object(self.downloader, "_validate_file_format", return_value=True),
            patch("ytmusicfs.downloader.http_head") as mock_head,
            patch("ytmusicfs.downloader.http_get") as mock_get,
        ):
            head_response = MagicMock()
            head_response.status_code = 200
            head_response.headers = {"content-length": str(len(new_data))}
            mock_head.return_value = head_response

            get_response = MagicMock()
            get_response.status_code = 200
            get_response.iter_content.return_value = [new_data]
            mock_context = MagicMock()
            mock_context.__enter__.return_value = get_response
            mock_context.__exit__.return_value = None
            mock_get.return_value = mock_context

            result = self.downloader._download_task(
                DownloadRequest(
                    video_id=video_id,
                    stream_url="https://example.com/audio.m4a",
                    path=path,
                    format_id="141",
                    retries=1,
                    chunk_size=len(new_data),
                )
            )

        self.assertTrue(result)
        self.assertEqual(audio_path.read_bytes(), new_data)

    # Helpers for the tests below.

    VALID_M4A = b"\x00\x00\x00\x18ftypm4a " + (b"\x00" * 90)

    @property
    def audio_path(self) -> Path:
        return self.cache_dir / "audio" / "abc123.m4a"

    @property
    def status_path(self) -> Path:
        return self.cache_dir / "audio" / "abc123.status"

    @staticmethod
    def _head(status_code: int, content_length: int | None = None) -> MagicMock:
        response = MagicMock()
        response.status_code = status_code
        response.headers = (
            {} if content_length is None else {"content-length": str(content_length)}
        )
        return response

    @staticmethod
    def _get(status_code: int, chunks) -> MagicMock:
        response = MagicMock()
        response.status_code = status_code
        response.iter_content.return_value = chunks
        context = MagicMock()
        context.__enter__.return_value = response
        context.__exit__.return_value = None
        return context

    @staticmethod
    def _request(**overrides) -> DownloadRequest:
        values = {
            "video_id": "abc123",
            "stream_url": "https://example.com/audio.m4a",
            "path": "/liked_songs/song.m4a",
            "format_id": "141",
            "retries": 1,
            "chunk_size": 1024,
        }
        values.update(overrides)
        return DownloadRequest(**values)

    def test_download_file_skips_complete_download(self):
        self.audio_path.write_bytes(self.VALID_M4A)
        self.status_path.write_text("complete:141")

        self.assertTrue(self.downloader.download_file(self._request()))

        self.thread_manager.submit_task.assert_not_called()
        progress = self.downloader.get_progress("abc123")
        self.assertEqual(progress["status"], DownloadStatus.COMPLETE)
        self.assertEqual(progress["total"], len(self.VALID_M4A))

    def test_download_file_skips_active_download(self):
        self.downloader.active_downloads["abc123"] = {
            "status": DownloadStatus.DOWNLOADING
        }

        self.assertTrue(self.downloader.download_file(self._request()))

        self.thread_manager.submit_task.assert_not_called()

    def test_download_file_submits_io_task(self):
        request = self._request()

        self.assertTrue(self.downloader.download_file(request))

        self.thread_manager.submit_task.assert_called_once_with(
            "io", self.downloader._download_task, request
        )

    def test_download_file_now_skips_complete_or_starting_download(self):
        with patch.object(self.downloader, "_download_task") as task:
            self.downloader.active_downloads["abc123"] = {
                "status": DownloadStatus.STARTING
            }
            self.assertTrue(self.downloader.download_file_now(self._request()))

            self.downloader.active_downloads.clear()
            self.audio_path.write_bytes(self.VALID_M4A)
            self.status_path.write_text("complete:141")
            self.assertTrue(self.downloader.download_file_now(self._request()))

        task.assert_not_called()

    @patch("ytmusicfs.downloader.http_get")
    @patch("ytmusicfs.downloader.http_head")
    def test_download_task_restarts_when_server_ignores_range(
        self, mock_head, mock_get
    ):
        self.audio_path.write_bytes(b"stale partial bytes")
        head_ranges = []

        def head(url, headers, **kwargs):
            head_ranges.append(headers.get("Range"))
            return self._head(200, len(self.VALID_M4A))

        mock_head.side_effect = head
        mock_get.return_value = self._get(200, [self.VALID_M4A])

        self.assertTrue(self.downloader._download_task(self._request()))

        self.assertEqual(self.audio_path.read_bytes(), self.VALID_M4A)
        self.assertEqual(head_ranges, ["bytes=19-"])
        self.assertNotIn("Range", mock_get.call_args.kwargs["headers"])
        self.update_callback.assert_called_once_with(
            "/liked_songs/song.m4a", len(self.VALID_M4A)
        )
        self.assertEqual(self.status_path.read_text(), "complete:141")

    @patch("ytmusicfs.downloader.time.sleep")
    @patch("ytmusicfs.downloader.http_get")
    @patch("ytmusicfs.downloader.http_head")
    def test_download_task_retries_after_transient_failure(
        self, mock_head, mock_get, mock_sleep
    ):
        mock_head.side_effect = [
            OSError("connection reset"),
            self._head(200, len(self.VALID_M4A)),
        ]
        mock_get.return_value = self._get(200, [self.VALID_M4A])

        self.assertTrue(self.downloader._download_task(self._request(retries=3)))

        self.assertEqual(mock_head.call_count, 2)
        self.assertAlmostEqual(sum(c.args[0] for c in mock_sleep.call_args_list), 1.0)
        self.assertEqual(
            self.downloader.get_progress("abc123")["status"], DownloadStatus.COMPLETE
        )

    @patch("ytmusicfs.downloader.time.sleep")
    @patch("ytmusicfs.downloader.http_get")
    @patch("ytmusicfs.downloader.http_head")
    def test_download_task_marks_failed_after_last_retry(
        self, mock_head, mock_get, mock_sleep
    ):
        mock_head.return_value = self._head(200, len(self.VALID_M4A))
        mock_get.return_value = self._get(500, [])

        self.assertFalse(self.downloader._download_task(self._request(retries=3)))

        self.assertEqual(mock_get.call_count, 3)
        self.assertAlmostEqual(sum(c.args[0] for c in mock_sleep.call_args_list), 3.0)
        self.assertEqual(self.status_path.read_text(), "failed:141")
        self.assertEqual(
            self.downloader.get_progress("abc123")["status"], DownloadStatus.FAILED
        )

    @patch("ytmusicfs.downloader.http_get")
    @patch("ytmusicfs.downloader.http_head")
    def test_download_task_with_zero_retries_marks_failed_without_requests(
        self, mock_head, mock_get
    ):
        self.assertFalse(self.downloader._download_task(self._request(retries=0)))

        mock_head.assert_not_called()
        mock_get.assert_not_called()
        # The status must not stay "downloading" forever.
        self.assertEqual(self.status_path.read_text(), "failed:141")
        self.assertEqual(
            self.downloader.get_progress("abc123")["status"], DownloadStatus.FAILED
        )

    @patch("ytmusicfs.downloader.http_get")
    @patch("ytmusicfs.downloader.http_head")
    def test_download_task_fails_when_body_is_shorter_than_expected(
        self, mock_head, mock_get
    ):
        mock_head.return_value = self._head(200, len(self.VALID_M4A) + 50)
        mock_get.return_value = self._get(200, [self.VALID_M4A])

        self.assertFalse(self.downloader._download_task(self._request()))

        self.assertEqual(self.status_path.read_text(), "failed:141")

    @patch("ytmusicfs.downloader.http_get")
    @patch("ytmusicfs.downloader.http_head")
    def test_download_task_fails_when_file_is_not_m4a(self, mock_head, mock_get):
        garbage = b"<html>" + b"x" * 200
        mock_head.return_value = self._head(200, len(garbage))
        mock_get.return_value = self._get(200, [garbage])

        self.assertFalse(self.downloader._download_task(self._request()))

        self.assertEqual(self.status_path.read_text(), "failed:141")
        # Bad bytes are dropped so the next attempt starts over, not resumes.
        self.assertFalse(self.audio_path.exists())

    @patch("ytmusicfs.downloader.time.sleep")
    @patch("ytmusicfs.downloader.http_get")
    @patch("ytmusicfs.downloader.http_head")
    def test_download_task_retries_invalid_file_from_scratch(
        self, mock_head, mock_get, mock_sleep
    ):
        garbage = b"<html>" + b"x" * 200
        mock_head.side_effect = [
            self._head(200, len(garbage)),
            self._head(200, len(self.VALID_M4A)),
        ]
        mock_get.side_effect = [
            self._get(200, [garbage]),
            self._get(200, [self.VALID_M4A]),
        ]

        self.assertTrue(self.downloader._download_task(self._request(retries=2)))

        self.assertNotIn("Range", mock_get.call_args.kwargs["headers"])
        self.assertEqual(self.audio_path.read_bytes(), self.VALID_M4A)

    @patch("ytmusicfs.downloader.http_head")
    def test_download_task_does_nothing_after_shutdown(self, mock_head):
        self.thread_manager.is_shutdown.return_value = True

        self.assertFalse(self.downloader._download_task(self._request()))

        mock_head.assert_not_called()
        self.assertFalse(self.status_path.exists())

    @patch("ytmusicfs.downloader.time.sleep")
    @patch("ytmusicfs.downloader.http_get")
    @patch("ytmusicfs.downloader.http_head")
    def test_download_task_stops_retry_backoff_on_shutdown(
        self, mock_head, mock_get, mock_sleep
    ):
        mock_head.return_value = self._head(200, len(self.VALID_M4A))
        mock_get.return_value = self._get(500, [])
        mock_sleep.side_effect = lambda _seconds: setattr(
            self.thread_manager.is_shutdown, "return_value", True
        )

        self.assertFalse(self.downloader._download_task(self._request(retries=3)))

        mock_get.assert_called_once()
        mock_sleep.assert_called_once()

    @patch("ytmusicfs.downloader.time.sleep")
    @patch("ytmusicfs.downloader.http_get")
    @patch("ytmusicfs.downloader.http_head")
    def test_stop_download_cancels_in_flight_download_without_retry(
        self, mock_head, mock_get, mock_sleep
    ):
        def chunks():
            yield self.VALID_M4A[:50]
            self.downloader.stop_download("abc123")
            yield self.VALID_M4A[50:]

        mock_head.return_value = self._head(200, len(self.VALID_M4A))
        mock_get.return_value = self._get(200, chunks())

        self.assertFalse(self.downloader._download_task(self._request(retries=3)))

        self.assertEqual(self.audio_path.read_bytes(), self.VALID_M4A[:50])
        self.assertEqual(self.status_path.read_text(), "interrupted")
        self.assertTrue(self.downloader.get_progress("abc123")["stop_requested"])
        mock_get.assert_called_once()
        mock_sleep.assert_not_called()

    def test_stop_download_leaves_complete_status_untouched(self):
        self.status_path.write_text("complete:141")
        self.downloader.active_downloads["abc123"] = {"status": DownloadStatus.COMPLETE}

        self.downloader.stop_download("abc123")

        self.assertEqual(self.status_path.read_text(), "complete:141")

    def test_stop_download_ignores_unknown_video(self):
        self.downloader.stop_download("missing")
        self.assertIsNone(self.downloader.get_progress("missing"))
        self.assertFalse(self.status_path.exists())

    def test_stop_download_tolerates_unwritable_status(self):
        self.status_path.mkdir()
        self.downloader.active_downloads["abc123"] = {
            "status": DownloadStatus.DOWNLOADING
        }

        self.downloader.stop_download("abc123")

        self.assertTrue(self.downloader.get_progress("abc123")["stop_requested"])

    @patch("ytmusicfs.downloader.http_get")
    @patch("ytmusicfs.downloader.http_head")
    def test_download_task_tracks_progress_across_chunks(self, mock_head, mock_get):
        chunk_size = 2
        data = self.VALID_M4A + b"\x00" * (chunk_size * 50 - len(self.VALID_M4A))
        chunks = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]
        mock_head.return_value = self._head(200, len(data))
        mock_get.return_value = self._get(200, chunks)

        self.assertTrue(
            self.downloader._download_task(self._request(chunk_size=chunk_size))
        )

        self.assertEqual(
            self.downloader.get_progress("abc123"),
            {
                "status": DownloadStatus.COMPLETE,
                "progress": len(data),
                "total": len(data),
            },
        )

    def test_download_task_keeps_same_format_complete_download(self):
        self.audio_path.write_bytes(self.VALID_M4A)
        self.status_path.write_text("complete:140")

        with patch("ytmusicfs.downloader.http_head") as mock_head:
            self.assertTrue(
                self.downloader._download_task(self._request(format_id="140"))
            )

        mock_head.assert_not_called()

    def test_cached_status_format_parses_known_prefixes(self):
        self.assertIsNone(Downloader._cached_status_format(self.status_path))
        for text, expected in (
            ("complete:141", "141"),
            ("downloading:140", "140"),
            ("failed:139", "139"),
            ("interrupted", None),
        ):
            self.status_path.write_text(text)
            self.assertEqual(
                Downloader._cached_status_format(self.status_path), expected
            )

    def test_format_quality_ranks_known_formats(self):
        self.assertGreater(
            Downloader._format_quality("141"), Downloader._format_quality("140")
        )
        self.assertGreater(
            Downloader._format_quality("140"), Downloader._format_quality("139")
        )
        self.assertEqual(Downloader._format_quality("251"), 0)

    def test_is_download_complete_rejects_other_format_or_invalid_file(self):
        self.audio_path.write_bytes(self.VALID_M4A)
        self.status_path.write_text("complete:140")
        self.assertFalse(self.downloader._is_download_complete("abc123", "141"))

        self.audio_path.write_bytes(b"x" * 200)
        self.status_path.write_text("complete:141")
        self.assertFalse(self.downloader._is_download_complete("abc123", "141"))

    def test_is_download_complete_handles_unreadable_status(self):
        self.status_path.mkdir()
        self.audio_path.write_bytes(self.VALID_M4A)

        self.assertFalse(self.downloader._is_download_complete("abc123", "141"))

    def test_validate_file_format_checks_size_and_ftyp_box(self):
        self.assertFalse(self.downloader._validate_file_format(self.audio_path))

        self.audio_path.write_bytes(self.VALID_M4A[:50])
        self.assertFalse(self.downloader._validate_file_format(self.audio_path))

        self.audio_path.write_bytes(b"\x00" * 200 + b"ftyp" + b"\x00" * 10)
        self.assertTrue(self.downloader._validate_file_format(self.audio_path))

        self.audio_path.write_bytes(b"\x00" * 5000 + b"ftyp")
        self.assertFalse(self.downloader._validate_file_format(self.audio_path))

    def test_validate_file_format_returns_false_on_read_error(self):
        self.audio_path.write_bytes(self.VALID_M4A)
        with patch("builtins.open", side_effect=PermissionError("denied")):
            self.assertFalse(self.downloader._validate_file_format(self.audio_path))


if __name__ == "__main__":
    unittest.main()
