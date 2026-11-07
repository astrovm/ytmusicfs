#!/usr/bin/env python3

"""One writer at a time may add bytes to a track's cached audio file.

The cache file is appended to by the downloader and, track by track, by every
open handle. If two of them write at once the bytes repeat and the file is
still marked complete. These tests use the real FileHandler and Downloader
with fake HTTP and fake audio, and compare the cached bytes with the source.
"""

import logging
import random
import shutil
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from ytmusicfs.dependencies import DownloaderDependencies, FileHandlerDependencies
from ytmusicfs.downloader import Downloader
from ytmusicfs.file_handler import FileHandler
from ytmusicfs.models import DownloadRequest, DownloadStatus

HEADER = b"\x00\x00\x00\x20ftypM4A \x00\x00\x02\x00"
SOURCE = HEADER + random.Random(7).randbytes(1536 * 1024)
OTHER_FORMAT = HEADER + random.Random(9).randbytes(1536 * 1024)
PATH = "/liked_songs/a.m4a"
URL = "http://fake.invalid/a"


class FakeResponse:
    def __init__(self, data, status_code, on_chunk=None):
        self.data = data
        self.status_code = status_code
        self.on_chunk = on_chunk
        self.headers = {"Content-Length": str(len(data))}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def iter_content(self, chunk_size=65536):
        for start in range(0, len(self.data), 256 * 1024):
            yield self.data[start : start + 256 * 1024]
            if self.on_chunk:
                self.on_chunk(start)


def serve(headers, audio=SOURCE, on_chunk=None):
    """Answer a (possibly ranged) request the way a CDN does."""
    requested = (headers or {}).get("Range")
    if not requested:
        return FakeResponse(audio, 200, on_chunk)
    first, _, last = requested.removeprefix("bytes=").partition("-")
    end = int(last) if last else len(audio) - 1
    return FakeResponse(audio[int(first) : end + 1], 206, on_chunk)


class ThreadedTasks:
    """A thread manager that runs submitted tasks on their own threads."""

    def __init__(self):
        self.threads = []
        self.shutdown = False

    def create_lock(self):
        return threading.RLock()

    def is_shutdown(self):
        return self.shutdown

    def submit_task(self, _pool, fn, *args, **kwargs):
        thread = threading.Thread(target=fn, args=args, kwargs=kwargs)
        self.threads.append(thread)
        thread.start()
        return Mock()

    def join(self):
        for thread in self.threads:
            thread.join(20)


class AudioFileOwnershipTestCase(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir)
        self.audio_dir = self.cache_dir / "audio"
        self.tasks = ThreadedTasks()
        cache = Mock()
        cache.get_unavailable_track.return_value = None
        cache.is_track_unavailable.return_value = False
        self.handler = FileHandler(
            FileHandlerDependencies(
                thread_manager=self.tasks,
                cache_dir=self.cache_dir,
                cache=cache,
                logger=logging.getLogger("test"),
                update_file_size=Mock(),
                yt_dlp=Mock(),
                browser="",
            )
        )
        self.audio_dir.mkdir(parents=True)
        self.cached = self.audio_dir / "vid.m4a"
        self.status = self.audio_dir / "vid.status"

    def tearDown(self):
        self.tasks.join()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def request(self, **overrides):
        values = {
            "video_id": "vid",
            "stream_url": URL,
            "path": PATH,
            "format_id": "141",
            "headers": {},
            "cookies": None,
        }
        values.update(overrides)
        return DownloadRequest(**values)

    def open_handle(self, format_id="141"):
        fh = self.handler.open(PATH, "vid")
        self.handler.open_files[fh].update(
            stream_url=URL, format_id=format_id, headers={}, cookies=None
        )
        return fh

    def fake_http(self, on_chunk=None, head=None):
        return (
            patch(
                "ytmusicfs.downloader.http_head",
                head or (lambda _u, headers=None, **_k: serve(headers)),
            ),
            patch(
                "ytmusicfs.downloader.http_get",
                lambda _u, headers=None, **_k: serve(headers, on_chunk=on_chunk),
            ),
            patch(
                "ytmusicfs.file_handler.http_get",
                lambda _u, headers=None, **_k: serve(headers),
            ),
        )

    def assertCachedEqualsSource(self, source=SOURCE):
        data = self.cached.read_bytes()
        first_wrong = next(
            (i for i, (a, b) in enumerate(zip(data, source, strict=False)) if a != b),
            None,
        )
        self.assertEqual(
            (len(data), first_wrong), (len(source), None), "cached bytes differ"
        )


class TestSecondDownloadRequest(AudioFileOwnershipTestCase):
    def test_second_request_while_the_first_is_starting_runs_no_second_download(
        self,
    ):
        probing = threading.Event()
        release = threading.Event()

        def slow_head(_url, headers=None, **_kwargs):
            probing.set()
            release.wait(10)
            return serve(headers)

        head, get, _ = self.fake_http(head=slow_head)[:3]
        with head, get:
            self.handler.downloader.download_file(self.request())
            self.assertTrue(probing.wait(10))
            self.assertEqual(
                self.handler.downloader.get_progress("vid")["status"],
                DownloadStatus.STARTING,
            )
            self.handler.downloader.download_file(self.request())
            release.set()
            self.tasks.join()

        self.assertEqual(len(self.tasks.threads), 1)
        self.assertCachedEqualsSource()
        self.assertEqual(self.status.read_text(), "complete:141")

    def test_request_right_after_the_first_is_queued_runs_no_second_download(self):
        # The first task was submitted but no worker picked it up yet.
        queued = Mock()
        self.tasks.submit_task = lambda *args, **kwargs: queued(*args)

        self.assertTrue(self.handler.downloader.download_file(self.request()))
        self.assertTrue(self.handler.downloader.download_file(self.request()))
        self.assertTrue(self.handler.downloader.download_file_now(self.request()))

        queued.assert_called_once()


class TestSecondHandleDuringDownload(AudioFileOwnershipTestCase):
    def test_a_second_handle_does_not_append_to_a_file_being_downloaded(self):
        reads = []

        def second_handle_reads(start):
            if reads or start != 256 * 1024:
                return
            fh = self.open_handle()
            size = self.cached.stat().st_size
            reads.append(self.handler.read(PATH, 4096, size - 100, fh))
            reads.append(size - 100)

        head, get, handler_get = self.fake_http(on_chunk=second_handle_reads)
        with head, get, handler_get:
            self.assertTrue(
                self.handler.downloader.download_file_now(self.request()),
            )

        self.assertTrue(reads, "the second handle never read")
        self.assertEqual(reads[0], SOURCE[reads[1] : reads[1] + 4096])
        self.assertCachedEqualsSource()
        self.assertEqual(self.status.read_text(), "complete:141")

    def test_a_fresh_handle_reads_the_right_bytes_after_both_wrote(self):
        def second_handle_reads(start):
            if start == 256 * 1024:
                fh = self.open_handle()
                self.handler.read(PATH, 4096, self.cached.stat().st_size - 100, fh)

        head, get, handler_get = self.fake_http(on_chunk=second_handle_reads)
        with head, get, handler_get:
            self.handler.downloader.download_file_now(self.request())
            fh = self.handler.open(PATH, "vid")
            data = self.handler.read(PATH, 4096, 1024 * 1024, fh)

        self.assertEqual(data, SOURCE[1024 * 1024 : 1024 * 1024 + 4096])

    def test_two_handles_reading_overlapping_ranges_cache_each_byte_once(self):
        first = self.open_handle()
        second = self.open_handle()
        step = FileHandler.READAHEAD_INITIAL_BYTES
        with self.fake_http()[2]:
            self.handler.read(PATH, 4096, 0, first)
            # Reaches past the 256 KiB the first handle cached, so it is fetched.
            self.handler.read(PATH, 4096, step - 2048, second)

        self.assertEqual(self.cached.read_bytes(), SOURCE[: 2 * step - 2048])
        self.assertEqual(self.status.read_text(), "partial:141")


class TestHandleLeavesFilesItDoesNotOwn(AudioFileOwnershipTestCase):
    def seed(self, status, content=SOURCE[:1000]):
        self.cached.write_bytes(content)
        self.status.write_text(status)

    def append_next(self, format_id="141"):
        fh = self.open_handle(format_id)
        info = self.handler.open_files[fh]
        self.handler._write_progressive_audio_cache(info, 1000, SOURCE[1000:2000])
        return self.cached.read_bytes(), self.status.read_text()

    def test_it_appends_to_its_own_partial_file(self):
        self.seed("partial:141")
        self.assertEqual(self.append_next(), (SOURCE[:2000], "partial:141"))

    def test_it_starts_a_file_that_has_no_status_yet(self):
        fh = self.open_handle()
        self.handler._write_progressive_audio_cache(
            self.handler.open_files[fh], 0, SOURCE[:500]
        )
        self.assertEqual(self.cached.read_bytes(), SOURCE[:500])
        self.assertEqual(self.status.read_text(), "partial:141")

    def test_it_does_not_mix_in_bytes_of_another_format(self):
        self.seed("partial:140", OTHER_FORMAT[:1000])
        self.assertEqual(self.append_next(), (OTHER_FORMAT[:1000], "partial:140"))

    def test_it_does_not_touch_a_complete_download(self):
        self.seed("complete:141")
        self.assertEqual(self.append_next(), (SOURCE[:1000], "complete:141"))

    def test_it_does_not_touch_a_download_marked_as_running(self):
        self.seed("downloading:141")
        self.assertEqual(self.append_next(), (SOURCE[:1000], "downloading:141"))

    def test_it_does_not_touch_a_failed_download(self):
        self.seed("failed:141")
        self.assertEqual(self.append_next(), (SOURCE[:1000], "failed:141"))

    def test_it_does_not_touch_a_file_the_downloader_is_starting(self):
        self.seed("partial:141")
        self.handler.downloader.active_downloads["vid"] = {
            "status": DownloadStatus.STARTING
        }
        self.assertEqual(self.append_next(), (SOURCE[:1000], "partial:141"))

    def test_it_does_not_count_a_write_it_did_not_make(self):
        self.seed("complete:141")
        stats = Mock()
        self.handler.record_stat_callback = stats
        self.append_next()
        stats.assert_not_called()


class TestProgressiveWriteOffsets(AudioFileOwnershipTestCase):
    def test_an_overlapping_read_appends_only_the_new_bytes(self):
        self.cached.write_bytes(SOURCE[:100])
        fh = self.open_handle()

        self.handler._write_progressive_audio_cache(
            self.handler.open_files[fh], 50, SOURCE[50:150]
        )

        self.assertEqual(self.cached.read_bytes(), SOURCE[:150])
        self.assertEqual(self.status.read_text(), "partial:141")

    def test_a_read_past_the_end_of_the_file_leaves_a_gap_unwritten(self):
        self.cached.write_bytes(SOURCE[:100])
        fh = self.open_handle()

        self.handler._write_progressive_audio_cache(
            self.handler.open_files[fh], 101, SOURCE[101:200]
        )

        self.assertEqual(self.cached.read_bytes(), SOURCE[:100])

    def test_a_read_inside_the_file_changes_nothing(self):
        self.cached.write_bytes(SOURCE[:100])
        fh = self.open_handle()

        self.handler._write_progressive_audio_cache(
            self.handler.open_files[fh], 10, SOURCE[10:100]
        )

        self.assertEqual(self.cached.read_bytes(), SOURCE[:100])
        self.assertFalse(self.status.exists())


class TestCacheDownloadStartsOnce(AudioFileOwnershipTestCase):
    def read_through(self, fh, stop, step=128 * 1024):
        for offset in range(0, stop, step):
            self.handler.read(PATH, step, offset, fh)

    def test_crossing_the_threshold_marks_the_handle_as_caching(self):
        self.handler.downloader = Mock()
        fh = self.open_handle()
        info = self.handler.open_files[fh]
        threshold = FileHandler.CACHE_START_BYTES

        self.handler._maybe_start_cache_download(PATH, info, threshold - 1)
        self.assertFalse(info.get("cache_started"))
        self.handler.downloader.download_file.assert_not_called()

        self.handler._maybe_start_cache_download(PATH, info, 1)
        self.assertIs(info["cache_started"], True)
        self.handler.downloader.download_file.assert_called_once()

    def test_each_read_past_the_threshold_does_not_start_the_download_again(self):
        self.handler.downloader = Mock()
        fh = self.open_handle()

        with self.fake_http()[2]:
            self.read_through(fh, 1024 * 1024)

        self.handler.downloader.download_file.assert_called_once()

    def test_a_handle_that_started_the_download_stops_appending(self):
        fh = self.open_handle()
        info = self.handler.open_files[fh]

        with (
            patch.object(self.handler.downloader, "download_file"),
            self.fake_http()[2],
        ):
            self.read_through(fh, 1024 * 1024)
            self.assertTrue(info["cache_started"])
            size_at_start = self.cached.stat().st_size
            self.handler.read(PATH, 128 * 1024, 1024 * 1024, fh)

        self.assertGreater(size_at_start, 0)
        self.assertEqual(self.cached.stat().st_size, size_at_start)

    def test_the_download_it_started_leaves_exact_bytes(self):
        fh = self.open_handle()
        head, get, handler_get = self.fake_http()
        with head, get, handler_get:
            self.read_through(fh, 1024 * 1024)
            self.tasks.join()

        self.assertCachedEqualsSource()
        self.assertEqual(self.status.read_text(), "complete:141")


class TestDownloaderOwnership(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.tasks = Mock()
        self.tasks.create_lock.return_value = threading.Lock()
        self.tasks.is_shutdown.return_value = False
        self.downloader = Downloader(
            DownloaderDependencies(
                thread_manager=self.tasks,
                cache_dir=Path(self.temp_dir),
                logger=logging.getLogger("test"),
                update_file_size=Mock(),
            )
        )

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def request(self):
        return DownloadRequest(
            video_id="vid", stream_url=URL, path=PATH, format_id="141"
        )

    def test_the_guard_runs_the_write_when_no_download_is_active(self):
        self.assertEqual(self.downloader.run_unless_downloading("vid", lambda: 7), 7)

    def test_the_guard_runs_the_write_after_a_download_ended(self):
        for status in (
            DownloadStatus.COMPLETE,
            DownloadStatus.FAILED,
            DownloadStatus.STOPPED,
        ):
            self.downloader.active_downloads["vid"] = {"status": status}
            self.assertEqual(
                self.downloader.run_unless_downloading("vid", lambda: 7), 7
            )

    def test_the_guard_refuses_the_write_while_a_download_is_active(self):
        for status in (DownloadStatus.STARTING, DownloadStatus.DOWNLOADING):
            self.downloader.active_downloads["vid"] = {"status": status}
            write = Mock()
            self.assertIsNone(self.downloader.run_unless_downloading("vid", write))
            write.assert_not_called()

    def test_the_guard_ignores_downloads_of_other_tracks(self):
        self.downloader.active_downloads["other"] = {
            "status": DownloadStatus.DOWNLOADING
        }
        self.assertEqual(self.downloader.run_unless_downloading("vid", lambda: 7), 7)

    def test_a_queued_download_claims_the_track(self):
        self.downloader.download_file(self.request())

        self.assertEqual(
            self.downloader.get_progress("vid")["status"], DownloadStatus.STARTING
        )
        self.assertIsNone(self.downloader.run_unless_downloading("vid", lambda: 7))

    def test_a_download_that_could_not_be_queued_gives_the_claim_back(self):
        self.tasks.submit_task.side_effect = RuntimeError("pool is shut down")

        with self.assertRaises(RuntimeError):
            self.downloader.download_file(self.request())

        self.assertEqual(self.downloader.run_unless_downloading("vid", lambda: 7), 7)
        self.tasks.submit_task.side_effect = None
        self.downloader.download_file(self.request())
        self.tasks.submit_task.assert_called()

    def test_a_download_skipped_at_shutdown_gives_the_claim_back(self):
        self.downloader.download_file(self.request())
        self.tasks.is_shutdown.return_value = True

        self.assertFalse(self.downloader._download_task(self.request()))

        self.assertEqual(
            self.downloader.get_progress("vid")["status"], DownloadStatus.STOPPED
        )

    def test_a_download_that_raises_gives_the_claim_back(self):
        with (
            patch(
                "ytmusicfs.downloader.ensure_headers_and_cookies",
                side_effect=OSError("x"),
            ),
            self.assertRaises(OSError),
        ):
            self.downloader.download_file_now(self.request())

        self.assertEqual(self.downloader.run_unless_downloading("vid", lambda: 7), 7)

    def test_a_stopped_download_can_be_requested_again(self):
        self.downloader.active_downloads["vid"] = {"status": DownloadStatus.STOPPED}

        self.downloader.download_file(self.request())

        self.tasks.submit_task.assert_called_once()
