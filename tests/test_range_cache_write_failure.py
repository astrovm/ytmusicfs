#!/usr/bin/env python3

"""A full cache disk must not fail a read whose bytes were already fetched.

The range cache only speeds up later reads. When writing it fails (ENOSPC, a
read-only disk), the read must still return the bytes it downloaded, and the
failure must not leave a half-written part that later reads could trust.
"""

import errno
import logging
import random
import shutil
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from ytmusicfs.dependencies import FileHandlerDependencies
from ytmusicfs.file_handler import FileHandler

SOURCE = b"\x00\x00\x00\x20ftypM4A \x00\x00\x02\x00" + random.Random(3).randbytes(
    1024 * 1024
)
PATH = "/liked_songs/a.m4a"
URL = "http://fake.invalid/a"


class FakeResponse:
    status_code = 206

    def __init__(self, data):
        self.data = data
        self.headers = {"Content-Length": str(len(data))}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def iter_content(self, chunk_size=65536):
        yield self.data


def fake_http_get(_url, headers=None, **_kwargs):
    first, _, last = headers["Range"].removeprefix("bytes=").partition("-")
    return FakeResponse(SOURCE[int(first) : int(last) + 1])


def disk_full(self, data):
    if self.suffix in {".part", ".tmp"}:
        raise OSError(errno.ENOSPC, "No space left on device")
    return original_write_bytes(self, data)


original_write_bytes = Path.write_bytes


class TestRangeCacheWriteFailure(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir)
        tasks = Mock()
        tasks.create_lock.side_effect = threading.RLock
        cache = Mock()
        cache.get_unavailable_track.return_value = None
        cache.is_track_unavailable.return_value = False
        self.handler = FileHandler(
            FileHandlerDependencies(
                thread_manager=tasks,
                cache_dir=self.cache_dir,
                cache=cache,
                logger=logging.getLogger("test"),
                update_file_size=Mock(),
                yt_dlp=Mock(),
                browser="",
            )
        )
        self.fh = self.handler.open(PATH, "vid")
        self.handler.open_files[self.fh].update(
            stream_url=URL, format_id="141", headers={}, cookies=None
        )
        self.parts = self.cache_dir / FileHandler.RANGE_CACHE_DIR / "141" / "vid"

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def read(self, offset=0, size=4096):
        with patch("ytmusicfs.file_handler.http_get", fake_http_get):
            return self.handler.read(PATH, size, offset, self.fh)

    def leftovers(self):
        return (
            sorted(p.name for p in self.parts.glob("*")) if self.parts.exists() else []
        )

    def test_a_read_returns_its_bytes_when_the_range_cache_is_full(self):
        with patch.object(Path, "write_bytes", disk_full):
            data = self.read()

        self.assertEqual(data, SOURCE[:4096])

    def test_a_failed_range_write_leaves_no_part_behind(self):
        with patch.object(Path, "write_bytes", disk_full):
            self.read()

        self.assertEqual(self.leftovers(), [])

    def test_the_audio_cache_still_gets_the_bytes_when_the_range_cache_is_full(self):
        with patch.object(Path, "write_bytes", disk_full):
            self.read()

        cached = self.cache_dir / "audio" / "vid.m4a"
        self.assertEqual(
            cached.read_bytes(), SOURCE[: FileHandler.READAHEAD_INITIAL_BYTES]
        )

    def test_the_range_is_cached_again_once_there_is_space(self):
        with patch.object(Path, "write_bytes", disk_full):
            self.read()
        self.read(offset=FileHandler.READAHEAD_INITIAL_BYTES * 2)

        self.assertEqual(len(self.leftovers()), 1)
        self.assertTrue(self.leftovers()[0].endswith(".part"))

    def test_a_successful_range_write_leaves_only_the_part(self):
        self.read()

        end = FileHandler.READAHEAD_INITIAL_BYTES
        self.assertEqual(self.leftovers(), [f"0-{end}.part"])
        self.assertEqual((self.parts / f"0-{end}.part").read_bytes(), SOURCE[:end])
