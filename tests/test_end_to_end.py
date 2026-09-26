#!/usr/bin/env python3
"""Mount the filesystem through FUSE and use it like a music player would."""

import os
import random
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

MOUNT_TIMEOUT_SECONDS = 20
UNMOUNT_TIMEOUT_SECONDS = 5
# An M4A header followed by enough data to need several streamed ranges.
AUDIO = b"\x00\x00\x00\x20ftypM4A \x00\x00\x02\x00" + random.Random(7).randbytes(
    3 * 1024 * 1024
)


def _fuse_unavailable_reason() -> str | None:
    if not os.access("/dev/fuse", os.R_OK | os.W_OK):
        return "/dev/fuse is not accessible"
    if not shutil.which("fusermount") and not shutil.which("fusermount3"):
        return "fusermount is not installed"
    return None


FUSE_UNAVAILABLE = _fuse_unavailable_reason()
if FUSE_UNAVAILABLE and os.environ.get("YTMUSICFS_REQUIRE_FUSE"):
    raise RuntimeError(f"End-to-end tests are required but {FUSE_UNAVAILABLE}")


@unittest.skipIf(FUSE_UNAVAILABLE, str(FUSE_UNAVAILABLE))
class TestYouTubeMusicFSEndToEnd(unittest.TestCase):
    """One real mount, shared by every test, backed by a local fake YouTube."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_dir = Path(tempfile.mkdtemp())
        cls.mount_point = cls.temp_dir / "mnt"
        cls.mount_point.mkdir()
        audio_file = cls.temp_dir / "audio.m4a"
        audio_file.write_bytes(AUDIO)
        cls.log = (cls.temp_dir / "mount.log").open("w")
        cls.process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "tests.fake_youtube_mount",
                str(cls.mount_point),
                str(cls.temp_dir / "cache"),
                str(audio_file),
            ],
            cwd=Path(__file__).resolve().parent.parent,
            stdout=cls.log,
            stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + MOUNT_TIMEOUT_SECONDS
        while not os.path.ismount(cls.mount_point):
            if cls.process.poll() is not None or time.monotonic() > deadline:
                cls._unmount()
                output = (cls.temp_dir / "mount.log").read_text()
                raise RuntimeError(f"Mount did not come up:\n{output}")
            time.sleep(0.05)

    @classmethod
    def tearDownClass(cls) -> None:
        cls._unmount()
        shutil.rmtree(cls.temp_dir, ignore_errors=True)

    @classmethod
    def _unmount(cls) -> None:
        if os.path.ismount(cls.mount_point):
            fusermount = shutil.which("fusermount") or "fusermount3"
            subprocess.run([fusermount, "-u", str(cls.mount_point)], check=False)
        try:
            # Unmounting must not wait on background downloads or pre-caching.
            cls.process.wait(timeout=UNMOUNT_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            cls.process.kill()
            cls.process.wait()
            raise AssertionError(
                f"Mount process still running {UNMOUNT_TIMEOUT_SECONDS}s after unmount"
            ) from None
        finally:
            cls.log.close()

    def _track(self, index: int, playlist: str = "Mix") -> Path:
        return self.mount_point / "playlists" / playlist / f"Artist - Song {index}.m4a"

    def test_root_lists_library_sections(self):
        entries = set(os.listdir(self.mount_point))

        self.assertTrue({"playlists", "liked_songs", "albums"} <= entries, entries)

    def test_playlist_lists_its_tracks(self):
        entries = sorted(os.listdir(self.mount_point / "playlists" / "Mix"))

        self.assertEqual(entries, [self._track(i).name for i in range(3)])

    def test_sequential_read_returns_whole_stream(self):
        self.assertEqual(self._track(0).read_bytes(), AUDIO)
        self.assertEqual(self._track(0).stat().st_size, len(AUDIO))

    def test_seek_read_returns_requested_range(self):
        offset = 2 * 1024 * 1024 + 123
        with self._track(1).open("rb") as track:
            track.seek(offset)
            self.assertEqual(track.read(4096), AUDIO[offset : offset + 4096])
            track.seek(0)
            self.assertEqual(track.read(16), AUDIO[:16])

    def test_track_opens_by_path_before_its_playlist_is_listed(self):
        # Nothing else lists "Road Trip", like a player resuming a saved queue.
        self.assertEqual(self._track(2, "Road Trip").read_bytes()[:16], AUDIO[:16])

    def test_missing_playlist_raises_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            (self.mount_point / "playlists" / "Missing").stat()

    def test_missing_track_raises_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            (self.mount_point / "playlists" / "Mix" / "Nobody - Nothing.m4a").stat()


if __name__ == "__main__":
    unittest.main()
