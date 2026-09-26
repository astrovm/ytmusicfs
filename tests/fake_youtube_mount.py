#!/usr/bin/env python3
"""Mount YTMusicFS for real, with YouTube replaced by local fakes.

Run as ``python -m tests.fake_youtube_mount MOUNT_POINT CACHE_DIR AUDIO_FILE``.
The audio file is served over HTTP as every track's stream, so the end-to-end
tests can compare what they read through the mount with the source bytes.
"""

import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from unittest.mock import patch

from ytmusicfs import filesystem
from ytmusicfs.yt_dlp_utils import YTDLPUtils

TRACKS = [
    {"id": f"vid{index:08d}", "title": f"Song {index}", "uploader": "Artist"}
    for index in range(3)
]


def serve_audio(audio: bytes) -> str:
    """Serve audio with HTTP range support and return its URL."""

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args: Any) -> None:
            pass

        def _range(self) -> tuple[int, int, int]:
            header = self.headers.get("Range")
            if not header:
                return 0, len(audio) - 1, 200
            first, last = header.removeprefix("bytes=").split("-")
            start = int(first)
            end = min(int(last) if last else len(audio) - 1, len(audio) - 1)
            return start, end, 206 if start < len(audio) else 416

        def do_HEAD(self) -> None:
            start, end, status = self._range()
            self.send_response(status)
            self.send_header("Content-Length", str(max(end - start + 1, 0)))
            self.end_headers()

        def do_GET(self) -> None:
            start, end, status = self._range()
            body = audio[start : end + 1] if status != 416 else b""
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            if status == 206:
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(audio)}")
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return f"http://127.0.0.1:{server.server_port}/audio.m4a"


class FakeClient:
    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        pass

    def get_library_playlists(self, limit: int = 100) -> list[dict[str, str]]:
        return [
            {"title": "Mix", "playlistId": "PL1"},
            {"title": "Road Trip", "playlistId": "PL2"},
        ]

    def get_library_albums(self, limit: int = 100) -> list[dict[str, str]]:
        return []


def main() -> None:
    mount_point, cache_dir, audio_file = sys.argv[1:4]
    url = serve_audio(Path(audio_file).read_bytes())

    def fake_stream(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {"stream_url": url, "http_headers": {}, "format_id": "141"}

    with (
        patch.object(filesystem, "YouTubeMusicClient", FakeClient),
        patch.object(filesystem, "YTMusicAuthAdapter", lambda **_kwargs: None),
        patch.object(
            YTDLPUtils, "extract_playlist_content", lambda *_args, **_kw: TRACKS
        ),
        patch.object(YTDLPUtils, "extract_stream_url", fake_stream),
        patch.object(YTDLPUtils, "get_last_playlist_total_count", lambda *_args: None),
    ):
        filesystem.mount_ytmusicfs(
            mount_point, cache_dir=cache_dir, foreground=True, browser="brave"
        )


if __name__ == "__main__":
    main()
