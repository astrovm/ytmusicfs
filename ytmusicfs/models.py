from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, TypedDict

if TYPE_CHECKING:
    import threading


class EntryType(str, Enum):
    FILE = "file"
    DIRECTORY = "directory"


class RefreshStatus(str, Enum):
    FRESH = "fresh"
    PENDING = "pending"
    STALE = "stale"


class DownloadStatus(str, Enum):
    STARTING = "starting"
    DOWNLOADING = "downloading"
    COMPLETE = "complete"
    FAILED = "failed"
    STOPPED = "stopped"


class PlaylistType(str, Enum):
    PLAYLIST = "playlist"
    ALBUM = "album"
    LIKED_SONGS = "liked_songs"


class RegistryEntry(TypedDict):
    name: str
    id: str
    type: str
    path: str


class TrackData(TypedDict, total=False):
    title: str
    artist: str
    artists: list[dict[str, Any]]
    album: str | dict[str, Any]
    album_artist: str
    duration: str | int
    duration_seconds: int | None
    duration_formatted: str
    trackNumber: int
    track_number: int
    year: int
    genre: str
    videoId: str
    id: str
    filename: str
    uploader: str
    is_directory: bool
    is_new_duration: bool


class StreamResult(TypedDict, total=False):
    status: str
    error: str
    stream_url: str
    format_id: str
    http_headers: dict[str, str]
    cookies: dict[str, str]
    duration: int


class FileHandleState(TypedDict, total=False):
    cache_path: str
    video_id: str
    stream_url: str | None
    format_id: str | None
    headers: dict[str, str] | None
    cookies: dict[str, str] | None
    status: str
    error: str | None
    path: str
    bytes_read: int
    cache_started: bool
    stream_extracted: bool
    read_calls: int
    requested_bytes: int
    read_ranges: list[dict[str, int]]
    opened_at: float
    initialized_event: threading.Event


class DownloadProgress(TypedDict, total=False):
    status: str
    progress: int
    total: int
    stop_requested: bool


@dataclass(frozen=True)
class DownloadRequest:
    video_id: str
    stream_url: str
    path: str
    format_id: str
    headers: dict[str, Any] | None = None
    cookies: dict[str, Any] | None = None
    retries: int = 3
    chunk_size: int = 8192
