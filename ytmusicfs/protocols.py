from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    import builtins
    import threading
    from concurrent.futures import Future


class ThreadManagerProtocol(Protocol):
    def create_lock(self) -> threading.RLock: ...

    def submit_task(
        self, pool_name: str, fn: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> Future[Any]: ...


class FileCacheProtocol(Protocol):
    def get_unavailable_track(self, video_id: str) -> dict[str, Any] | None: ...

    def is_track_unavailable(self, video_id: str) -> bool: ...

    def mark_unavailable_track(
        self, video_id: str, path: str | None, reason: str
    ) -> None: ...

    def set_durations_batch(self, durations: dict[str, int]) -> None: ...


class ContentCacheProtocol(Protocol):
    def get(self, path: str) -> Any | None: ...

    def set(self, key: str, value: Any) -> None: ...

    def get_directory_listing_with_attrs(
        self, path: str
    ) -> dict[str, dict[str, Any]] | None: ...

    def get_refresh_metadata(self, key: str) -> tuple[float | None, str | None]: ...

    def set_refresh_metadata(self, key: str, timestamp: float, status: str) -> None: ...

    def get_unavailable_tracks(self) -> list[dict[str, Any]]: ...

    def get_unavailable_video_ids(self) -> builtins.set[str]: ...

    def set_durations_batch(self, durations: dict[str, int]) -> None: ...

    def clear_unavailable_track(
        self, video_id: str, path: str | None = None
    ) -> None: ...

    def delete(self, path: str) -> None: ...

    def is_no_replacement(self, video_id: str) -> bool: ...

    def mark_no_replacement(
        self, video_id: str, path: str, ttl: int = 86400
    ) -> None: ...

    def record_repair_trigger(self, repairs: list[dict[str, Any]]) -> None: ...


class MusicClientProtocol(Protocol):
    def get_library_playlists(self, limit: int = 1000) -> list[dict[Any, Any]]: ...

    def get_library_albums(self, limit: int = 1000) -> list[dict[Any, Any]]: ...

    def get_album(self, browse_id: str) -> dict[Any, Any]: ...

    def rate_song(self, video_id: str, rating: str) -> dict[Any, Any] | None: ...

    def search(
        self,
        query: str,
        filter_type: str | None = None,
        scope: str | None = None,
        limit: int = 100,
        ignore_spelling: bool = False,
    ) -> list[dict[Any, Any]]: ...


class TrackProcessorProtocol(Protocol):
    def sanitize_filename(self, name: str) -> str: ...

    def extract_track_info(self, track: dict[str, Any]) -> dict[str, Any]: ...


class YTDLPProtocol(Protocol):
    def extract_playlist_content(
        self, playlist_id: str, limit: int, browser: str
    ) -> list[dict[str, Any]]: ...

    def get_last_playlist_total_count(self, playlist_id: str) -> int | None: ...

    def extract_stream_url_async(
        self, video_id: str, browser: str
    ) -> Future[object]: ...

    def extract_stream_url(self, video_id: str, browser: str) -> dict[str, Any]: ...


DirectoryCacheCallback = Callable[[str, list[dict[str, Any]]], None]
UnavailableCallback = Callable[[str, str], str | None]
FileSizeCallback = Callable[[str, int], None]
FileSizeLookup = Callable[[str], int | None]
StatCallback = Callable[[str], None]
