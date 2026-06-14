from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import logging
    from pathlib import Path

    from ytmusicfs.protocols import (
        ContentCacheProtocol,
        FileCacheProtocol,
        FileSizeCallback,
        FileSizeLookup,
        MusicClientProtocol,
        StatCallback,
        ThreadManagerProtocol,
        TrackProcessorProtocol,
        UnavailableCallback,
        YTDLPProtocol,
    )


@dataclass(frozen=True)
class DownloaderDependencies:
    thread_manager: ThreadManagerProtocol
    cache_dir: Path
    logger: logging.Logger
    update_file_size: FileSizeCallback


@dataclass(frozen=True)
class FileHandlerDependencies:
    thread_manager: ThreadManagerProtocol
    cache_dir: Path
    cache: FileCacheProtocol
    logger: logging.Logger
    update_file_size: FileSizeCallback
    yt_dlp: YTDLPProtocol
    browser: str
    record_stat: StatCallback | None = None
    get_file_size: FileSizeLookup | None = None
    on_stream_unavailable: UnavailableCallback | None = None


@dataclass(frozen=True)
class ContentFetcherDependencies:
    client: MusicClientProtocol
    processor: TrackProcessorProtocol
    cache: ContentCacheProtocol
    logger: logging.Logger
    yt_dlp: YTDLPProtocol
    browser: str


@dataclass(frozen=True)
class RepairDependencies:
    client: MusicClientProtocol
    cache: ContentCacheProtocol
    processor: TrackProcessorProtocol
    yt_dlp: YTDLPProtocol
    browser: str
    sync_account: bool = False
    logger: logging.Logger | None = None
