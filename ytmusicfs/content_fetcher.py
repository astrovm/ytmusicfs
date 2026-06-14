#!/usr/bin/env python3

import time
import traceback
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, cast

from ytmusicfs.dependencies import ContentFetcherDependencies, RepairDependencies
from ytmusicfs.models import RefreshStatus, RegistryEntry
from ytmusicfs.yt_dlp_utils import PARTIAL_PLAYLIST_COMPLETE_RATIO


@dataclass(frozen=True)
class RefreshRequest:
    cache_key: str
    fetch_tracks: Callable[[int], list[dict[str, Any]]]
    path: str
    limit: int = 10000
    force: bool = False
    expected_total: Callable[[], int | None] | None = None


class ContentFetcher:
    """Handles fetching and processing of YouTube Music content."""

    PLAYLIST_REGISTRY_CACHE_KEY = "playlist_registry_entries"
    MIN_REGISTRY_RETAIN_RATIO = 0.8

    def __init__(self, dependencies: ContentFetcherDependencies) -> None:
        self.client = dependencies.client
        self.processor = dependencies.processor
        self.cache = dependencies.cache
        self.logger = dependencies.logger
        self.browser = dependencies.browser
        self.yt_dlp_utils = dependencies.yt_dlp
        self.PLAYLIST_REGISTRY: list[RegistryEntry] = []
        self._registry_by_path: dict[str, RegistryEntry] = {}
        self._registry_by_name: dict[str, RegistryEntry] = {}
        self._registry_by_name_type: dict[tuple[str, str], RegistryEntry] = {}
        self._indexed_registry = self.PLAYLIST_REGISTRY
        self.cache_directory_callback: (
            Callable[[str, list[dict[str, Any]]], None] | None
        ) = None
        self._initialize_playlist_registry()
        self.logger.info("Preloaded playlist registry at initialization")

    def get_playlist_id_from_name(
        self, name: str, type_filter: str | None = None
    ) -> str | None:
        """Return a registry ID using the prebuilt lookup indexes."""
        self._ensure_registry_indexes()
        entry = (
            self._registry_by_name.get(name)
            if type_filter is None
            else self._registry_by_name_type.get((name, type_filter))
        )
        entry_id = entry.get("id") if entry else None
        return entry_id if isinstance(entry_id, str) else None

    def get_playlist_entry_from_path(self, path: str) -> RegistryEntry | None:
        """Return a registry entry by its mounted path."""
        self._ensure_registry_indexes()
        return self._registry_by_path.get(path)

    def _ensure_registry_indexes(self) -> None:
        if self.PLAYLIST_REGISTRY is not self._indexed_registry:
            self._set_playlist_registry(self.PLAYLIST_REGISTRY)

    def _set_playlist_registry(self, registry: list[RegistryEntry]) -> None:
        """Replace the registry and rebuild its read-optimized indexes."""
        self.PLAYLIST_REGISTRY = registry
        self._registry_by_path = {
            entry["path"]: entry
            for entry in registry
            if isinstance(entry.get("path"), str)
        }
        self._registry_by_name = {}
        self._registry_by_name_type = {}
        for entry in registry:
            name = entry["name"]
            entry_type = entry["type"]
            self._registry_by_name.setdefault(name, entry)
            self._registry_by_name_type[(name, entry_type)] = entry
        self._indexed_registry = registry

    def _initialize_playlist_registry(self, force_refresh: bool = False) -> None:
        """Initialize or refresh the playlist registry with all playlist types.

        Args:
            force_refresh: Whether to force a refresh even if the registry was recently refreshed
        """
        cache_key = "playlist_registry"
        last_refresh, _status = self.cache.get_refresh_metadata(cache_key)
        refresh_interval = 3600  # 1 hour default

        if (
            not force_refresh
            and self.PLAYLIST_REGISTRY
            and last_refresh
            and (time.time() - last_refresh < refresh_interval)
        ):
            self.logger.debug(
                f"Using existing playlist registry (last refreshed: {int(time.time() - last_refresh)}s ago)"
            )
            return

        cached_data = self.cache.get(self.PLAYLIST_REGISTRY_CACHE_KEY)
        cached_registry = (
            [
                cast("RegistryEntry", entry)
                for entry in cached_data
                if isinstance(entry, dict)
                and all(
                    isinstance(entry.get(key), str)
                    for key in ("name", "id", "type", "path")
                )
            ]
            if isinstance(cached_data, list)
            else self._registry_from_cached_root_listings()
        )

        registry: list[RegistryEntry] = [
            {
                "name": "liked_songs",
                "id": "LM",
                "type": "liked_songs",
                "path": "/liked_songs",
            }
        ]

        playlists = self.client.get_library_playlists(limit=1000)
        for p in playlists:
            playlist_id = p.get("playlistId")
            if not playlist_id:
                self.logger.warning("Skipping playlist without playlistId: %s", p)
                continue
            if playlist_id == "SE":
                self.logger.info(
                    "Skipping podcast playlist (SE) - podcasts not supported"
                )
                continue

            sanitized_name = self._sanitize_registry_name(
                p["title"], "playlist", playlist_id
            )
            path = f"/playlists/{sanitized_name}"
            registry.append(
                {
                    "name": sanitized_name,
                    "id": playlist_id,
                    "type": "playlist",
                    "path": path,
                }
            )

        albums = self.client.get_library_albums(limit=1000)
        for a in albums:
            album_id = a.get("browseId")
            if not album_id:
                self.logger.warning("Skipping album without browseId: %s", a)
                continue
            sanitized_name = self._sanitize_registry_name(a["title"], "album", album_id)
            path = f"/albums/{sanitized_name}"
            registry.append(
                {
                    "name": sanitized_name,
                    "id": album_id,
                    "type": "album",
                    "path": path,
                }
            )

        if self._is_suspiciously_partial_registry(registry, cached_registry):
            self.logger.warning(
                "Fetched playlist registry has %d entries vs %d cached; using cached registry",
                len(registry),
                len(cached_registry),
            )
            registry = cached_registry
        else:
            self.cache.set(self.PLAYLIST_REGISTRY_CACHE_KEY, registry)

        self._set_playlist_registry(registry)
        self.logger.info("Initialized playlist registry with %d entries", len(registry))

        self.cache.set_refresh_metadata(cache_key, time.time(), "fresh")

    def _is_suspiciously_partial_registry(
        self, registry: list[RegistryEntry], cached_registry: list[Any]
    ) -> bool:
        cached_entries = [entry for entry in cached_registry if isinstance(entry, dict)]
        if not cached_entries:
            return False
        if len(registry) < len(cached_entries) * self.MIN_REGISTRY_RETAIN_RATIO:
            return True

        cached_paths = {
            str(entry.get("path"))
            for entry in cached_entries
            if entry.get("type") in {"playlist", "album"} and entry.get("path")
        }
        current_paths = {
            str(entry.get("path"))
            for entry in registry
            if entry.get("type") in {"playlist", "album"} and entry.get("path")
        }
        if not cached_paths:
            return False
        missing_ratio = len(cached_paths - current_paths) / len(cached_paths)
        return missing_ratio > (1 - self.MIN_REGISTRY_RETAIN_RATIO)

    def _registry_from_cached_root_listings(self) -> list[RegistryEntry]:
        registry: list[RegistryEntry] = [
            {
                "name": "liked_songs",
                "id": "LM",
                "type": "liked_songs",
                "path": "/liked_songs",
            }
        ]
        for root_path, entry_type, id_key in (
            ("/playlists", "playlist", "playlistId"),
            ("/albums", "album", "browseId"),
        ):
            listing = self.cache.get_directory_listing_with_attrs(root_path)
            if not isinstance(listing, dict):
                continue
            for name, attrs in listing.items():
                if name in (".", "..") or not isinstance(attrs, dict):
                    continue
                entry_id = attrs.get(id_key) or attrs.get("id")
                if not entry_id:
                    continue
                registry.append(
                    {
                        "name": name,
                        "id": entry_id,
                        "type": entry_type,
                        "path": f"{root_path}/{name}",
                    }
                )
        return registry

    def _sanitize_registry_name(self, title: str, item_type: str, item_id: str) -> str:
        sanitized_name = self.processor.sanitize_filename(title)
        if sanitized_name:
            return sanitized_name

        fallback = self.processor.sanitize_filename(f"{item_type}_{item_id}")
        self.logger.warning(
            "Using fallback name %s for %s with filesystem-reserved title: %r",
            fallback,
            item_type,
            title,
        )
        return fallback

    def fetch_playlist_content(
        self,
        playlist_id: str | None,
        path: str,
        limit: int = 10000,
        force_refresh: bool = False,
    ) -> list[str]:
        """Fetch playlist content with a specified limit and cache durations.

        Args:
            playlist_id: Playlist ID (e.g., 'PL123', 'LM', 'MPREb_abc123')
            path: Filesystem path for caching
            limit: Maximum number of tracks to fetch (default: 10000)
            force_refresh: If True, fetch fresh data and merge with existing cache (default: False)

        Returns:
            List of track filenames
        """
        if playlist_id == "SE":
            self.logger.info("Skipping podcast playlist (SE) - podcasts not supported")
            return []

        # All playlist types share one processed-cache naming scheme.
        cache_key = f"{path}_processed"
        if not playlist_id:
            self._initialize_playlist_registry(force_refresh=True)
            playlist_entry = self.get_playlist_entry_from_path(path)
            if playlist_entry:
                return self.fetch_playlist_content(
                    playlist_entry["id"], path, limit, force_refresh
                )
            existing_tracks = self.cache.get(cache_key)
            if not isinstance(existing_tracks, list):
                existing_tracks = []
            if existing_tracks:
                self.logger.warning("Missing playlist ID for %s, using cache", path)
                self._cache_directory_listing_with_attrs(path, existing_tracks)
                return [track["filename"] for track in existing_tracks]
            self.logger.error("Missing playlist ID for %s", path)
            return []

        expected_total: int | None = None

        def fetch_tracks(lim: int) -> list[dict[str, Any]]:
            nonlocal expected_total
            if path.startswith("/albums/"):
                result = self.client.get_album(playlist_id)
                expected_total = self._api_track_count(result)
                return self._api_track_entries(result, lim)
            return self._fetch_playlist_tracks(playlist_id, lim)

        def get_expected_total() -> int | None:
            if path.startswith("/albums/"):
                return expected_total
            return self._get_expected_total_count(playlist_id)

        tracks = self.refresh_content(
            RefreshRequest(
                cache_key=cache_key,
                fetch_tracks=fetch_tracks,
                path=path,
                limit=limit,
                force=force_refresh,
                expected_total=get_expected_total,
            )
        )

        return [track["filename"] for track in tracks]

    def refresh_liked_songs_automatic(self) -> None:
        """Refresh liked songs from YouTube Music outside FUSE request handling."""
        entry = next(
            (p for p in self.PLAYLIST_REGISTRY if p["type"] == "liked_songs"), None
        )
        if not entry:
            self.logger.error("Liked songs not found")
            return

        self.logger.info("Refreshing liked songs in background")
        self.refresh_content(
            RefreshRequest(
                cache_key=f"{entry['path']}_processed",
                fetch_tracks=lambda limit: self._fetch_playlist_tracks(
                    entry["id"], limit
                ),
                path=entry["path"],
                force=True,
                expected_total=lambda: self._get_expected_total_count(entry["id"]),
            )
        )
        self._repair_unavailable_liked_songs_locally()

    def readdir_playlist_by_type(
        self, playlist_type: str | None = None, directory_path: str | None = None
    ) -> list[str]:
        """List playlists/albums/liked_songs instantly using cached data."""
        if not directory_path:
            directory_path = {
                "playlist": "/playlists",
                "album": "/albums",
                "liked_songs": "/liked_songs",
            }.get(playlist_type or "", "")
            if not directory_path:
                self.logger.error(f"Invalid playlist type: {playlist_type}")
                return [".", ".."]

        cache_key = f"{directory_path}_listing"
        cached_listing = self.cache.get_directory_listing_with_attrs(directory_path)
        # Liked songs use the shorter content TTL, not the root-listing TTL.
        if cached_listing and playlist_type in ("playlist", "album"):
            self.logger.debug(f"Instant cache hit for {directory_path}")
            return [".", "..", *self._filter_unavailable_listing(cached_listing)]

        if playlist_type == "liked_songs":
            tracks = self.cache.get("/liked_songs_processed")
            if not isinstance(tracks, list):
                self.logger.info("Liked songs cache is empty; waiting for refresh")
                return [".", ".."]
            self._cache_directory_listing_with_attrs("/liked_songs", tracks)
            return [".", ".."] + [
                track["filename"]
                for track in tracks
                if not self._is_track_unavailable(track)
            ]

        entries = [p for p in self.PLAYLIST_REGISTRY if p["type"] == playlist_type]
        if not entries:
            self.logger.warning(f"No {playlist_type} entries found")
            return [".", ".."]

        processed_entries = []
        for entry in entries:
            processed_entry = {
                "filename": entry["name"],
                "is_directory": True,
                "id": entry["id"],
            }
            if entry["type"] == "playlist":
                processed_entry["playlistId"] = entry["id"]
            elif entry["type"] == "album":
                processed_entry["browseId"] = entry["id"]
            processed_entries.append(processed_entry)
        self._cache_directory_listing_with_attrs(directory_path, processed_entries)
        self.cache.set_refresh_metadata(cache_key, time.time(), "fresh")
        return [".", ".."] + [e["name"] for e in entries]

    def _filter_unavailable_listing(
        self, listing: dict[str, dict[str, Any]]
    ) -> dict[str, dict[str, Any]]:
        unavailable_ids = self.cache.get_unavailable_video_ids()
        return {
            filename: attrs
            for filename, attrs in listing.items()
            if not attrs.get("videoId") or attrs["videoId"] not in unavailable_ids
        }

    def _is_track_unavailable(self, track: dict[str, Any]) -> bool:
        video_id = track.get("videoId")
        return bool(video_id and video_id in self.cache.get_unavailable_video_ids())

    def _repair_unavailable_liked_songs_locally(self) -> bool:
        if not any(
            str(track.get("path", "")).startswith("/liked_songs/")
            for track in self.cache.get_unavailable_tracks()
        ):
            return False

        from ytmusicfs.repair import LikedSongsRepairer

        stats = LikedSongsRepairer(
            RepairDependencies(
                client=self.client,
                cache=self.cache,
                processor=self.processor,
                yt_dlp=self.yt_dlp_utils,
                browser=self.browser,
                logger=self.logger,
            )
        ).repair()
        return stats["repaired"] > 0

    def _fetch_playlist_tracks(
        self, playlist_id: str, limit: int
    ) -> list[dict[str, Any]]:
        return self.yt_dlp_utils.extract_playlist_content(
            playlist_id, limit, self.browser
        )

    @staticmethod
    def _api_track_count(result: object) -> int | None:
        count = result.get("trackCount") if isinstance(result, dict) else None
        return count if isinstance(count, int) else None

    @staticmethod
    def _api_track_entries(result: object, limit: int) -> list[dict[str, Any]]:
        if not isinstance(result, dict):
            return []

        album = result.get("title") if isinstance(result.get("title"), str) else None
        year = result.get("year")
        tracks = result.get("tracks", [])
        if not isinstance(tracks, list):
            return []

        entries = []
        for track in tracks[:limit]:
            if not isinstance(track, dict) or not track.get("videoId"):
                continue
            entry = dict(track)
            if album and "album" not in entry:
                entry["album"] = album
            if year and "year" not in entry:
                entry["year"] = year
            entries.append(entry)
        return entries

    def _get_expected_total_count(self, playlist_id: str) -> int | None:
        if not playlist_id:
            return None
        total_count = self.yt_dlp_utils.get_last_playlist_total_count(playlist_id)
        return total_count if isinstance(total_count, int) else None

    def _process_track_entry(self, entry: dict[str, Any]) -> dict[str, Any] | None:
        video_id = entry.get("videoId") or entry.get("id")
        if not video_id:
            return None

        track_info = dict(entry)
        track_info["videoId"] = video_id

        duration = entry.get("duration")
        if "duration_seconds" not in track_info and isinstance(duration, int):
            track_info["duration_seconds"] = duration

        if "artist" not in track_info and "artists" not in track_info:
            track_info["artist"] = entry.get("uploader", "Unknown Artist")

        processed_track = self.processor.extract_track_info(track_info)
        processed_track["filename"] = self.processor.sanitize_filename(
            f"{processed_track['artist']} - {processed_track['title']}.m4a"
        )
        processed_track["is_directory"] = False
        return processed_track

    def _cache_directory_listing_with_attrs(
        self, dir_path: str, processed_tracks: list[dict[str, Any]]
    ) -> None:
        """Cache directory listing with file attributes for efficient lookups.

        Args:
            dir_path: Directory path
            processed_tracks: List of processed track dictionaries
        """
        if self.cache_directory_callback:
            self.cache_directory_callback(dir_path, processed_tracks)
        else:
            self.logger.warning(
                "No callback set for caching directory listings with attributes"
            )

    def refresh_content(self, request: RefreshRequest) -> list[dict[str, Any]]:
        """Return cached tracks or refresh and persist a complete usable listing."""
        existing_tracks = self._cached_tracks(request.cache_key)
        if existing_tracks and not request.force:
            return self._serve_cached_tracks(request, existing_tracks)

        self.logger.info("Refreshing content for %s", request.path)
        self._set_refresh_status(request.cache_key, RefreshStatus.PENDING)
        try:
            return self._refresh_tracks(request, existing_tracks)
        except Exception as error:
            self.logger.error("Refresh failed for %s: %s", request.path, error)
            self.logger.error(traceback.format_exc())
            self._set_refresh_status(request.cache_key, RefreshStatus.STALE)
            return existing_tracks

    def _cached_tracks(self, cache_key: str) -> list[dict[str, Any]]:
        tracks = self.cache.get(cache_key)
        return tracks if isinstance(tracks, list) else []

    def _serve_cached_tracks(
        self, request: RefreshRequest, tracks: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        last_refresh, _ = self.cache.get_refresh_metadata(request.cache_key)
        age = (
            "unknown" if not last_refresh else f"{int(time.time() - last_refresh)}s ago"
        )
        self.logger.debug(
            "Using %d cached tracks for %s (last refresh: %s)",
            len(tracks),
            request.path,
            age,
        )
        for track in tracks:
            track["is_directory"] = False
        self._cache_directory_listing_with_attrs(request.path, tracks)
        return tracks

    def _refresh_tracks(
        self, request: RefreshRequest, existing_tracks: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        fetched_tracks = request.fetch_tracks(request.limit)
        expected_total = request.expected_total() if request.expected_total else None
        is_partial = self._is_partial_fetch(fetched_tracks, expected_total)
        self.logger.info("Fetched %d items for %s", len(fetched_tracks), request.path)

        if is_partial:
            self.logger.warning(
                "Partial fetch returned %d of %d tracks for %s",
                len(fetched_tracks),
                expected_total,
                request.path,
            )
        if not fetched_tracks and existing_tracks:
            return self._keep_cached_tracks(request, existing_tracks, "No content")

        new_tracks, durations = self._process_track_entries(fetched_tracks)
        if self._should_keep_cached(existing_tracks, new_tracks, is_partial):
            return self._keep_cached_tracks(
                request, existing_tracks, f"Partial fetch returned {len(new_tracks)}"
            )

        if durations:
            self.cache.set_durations_batch(durations)
        result_tracks = (
            self._merge_tracks(new_tracks, existing_tracks)
            if existing_tracks and is_partial
            else new_tracks
        )
        if existing_tracks and is_partial:
            self.logger.info(
                "Merged %d fetched tracks with %d cached tracks for %s",
                len(new_tracks),
                len(existing_tracks),
                request.path,
            )
        self._store_refreshed_tracks(request, result_tracks, is_partial)
        return result_tracks

    @staticmethod
    def _is_partial_fetch(
        tracks: list[dict[str, Any]], expected_total: int | None
    ) -> bool:
        return bool(
            expected_total
            and len(tracks) < expected_total * PARTIAL_PLAYLIST_COMPLETE_RATIO
        )

    def _process_track_entries(
        self, entries: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], dict[str, int]]:
        tracks = []
        durations = {}
        for entry in entries:
            if not entry:
                continue
            video_id = entry.get("videoId") or entry.get("id")
            duration = entry.get("duration")
            if video_id and isinstance(duration, int):
                durations[video_id] = duration
            processed = self._process_track_entry(entry)
            if processed:
                tracks.append(processed)
        return tracks, durations

    @staticmethod
    def _should_keep_cached(
        existing_tracks: list[dict[str, Any]],
        new_tracks: list[dict[str, Any]],
        is_partial: bool,
    ) -> bool:
        return bool(
            existing_tracks and is_partial and len(new_tracks) < len(existing_tracks)
        )

    def _keep_cached_tracks(
        self,
        request: RefreshRequest,
        existing_tracks: list[dict[str, Any]],
        reason: str,
    ) -> list[dict[str, Any]]:
        self.logger.warning(
            "%s for %s; keeping %d cached tracks",
            reason,
            request.path,
            len(existing_tracks),
        )
        self._set_refresh_status(request.cache_key, RefreshStatus.STALE)
        return existing_tracks

    def _store_refreshed_tracks(
        self,
        request: RefreshRequest,
        tracks: list[dict[str, Any]],
        is_partial: bool,
    ) -> None:
        self.cache.set(request.cache_key, tracks)
        self._cache_directory_listing_with_attrs(request.path, tracks)
        status = RefreshStatus.STALE if is_partial else RefreshStatus.FRESH
        self._set_refresh_status(request.cache_key, status)

    def _set_refresh_status(self, cache_key: str, status: RefreshStatus) -> None:
        self.cache.set_refresh_metadata(cache_key, time.time(), status.value)

    def _merge_tracks(
        self, new_tracks: list[dict[str, Any]], existing_tracks: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        merged = []
        seen_ids = set()

        for track in [*new_tracks, *existing_tracks]:
            video_id = track.get("videoId")
            if video_id:
                if video_id in seen_ids:
                    continue
                seen_ids.add(video_id)
            merged.append(track)

        return merged
