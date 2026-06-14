#!/usr/bin/env python3

import logging
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ytmusicfs.dependencies import RepairDependencies


@dataclass(frozen=True)
class LikedSongRepair:
    path: str
    old_video_id: str
    new_video_id: str
    old_track: dict[str, Any] | None
    replacement: dict[str, Any]


class LikedSongsRepairer:
    """Repair unavailable cached track video IDs with verified replacements."""

    def __init__(self, dependencies: RepairDependencies) -> None:
        self.client = dependencies.client
        self.cache = dependencies.cache
        self.processor = dependencies.processor
        self.yt_dlp_utils = dependencies.yt_dlp
        self.browser = dependencies.browser
        self.sync_account = dependencies.sync_account
        self.logger = dependencies.logger or logging.getLogger("YTMusicFS")

    def repair(self) -> dict[str, int]:
        repairs, dead_tracks, stats = self.plan_repairs()
        stats["repaired"] = self.apply_repairs(repairs)
        stats["removed"] = self.apply_removals(dead_tracks)
        return stats

    def plan_repairs(
        self,
    ) -> tuple[list[LikedSongRepair], list[tuple[str, str]], dict[str, int]]:
        stats = {"checked": 0, "repaired": 0, "skipped": 0, "failed": 0, "removed": 0}
        repairs: list[LikedSongRepair] = []
        dead_tracks: list[tuple[str, str]] = []
        unavailable_tracks = [
            track
            for track in self.cache.get_unavailable_tracks()
            if str(track.get("path", "")).startswith("/liked_songs/")
        ]

        for unavailable in unavailable_tracks:
            stats["checked"] += 1
            try:
                video_id = str(unavailable.get("videoId") or "")
                known_no_replacement = self.cache.is_no_replacement(video_id)
                repair = self._plan_one(unavailable)
                if repair:
                    repairs.append(repair)
                else:
                    if known_no_replacement:
                        dead_tracks.append(
                            (video_id, str(unavailable.get("path") or ""))
                        )
                    else:
                        stats["skipped"] += 1
            except Exception as exc:
                stats["failed"] += 1
                self.logger.warning(
                    "Failed to repair liked song %s: %s",
                    unavailable.get("path") or unavailable.get("videoId"),
                    exc,
                )
        return repairs, dead_tracks, stats

    def apply_repairs(self, repairs: list[LikedSongRepair]) -> int:
        repaired = 0
        for repair in repairs:
            if self.sync_account:
                self.client.rate_song(repair.new_video_id, "LIKE")
                self.client.rate_song(repair.old_video_id, "INDIFFERENT")
            self._replace_cached_liked_track(
                repair.old_video_id,
                repair.path,
                repair.old_track,
                repair.replacement,
            )
            self.cache.clear_unavailable_track(repair.old_video_id, repair.path)
            repaired += 1
            self.logger.info(
                "Repaired liked song %s locally%s: %s -> %s",
                repair.path,
                " and in account" if self.sync_account else "",
                repair.old_video_id,
                repair.new_video_id,
            )
        if repaired:
            self.cache.record_repair_trigger(
                [
                    {
                        "old_video_id": r.old_video_id,
                        "path": r.path,
                        "new_video_id": r.new_video_id,
                    }
                    for r in repairs
                ]
            )
        return repaired

    def apply_removals(self, dead_tracks: list[tuple[str, str]]) -> int:
        """Remove confirmed dead tracks from account and local cache.

        Args:
            dead_tracks: List of (video_id, path) tuples.

        Returns:
            Number of tracks removed.
        """
        removed = 0
        for video_id, path in dead_tracks:
            if self.sync_account:
                self.client.rate_song(video_id, "INDIFFERENT")
                self.logger.info("Removed dead track from account: %s", path)
            self._remove_dead_track_from_cache(video_id, path)
            removed += 1
            self.logger.info("Removed dead track from cache: %s", path)
        return removed

    def _plan_one(self, unavailable: dict[str, Any]) -> LikedSongRepair | None:
        old_video_id = str(unavailable.get("videoId") or "")
        path = str(unavailable.get("path") or "")
        if not old_video_id or not path:
            return None

        old_track = self._find_cached_track(old_video_id, path)
        artist, title = self._artist_title_from_track_or_path(old_track, path)
        if not artist or not title:
            self.logger.info("Skipping %s: cannot derive artist/title", path)
            return None

        replacement = self._find_replacement(old_video_id, artist, title)
        if replacement is None:
            self.cache.mark_no_replacement(old_video_id, path)
            self.logger.info(
                "No verified replacement found for %s, marked for future skip", path
            )
            return None

        new_video_id = str(replacement["videoId"])
        return LikedSongRepair(
            path=path,
            old_video_id=old_video_id,
            new_video_id=new_video_id,
            old_track=old_track,
            replacement=replacement,
        )

    def _find_replacement(
        self, old_video_id: str, artist: str, title: str
    ) -> dict[str, Any] | None:
        query = f"{artist} {title}"
        candidates = self.client.search(
            query,
            filter_type="songs",
            limit=10,
            ignore_spelling=True,
        )
        scored = [
            (self._match_score(candidate, artist, title), candidate)
            for candidate in candidates
            if candidate.get("videoId") and candidate.get("videoId") != old_video_id
        ]
        scored.sort(key=lambda item: item[0], reverse=True)

        for score, candidate in scored:
            if score < 5:
                continue
            video_id = str(candidate["videoId"])
            try:
                self.yt_dlp_utils.extract_stream_url(video_id, self.browser)
            except Exception as exc:
                self.logger.debug(
                    "Skipping replacement candidate %s for %s: %s",
                    video_id,
                    query,
                    exc,
                )
                continue
            return dict(candidate)
        return None

    def _find_cached_track(self, video_id: str, path: str) -> dict[str, Any] | None:
        tracks = self.cache.get(f"{Path(path).parent!s}_processed")
        if not isinstance(tracks, list):
            return None
        filename = Path(path).name
        for track in tracks:
            if not isinstance(track, dict):
                continue
            if track.get("videoId") == video_id or track.get("filename") == filename:
                return dict(track)
        return None

    def _replace_cached_liked_track(
        self,
        old_video_id: str,
        path: str,
        old_track: dict[str, Any] | None,
        replacement: dict[str, Any],
    ) -> None:
        tracks = self.cache.get(f"{Path(path).parent!s}_processed")
        if not isinstance(tracks, list):
            return

        filename = Path(path).name
        replacement_track = self.processor.extract_track_info(
            {
                **replacement,
                "videoId": replacement["videoId"],
                "duration_seconds": replacement.get("duration_seconds")
                or replacement.get("duration"),
            }
        )
        replacement_track.pop("is_new_duration", None)
        replacement_track["filename"] = filename
        replacement_track["is_directory"] = False

        changed = False
        updated_tracks = []
        for track in tracks:
            if isinstance(track, dict) and (
                track.get("videoId") == old_video_id
                or track.get("filename") == filename
            ):
                merged = dict(old_track or track)
                merged.update(replacement_track)
                updated_tracks.append(merged)
                changed = True
            else:
                updated_tracks.append(track)

        if changed:
            self._persist_track_cache(updated_tracks, path)
            self.cache.set(f"video_id:{path}", replacement_track["videoId"])

    def _remove_dead_track_from_cache(self, video_id: str, path: str) -> None:
        """Remove an unavailable track that has no verified replacement."""
        tracks = self.cache.get("/liked_songs_processed")
        if not isinstance(tracks, list):
            return
        before = len(tracks)
        updated = [
            t for t in tracks if isinstance(t, dict) and t.get("videoId") != video_id
        ]
        if len(updated) < before:
            self._persist_liked_songs_cache(updated, path)
            self.logger.info("Removed dead track %s from liked songs cache", path)

    def _persist_liked_songs_cache(
        self, tracks: list[dict[str, Any]], path: str
    ) -> None:
        self._persist_track_cache(tracks, path)

    def _persist_track_cache(self, tracks: list[dict[str, Any]], path: str) -> None:
        """Persist updated track list and invalidate derived caches."""
        parent = str(Path(path).parent)
        self.cache.set(f"{parent}_processed", tracks)
        self.cache.delete(f"{parent}_listing_with_attrs")
        self.cache.delete(f"{parent}_listing")
        self.cache.delete(f"video_id:{path}")

    def _artist_title_from_track_or_path(
        self, track: dict[str, Any] | None, path: str
    ) -> tuple[str | None, str | None]:
        if track:
            artist = track.get("artist")
            title = track.get("title")
            if isinstance(artist, str) and isinstance(title, str):
                return artist, title

        stem = Path(path).stem
        if " - " not in stem:
            return None, None
        artist, title = stem.split(" - ", 1)
        return artist, title

    def _match_score(self, candidate: dict[str, Any], artist: str, title: str) -> int:
        score = 0
        candidate_title = str(candidate.get("title") or "")
        candidate_artists = " ".join(
            a.get("name", "")
            for a in candidate.get("artists", [])
            if isinstance(a, dict)
        )

        title_norm = self._normalize(title)
        candidate_title_norm = self._normalize(candidate_title)
        artist_norm = self._normalize(artist)
        candidate_artists_norm = self._normalize(candidate_artists)

        if artist_norm and (
            artist_norm in candidate_artists_norm
            or candidate_artists_norm in artist_norm
        ):
            score += 3

        if title_norm and title_norm == candidate_title_norm:
            score += 5
        else:
            title_tokens = self._tokens(title_norm)
            candidate_tokens = self._tokens(candidate_title_norm)
            if title_tokens:
                overlap = len(title_tokens & candidate_tokens)
                score += min(overlap, 4)

        return score

    def _tokens(self, value: str) -> set[str]:
        return {token for token in value.split() if len(token) >= 4}

    def _normalize(self, value: str) -> str:
        normalized = unicodedata.normalize("NFKD", value).casefold()
        normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
        normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
        return " ".join(normalized.split())
