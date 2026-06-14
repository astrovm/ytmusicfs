#!/usr/bin/env python3

from __future__ import annotations

import json
import timeit
from typing import TYPE_CHECKING, Any

from ytmusicfs.content_fetcher import ContentFetcher

if TYPE_CHECKING:
    from ytmusicfs.models import RegistryEntry

REGISTRY_SIZE = 10_000
LOOKUPS = 100_000


def build_fetcher() -> ContentFetcher:
    fetcher = object.__new__(ContentFetcher)
    registry: list[RegistryEntry] = [
        {
            "name": f"playlist_{index}",
            "id": f"PL{index}",
            "type": "playlist",
            "path": f"/playlists/playlist_{index}",
        }
        for index in range(REGISTRY_SIZE)
    ]
    fetcher._set_playlist_registry(registry)
    return fetcher


def benchmark_registry_lookup(fetcher: ContentFetcher) -> float:
    target = f"playlist_{REGISTRY_SIZE - 1}"
    return timeit.timeit(
        lambda: fetcher.get_playlist_id_from_name(target, "playlist"),
        number=LOOKUPS,
    )


def benchmark_track_merge(fetcher: ContentFetcher) -> float:
    existing: list[dict[str, Any]] = [
        {"videoId": f"old_{index}"} for index in range(REGISTRY_SIZE)
    ]
    fresh: list[dict[str, Any]] = [
        {"videoId": f"new_{index}"} for index in range(REGISTRY_SIZE)
    ]
    return timeit.timeit(
        lambda: fetcher._merge_tracks(fresh, existing),
        number=100,
    )


def main() -> None:
    fetcher = build_fetcher()
    results = {
        "registry_entries": REGISTRY_SIZE,
        "registry_lookups": LOOKUPS,
        "registry_lookup_seconds": benchmark_registry_lookup(fetcher),
        "merge_tracks_per_run": REGISTRY_SIZE * 2,
        "merge_runs": 100,
        "merge_seconds": benchmark_track_merge(fetcher),
    }
    print(json.dumps(results, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
