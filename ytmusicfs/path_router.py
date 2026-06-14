#!/usr/bin/env python3

import logging
import re
from collections.abc import Callable
from typing import Any

RouteHandler = Callable[..., list[str]]
PatternHandler = tuple[re.Pattern[str], RouteHandler]
EMPTY_DIRECTORY = [".", ".."]
STATIC_LIBRARY_DIRS = {"albums", "liked_songs", "playlists"}


class PathRouter:
    """Router for handling FUSE filesystem paths."""

    def __init__(self) -> None:
        self.handlers: dict[str, RouteHandler] = {}
        self.subpath_handlers: list[tuple[str, RouteHandler]] = []
        self.pattern_handlers: list[PatternHandler] = []
        self.fetcher: Any = None
        self.cache: Any = None
        self.logger = logging.getLogger("YTMusicFS")

    def set_fetcher(self, fetcher: Any) -> None:
        """Use the fetcher's cache for route validation."""
        self.fetcher = fetcher
        if hasattr(fetcher, "cache"):
            self.cache = fetcher.cache

    def set_cache(self, cache: Any) -> None:
        self.cache = cache

    def register(self, path: str, handler: RouteHandler) -> None:
        """Register an exact directory route."""
        self.handlers[path] = handler
        if self.cache:
            self.cache.mark_valid(path, is_directory=True)

    def register_subpath(self, prefix: str, handler: RouteHandler) -> None:
        """Register a route that receives the full matching path."""
        self.subpath_handlers.append((prefix, handler))
        if self.cache:
            self.cache.mark_valid(prefix, is_directory=True)

    def register_dynamic(self, pattern: str, handler: RouteHandler) -> None:
        """Register a wildcard route and compile it once."""
        self.pattern_handlers.append((self._compile_pattern(pattern), handler))

        if self.cache:
            prefix = pattern.split("*", 1)[0].rstrip("/")
            if prefix:
                self.cache.mark_valid(prefix, is_directory=True)

    @staticmethod
    def _compile_pattern(pattern: str) -> re.Pattern[str]:
        escaped = re.escape(pattern)
        regex = escaped.replace(r"\*\*", "(.+)").replace(r"\*", "([^/]+)")
        return re.compile(f"^{regex}$")

    def _match_wildcard_pattern(
        self, pattern: str | re.Pattern[str], path: str
    ) -> tuple[bool, list[str]]:
        compiled = (
            self._compile_pattern(pattern) if isinstance(pattern, str) else pattern
        )
        match = compiled.fullmatch(path)
        return (True, list(match.groups())) if match else (False, [])

    def validate_path(self, path: str) -> bool:
        """Check if a path is potentially valid based on registered handlers.

        Args:
            path: The path to validate

        Returns:
            Boolean indicating if the path might be valid
        """
        if not self.validate_level2_path(path):
            return False

        if path in self.handlers:
            return True

        for prefix, _ in self.subpath_handlers:
            if path.startswith(prefix):
                return True

        for pattern, _ in self.pattern_handlers:
            match_success, _ = self._match_wildcard_pattern(pattern, path)
            if match_success:
                return True

        if self.cache:
            return bool(self.cache.is_valid_path(path))

        return False

    def validate_level2_path(self, path: str) -> bool:
        """Validate a level 2 path specifically for albums/playlists.

        This method checks if paths like /albums/X, /playlists/Y actually refer
        to existing items in our data. This prevents tab completion from creating
        invalid directory entries.

        Args:
            path: The path to validate

        Returns:
            True if the path is valid, False otherwise
        """
        parts = path.split("/")
        if len(parts) != 3:
            return True

        if parts[1] not in STATIC_LIBRARY_DIRS:
            return True

        if self.cache:
            parent_dir = f"/{parts[1]}"
            dir_listing = self.cache.get_directory_listing_with_attrs(parent_dir)
            if dir_listing and parts[2] not in dir_listing:
                self.logger.debug(
                    "Invalid level 2 path, not in directory listing: %s", path
                )
                return False

        return True

    def _resolve_handler(
        self, path: str
    ) -> tuple[RouteHandler, tuple[Any, ...]] | None:
        exact_handler = self.handlers.get(path)
        if exact_handler:
            return exact_handler, ()

        for prefix, handler in self.subpath_handlers:
            if path.startswith(prefix):
                return handler, (path,)

        for pattern, handler in self.pattern_handlers:
            matched, values = self._match_wildcard_pattern(pattern, path)
            if matched:
                return handler, (path, *values)

        return None

    def _cache_result(self, path: str, result: list[str]) -> None:
        """Remember successful listings so later FUSE lookups stay local."""
        if not self.cache or path == "/" or len(result) <= len(EMPTY_DIRECTORY):
            return

        self.cache.mark_valid(path, is_directory=True)
        for entry in result:
            if entry in EMPTY_DIRECTORY:
                continue
            self.cache.mark_valid(
                f"{path}/{entry}",
                is_directory=False if entry.endswith(".m4a") else None,
            )

    def route(self, path: str) -> list[str]:
        """Return the listing produced by the first matching route."""
        if not self.validate_level2_path(path):
            self.logger.debug("Path %s failed level 2 validation", path)
            return EMPTY_DIRECTORY.copy()

        resolved = self._resolve_handler(path)
        if not resolved:
            self.logger.debug("No handler found for %s", path)
            return EMPTY_DIRECTORY.copy()

        handler, args = resolved
        try:
            result = handler(*args)
        except Exception:
            self.logger.exception("Route handler failed for %s", path)
            return EMPTY_DIRECTORY.copy()

        if not result:
            return EMPTY_DIRECTORY.copy()

        self._cache_result(path, result)
        return result
