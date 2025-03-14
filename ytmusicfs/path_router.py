#!/usr/bin/env python3

from typing import Dict, Callable, List
import re


class PathRouter:
    """Router for handling FUSE filesystem paths."""

    def __init__(self):
        """Initialize the path router with empty handler collections."""
        self.handlers: Dict[str, Callable] = {}
        self.subpath_handlers: List[tuple[str, Callable]] = []
        self.pattern_handlers: List[tuple[str, Callable]] = []  # For wildcard patterns

        # Content fetcher will be set later by the filesystem
        self.fetcher = None

    def set_fetcher(self, fetcher):
        """Set the content fetcher instance used by handlers.

        Args:
            fetcher: ContentFetcher instance
        """
        self.fetcher = fetcher

        # Register dynamic handler for categorized search results
        self.register_dynamic(
            "/search/*/*/*/*",
            lambda path, scope, category, query, subpath: [".", ".."]
            + self.fetcher.readdir_search_item_content(
                path, scope="library" if scope == "library" else None
            ),
        )

    def register(self, path: str, handler: Callable) -> None:
        """Register a handler for an exact path match.

        Args:
            path: The exact path to match
            handler: The handler function to call
        """
        self.handlers[path] = handler

    def register_subpath(self, prefix: str, handler: Callable) -> None:
        """Register a handler for a path prefix match.

        Args:
            prefix: The path prefix to match
            handler: The handler function to call with the full path
        """
        self.subpath_handlers.append((prefix, handler))

    def register_dynamic(self, pattern: str, handler: Callable) -> None:
        """Register a handler for a path pattern with wildcards.

        Wildcards:
        - * matches any sequence of characters within a path segment
        - ** matches any sequence of characters across multiple path segments

        Args:
            pattern: The path pattern to match (e.g., "/playlists/*", "/artists/**/tracks")
            handler: The handler function to call with the full path
        """
        self.pattern_handlers.append((pattern, handler))

    def _match_wildcard_pattern(self, pattern: str, path: str) -> tuple[bool, list]:
        """Check if a path matches a wildcard pattern and extract wildcard values.

        Args:
            pattern: The pattern with wildcards to match against
            path: The path to check

        Returns:
            Tuple of (match_success, captured_values)
        """
        # Escape special regex characters except * which we'll handle specially
        regex_pattern = (
            re.escape(pattern).replace("\\*\\*", "(.+)").replace("\\*", "([^/]+)")
        )

        # Add start and end anchors
        regex_pattern = f"^{regex_pattern}$"

        # Match the path against the pattern
        match = re.match(regex_pattern, path)
        if match:
            # Return captured values
            return True, list(match.groups())
        return False, []

    def route(self, path: str) -> List[str]:
        """Route a path to the appropriate handler.

        Args:
            path: The path to route

        Returns:
            List of directory entries from the handler
        """
        # First try exact matches
        if path in self.handlers:
            return self.handlers[path]()

        # Then try prefix matches
        for prefix, handler in self.subpath_handlers:
            if path.startswith(prefix):
                return handler(path)

        # Finally try pattern matches
        for pattern, handler in self.pattern_handlers:
            match_success, captured_values = self._match_wildcard_pattern(pattern, path)
            if match_success:
                # Pass both the full path and the captured values
                if captured_values:
                    return handler(path, *captured_values)
                else:
                    return handler(path)

        return [".", ".."]
