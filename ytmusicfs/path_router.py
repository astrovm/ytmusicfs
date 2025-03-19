#!/usr/bin/env python3

from typing import Dict, Callable, List
import re
import logging


class PathRouter:
    """Router for handling FUSE filesystem paths."""

    def __init__(self):
        """Initialize the path router with empty handler collections."""
        self.handlers: Dict[str, Callable] = {}
        self.subpath_handlers: List[tuple[str, Callable]] = []
        self.pattern_handlers: List[tuple[str, Callable]] = []  # For wildcard patterns

        # Content fetcher will be set later by the filesystem
        self.fetcher = None

        # Cache manager will be set later by the filesystem
        self.cache = None

    def set_fetcher(self, fetcher):
        """Set the content fetcher instance used by handlers.

        Args:
            fetcher: ContentFetcher instance
        """
        self.fetcher = fetcher
        # Also get a reference to the cache manager from the fetcher
        if hasattr(fetcher, "cache"):
            self.cache = fetcher.cache

    def set_cache(self, cache):
        """Set the cache manager instance directly.

        Args:
            cache: CacheManager instance
        """
        self.cache = cache

    def register(self, path: str, handler: Callable) -> None:
        """Register a handler for an exact path match.

        Args:
            path: The exact path to match
            handler: The handler function to call
        """
        self.handlers[path] = handler

        # Pre-validate this path as a directory in the cache if available
        if self.cache:
            self.cache.mark_valid(path, is_directory=True)

    def register_subpath(self, prefix: str, handler: Callable) -> None:
        """Register a handler for a path prefix match.

        Args:
            prefix: The path prefix to match
            handler: The handler function to call with the full path
        """
        self.subpath_handlers.append((prefix, handler))

        # Pre-validate this path as a directory in the cache if available
        if self.cache:
            self.cache.mark_valid(prefix, is_directory=True)

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

        # If the pattern has a fixed prefix before any wildcard, pre-validate that
        if self.cache:
            prefix = pattern.split("*")[0].rstrip("/")
            if prefix:
                self.cache.mark_valid(prefix, is_directory=True)

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

    def validate_path(self, path: str) -> bool:
        """Check if a path is potentially valid based on registered handlers.

        Args:
            path: The path to validate

        Returns:
            Boolean indicating if the path might be valid
        """
        # First, check level 2 paths for validity
        if not self.validate_level2_path(path):
            return False

        # Check if path is registered directly
        if path in self.handlers:
            return True

        # Check prefix matches
        for prefix, _ in self.subpath_handlers:
            if path.startswith(prefix):
                return True

        # Check pattern matches
        for pattern, _ in self.pattern_handlers:
            match_success, _ = self._match_wildcard_pattern(pattern, path)
            if match_success:
                return True

        # Use the cache if available
        if self.cache:
            return self.cache.is_valid_path(path)

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
        logger = (
            logging.getLogger("YTMusicFS")
            if not hasattr(self, "logger")
            else self.logger
        )

        # Only process level 2 paths
        parts = path.split("/")
        if len(parts) != 3:
            return True  # Not a level 2 path, so don't reject it here

        # Only check specific directories
        if parts[1] not in ["albums", "playlists", "liked_songs"]:
            return True  # Not an album/playlist path, so don't reject it here

        # Check if this is in the parent directory listing
        if self.cache:
            parent_dir = f"/{parts[1]}"
            dir_listing = self.cache.get_directory_listing_with_attrs(parent_dir)
            if dir_listing and parts[2] not in dir_listing:
                # This path doesn't exist in our data
                logger.debug(f"Invalid level 2 path, not in directory listing: {path}")
                return False

        return True

    def route(self, path: str) -> List[str]:
        """Route a path to the appropriate handler.

        Args:
            path: The path to route

        Returns:
            List of directory entries from the handler
        """
        result = None
        logger = (
            logging.getLogger("YTMusicFS")
            if not hasattr(self, "logger")
            else self.logger
        )

        # Early validation - if this is an invalid level 2 path, return empty dir
        if not self.validate_level2_path(path):
            logger.debug(f"Path {path} failed level 2 validation, returning empty dir")
            return [".", ".."]

        # First try exact matches
        if path in self.handlers:
            try:
                logger.debug(f"Found exact handler for {path}")
                result = self.handlers[path]()
                # Mark this path as valid in the cache
                if (
                    self.cache and path != "/" and len(result) > 2
                ):  # More than just "." and ".."
                    self.cache.mark_valid(path, is_directory=True)
            except Exception as e:
                logger.error(f"Error calling handler for {path}: {e}")
                import traceback

                logger.error(traceback.format_exc())
                return [".", ".."]

        # Then try prefix matches
        elif not result:
            for prefix, handler in self.subpath_handlers:
                if path.startswith(prefix):
                    try:
                        logger.debug(
                            f"Found prefix handler for {path} with prefix {prefix}"
                        )
                        result = handler(path)
                        # Mark this path as valid in the cache
                        if (
                            self.cache and path != "/" and len(result) > 2
                        ):  # More than just "." and ".."
                            self.cache.mark_valid(path, is_directory=True)
                        break
                    except Exception as e:
                        logger.error(f"Error calling prefix handler for {path}: {e}")
                        import traceback

                        logger.error(traceback.format_exc())
                        return [".", ".."]

        # Finally try pattern matches
        if not result:
            for pattern, handler in self.pattern_handlers:
                try:
                    match_success, captured_values = self._match_wildcard_pattern(
                        pattern, path
                    )
                    if match_success:
                        logger.debug(
                            f"Found pattern handler for {path} with pattern {pattern}"
                        )
                        # Mark this path as valid in the cache
                        if self.cache:
                            self.cache.mark_valid(path, is_directory=True)

                        # Pass both the full path and the captured values
                        if captured_values:
                            result = handler(path, *captured_values)
                        else:
                            result = handler(path)
                        break
                except Exception as e:
                    logger.error(f"Error calling pattern handler for {path}: {e}")
                    import traceback

                    logger.error(traceback.format_exc())
                    return [".", ".."]

        # Default to empty dir if no handler matched
        if not result:
            logger.debug(f"No handler found for {path}")
            result = [".", ".."]

        # Process results to mark individual entries as valid
        if (
            self.cache and path != "/" and len(result) > 2
        ):  # More than just "." and ".."
            # Mark each file as valid in the parent directory
            for entry in result:
                if entry not in [".", ".."]:
                    entry_path = f"{path}/{entry}"
                    # We don't know if it's a directory yet, so don't specify is_directory
                    self.cache.mark_valid(entry_path)

        return result
