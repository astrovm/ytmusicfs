#!/usr/bin/env python3

import unittest
from unittest.mock import Mock, patch
import logging

# Import the class to test
from ytmusicfs.path_router import PathRouter


class TestPathRouter(unittest.TestCase):
    """Test case for PathRouter class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create the instance to test
        self.router = PathRouter()

        # Mock dependencies
        self.mock_fetcher = Mock()
        self.mock_cache = Mock()

        # Configure mock behavior
        self.mock_cache.get_directory_listing_with_attrs.return_value = {
            ".": {"is_dir": True},
            "..": {"is_dir": True},
            "my_playlist": {"is_dir": True},
            "popular": {"is_dir": True},
            "workout_mix": {"is_dir": True},
        }

        # Set up mocks
        self.router.set_fetcher(self.mock_fetcher)
        self.router.set_cache(self.mock_cache)

        # Patch validate_level2_path method to avoid cache checks
        self.original_validate_level2_path = self.router.validate_level2_path
        self.router.validate_level2_path = lambda path: True

    def tearDown(self):
        """Clean up after each test."""
        # Restore original method
        self.router.validate_level2_path = self.original_validate_level2_path

    def test_register_and_route_exact_path(self):
        """Test registering and routing an exact path."""
        # Test data
        path = "/playlists"
        expected_result = ["playlist1", "playlist2"]

        # Create a mock handler
        mock_handler = Mock(return_value=expected_result)

        # Register the handler
        self.router.register(path, mock_handler)

        # Route the path
        result = self.router.route(path)

        # Verify the handler was called and returned expected result
        mock_handler.assert_called_once()
        self.assertEqual(result, expected_result)

        # Check that the path was validated in the cache
        self.assertIn(path, self.router.handlers)

    def test_register_and_route_subpath(self):
        """Test registering and routing a subpath."""
        # Test data
        prefix = "/playlists/"
        path = "/playlists/my_playlist"
        expected_result = ["song1.m4a", "song2.m4a"]

        # Create a mock handler
        mock_handler = Mock(return_value=expected_result)

        # Register the handler
        self.router.register_subpath(prefix, mock_handler)

        # Route the path
        result = self.router.route(path)

        # Verify the handler was called with correct arguments
        mock_handler.assert_called_once_with(path)
        self.assertEqual(result, expected_result)

    def test_register_and_route_dynamic_path(self):
        """Test registering and routing a dynamic (wildcard) path."""
        # Test data
        pattern = "/playlists/*/song_*.m4a"
        path = "/playlists/my_playlist/song_01.m4a"
        expected_result = ["audio_data"]

        # Create a mock handler with the correct signature for wildcards
        def mock_handler(path, *wildcards):
            # wildcards should be ['my_playlist', '01']
            return expected_result

        # Register the handler
        self.router.register_dynamic(pattern, mock_handler)

        # Route the path
        result = self.router.route(path)

        # Verify the correct result
        self.assertEqual(result, expected_result)

    def test_match_wildcard_pattern(self):
        """Test the internal wildcard pattern matching logic."""
        # Test cases with various patterns and paths
        test_cases = [
            # pattern, path, should_match, expected_wildcards
            (
                "/playlists/*/song.m4a",
                "/playlists/my_playlist/song.m4a",
                True,
                ["my_playlist"],
            ),
            (
                "/playlists/*/song_*.m4a",
                "/playlists/my_playlist/song_01.m4a",
                True,
                ["my_playlist", "01"],
            ),
            (
                "/playlists/*/*/*.m4a",
                "/playlists/genre/artist/song.m4a",
                True,
                ["genre", "artist", "song"],
            ),
            ("/playlists/*/song.m4a", "/playlists/song.m4a", False, []),
            ("/playlists/*/song.m4a", "/albums/my_album/song.m4a", False, []),
        ]

        for pattern, path, should_match, expected_wildcards in test_cases:
            matched, wildcards = self.router._match_wildcard_pattern(pattern, path)

            # Verify match result
            self.assertEqual(matched, should_match)

            # If should match, verify wildcards
            if should_match:
                self.assertEqual(wildcards, expected_wildcards)

    def test_validate_path(self):
        """Test path validation logic."""
        # Override the validate_level2_path method temporarily
        self.router.validate_level2_path = self.original_validate_level2_path

        # Configure cache to return different directory listings based on the path
        def mock_get_directory_listing(path):
            if path == "/playlists":
                return {
                    ".": {"is_dir": True},
                    "..": {"is_dir": True},
                    "my_playlist": {"is_dir": True},
                    "popular": {"is_dir": True},
                }
            elif path == "/albums":
                return {
                    ".": {"is_dir": True},
                    "..": {"is_dir": True},
                    "my_album": {"is_dir": True},
                }
            return None

        self.mock_cache.get_directory_listing_with_attrs.side_effect = (
            mock_get_directory_listing
        )

        # Configure is_valid_path to return True only for expected paths
        def mock_is_valid_path(path):
            valid_paths = ["/", "/playlists", "/albums"]
            return path in valid_paths

        self.mock_cache.is_valid_path.side_effect = mock_is_valid_path

        # Set up handlers for different path patterns
        self.router.register("/", lambda: ["root"])
        self.router.register("/playlists", lambda: ["playlists"])
        self.router.register("/albums", lambda: ["albums"])
        self.router.register_subpath("/playlists/", lambda path: ["playlist_content"])
        self.router.register_dynamic(
            "/albums/*/song_*.m4a", lambda path, *wildcards: ["song"]
        )

        # Test root and top-level directories
        self.assertTrue(self.router.validate_path("/"))
        self.assertTrue(self.router.validate_path("/playlists"))
        self.assertTrue(self.router.validate_path("/albums"))

        # Test paths that don't exist
        self.assertFalse(self.router.validate_path("/invalid"))

    def test_multiple_handlers_precedence(self):
        """Test that handlers are called in the correct order of precedence."""
        # Set up handlers with overlapping paths
        exact_handler = Mock(return_value=["exact"])
        subpath_handler = Mock(return_value=["subpath"])
        dynamic_handler = Mock(return_value=["dynamic"])

        self.router.register("/playlists/popular", exact_handler)
        self.router.register_subpath("/playlists/", subpath_handler)
        self.router.register_dynamic("/playlists/*", dynamic_handler)

        # Test exact match takes precedence
        result = self.router.route("/playlists/popular")
        exact_handler.assert_called_once()
        subpath_handler.assert_not_called()
        dynamic_handler.assert_not_called()
        self.assertEqual(result, ["exact"])

        # Reset mocks
        exact_handler.reset_mock()
        subpath_handler.reset_mock()
        dynamic_handler.reset_mock()

        # Test subpath takes precedence over dynamic
        result = self.router.route("/playlists/other")
        exact_handler.assert_not_called()
        subpath_handler.assert_called_once()
        dynamic_handler.assert_not_called()
        self.assertEqual(result, ["subpath"])


if __name__ == "__main__":
    unittest.main()
