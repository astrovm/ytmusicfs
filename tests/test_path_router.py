#!/usr/bin/env python3

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, call

from ytmusicfs.path_router import EMPTY_DIRECTORY, PathRouter


class TestPathRouter(unittest.TestCase):
    def setUp(self):
        self.router = PathRouter()
        self.mock_fetcher = Mock()
        self.mock_cache = Mock()
        self.mock_cache.get_directory_listing_with_attrs.return_value = {
            ".": {"is_dir": True},
            "..": {"is_dir": True},
            "my_playlist": {"is_dir": True},
            "popular": {"is_dir": True},
            "workout_mix": {"is_dir": True},
        }
        self.router.set_fetcher(self.mock_fetcher)
        self.router.set_cache(self.mock_cache)

        # Most routing tests don't exercise level-2 validation against the cache.
        self.original_validate_level2_path = self.router.validate_level2_path
        self.router.validate_level2_path = lambda path: True

    def tearDown(self):
        self.router.validate_level2_path = self.original_validate_level2_path

    def test_register_and_route_exact_path(self):
        path = "/playlists"
        expected_result = ["playlist1", "playlist2"]
        mock_handler = Mock(return_value=expected_result)

        self.router.register(path, mock_handler)
        result = self.router.route(path)

        mock_handler.assert_called_once()
        self.assertEqual(result, expected_result)
        self.assertIn(path, self.router.handlers)

    def test_route_subpath_handler_receives_full_path(self):
        path = "/playlists/my_playlist"
        expected_result = ["song1.m4a", "song2.m4a"]
        mock_handler = Mock(return_value=expected_result)

        self.router.register_subpath("/playlists/", mock_handler)
        result = self.router.route(path)

        mock_handler.assert_called_once_with(path)
        self.assertEqual(result, expected_result)

    def test_register_and_route_dynamic_path(self):
        handler = Mock(return_value=["audio_data"])

        self.router.register_dynamic("/playlists/*/song_*.m4a", handler)
        result = self.router.route("/playlists/my_playlist/song_01.m4a")

        self.assertEqual(result, ["audio_data"])
        handler.assert_called_once_with(
            "/playlists/my_playlist/song_01.m4a", "my_playlist", "01"
        )

    def test_match_wildcard_pattern_captures_single_segments(self):
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
            with self.subTest(pattern=pattern, path=path):
                matched, wildcards = self.router._match_wildcard_pattern(pattern, path)
                self.assertEqual(matched, should_match)
                self.assertEqual(wildcards, expected_wildcards)

    def test_match_wildcard_pattern_double_star_spans_segments(self):
        matched, wildcards = self.router._match_wildcard_pattern(
            "/albums/**", "/albums/a/b/c.m4a"
        )

        self.assertTrue(matched)
        self.assertEqual(wildcards, ["a/b/c.m4a"])

    def test_match_wildcard_pattern_escapes_regex_characters(self):
        matched, _ = self.router._match_wildcard_pattern("/a.b/*", "/axb/c")

        self.assertFalse(matched)

    def test_validate_path_accepts_registered_and_rejects_unknown(self):
        self.router.validate_level2_path = self.original_validate_level2_path

        def mock_get_directory_listing(path):
            if path == "/playlists":
                return {".": {}, "..": {}, "my_playlist": {}, "popular": {}}
            if path == "/albums":
                return {".": {}, "..": {}, "my_album": {}}
            return None

        self.mock_cache.get_directory_listing_with_attrs.side_effect = (
            mock_get_directory_listing
        )
        self.mock_cache.is_valid_path.side_effect = lambda path: path in [
            "/",
            "/playlists",
            "/albums",
        ]
        self.router.register("/", lambda: ["root"])
        self.router.register("/playlists", lambda: ["playlists"])
        self.router.register("/albums", lambda: ["albums"])
        self.router.register_subpath("/playlists/", lambda path: ["playlist_content"])
        self.router.register_dynamic(
            "/albums/*/song_*.m4a", lambda path, *wildcards: ["song"]
        )

        self.assertTrue(self.router.validate_path("/"))
        self.assertTrue(self.router.validate_path("/playlists"))
        self.assertTrue(self.router.validate_path("/albums"))
        self.assertFalse(self.router.validate_path("/invalid"))

    def test_validate_path_accepts_subpath_and_dynamic_matches(self):
        self.mock_cache.is_valid_path.return_value = False
        self.router.register_subpath("/playlists/", Mock())
        self.router.register_dynamic("/albums/*/song_*.m4a", Mock())

        self.assertTrue(self.router.validate_path("/playlists/x/y.m4a"))
        self.assertTrue(self.router.validate_path("/albums/a/song_1.m4a"))
        self.assertFalse(self.router.validate_path("/albums/a/other.m4a"))
        self.mock_cache.is_valid_path.assert_called_once_with("/albums/a/other.m4a")

    def test_validate_path_rejects_path_failing_level2_validation(self):
        self.router.validate_level2_path = self.original_validate_level2_path
        self.router.register_subpath("/playlists/", Mock())

        self.assertFalse(self.router.validate_path("/playlists/missing"))

    def test_validate_path_without_cache_rejects_unregistered_path(self):
        router = PathRouter()

        self.assertFalse(router.validate_path("/unknown"))

    def test_multiple_handlers_precedence(self):
        exact_handler = Mock(return_value=["exact"])
        subpath_handler = Mock(return_value=["subpath"])
        dynamic_handler = Mock(return_value=["dynamic"])
        self.router.register("/playlists/popular", exact_handler)
        self.router.register_subpath("/playlists/", subpath_handler)
        self.router.register_dynamic("/playlists/*", dynamic_handler)

        self.assertEqual(self.router.route("/playlists/popular"), ["exact"])
        subpath_handler.assert_not_called()
        dynamic_handler.assert_not_called()

        exact_handler.reset_mock()
        self.assertEqual(self.router.route("/playlists/other"), ["subpath"])
        exact_handler.assert_not_called()
        dynamic_handler.assert_not_called()

    def test_route_tries_later_handlers_when_earlier_ones_do_not_match(self):
        albums_handler = Mock(return_value=["album"])
        playlists_handler = Mock(return_value=["playlist"])
        artist_handler = Mock(return_value=["artist"])
        track_handler = Mock(return_value=["track"])
        self.router.register_subpath("/albums/", albums_handler)
        self.router.register_subpath("/playlists/", playlists_handler)
        self.router.register_dynamic("/artists/*", artist_handler)
        self.router.register_dynamic("/artists/*/*", track_handler)

        self.assertEqual(self.router.route("/playlists/mix"), ["playlist"])
        playlists_handler.assert_called_once_with("/playlists/mix")
        albums_handler.assert_not_called()

        self.assertEqual(self.router.route("/artists/Band/Song"), ["track"])
        track_handler.assert_called_once_with("/artists/Band/Song", "Band", "Song")
        artist_handler.assert_not_called()

    def test_route_returns_empty_directory_when_no_handler_matches(self):
        result = self.router.route("/nowhere")

        self.assertEqual(result, EMPTY_DIRECTORY)
        self.assertIsNot(result, EMPTY_DIRECTORY)

    def test_route_returns_empty_directory_when_handler_raises(self):
        self.router.register("/boom", Mock(side_effect=RuntimeError("fail")))

        with self.assertLogs("YTMusicFS", level="ERROR"):
            result = self.router.route("/boom")

        self.assertEqual(result, [".", ".."])

    def test_route_returns_empty_directory_when_handler_returns_nothing(self):
        self.router.register("/empty", Mock(return_value=[]))

        self.assertEqual(self.router.route("/empty"), [".", ".."])

    def test_route_returns_empty_directory_for_invalid_level2_path(self):
        self.router.validate_level2_path = self.original_validate_level2_path
        handler = Mock(return_value=["x"])
        self.router.register_subpath("/albums/", handler)

        self.assertEqual(self.router.route("/albums/unknown"), [".", ".."])
        handler.assert_not_called()

    def test_route_marks_listing_entries_valid_in_cache(self):
        self.router.register(
            "/playlists/mix", Mock(return_value=[".", "..", "a.m4a", "sub"])
        )
        self.mock_cache.reset_mock()

        self.router.route("/playlists/mix")

        self.mock_cache.mark_valid.assert_has_calls(
            [
                call("/playlists/mix", is_directory=True),
                call("/playlists/mix/a.m4a", is_directory=False),
                call("/playlists/mix/sub", is_directory=None),
            ]
        )
        self.assertEqual(self.mock_cache.mark_valid.call_count, 3)

    def test_route_does_not_cache_root_or_empty_listings(self):
        self.router.register("/", Mock(return_value=[".", "..", "playlists"]))
        self.router.register("/dots", Mock(return_value=[".", ".."]))
        self.mock_cache.reset_mock()

        self.router.route("/")
        self.router.route("/dots")

        self.mock_cache.mark_valid.assert_not_called()


class TestPathRouterRegistration(unittest.TestCase):
    def test_set_fetcher_adopts_fetcher_cache(self):
        router = PathRouter()
        cache = Mock()

        router.set_fetcher(SimpleNamespace(cache=cache))

        self.assertIs(router.cache, cache)

    def test_set_fetcher_without_cache_attribute_leaves_cache_unset(self):
        router = PathRouter()

        router.set_fetcher(SimpleNamespace())

        self.assertIsNone(router.cache)

    def test_register_marks_routes_valid_in_cache(self):
        router = PathRouter()
        cache = Mock()
        router.set_cache(cache)

        router.register("/playlists", Mock())
        router.register_subpath("/albums/", Mock())
        router.register_dynamic("/liked_songs/*", Mock())
        router.register_dynamic("*", Mock())

        self.assertEqual(
            cache.mark_valid.call_args_list,
            [
                call("/playlists", is_directory=True),
                call("/albums/", is_directory=True),
                call("/liked_songs", is_directory=True),
            ],
        )

    def test_register_without_cache_only_stores_handlers(self):
        router = PathRouter()

        router.register("/a", Mock())
        router.register_subpath("/b/", Mock())
        router.register_dynamic("/c/*", Mock())

        self.assertIn("/a", router.handlers)
        self.assertEqual(len(router.subpath_handlers), 1)
        self.assertEqual(len(router.pattern_handlers), 1)


class TestPathRouterLevel2Validation(unittest.TestCase):
    def setUp(self):
        self.router = PathRouter()
        self.cache = Mock()
        self.cache.get_directory_listing_with_attrs.return_value = {
            ".": {},
            "..": {},
            "Mix": {},
        }
        self.router.set_cache(self.cache)

    def test_validate_level2_path_ignores_other_depths(self):
        self.assertTrue(self.router.validate_level2_path("/playlists"))
        self.assertTrue(self.router.validate_level2_path("/playlists/Mix/a.m4a"))
        self.cache.get_directory_listing_with_attrs.assert_not_called()

    def test_validate_level2_path_ignores_non_library_dirs(self):
        self.assertTrue(self.router.validate_level2_path("/other/Mix2"))
        self.cache.get_directory_listing_with_attrs.assert_not_called()

    def test_validate_level2_path_checks_parent_listing(self):
        self.assertTrue(self.router.validate_level2_path("/playlists/Mix"))
        self.assertFalse(self.router.validate_level2_path("/playlists/Nope"))
        self.cache.get_directory_listing_with_attrs.assert_called_with("/playlists")

    def test_validate_level2_path_accepts_when_parent_not_cached(self):
        self.cache.get_directory_listing_with_attrs.return_value = None

        self.assertTrue(self.router.validate_level2_path("/albums/Anything"))

    def test_validate_level2_path_without_cache_accepts(self):
        self.assertTrue(PathRouter().validate_level2_path("/albums/Anything"))


if __name__ == "__main__":
    unittest.main()
