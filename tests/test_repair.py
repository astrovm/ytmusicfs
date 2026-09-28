import logging
import unittest
from unittest.mock import Mock

from ytmusicfs.dependencies import RepairDependencies
from ytmusicfs.repair import LikedSongRepair, LikedSongsRepairer


class TestLikedSongsRepairer(unittest.TestCase):
    def setUp(self):
        self.client = Mock()
        self.cache = Mock()
        self.processor = Mock()
        self.yt_dlp_utils = Mock()
        self.cache.is_no_replacement.return_value = False
        self.repairer = LikedSongsRepairer(
            RepairDependencies(
                client=self.client,
                cache=self.cache,
                processor=self.processor,
                yt_dlp=self.yt_dlp_utils,
                browser="brave",
                sync_account=True,
                logger=logging.getLogger("test"),
            )
        )

    def test_repair_likes_verified_replacement_and_unlikes_old_video(self):
        self.cache.get_unavailable_tracks.return_value = [
            {
                "videoId": "old",
                "path": "/liked_songs/Artist - Song.m4a",
                "reason": "Video unavailable",
            }
        ]
        self.cache.get.return_value = [
            {
                "videoId": "old",
                "artist": "Artist",
                "title": "Song",
                "filename": "Artist - Song.m4a",
            }
        ]
        self.client.search.return_value = [
            {
                "videoId": "new",
                "title": "Song",
                "artists": [{"name": "Artist"}],
                "duration": 123,
            }
        ]
        self.yt_dlp_utils.extract_stream_url.return_value = {"format_id": "141"}
        self.processor.extract_track_info.return_value = {
            "videoId": "new",
            "artist": "Artist",
            "title": "Song",
            "duration_seconds": 123,
        }

        stats = self.repairer.repair()

        self.assertEqual(
            stats,
            {"checked": 1, "repaired": 1, "removed": 0, "skipped": 0, "failed": 0},
        )
        self.client.rate_song.assert_any_call("new", "LIKE")
        self.client.rate_song.assert_any_call("old", "INDIFFERENT")
        self.cache.clear_unavailable_track.assert_called_once_with(
            "old", "/liked_songs/Artist - Song.m4a"
        )

    def test_plan_repairs_does_not_mutate_account_or_cache(self):
        self.cache.get_unavailable_tracks.return_value = [
            {"videoId": "old", "path": "/liked_songs/Artist - Song.m4a"}
        ]
        self.cache.get.return_value = [
            {
                "videoId": "old",
                "artist": "Artist",
                "title": "Song",
                "filename": "Artist - Song.m4a",
            }
        ]
        self.client.search.return_value = [
            {
                "videoId": "new",
                "title": "Song",
                "artists": [{"name": "Artist"}],
                "duration": 123,
            }
        ]
        self.yt_dlp_utils.extract_stream_url.return_value = {"format_id": "141"}

        repairs, dead_tracks, stats = self.repairer.plan_repairs()

        self.assertEqual(
            stats,
            {"checked": 1, "repaired": 0, "removed": 0, "skipped": 0, "failed": 0},
        )
        self.assertEqual(len(repairs), 1)
        self.assertEqual(len(dead_tracks), 0)
        self.assertEqual(repairs[0].old_video_id, "old")
        self.assertEqual(repairs[0].new_video_id, "new")
        self.client.rate_song.assert_not_called()
        self.cache.clear_unavailable_track.assert_not_called()

    def test_repair_accepts_best_available_replacement_stream(self):
        self.cache.get_unavailable_tracks.return_value = [
            {"videoId": "old", "path": "/liked_songs/Artist - Song.m4a"}
        ]
        self.cache.get.return_value = [
            {
                "videoId": "old",
                "artist": "Artist",
                "title": "Song",
                "filename": "Artist - Song.m4a",
            }
        ]
        self.client.search.return_value = [
            {
                "videoId": "new",
                "title": "Song",
                "artists": [{"name": "Artist"}],
                "duration": 123,
            }
        ]
        self.yt_dlp_utils.extract_stream_url.return_value = {"format_id": "140"}
        self.processor.extract_track_info.return_value = {
            "videoId": "new",
            "artist": "Artist",
            "title": "Song",
            "duration_seconds": 123,
        }

        stats = self.repairer.repair()

        self.assertEqual(
            stats,
            {"checked": 1, "repaired": 1, "removed": 0, "skipped": 0, "failed": 0},
        )
        self.client.rate_song.assert_any_call("new", "LIKE")
        self.client.rate_song.assert_any_call("old", "INDIFFERENT")
        self.cache.clear_unavailable_track.assert_called_once_with(
            "old", "/liked_songs/Artist - Song.m4a"
        )

    def test_repair_removes_previously_confirmed_no_replacement_track(self):
        self.cache.is_no_replacement.return_value = True
        self.cache.get_unavailable_tracks.return_value = [
            {"videoId": "old", "path": "/liked_songs/Artist - Song.m4a"}
        ]
        self.cache.get.return_value = [
            {
                "videoId": "old",
                "artist": "Artist",
                "title": "Song",
                "filename": "Artist - Song.m4a",
            }
        ]
        self.client.search.return_value = []

        stats = self.repairer.repair()

        self.assertEqual(
            stats,
            {"checked": 1, "repaired": 0, "removed": 1, "skipped": 0, "failed": 0},
        )
        self.client.rate_song.assert_called_once_with("old", "INDIFFERENT")

    def test_local_repair_does_not_mutate_account(self):
        self.repairer.sync_account = False
        self.cache.get_unavailable_tracks.return_value = [
            {"videoId": "old", "path": "/liked_songs/Artist - Song.m4a"}
        ]
        self.cache.get.return_value = [
            {
                "videoId": "old",
                "artist": "Artist",
                "title": "Song",
                "filename": "Artist - Song.m4a",
            }
        ]
        self.client.search.return_value = [
            {
                "videoId": "new",
                "title": "Song",
                "artists": [{"name": "Artist"}],
                "duration": 123,
            }
        ]
        self.yt_dlp_utils.extract_stream_url.return_value = {"format_id": "141"}
        self.processor.extract_track_info.return_value = {
            "videoId": "new",
            "artist": "Artist",
            "title": "Song",
            "duration_seconds": 123,
        }

        stats = self.repairer.repair()

        self.assertEqual(
            stats,
            {"checked": 1, "repaired": 1, "removed": 0, "skipped": 0, "failed": 0},
        )
        self.client.rate_song.assert_not_called()
        self.cache.clear_unavailable_track.assert_called_once_with(
            "old", "/liked_songs/Artist - Song.m4a"
        )

    def test_repair_counts_search_failure_as_failed_not_skipped(self):
        self.cache.get_unavailable_tracks.return_value = [
            {"videoId": "old", "path": "/liked_songs/Artist - Song.m4a"}
        ]
        self.cache.get.return_value = [
            {
                "videoId": "old",
                "artist": "Artist",
                "title": "Song",
                "filename": "Artist - Song.m4a",
            }
        ]
        self.client.search.side_effect = RuntimeError("search failed")

        stats = self.repairer.repair()

        self.assertEqual(
            stats,
            {"checked": 1, "repaired": 0, "removed": 0, "skipped": 0, "failed": 1},
        )
        self.client.rate_song.assert_not_called()
        self.cache.clear_unavailable_track.assert_not_called()

    def test_repair_ignores_unavailable_entries_outside_liked_songs(self):
        self.cache.get_unavailable_tracks.return_value = [
            {"videoId": "old", "path": "/playlists/Mix/Artist - Song.m4a"}
        ]

        stats = self.repairer.repair()

        self.assertEqual(
            stats,
            {"checked": 0, "repaired": 0, "removed": 0, "skipped": 0, "failed": 0},
        )
        self.client.search.assert_not_called()

    def test_local_plan_supports_playlist_cached_tracks(self):
        self.cache.get.return_value = [
            {
                "videoId": "old",
                "artist": "Artist",
                "title": "Song",
                "filename": "Artist - Song.m4a",
            }
        ]
        self.client.search.return_value = [
            {
                "videoId": "new",
                "title": "Song",
                "artists": [{"name": "Artist"}],
                "duration": 123,
            }
        ]
        self.yt_dlp_utils.extract_stream_url.return_value = {"format_id": "141"}

        repair = self.repairer._plan_one(
            {"videoId": "old", "path": "/playlists/Mix/Artist - Song.m4a"}
        )

        self.assertIsNotNone(repair)
        self.assertEqual(repair.new_video_id, "new")
        self.cache.get.assert_called_with("/playlists/Mix_processed")

    def test_playlist_replacement_persists_parent_cache_only(self):
        path = "/playlists/Mix/Artist - Song.m4a"
        self.cache.get.return_value = [
            {
                "videoId": "old",
                "artist": "Artist",
                "title": "Song",
                "filename": "Artist - Song.m4a",
            }
        ]
        self.processor.extract_track_info.return_value = {
            "videoId": "new",
            "artist": "Artist",
            "title": "Song",
            "duration_seconds": 123,
        }

        self.repairer._replace_cached_liked_track(
            "old",
            path,
            None,
            {"videoId": "new", "title": "Song", "duration": 123},
        )

        self.assertEqual(
            self.cache.set.call_args_list[0].args[0], "/playlists/Mix_processed"
        )
        updated = self.cache.set.call_args_list[0].args[1]
        self.assertEqual(updated[0]["videoId"], "new")
        self.cache.set.assert_any_call(f"video_id:{path}", "new")
        self.cache.delete.assert_any_call("/playlists/Mix_listing_with_attrs")
        self.cache.delete.assert_any_call("/playlists/Mix_listing")
        self.cache.delete.assert_any_call(f"video_id:{path}")

    def test_plan_repairs_skips_track_without_replacement_and_marks_it(self):
        path = "/liked_songs/Artist - Song.m4a"
        self.cache.get_unavailable_tracks.return_value = [
            {"videoId": "old", "path": path}
        ]
        self.cache.get.return_value = None
        self.client.search.return_value = []

        repairs, dead_tracks, stats = self.repairer.plan_repairs()

        self.assertEqual((repairs, dead_tracks), ([], []))
        self.assertEqual(stats["skipped"], 1)
        self.cache.mark_no_replacement.assert_called_once_with("old", path)

    def test_plan_one_returns_none_for_missing_video_id_or_path(self):
        self.assertIsNone(self.repairer._plan_one({"path": "/liked_songs/a.m4a"}))
        self.assertIsNone(self.repairer._plan_one({"videoId": "old"}))
        self.client.search.assert_not_called()

    def test_plan_one_skips_when_artist_and_title_cannot_be_derived(self):
        self.cache.get.return_value = []

        with self.assertLogs("test", level="INFO"):
            result = self.repairer._plan_one(
                {"videoId": "old", "path": "/liked_songs/NoSeparator.m4a"}
            )

        self.assertIsNone(result)
        self.client.search.assert_not_called()

    def test_plan_one_derives_artist_and_title_from_filename(self):
        self.cache.get.return_value = [{"videoId": "other", "filename": "x.m4a"}]
        self.client.search.return_value = [
            {"videoId": "new", "title": "Song", "artists": [{"name": "Artist"}]}
        ]

        repair = self.repairer._plan_one(
            {"videoId": "old", "path": "/liked_songs/Artist - Song.m4a"}
        )

        self.assertEqual(repair.new_video_id, "new")
        self.assertIsNone(repair.old_track)
        self.client.search.assert_called_once_with(
            "Artist Song", filter_type="songs", limit=10, ignore_spelling=True
        )

    def test_plan_one_uses_filename_when_cached_track_lacks_title(self):
        self.cache.get.return_value = [
            {"videoId": "old", "artist": "Cached Artist", "title": None}
        ]
        self.client.search.return_value = [
            {"videoId": "new", "title": "Song", "artists": [{"name": "Artist"}]}
        ]

        repair = self.repairer._plan_one(
            {"videoId": "old", "path": "/liked_songs/Artist - Song.m4a"}
        )

        self.assertEqual(repair.new_video_id, "new")
        self.assertEqual(repair.old_track["artist"], "Cached Artist")
        self.client.search.assert_called_once_with(
            "Artist Song", filter_type="songs", limit=10, ignore_spelling=True
        )

    def test_find_replacement_ignores_low_scores_and_same_video(self):
        self.client.search.return_value = [
            {"videoId": "old", "title": "Song", "artists": [{"name": "Artist"}]},
            {"title": "Song", "artists": [{"name": "Artist"}]},
            {"videoId": "weak", "title": "Something Else", "artists": []},
        ]

        self.assertIsNone(self.repairer._find_replacement("old", "Artist", "Song"))
        self.yt_dlp_utils.extract_stream_url.assert_not_called()

    def test_find_replacement_skips_candidates_without_playable_stream(self):
        self.client.search.return_value = [
            {"videoId": "broken", "title": "Song", "artists": [{"name": "Artist"}]},
            {"videoId": "ok", "title": "Song", "artists": []},
        ]
        self.yt_dlp_utils.extract_stream_url.side_effect = [
            RuntimeError("unavailable"),
            {"format_id": "141"},
        ]

        result = self.repairer._find_replacement("old", "Artist", "Song")

        self.assertEqual(result["videoId"], "ok")
        self.assertEqual(self.yt_dlp_utils.extract_stream_url.call_count, 2)

    def test_find_cached_track_matches_by_filename_and_skips_non_dicts(self):
        self.cache.get.return_value = [
            "garbage",
            {"videoId": "x", "filename": "Song.m4a", "title": "T"},
        ]

        track = self.repairer._find_cached_track("old", "/liked_songs/Song.m4a")

        self.assertEqual(track["title"], "T")

    def test_find_cached_track_returns_none_when_absent(self):
        self.cache.get.return_value = [{"videoId": "x", "filename": "Other.m4a"}]
        self.assertIsNone(self.repairer._find_cached_track("old", "/a/Song.m4a"))

        self.cache.get.return_value = {"not": "a list"}
        self.assertIsNone(self.repairer._find_cached_track("old", "/a/Song.m4a"))

    def test_replace_cached_track_keeps_other_tracks_and_old_metadata(self):
        path = "/liked_songs/Artist - Song.m4a"
        other = {"videoId": "keep", "filename": "Keep.m4a"}
        self.cache.get.return_value = [
            other,
            {"videoId": "old", "filename": "Artist - Song.m4a", "album": "Stale"},
        ]
        self.processor.extract_track_info.return_value = {
            "videoId": "new",
            "title": "Song",
            "is_new_duration": True,
        }

        self.repairer._replace_cached_liked_track(
            "old",
            path,
            {"videoId": "old", "album": "Original"},
            {"videoId": "new", "duration_seconds": 200},
        )

        info_arg = self.processor.extract_track_info.call_args.args[0]
        self.assertEqual(info_arg["duration_seconds"], 200)
        updated = self.cache.set.call_args_list[0].args[1]
        self.assertEqual(updated[0], other)
        self.assertEqual(
            updated[1],
            {
                "videoId": "new",
                "album": "Original",
                "title": "Song",
                "filename": "Artist - Song.m4a",
                "is_directory": False,
            },
        )

    def test_replace_cached_track_noop_when_cache_missing_or_unmatched(self):
        self.cache.get.return_value = None
        self.repairer._replace_cached_liked_track(
            "old", "/liked_songs/a.m4a", None, {"videoId": "new"}
        )
        self.processor.extract_track_info.assert_not_called()

        self.cache.get.return_value = [{"videoId": "x", "filename": "b.m4a"}]
        self.processor.extract_track_info.return_value = {"videoId": "new"}
        self.repairer._replace_cached_liked_track(
            "old", "/liked_songs/a.m4a", None, {"videoId": "new"}
        )
        self.cache.set.assert_not_called()
        self.cache.delete.assert_not_called()

    def test_apply_repairs_records_trigger_only_when_something_repaired(self):
        self.assertEqual(self.repairer.apply_repairs([]), 0)
        self.cache.record_repair_trigger.assert_not_called()

        self.cache.get.return_value = None
        repair = LikedSongRepair(
            path="/liked_songs/a.m4a",
            old_video_id="old",
            new_video_id="new",
            old_track=None,
            replacement={"videoId": "new"},
        )
        self.assertEqual(self.repairer.apply_repairs([repair]), 1)
        self.cache.record_repair_trigger.assert_called_once_with(
            [
                {
                    "old_video_id": "old",
                    "path": "/liked_songs/a.m4a",
                    "new_video_id": "new",
                }
            ]
        )

    def test_apply_removals_without_sync_only_updates_cache(self):
        self.repairer.sync_account = False
        self.cache.get.return_value = [
            {"videoId": "dead"},
            {"videoId": "alive"},
            "garbage",
        ]

        removed = self.repairer.apply_removals([("dead", "/liked_songs/a.m4a")])

        self.assertEqual(removed, 1)
        self.client.rate_song.assert_not_called()
        self.cache.set.assert_any_call("/liked_songs_processed", [{"videoId": "alive"}])
        self.cache.delete.assert_any_call("video_id:/liked_songs/a.m4a")

    def test_remove_dead_track_noop_when_cache_missing_or_track_absent(self):
        self.cache.get.return_value = None
        self.repairer._remove_dead_track_from_cache("dead", "/liked_songs/a.m4a")

        self.cache.get.return_value = [{"videoId": "alive"}]
        self.repairer._remove_dead_track_from_cache("dead", "/liked_songs/a.m4a")

        self.cache.set.assert_not_called()
        self.cache.delete.assert_not_called()


class TestLikedSongsRepairerMatchScore(unittest.TestCase):
    def setUp(self):
        self.repairer = LikedSongsRepairer(
            RepairDependencies(
                client=Mock(),
                cache=Mock(),
                processor=Mock(),
                yt_dlp=Mock(),
                browser="brave",
                sync_account=False,
                logger=None,
            )
        )

    def test_logger_defaults_to_ytmusicfs_logger(self):
        self.assertEqual(self.repairer.logger.name, "YTMusicFS")

    def test_match_score_exact_title_and_artist(self):
        candidate = {"title": "Song", "artists": [{"name": "Artist"}, "bad"]}

        self.assertEqual(self.repairer._match_score(candidate, "Artist", "Song"), 8)

    def test_match_score_normalizes_accents_and_punctuation(self):
        candidate = {"title": "Café—Déjà Vu!", "artists": [{"name": "BEYONCÉ"}]}

        self.assertEqual(
            self.repairer._match_score(candidate, "Beyonce", "cafe deja vu"), 8
        )

    def test_match_score_counts_long_token_overlap_capped_at_four(self):
        candidate = {
            "title": "alpha bravo charlie delta echoes (Live)",
            "artists": [],
        }

        score = self.repairer._match_score(
            candidate, "Nobody", "alpha bravo charlie delta echoes"
        )

        self.assertEqual(score, 4)

    def test_match_score_ignores_short_tokens(self):
        candidate = {"title": "a b cd live", "artists": [{"name": "Other"}]}

        self.assertEqual(self.repairer._match_score(candidate, "Me", "a b cd"), 0)

    def test_match_score_empty_title_and_artist_scores_zero(self):
        self.assertEqual(self.repairer._match_score({}, "", "!!!"), 0)

    def test_match_score_gives_no_artist_credit_for_missing_candidate_artists(self):
        candidate = {"title": "Other", "artists": []}

        self.assertEqual(self.repairer._match_score(candidate, "Artist", "Song"), 0)
