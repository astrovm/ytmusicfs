import logging
import unittest
from unittest.mock import Mock

from ytmusicfs.repair import LikedSongsRepairer


class TestLikedSongsRepairer(unittest.TestCase):
    def setUp(self):
        self.client = Mock()
        self.cache = Mock()
        self.processor = Mock()
        self.yt_dlp_utils = Mock()
        self.cache.is_no_replacement.return_value = False
        self.repairer = LikedSongsRepairer(
            client=self.client,
            cache=self.cache,
            processor=self.processor,
            yt_dlp_utils=self.yt_dlp_utils,
            browser="brave",
            sync_account=True,
            logger=logging.getLogger("test"),
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
