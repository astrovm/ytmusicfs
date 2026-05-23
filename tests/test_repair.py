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
        self.repairer = LikedSongsRepairer(
            client=self.client,
            cache=self.cache,
            processor=self.processor,
            yt_dlp_utils=self.yt_dlp_utils,
            browser="brave",
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
            stats, {"checked": 1, "repaired": 1, "skipped": 0, "failed": 0}
        )
        self.client.rate_song.assert_any_call("new", "LIKE")
        self.client.rate_song.assert_any_call("old", "INDIFFERENT")
        self.cache.clear_unavailable_track.assert_called_once_with(
            "old", "/liked_songs/Artist - Song.m4a"
        )

    def test_repair_skips_when_replacement_stream_is_not_highest_quality(self):
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
            }
        ]
        self.yt_dlp_utils.extract_stream_url.return_value = {"format_id": "140"}

        stats = self.repairer.repair()

        self.assertEqual(
            stats, {"checked": 1, "repaired": 0, "skipped": 1, "failed": 0}
        )
        self.client.rate_song.assert_not_called()
        self.cache.clear_unavailable_track.assert_not_called()

    def test_repair_ignores_unavailable_entries_outside_liked_songs(self):
        self.cache.get_unavailable_tracks.return_value = [
            {"videoId": "old", "path": "/playlists/Mix/Artist - Song.m4a"}
        ]

        stats = self.repairer.repair()

        self.assertEqual(
            stats, {"checked": 0, "repaired": 0, "skipped": 0, "failed": 0}
        )
        self.client.search.assert_not_called()
