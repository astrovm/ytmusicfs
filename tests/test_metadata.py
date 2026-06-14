import errno
import logging
import unittest
from unittest.mock import MagicMock, Mock

from ytmusicfs.metadata import MetadataManager


class TestMetadataManager(unittest.TestCase):
    def setUp(self):
        self.cache = Mock()
        self.logger = logging.getLogger("test")
        self.thread_manager = Mock()
        self.thread_manager.create_lock.return_value = MagicMock()
        self.metadata = MetadataManager(self.cache, self.logger, self.thread_manager)

    def test_get_video_id_repairs_stale_directory_type_for_audio_path(self):
        path = "/playlists/Mix/Artist - Song.m4a"
        self.cache.get_entry_type.return_value = "directory"
        self.cache.get.return_value = "video123"

        result = self.metadata.get_video_id(path)

        self.assertEqual(result, "video123")
        self.cache.mark_valid.assert_called_once_with(path, is_directory=False)

    def test_get_video_id_rejects_non_audio_directory(self):
        self.cache.get_entry_type.return_value = "directory"

        with self.assertRaises(OSError) as context:
            self.metadata.get_video_id("/playlists/Mix")

        self.assertEqual(context.exception.errno, errno.EINVAL)

    def test_get_video_id_scans_processed_tracks_once(self):
        path = "/playlists/Mix/Artist - Song.m4a"
        self.cache.get_entry_type.return_value = "file"
        self.cache.get_file_attrs_from_parent_dir.return_value = None
        self.cache.get.side_effect = [
            None,
            [{"filename": "Artist - Song.m4a", "videoId": "video123"}],
        ]
        content_fetcher = Mock()
        content_fetcher.get_playlist_entry_from_path.return_value = {"id": "mix"}
        self.metadata.set_content_fetcher(content_fetcher)

        result = self.metadata.get_video_id(path)

        self.assertEqual(result, "video123")
        self.assertEqual(self.cache.get.call_count, 2)
        self.cache.set.assert_called_once_with(f"video_id:{path}", "video123")


if __name__ == "__main__":
    unittest.main()
