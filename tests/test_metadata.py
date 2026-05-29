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


if __name__ == "__main__":
    unittest.main()
