#!/usr/bin/env python3

import os
import unittest
from unittest.mock import patch, MagicMock

from ytmusicfs import __version__
from ytmusicfs.utils.oauth_adapter import YTMusicOAuthAdapter


class TestBasics(unittest.TestCase):
    """Basic tests for YTMusicFS package."""

    def test_version(self):
        """Test that version is a string."""
        self.assertIsInstance(__version__, str)

    @patch("os.path.exists")
    @patch("builtins.open")
    @patch("json.load")
    @patch("ytmusicapi.YTMusic")
    def test_oauth_adapter_init(
        self, mock_ytmusic, mock_json_load, mock_open, mock_exists
    ):
        """Test YTMusicOAuthAdapter initialization."""
        # Mock prerequisites
        mock_exists.return_value = True
        mock_json_load.return_value = {
            "client_id": "fake_client_id",
            "client_secret": "fake_client_secret",
        }

        # Mock YTMusic instance
        mock_ytmusic_instance = MagicMock()
        mock_ytmusic_instance.get_library_playlists.return_value = []
        mock_ytmusic.return_value = mock_ytmusic_instance

        # Create adapter
        auth_file = "test_oauth.json"
        client_id = "test_client_id"
        client_secret = "test_client_secret"

        adapter = YTMusicOAuthAdapter(
            auth_file=auth_file, client_id=client_id, client_secret=client_secret
        )

        # Verify initialization
        self.assertEqual(adapter.auth_file, auth_file)
        self.assertEqual(adapter.client_id, client_id)
        self.assertEqual(adapter.client_secret, client_secret)

        # Verify YTMusic was initialized
        mock_ytmusic.assert_called_once()
        mock_ytmusic_instance.get_library_playlists.assert_called_once()


if __name__ == "__main__":
    unittest.main()
