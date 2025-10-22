#!/usr/bin/env python3

import json
import logging
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from ytmusicfs.oauth_adapter import YTMusicOAuthAdapter


class DummyOAuthCredentials:
    refresh_calls = []

    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret

    def refresh_token(self, refresh_token):
        DummyOAuthCredentials.refresh_calls.append(refresh_token)
        return {
            "access_token": "new-token",
            "expires_in": 1800,
            "token_type": "Bearer",
            "scope": "https://www.googleapis.com/auth/youtube.readonly",
        }


class DummyYTMusic:
    init_calls = 0
    playlist_calls = 0

    def __init__(self, *_, **__):
        DummyYTMusic.init_calls += 1
        self.context = {
            "context": {
                "client": {"clientName": "WEB_REMIX", "clientVersion": "1.000000"}
            }
        }

    def get_library_playlists(self, limit=1):  # pragma: no cover - small helper
        DummyYTMusic.playlist_calls += 1
        if DummyYTMusic.playlist_calls == 1:
            raise Exception("Server returned HTTP 400: Bad Request.")
        return []


class AlwaysFailingYTMusic(DummyYTMusic):
    def get_library_playlists(self, limit=1):
        raise Exception("Server returned HTTP 400: Bad Request.")


class TestYTMusicOAuthAdapter(unittest.TestCase):
    def setUp(self):
        DummyOAuthCredentials.refresh_calls = []
        DummyYTMusic.init_calls = 0
        DummyYTMusic.playlist_calls = 0
        self.logger = logging.getLogger("ytmusicfs-test")

    def _create_auth_files(
        self, directory: Path, include_refresh: bool = True
    ) -> tuple[str, str]:
        auth_file = directory / "oauth.json"
        credentials_file = directory / "credentials.json"

        oauth_payload = {
            "access_token": "old-token",
            "refresh_token": "refresh-token" if include_refresh else "",
            "expires_in": 3600,
            "expires_at": 0,
            "token_type": "Bearer",
            "scope": "https://www.googleapis.com/auth/youtube.readonly",
            "authorization": "Bearer old-token",
            "Authorization": "Bearer old-token",
        }
        auth_file.write_text(json.dumps(oauth_payload))

        credentials_payload = {"client_id": "client", "client_secret": "secret"}
        credentials_file.write_text(json.dumps(credentials_payload))

        return str(auth_file), str(credentials_file)

    def test_refreshes_token_before_retrying_initialization(self):
        with TemporaryDirectory() as tmpdir:
            directory = Path(tmpdir)
            auth_path, cred_path = self._create_auth_files(directory)

            with (
                patch("ytmusicfs.oauth_adapter.YTMusic", DummyYTMusic),
                patch(
                    "ytmusicfs.oauth_adapter.OAuthCredentials", DummyOAuthCredentials
                ),
                patch("ytmusicfs.oauth_adapter.time.time", return_value=1000),
                patch(
                    "ytmusicfs.oauth_adapter.YTMusicOAuthAdapter._fetch_web_client_context",
                    return_value=("1.20240925.01.00", "WEB_REMIX"),
                ),
            ):
                adapter = YTMusicOAuthAdapter(
                    auth_file=auth_path,
                    client_id="client",
                    client_secret="secret",
                    credentials_file=cred_path,
                    logger=self.logger,
                )
                auth_data = json.loads(Path(auth_path).read_text())

        self.assertIsInstance(adapter.ytmusic, DummyYTMusic)
        self.assertEqual(DummyYTMusic.init_calls, 2)
        self.assertEqual(DummyYTMusic.playlist_calls, 2)
        self.assertEqual(DummyOAuthCredentials.refresh_calls, ["refresh-token"])
        self.assertEqual(
            adapter.ytmusic.context["context"]["client"]["clientVersion"],
            "1.20240925.01.00",
        )

        self.assertEqual(auth_data["access_token"], "new-token")
        self.assertEqual(auth_data["expires_in"], 1800)
        self.assertEqual(auth_data["expires_at"], 1000 + 1800)
        self.assertEqual(auth_data["token_type"], "Bearer")
        self.assertNotIn("authorization", auth_data)
        self.assertNotIn("Authorization", auth_data)

    def test_skips_context_update_when_metadata_unavailable(self):
        with TemporaryDirectory() as tmpdir:
            directory = Path(tmpdir)
            auth_path, cred_path = self._create_auth_files(directory)

            with (
                patch("ytmusicfs.oauth_adapter.YTMusic", DummyYTMusic),
                patch(
                    "ytmusicfs.oauth_adapter.OAuthCredentials", DummyOAuthCredentials
                ),
                patch(
                    "ytmusicfs.oauth_adapter.YTMusicOAuthAdapter._fetch_web_client_context",
                    return_value=None,
                ),
            ):
                adapter = YTMusicOAuthAdapter(
                    auth_file=auth_path,
                    client_id="client",
                    client_secret="secret",
                    credentials_file=cred_path,
                    logger=self.logger,
                )

        self.assertIsInstance(adapter.ytmusic, DummyYTMusic)
        self.assertEqual(
            adapter.ytmusic.context["context"]["client"]["clientVersion"],
            "1.000000",
        )

    def test_raises_when_refresh_unavailable(self):
        with TemporaryDirectory() as tmpdir:
            directory = Path(tmpdir)
            auth_path, cred_path = self._create_auth_files(
                directory, include_refresh=False
            )

            with (
                patch("ytmusicfs.oauth_adapter.YTMusic", AlwaysFailingYTMusic),
                patch(
                    "ytmusicfs.oauth_adapter.OAuthCredentials", DummyOAuthCredentials
                ),
            ):
                with self.assertRaises(Exception):
                    YTMusicOAuthAdapter(
                        auth_file=auth_path,
                        client_id="client",
                        client_secret="secret",
                        credentials_file=cred_path,
                        logger=self.logger,
                    )

        self.assertEqual(DummyOAuthCredentials.refresh_calls, [])


if __name__ == "__main__":
    unittest.main()
