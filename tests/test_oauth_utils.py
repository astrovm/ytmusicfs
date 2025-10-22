from __future__ import annotations

import json
import logging
from pathlib import Path
from types import SimpleNamespace

from requests.structures import CaseInsensitiveDict

from ytmusicfs import oauth_adapter, oauth_setup
from ytmusicfs.oauth_utils import apply_server_client_version


class DummyResponse:
    def __init__(self, text: str) -> None:
        self.text = text


def test_apply_server_client_version_updates_client(monkeypatch) -> None:
    version = "1.2345.678"
    visitor = "visitor"
    response = DummyResponse(
        f"ytcfg.set({{\"INNERTUBE_CLIENT_VERSION\": \"{version}\", \"VISITOR_DATA\": \"{visitor}\"}});"
    )

    class DummyYTMusic:
        def __init__(self) -> None:
            self.context = {
                "context": {"client": {"clientVersion": "initial", "visitorData": "initial"}}
            }
            self._auth_headers = CaseInsensitiveDict(
                {
                    "X-YouTube-Client-Version": "initial",
                    "X-Goog-Visitor-Id": "initial",
                }
            )
            self.__dict__["base_headers"] = CaseInsensitiveDict(
                {
                    "X-YouTube-Client-Version": "initial",
                    "X-Goog-Visitor-Id": "initial",
                }
            )
            self.calls: list[tuple[str, bool]] = []

        def _send_get_request(self, url: str, params=None, use_base_headers: bool = False):
            self.calls.append((url, use_base_headers))
            return response

        @property
        def headers(self):
            return self.__dict__["base_headers"]

    dummy = DummyYTMusic()

    applied_version = apply_server_client_version(dummy)  # type: ignore[arg-type]

    assert applied_version == version
    assert dummy.context["context"]["client"]["clientVersion"] == version
    assert dummy.context["context"]["client"]["visitorData"] == visitor
    assert dummy._auth_headers["X-YouTube-Client-Version"] == version
    assert dummy._auth_headers["X-Goog-Visitor-Id"] == visitor
    assert (
        dummy.__dict__["base_headers"]["X-YouTube-Client-Version"] == version
    )
    assert dummy.__dict__["base_headers"]["X-Goog-Visitor-Id"] == visitor
    assert dummy.calls == [("https://music.youtube.com", True)]


def test_oauth_adapter_applies_server_version(monkeypatch, tmp_path: Path) -> None:
    call_count = 0

    def fake_apply(ytmusic, logger=None):
        nonlocal call_count
        call_count += 1

    monkeypatch.setattr(oauth_adapter, "apply_server_client_version", fake_apply)

    class DummyConfigManager:
        def __init__(self, auth_file, logger=None, **_kwargs):
            self.auth_file = Path(auth_file)

        def get_credentials(self):
            return (None, None)

    monkeypatch.setattr(oauth_adapter, "ConfigManager", DummyConfigManager)

    class DummyOAuthCredentials:
        def __init__(self, client_id, client_secret):
            self.client_id = client_id
            self.client_secret = client_secret

    monkeypatch.setattr(oauth_adapter, "OAuthCredentials", DummyOAuthCredentials)

    class DummyYTMusic:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
            self.context = {"context": {"client": {"clientVersion": "initial"}}}

        def get_library_playlists(self, limit: int):
            self.limit = limit
            return []

    monkeypatch.setattr(oauth_adapter, "YTMusic", DummyYTMusic)

    auth_file = tmp_path / "oauth.json"
    auth_file.write_text("{}")

    logger = logging.getLogger("test-adapter")

    adapter = oauth_adapter.YTMusicOAuthAdapter(
        auth_file=str(auth_file),
        client_id="client",
        client_secret="secret",
        logger=logger,
    )

    assert isinstance(adapter.ytmusic, DummyYTMusic)
    assert call_count == 1


def test_oauth_adapter_synchronizes_visitor_data(monkeypatch, tmp_path: Path) -> None:
    version = "2.468.0"
    visitor = "visitor-data"
    response = DummyResponse(
        "ytcfg.set({\"INNERTUBE_CLIENT_VERSION\": \"%s\", \"VISITOR_DATA\": \"%s\"});"
        % (version, visitor)
    )

    def passthrough_apply(ytmusic, logger=None):
        return apply_server_client_version(ytmusic, logger=logger)

    monkeypatch.setattr(oauth_adapter, "apply_server_client_version", passthrough_apply)

    class DummyConfigManager:
        def __init__(self, auth_file, logger=None, **_kwargs):
            self.auth_file = Path(auth_file)

        def get_credentials(self):
            return ("id", "secret")

    monkeypatch.setattr(oauth_adapter, "ConfigManager", DummyConfigManager)

    class DummyOAuthCredentials:
        def __init__(self, client_id, client_secret):
            self.client_id = client_id
            self.client_secret = client_secret

    monkeypatch.setattr(oauth_adapter, "OAuthCredentials", DummyOAuthCredentials)

    class DummyYTMusic:
        def __init__(self, *args, **kwargs):
            self.context = {
                "context": {"client": {"clientVersion": "initial", "visitorData": "initial"}}
            }
            self._auth_headers = CaseInsensitiveDict(
                {
                    "X-YouTube-Client-Version": "initial",
                    "X-Goog-Visitor-Id": "initial",
                }
            )
            self.__dict__["base_headers"] = CaseInsensitiveDict(
                {
                    "X-YouTube-Client-Version": "initial",
                    "X-Goog-Visitor-Id": "initial",
                }
            )

        def _send_get_request(self, url: str, params=None, use_base_headers: bool = False):
            return response

        def get_library_playlists(self, limit: int):
            self.limit = limit
            return []

        @property
        def headers(self):
            return self.__dict__["base_headers"]

    monkeypatch.setattr(oauth_adapter, "YTMusic", DummyYTMusic)

    auth_file = tmp_path / "oauth.json"
    auth_file.write_text("{}")

    adapter = oauth_adapter.YTMusicOAuthAdapter(
        auth_file=str(auth_file),
        client_id="client",
        client_secret="secret",
        logger=logging.getLogger("test-adapter-visitor"),
    )

    client_context = adapter.ytmusic.context["context"]["client"]
    assert client_context["clientVersion"] == version
    assert client_context["visitorData"] == visitor
    assert adapter.ytmusic._auth_headers["X-Goog-Visitor-Id"] == visitor
    assert adapter.ytmusic.__dict__["base_headers"]["X-Goog-Visitor-Id"] == visitor


def test_oauth_setup_cli_applies_server_version(monkeypatch, tmp_path: Path) -> None:
    call_count = 0

    def fake_apply(ytmusic, logger=None):
        nonlocal call_count
        call_count += 1

    monkeypatch.setattr(oauth_setup, "apply_server_client_version", fake_apply)

    class DummyConfigManager:
        def __init__(self, auth_file=None, credentials_file=None, logger=None):
            self.auth_file = Path(auth_file) if auth_file else tmp_path / "oauth.json"
            self.credentials_file = (
                Path(credentials_file)
                if credentials_file
                else tmp_path / "credentials.json"
            )

        def get_credentials(self):
            return (None, None)

        def save_credentials(self, client_id, client_secret):
            self.saved = (client_id, client_secret)

    monkeypatch.setattr(oauth_setup, "ConfigManager", DummyConfigManager)

    class DummyOAuthCredentials:
        def __init__(self, client_id, client_secret):
            self.client_id = client_id
            self.client_secret = client_secret

    monkeypatch.setattr(oauth_setup, "OAuthCredentials", DummyOAuthCredentials)

    class DummyYTMusic:
        def __init__(self, *args, **kwargs):
            self.context = {"context": {"client": {"clientVersion": "initial"}}}

        def get_library_playlists(self, limit: int):
            self.limit = limit
            return []

    monkeypatch.setattr(oauth_setup, "YTMusic", DummyYTMusic)

    def fake_setup(filepath, client_id, client_secret, open_browser):
        Path(filepath).write_text(
            json.dumps(
                {
                    "access_token": "token",
                    "client_id": client_id,
                    "client_secret": client_secret,
                }
            )
        )

    monkeypatch.setattr(oauth_setup, "ytmusic_setup_oauth", fake_setup)

    args = SimpleNamespace(
        client_id="client",
        client_secret="secret",
        auth_file=str(tmp_path / "oauth.json"),
        credentials_file=str(tmp_path / "credentials.json"),
        open_browser=False,
        debug=False,
        logger=logging.getLogger("test-cli"),
    )

    exit_code, _ = oauth_setup.main(args)

    assert exit_code == 0
    assert call_count == 1
