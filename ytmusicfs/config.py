#!/usr/bin/env python3

from pathlib import Path
from typing import Optional, Tuple
import json
import logging


class ConfigManager:
    """Centralized configuration manager for YTMusicFS."""

    DEFAULT_CONFIG_DIR = Path.home() / ".config" / "ytmusicfs"
    DEFAULT_AUTH_FILE = DEFAULT_CONFIG_DIR / "oauth.json"
    DEFAULT_CRED_FILE = DEFAULT_CONFIG_DIR / "credentials.json"
    DEFAULT_CACHE_DIR = Path.home() / ".cache" / "ytmusicfs"
    DEFAULT_CACHE_TIMEOUT = 2592000  # 30 days in seconds

    def __init__(
        self,
        auth_file: Optional[str] = None,
        credentials_file: Optional[str] = None,
        cache_dir: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize with optional overrides for defaults."""
        self.logger = logger or logging.getLogger(__name__)
        self.config_dir = self.DEFAULT_CONFIG_DIR
        self.config_dir.mkdir(parents=True, exist_ok=True)

        self.auth_file = Path(auth_file) if auth_file else self.DEFAULT_AUTH_FILE
        default_credentials_file = (
            self.auth_file.parent / "credentials.json"
            if auth_file
            else self.DEFAULT_CRED_FILE
        )
        self.credentials_file = (
            Path(credentials_file) if credentials_file else default_credentials_file
        )
        self.cache_dir = Path(cache_dir) if cache_dir else self.DEFAULT_CACHE_DIR
        self.cache_timeout = self.DEFAULT_CACHE_TIMEOUT

        self.client_id: Optional[str] = None
        self.client_secret: Optional[str] = None
        self._load_credentials()

    def _load_credentials(self) -> None:
        """Load client credentials from file if available."""
        if not self.credentials_file.exists():
            self.logger.debug(f"No credentials file found at {self.credentials_file}")
            return

        try:
            with open(self.credentials_file, "r") as f:
                creds = json.load(f)
                self.client_id = creds.get("client_id")
                self.client_secret = creds.get("client_secret")
            self.logger.info(f"Loaded credentials from {self.credentials_file}")
        except Exception as e:
            self.logger.warning(f"Failed to load credentials: {e}")

    def save_credentials(self, client_id: str, client_secret: str) -> None:
        """Save OAuth credentials to the credentials file."""
        self.client_id = client_id
        self.client_secret = client_secret
        self.credentials_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.credentials_file, "w") as f:
            json.dump(
                {"client_id": client_id, "client_secret": client_secret}, f, indent=2
            )
        self.logger.info(f"Saved credentials to {self.credentials_file}")

    def get_credentials(self) -> Tuple[Optional[str], Optional[str]]:
        """Return client_id and client_secret."""
        return self.client_id, self.client_secret
