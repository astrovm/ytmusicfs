#!/usr/bin/env python3

import json
import logging
from pathlib import Path
from typing import Any


class ConfigManager:
    """Centralized configuration manager for YTMusicFS."""

    DEFAULT_CACHE_DIR = Path.home() / ".cache" / "ytmusicfs"
    DEFAULT_CONFIG_DIR = Path.home() / ".config" / "ytmusicfs"
    CONFIG_FILE_NAME = "config.json"
    MOUNT_STATE_FILE_NAME = "mount.json"
    DEFAULT_CACHE_TIMEOUT = 2592000  # 30 days in seconds

    def __init__(
        self,
        cache_dir: str | None = None,
        config_dir: str | None = None,
        logger: logging.Logger | None = None,
    ):
        """Initialize with optional overrides for defaults."""
        self.logger = logger or logging.getLogger(__name__)
        self.cache_dir = Path(cache_dir) if cache_dir else self.DEFAULT_CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.config_dir = Path(config_dir) if config_dir else self.DEFAULT_CONFIG_DIR
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.config_file = self.config_dir / self.CONFIG_FILE_NAME
        self.mount_state_file = self.cache_dir / self.MOUNT_STATE_FILE_NAME
        self.cache_timeout = self.DEFAULT_CACHE_TIMEOUT

    def load_user_config(self) -> dict[str, Any]:
        """Load persisted user settings."""
        return self._load_json(self.config_file)

    def save_user_config(self, values: dict[str, Any]) -> None:
        """Persist user settings."""
        self._save_json(self.config_file, values)

    def load_mount_state(self) -> dict[str, Any]:
        """Load active mount state."""
        return self._load_json(self.mount_state_file)

    def save_mount_state(self, values: dict[str, Any]) -> None:
        """Persist active mount state."""
        self._save_json(self.mount_state_file, values)

    def clear_mount_state(self) -> None:
        """Remove active mount state if it exists."""
        self.mount_state_file.unlink(missing_ok=True)

    def _load_json(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}

        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            self.logger.warning("Ignoring unreadable config file %s: %s", path, error)
            return {}

        if not isinstance(data, dict):
            self.logger.warning("Ignoring invalid config file %s", path)
            return {}

        return data

    def _save_json(self, path: Path, values: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(values, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
