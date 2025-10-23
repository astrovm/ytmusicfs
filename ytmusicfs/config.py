#!/usr/bin/env python3

from pathlib import Path
from typing import Optional
import logging


class ConfigManager:
    """Centralized configuration manager for YTMusicFS."""

    DEFAULT_CONFIG_DIR = Path.home() / ".config" / "ytmusicfs"
    DEFAULT_AUTH_FILE = DEFAULT_CONFIG_DIR / "browser.json"
    DEFAULT_CACHE_DIR = Path.home() / ".cache" / "ytmusicfs"
    DEFAULT_CACHE_TIMEOUT = 2592000  # 30 days in seconds

    def __init__(
        self,
        auth_file: Optional[str] = None,
        cache_dir: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize with optional overrides for defaults."""
        self.logger = logger or logging.getLogger(__name__)
        self.config_dir = self.DEFAULT_CONFIG_DIR
        self.config_dir.mkdir(parents=True, exist_ok=True)

        self.auth_file = Path(auth_file) if auth_file else self.DEFAULT_AUTH_FILE
        self.auth_file.parent.mkdir(parents=True, exist_ok=True)
        self.cache_dir = Path(cache_dir) if cache_dir else self.DEFAULT_CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_timeout = self.DEFAULT_CACHE_TIMEOUT
