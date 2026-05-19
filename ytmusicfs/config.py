#!/usr/bin/env python3

import logging
from pathlib import Path
from typing import Optional


class ConfigManager:
    """Centralized configuration manager for YTMusicFS."""

    DEFAULT_CACHE_DIR = Path.home() / ".cache" / "ytmusicfs"
    DEFAULT_CACHE_TIMEOUT = 2592000  # 30 days in seconds

    def __init__(
        self,
        cache_dir: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize with optional overrides for defaults."""
        self.logger = logger or logging.getLogger(__name__)
        self.cache_dir = Path(cache_dir) if cache_dir else self.DEFAULT_CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_timeout = self.DEFAULT_CACHE_TIMEOUT
