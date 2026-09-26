#!/usr/bin/env python3

import json
from unittest.mock import Mock

import pytest

from ytmusicfs.config import ConfigManager


@pytest.fixture
def config(tmp_path):
    return ConfigManager(
        cache_dir=str(tmp_path / "cache"),
        config_dir=str(tmp_path / "config"),
        logger=Mock(),
    )


class TestConfigManager:
    def test_init_creates_explicit_directories(self, config, tmp_path):
        assert config.cache_dir == tmp_path / "cache"
        assert config.config_dir == tmp_path / "config"
        assert config.cache_dir.is_dir()
        assert config.config_dir.is_dir()
        assert config.config_file == tmp_path / "config" / "config.json"
        assert config.mount_state_file == tmp_path / "cache" / "mount.json"
        assert config.cache_timeout == ConfigManager.DEFAULT_CACHE_TIMEOUT

    def test_init_uses_default_directories_when_not_given(self, tmp_path, monkeypatch):
        monkeypatch.setattr(ConfigManager, "DEFAULT_CACHE_DIR", tmp_path / "dc")
        monkeypatch.setattr(ConfigManager, "DEFAULT_CONFIG_DIR", tmp_path / "dconf")

        config = ConfigManager()

        assert config.cache_dir == tmp_path / "dc"
        assert config.config_dir == tmp_path / "dconf"
        assert config.cache_dir.is_dir()
        assert config.config_dir.is_dir()

    def test_load_user_config_returns_empty_dict_when_missing(self, config):
        assert config.load_user_config() == {}

    def test_save_user_config_round_trips_as_sorted_json(self, config):
        config.save_user_config({"last_browser": "brave", "a": 1})

        assert config.load_user_config() == {"last_browser": "brave", "a": 1}
        text = config.config_file.read_text(encoding="utf-8")
        assert text.endswith("\n")
        assert text.index('"a"') < text.index('"last_browser"')

    def test_save_mount_state_recreates_missing_parent_directory(self, config):
        config.cache_dir.rmdir()

        config.save_mount_state({"mount_point": "/mnt/music"})

        assert config.load_mount_state() == {"mount_point": "/mnt/music"}

    def test_clear_mount_state_removes_file_and_tolerates_missing(self, config):
        config.save_mount_state({"mount_point": "/mnt/music"})

        config.clear_mount_state()
        config.clear_mount_state()

        assert not config.mount_state_file.exists()
        assert config.load_mount_state() == {}

    def test_load_ignores_invalid_json(self, config):
        config.config_file.write_text("{not json", encoding="utf-8")

        assert config.load_user_config() == {}
        config.logger.warning.assert_called_once()

    def test_load_ignores_unreadable_file(self, config):
        config.config_file.mkdir()

        assert config.load_user_config() == {}
        config.logger.warning.assert_called_once()

    def test_load_ignores_non_object_json(self, config):
        config.mount_state_file.write_text(json.dumps([1, 2]), encoding="utf-8")

        assert config.load_mount_state() == {}
        config.logger.warning.assert_called_once()
