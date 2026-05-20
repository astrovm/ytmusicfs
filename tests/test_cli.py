import argparse
import logging
import subprocess
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from ytmusicfs.cli import MountCommandHandler, UnmountCommandHandler
from ytmusicfs.config import ConfigManager


@pytest.fixture(autouse=True)
def config_dirs(tmp_path, monkeypatch):
    monkeypatch.setattr(ConfigManager, "DEFAULT_CONFIG_DIR", tmp_path / "config")
    monkeypatch.setattr(ConfigManager, "DEFAULT_CACHE_DIR", tmp_path / "cache-default")


def make_mount_args(tmp_path, mount_point=None, browser=None):
    return argparse.Namespace(
        mount_point=mount_point,
        browser=browser,
        cache_dir=str(tmp_path / "cache"),
        foreground=True,
        debug=False,
    )


def make_unmount_args(tmp_path, mount_point=None):
    return argparse.Namespace(
        mount_point=mount_point,
        cache_dir=str(tmp_path / "cache"),
        debug=False,
    )


@patch("ytmusicfs.cli.mount_filesystem")
def test_mount_saves_last_settings_and_active_state(mock_mount, tmp_path):
    mount_point = tmp_path / "music"
    args = make_mount_args(tmp_path, mount_point=str(mount_point), browser="brave")

    result = MountCommandHandler(args, logging.getLogger("test")).execute()

    config = ConfigManager(cache_dir=str(tmp_path / "cache"))
    assert result == 0
    assert config.load_user_config()["last_mount_point"] == str(mount_point.resolve())
    assert config.load_user_config()["last_browser"] == "brave"
    assert config.load_mount_state()["mount_point"] == str(mount_point.resolve())
    mock_mount.assert_called_once_with(
        mount_point=str(mount_point.resolve()),
        cache_dir=str(tmp_path / "cache"),
        foreground=True,
        browser="brave",
    )


@patch("ytmusicfs.cli.mount_filesystem")
def test_mount_reuses_saved_settings(mock_mount, tmp_path):
    mount_point = tmp_path / "music"
    config = ConfigManager(cache_dir=str(tmp_path / "cache"))
    config.save_user_config(
        {
            "last_mount_point": str(mount_point),
            "last_browser": "brave",
        }
    )
    args = make_mount_args(tmp_path)

    result = MountCommandHandler(args, logging.getLogger("test")).execute()

    assert result == 0
    mock_mount.assert_called_once_with(
        mount_point=str(mount_point.resolve()),
        cache_dir=str(tmp_path / "cache"),
        foreground=True,
        browser="brave",
    )


def test_mount_without_settings_fails(tmp_path):
    logger = Mock()
    args = make_mount_args(tmp_path)

    result = MountCommandHandler(args, logger).execute()

    assert result == 1
    logger.error.assert_called_once()


@patch("ytmusicfs.cli.subprocess.run")
@patch("ytmusicfs.cli.shutil.which", return_value="/usr/bin/fusermount")
@patch("ytmusicfs.cli.os.path.ismount", return_value=True)
def test_unmount_uses_active_mount_state(mock_ismount, mock_which, mock_run, tmp_path):
    mount_point = tmp_path / "music"
    mount_point.mkdir()
    config = ConfigManager(cache_dir=str(tmp_path / "cache"))
    config.save_mount_state({"mount_point": str(mount_point)})
    args = make_unmount_args(tmp_path)

    result = UnmountCommandHandler(args, logging.getLogger("test")).execute()

    assert result == 0
    mock_ismount.assert_called_once_with(mount_point.resolve())
    mock_run.assert_called_once_with(
        ["/usr/bin/fusermount", "-u", str(mount_point.resolve())],
        check=True,
        capture_output=True,
        text=True,
    )
    assert config.load_mount_state() == {}


@patch("ytmusicfs.cli.shutil.which", return_value="/usr/bin/fusermount")
@patch("ytmusicfs.cli.os.path.ismount", return_value=False)
def test_unmount_clears_stale_mount_state(mock_ismount, mock_which, tmp_path):
    mount_point = tmp_path / "music"
    mount_point.mkdir()
    config = ConfigManager(cache_dir=str(tmp_path / "cache"))
    config.save_mount_state({"mount_point": str(mount_point)})
    args = make_unmount_args(tmp_path)

    result = UnmountCommandHandler(args, logging.getLogger("test")).execute()

    assert result == 1
    assert config.load_mount_state() == {}


@patch("ytmusicfs.cli.subprocess.run")
@patch("ytmusicfs.cli.shutil.which", return_value="/usr/bin/fusermount")
@patch("ytmusicfs.cli.os.path.ismount", return_value=True)
def test_unmount_reports_fusermount_failure(
    mock_ismount, mock_which, mock_run, tmp_path
):
    mount_point = tmp_path / "music"
    mount_point.mkdir()
    mock_run.side_effect = subprocess.CalledProcessError(
        returncode=1,
        cmd=["fusermount", "-u", str(mount_point)],
        stderr="not mounted",
    )
    args = make_unmount_args(tmp_path, mount_point=str(mount_point))

    result = UnmountCommandHandler(args, logging.getLogger("test")).execute()

    assert result == 1


@patch(
    "pathlib.Path.read_text",
    return_value=(
        "39 31 0:35 / /tmp/other rw,nosuid,nodev - fuse.other other rw\n"
        "40 31 0:36 / /home/astro/Music/ytmusic rw,nosuid,nodev "
        "- fuse.ytmusicfs ytmusicfs rw\n"
    ),
)
def test_find_active_mount_point_from_mountinfo(mock_read_text):
    result = UnmountCommandHandler.find_active_mount_point()

    assert result == Path("/home/astro/Music/ytmusic")
