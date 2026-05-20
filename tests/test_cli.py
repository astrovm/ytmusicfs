import argparse
import logging
import sqlite3
import subprocess
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from ytmusicfs.cli import (
    CacheCommandHandler,
    ConfigCommandHandler,
    LogsCommandHandler,
    MountCommandHandler,
    MountInspector,
    ServiceCommandHandler,
    StatusCommandHandler,
    UnmountCommandHandler,
    positive_int,
)
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


def make_command_args(tmp_path, **kwargs):
    defaults = {
        "cache_dir": str(tmp_path / "cache"),
        "debug": False,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


@patch("ytmusicfs.cli.MountInspector.find_active_mount_point", return_value=None)
@patch("ytmusicfs.cli.mount_filesystem")
def test_mount_saves_last_settings_and_active_state(
    mock_mount, mock_active_mount, tmp_path
):
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


@patch("ytmusicfs.cli.MountInspector.find_active_mount_point")
@patch("ytmusicfs.cli.mount_filesystem")
def test_mount_refuses_when_already_mounted(mock_mount, mock_active_mount, tmp_path):
    mount_point = tmp_path / "music"
    active_mount = tmp_path / "active"
    mock_active_mount.return_value = active_mount
    args = make_mount_args(tmp_path, mount_point=str(mount_point), browser="brave")

    result = MountCommandHandler(args, logging.getLogger("test")).execute()

    assert result == 1
    mock_mount.assert_not_called()


@patch("ytmusicfs.cli.MountInspector.find_active_mount_point", return_value=None)
@patch("ytmusicfs.cli.mount_filesystem")
def test_mount_reuses_saved_settings(mock_mount, mock_active_mount, tmp_path):
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


@patch("ytmusicfs.cli.MountInspector.find_active_mount_point", return_value=None)
def test_mount_saves_settings_before_mount_call(mock_active_mount, tmp_path):
    mount_point = tmp_path / "music"
    args = make_mount_args(tmp_path, mount_point=str(mount_point), browser="brave")
    config = ConfigManager(cache_dir=str(tmp_path / "cache"))

    def assert_config_saved(**kwargs):
        assert config.load_user_config()["last_mount_point"] == str(
            mount_point.resolve()
        )
        assert config.load_user_config()["last_browser"] == "brave"

    with patch("ytmusicfs.cli.mount_filesystem", side_effect=assert_config_saved):
        result = MountCommandHandler(args, logging.getLogger("test")).execute()

    assert result == 0


def test_mount_without_settings_fails(tmp_path):
    logger = Mock()
    args = make_mount_args(tmp_path)

    result = MountCommandHandler(args, logger).execute()

    assert result == 1
    logger.error.assert_called_once()


@patch("ytmusicfs.cli.MountInspector.find_active_mount_point", return_value=None)
@patch("ytmusicfs.cli.mount_filesystem", side_effect=RuntimeError("auth failed"))
def test_mount_failure_does_not_save_last_settings(
    mock_mount, mock_active_mount, tmp_path
):
    mount_point = tmp_path / "music"
    args = make_mount_args(tmp_path, mount_point=str(mount_point), browser="brave")

    result = MountCommandHandler(args, logging.getLogger("test")).execute()

    config = ConfigManager(cache_dir=str(tmp_path / "cache"))
    assert result == 1
    assert config.load_user_config() == {}
    assert config.load_mount_state() == {}


@patch("ytmusicfs.cli.MountInspector.find_active_mount_point", return_value=None)
@patch("ytmusicfs.cli.mount_filesystem", side_effect=RuntimeError("auth failed"))
def test_mount_failure_restores_previous_settings(
    mock_mount, mock_active_mount, tmp_path
):
    previous_mount = tmp_path / "previous"
    failed_mount = tmp_path / "failed"
    config = ConfigManager(cache_dir=str(tmp_path / "cache"))
    config.save_user_config(
        {
            "last_mount_point": str(previous_mount),
            "last_browser": "brave",
        }
    )
    args = make_mount_args(tmp_path, mount_point=str(failed_mount), browser="firefox")

    result = MountCommandHandler(args, logging.getLogger("test")).execute()

    assert result == 1
    assert config.load_user_config()["last_mount_point"] == str(previous_mount)
    assert config.load_user_config()["last_browser"] == "brave"
    assert config.load_mount_state() == {}


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
    mock_ismount.assert_called_with(mount_point.resolve())
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
@patch("ytmusicfs.cli.os.path.ismount", side_effect=[False, True])
@patch("ytmusicfs.cli.MountInspector.find_active_mount_point")
def test_unmount_uses_detected_mount_when_state_is_stale(
    mock_active_mount, mock_ismount, mock_which, mock_run, tmp_path
):
    stale_mount = tmp_path / "stale"
    active_mount = tmp_path / "active"
    stale_mount.mkdir()
    active_mount.mkdir()
    mock_active_mount.return_value = active_mount
    config = ConfigManager(cache_dir=str(tmp_path / "cache"))
    config.save_mount_state({"mount_point": str(stale_mount)})
    args = make_unmount_args(tmp_path)

    result = UnmountCommandHandler(args, logging.getLogger("test")).execute()

    assert result == 0
    mock_run.assert_called_once_with(
        ["/usr/bin/fusermount", "-u", str(active_mount)],
        check=True,
        capture_output=True,
        text=True,
    )


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
    result = MountInspector.find_active_mount_point()

    assert result == Path("/home/astro/Music/ytmusic")


@patch("ytmusicfs.cli.MountInspector.find_active_mount_point", return_value=None)
def test_status_clears_stale_mount_state(mock_active_mount, tmp_path):
    config = ConfigManager(cache_dir=str(tmp_path / "cache"))
    config.save_mount_state({"mount_point": str(tmp_path / "music")})
    args = make_command_args(tmp_path)

    result = StatusCommandHandler(args, logging.getLogger("test")).execute()

    assert result == 0
    assert config.load_mount_state() == {}


def test_config_set_mount_point_and_browser(tmp_path):
    mount_point = tmp_path / "music"
    browser_args = make_command_args(
        tmp_path,
        config_action="set",
        key="browser",
        value="brave",
    )
    mount_args = make_command_args(
        tmp_path,
        config_action="set",
        key="mount-point",
        value=str(mount_point),
    )

    assert ConfigCommandHandler(browser_args, logging.getLogger("test")).execute() == 0
    assert ConfigCommandHandler(mount_args, logging.getLogger("test")).execute() == 0

    config = ConfigManager(cache_dir=str(tmp_path / "cache"))
    assert config.load_user_config()["last_browser"] == "brave"
    assert config.load_user_config()["last_mount_point"] == str(mount_point.resolve())


@patch("ytmusicfs.cli.MountInspector.find_active_mount_point", return_value=None)
def test_cache_clear_removes_sqlite_files(mock_active_mount, tmp_path):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    for name in CacheCommandHandler.CACHE_FILES:
        (cache_dir / name).write_text("cache", encoding="utf-8")
    audio_dir = cache_dir / "audio"
    audio_dir.mkdir()
    (audio_dir / "song.m4a").write_bytes(b"cache")
    args = make_command_args(tmp_path, cache_action="clear")

    result = CacheCommandHandler(args, logging.getLogger("test")).execute()

    assert result == 0
    assert not any(
        (cache_dir / name).exists() for name in CacheCommandHandler.CACHE_FILES
    )
    assert not audio_dir.exists()


@patch("ytmusicfs.cli.MountInspector.find_active_mount_point")
def test_cache_clear_refuses_while_mounted(mock_active_mount, tmp_path):
    mock_active_mount.return_value = tmp_path / "music"
    args = make_command_args(tmp_path, cache_action="clear")

    result = CacheCommandHandler(args, logging.getLogger("test")).execute()

    assert result == 1


def test_cache_stats_reads_database_counts(tmp_path):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    db_path = cache_dir / "cache.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("CREATE TABLE cache_entries (key TEXT)")
        conn.execute("INSERT INTO cache_entries VALUES ('one')")

    stats = CacheCommandHandler.read_database_stats(db_path)

    assert stats["cache_entries"] == 1


def test_cache_audio_stats_counts_files(tmp_path):
    cache_dir = tmp_path / "cache"
    audio_dir = cache_dir / "audio"
    audio_dir.mkdir(parents=True)
    (audio_dir / "one.m4a").write_bytes(b"123")
    (audio_dir / "two.m4a").write_bytes(b"45")
    args = make_command_args(tmp_path, cache_action="stats")
    handler = CacheCommandHandler(args, logging.getLogger("test"))

    assert handler.audio_stats() == (2, 5)


def test_positive_int_rejects_zero():
    with pytest.raises(argparse.ArgumentTypeError):
        positive_int("0")


@patch("ytmusicfs.cli.subprocess.run")
@patch("ytmusicfs.cli.shutil.which", return_value="/usr/bin/ytmusicfs")
def test_service_install_writes_user_unit(mock_which, mock_run, tmp_path, monkeypatch):
    service_file = tmp_path / "systemd" / "ytmusicfs.service"
    monkeypatch.setattr("ytmusicfs.cli.SYSTEMD_USER_DIR", service_file.parent)
    monkeypatch.setattr("ytmusicfs.cli.SYSTEMD_SERVICE_FILE", service_file)
    config = ConfigManager(cache_dir=str(tmp_path / "cache"))
    config.save_user_config(
        {
            "last_mount_point": str(tmp_path / "music"),
            "last_browser": "brave",
        }
    )
    args = argparse.Namespace(service_action="install", debug=False)

    result = ServiceCommandHandler(args, logging.getLogger("test")).execute()

    assert result == 0
    assert "ExecStart=/usr/bin/ytmusicfs mount --foreground" in service_file.read_text(
        encoding="utf-8"
    )
    assert mock_run.call_count == 2
    mock_run.assert_any_call(
        ["systemctl", "--user", "daemon-reload"],
        check=True,
        capture_output=True,
        text=True,
    )
    mock_run.assert_any_call(
        ["systemctl", "--user", "enable", "ytmusicfs.service"],
        check=True,
        capture_output=True,
        text=True,
    )


def test_service_install_requires_saved_settings(tmp_path, monkeypatch):
    service_file = tmp_path / "systemd" / "ytmusicfs.service"
    monkeypatch.setattr("ytmusicfs.cli.SYSTEMD_USER_DIR", service_file.parent)
    monkeypatch.setattr("ytmusicfs.cli.SYSTEMD_SERVICE_FILE", service_file)
    args = argparse.Namespace(service_action="install", debug=False)

    result = ServiceCommandHandler(args, logging.getLogger("test")).execute()

    assert result == 1
    assert not service_file.exists()


def test_logs_shows_last_50_lines_by_default(tmp_path, monkeypatch, capsys):
    log_file = tmp_path / "ytmusicfs.log"
    lines = [f"line {i}" for i in range(60)]
    log_file.write_text("\n".join(lines), encoding="utf-8")
    monkeypatch.setattr("ytmusicfs.cli.LOG_FILE", log_file)
    args = argparse.Namespace(tail=50, path=False)

    result = LogsCommandHandler(args, logging.getLogger("test")).execute()

    captured = capsys.readouterr()
    assert result == 0
    assert "line 10" in captured.out
    assert "line 59" in captured.out
    assert "line 0" not in captured.out


def test_logs_shows_custom_tail_count(tmp_path, monkeypatch, capsys):
    log_file = tmp_path / "ytmusicfs.log"
    lines = [f"line {i}" for i in range(20)]
    log_file.write_text("\n".join(lines), encoding="utf-8")
    monkeypatch.setattr("ytmusicfs.cli.LOG_FILE", log_file)
    args = argparse.Namespace(tail=5, path=False)

    result = LogsCommandHandler(args, logging.getLogger("test")).execute()

    captured = capsys.readouterr()
    assert result == 0
    assert captured.out.strip().splitlines() == [
        "line 15",
        "line 16",
        "line 17",
        "line 18",
        "line 19",
    ]


def test_logs_path_shows_file_path(tmp_path, monkeypatch, capsys):
    log_file = tmp_path / "ytmusicfs.log"
    log_file.write_text("some log content", encoding="utf-8")
    monkeypatch.setattr("ytmusicfs.cli.LOG_FILE", log_file)
    args = argparse.Namespace(tail=50, path=True)

    result = LogsCommandHandler(args, logging.getLogger("test")).execute()

    captured = capsys.readouterr()
    assert result == 0
    assert str(log_file) in captured.out


def test_logs_missing_file_returns_error(tmp_path, monkeypatch, capsys):
    log_file = tmp_path / "nonexistent.log"
    monkeypatch.setattr("ytmusicfs.cli.LOG_FILE", log_file)
    args = argparse.Namespace(tail=50, path=False)

    result = LogsCommandHandler(args, logging.getLogger("test")).execute()

    captured = capsys.readouterr()
    assert result == 1
    assert "No log file found" in captured.err
