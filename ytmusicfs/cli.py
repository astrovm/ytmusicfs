#!/usr/bin/env python3

import argparse
import importlib.util
import logging
import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any, ClassVar

from ytmusicfs import __version__
from ytmusicfs.config import ConfigManager

LOG_DIR = Path.home() / ".cache" / "ytmusicfs" / "logs"
LOG_FILE = LOG_DIR / "ytmusicfs.log"
SYSTEMD_USER_DIR = Path.home() / ".config" / "systemd" / "user"
SYSTEMD_SERVICE_FILE = SYSTEMD_USER_DIR / "ytmusicfs.service"
JS_RUNTIMES = ("node", "bun", "deno", "quickjs")


def add_common_options(parser: argparse.ArgumentParser) -> None:
    """Add options shared by commands that touch config/cache state."""
    parser.add_argument("--cache-dir", "-c", help="Cache directory")
    parser.add_argument(
        "--debug", "-d", action="store_true", help="Enable debug logging"
    )


def add_deferred_common_options(parser: argparse.ArgumentParser) -> None:
    """Allow shared options after nested subcommands without changing defaults."""
    parser.add_argument(
        "--cache-dir",
        "-c",
        default=argparse.SUPPRESS,
        help="Cache directory",
    )
    parser.add_argument(
        "--debug",
        "-d",
        action="store_true",
        default=argparse.SUPPRESS,
        help="Enable debug logging",
    )


def add_debug_option(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--debug", "-d", action="store_true", help="Enable debug logging"
    )


def add_deferred_debug_option(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--debug",
        "-d",
        action="store_true",
        default=argparse.SUPPRESS,
        help="Enable debug logging",
    )


def print_error(message: str) -> None:
    """Print a user-facing error."""
    print(message, file=sys.stderr)


def positive_int(value: str) -> int:
    """Parse a positive integer for CLI options."""
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be greater than 0")
    return parsed


def setup_logging(args: argparse.Namespace) -> logging.Logger:
    """Configure logging based on command-line arguments.

    Args:
        args: Parsed command-line arguments.

    Returns:
        Configured logger instance.
    """
    log_level = logging.DEBUG if args.debug else logging.INFO
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    handlers = [logging.StreamHandler(sys.stdout)]
    foreground = getattr(args, "foreground", False)
    if not foreground and not getattr(args, "skip_log_file", False):
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(str(LOG_FILE)))

    logging.basicConfig(level=log_level, format=log_format, handlers=handlers)

    return logging.getLogger("YTMusicFS")


def mount_filesystem(
    mount_point: str,
    cache_dir: str,
    foreground: bool,
    browser: str,
) -> None:
    """Import and mount the filesystem only when the mount command runs."""
    from ytmusicfs.filesystem import mount_ytmusicfs

    mount_ytmusicfs(
        mount_point=mount_point,
        cache_dir=cache_dir,
        foreground=foreground,
        browser=browser,
    )


class MountCommandHandler:
    """Handles the 'mount' command logic."""

    def __init__(self, args: argparse.Namespace, logger: logging.Logger):
        """Initialize the mount command handler.

        Args:
            args: Parsed command-line arguments.
            logger: Logger instance.
        """
        self.args = args
        self.logger = logger
        self.config = ConfigManager(
            cache_dir=args.cache_dir,
            logger=logger,
        )

    def resolve_mount_settings(self) -> tuple[Path, str] | None:
        """Resolve mount settings from CLI options and saved config."""
        saved_config = self.config.load_user_config()
        mount_point_value = self.args.mount_point or saved_config.get(
            "last_mount_point"
        )
        browser = self.args.browser or saved_config.get("last_browser")

        if not mount_point_value or not browser:
            self.logger.error(
                "Missing mount settings. Run: ytmusicfs mount "
                "--mount-point ~/Music/ytmusic --browser brave"
            )
            return None

        return Path(mount_point_value).expanduser(), str(browser)

    def execute(self) -> int:
        """Execute the mount command.

        Returns:
            Exit code (0 for success, 1 for failure).
        """
        self.logger.info(f"YTMusicFS version {__version__}")
        settings = self.resolve_mount_settings()
        if settings is None:
            return 1

        mount_point, browser = settings
        mount_point.mkdir(parents=True, exist_ok=True)
        mount_point = mount_point.resolve()
        previous_user_config = self.config.load_user_config()

        active_mount = MountInspector.find_active_mount_point()
        if active_mount:
            self.config.save_mount_state(
                {
                    "mount_point": str(active_mount),
                    "browser": browser,
                    "pid": os.getpid(),
                }
            )
            self.logger.error(
                f"YTMusicFS is already mounted at {active_mount}. "
                "Run: ytmusicfs unmount"
            )
            return 1

        self.config.save_user_config(
            {
                "last_mount_point": str(mount_point),
                "last_browser": browser,
            }
        )
        self.config.save_mount_state(
            {
                "mount_point": str(mount_point),
                "browser": browser,
                "pid": os.getpid(),
            }
        )

        try:
            self.logger.info(f"Mounting at {mount_point}")
            mount_filesystem(
                mount_point=str(mount_point),
                cache_dir=str(self.config.cache_dir),
                foreground=self.args.foreground,
                browser=browser,
            )
            return 0
        except Exception as e:
            self.config.save_user_config(previous_user_config)
            self.config.clear_mount_state()
            self.logger.error(f"Mount failed: {e}")
            return 1


class UnmountCommandHandler:
    """Handles the 'unmount' command logic."""

    def __init__(self, args: argparse.Namespace, logger: logging.Logger):
        """Initialize the unmount command handler."""
        self.args = args
        self.logger = logger
        self.config = ConfigManager(
            cache_dir=args.cache_dir,
            logger=logger,
        )

    def resolve_mount_point(self) -> Path | None:
        """Resolve the mount point from CLI option or active mount state."""
        if self.args.mount_point:
            return Path(self.args.mount_point).expanduser().resolve()

        detected_mount_point = MountInspector.find_active_mount_point()
        mount_state = self.config.load_mount_state()
        mount_point = mount_state.get("mount_point")
        if mount_point:
            saved_mount_point = Path(mount_point).expanduser().resolve()
            if saved_mount_point.exists() and os.path.ismount(saved_mount_point):
                return saved_mount_point
            self.config.clear_mount_state()

        if detected_mount_point is not None:
            return detected_mount_point

        self.logger.error("No active ytmusicfs mount found.")
        return None

    def execute(self) -> int:
        """Execute the unmount command."""
        mount_point = self.resolve_mount_point()
        if mount_point is None:
            return 1

        if not mount_point.exists() or not os.path.ismount(mount_point):
            self.config.clear_mount_state()
            self.logger.error(f"No active mount at {mount_point}")
            return 1

        command = self.find_unmount_command()
        if command is None:
            self.logger.error("Missing fusermount or fusermount3 in PATH.")
            return 1

        try:
            subprocess.run(
                [command, "-u", str(mount_point)],
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as error:
            stderr = error.stderr.strip()
            message = stderr or f"exit code {error.returncode}"
            self.logger.error(f"Unmount failed: {message}")
            return 1

        self.config.clear_mount_state()
        self.logger.info(f"Unmounted {mount_point}")
        return 0

    @staticmethod
    def find_unmount_command() -> str | None:
        """Find the FUSE unmount helper."""
        return shutil.which("fusermount") or shutil.which("fusermount3")


class MountInspector:
    """Read ytmusicfs mount state from the kernel mount table."""

    @staticmethod
    def find_active_mount_point() -> Path | None:
        mountinfo = Path("/proc/self/mountinfo")
        try:
            lines = mountinfo.read_text(encoding="utf-8").splitlines()
        except OSError:
            return None

        for line in lines:
            fields = line.split()
            if " - " not in line or len(fields) < 10:
                continue

            separator_index = fields.index("-")
            mount_point = fields[4].replace("\\040", " ")
            filesystem_type = fields[separator_index + 1]
            mount_source = fields[separator_index + 2]
            if "ytmusicfs" in filesystem_type or "ytmusicfs" in mount_source:
                return Path(mount_point)

        return None


class StatusCommandHandler:
    """Show saved settings and active mount state."""

    def __init__(self, args: argparse.Namespace, logger: logging.Logger):
        self.args = args
        self.logger = logger
        self.config = ConfigManager(cache_dir=args.cache_dir, logger=logger)

    def execute(self) -> int:
        user_config = self.config.load_user_config()
        mount_state = self.config.load_mount_state()
        active_mount = MountInspector.find_active_mount_point()
        saved_mount = mount_state.get("mount_point")

        print(f"Version: {__version__}")
        print(f"Cache dir: {self.config.cache_dir}")
        print(f"Config file: {self.config.config_file}")
        print(f"Saved mount point: {user_config.get('last_mount_point', 'not set')}")
        print(f"Saved browser: {user_config.get('last_browser', 'not set')}")

        if active_mount:
            print(f"Active mount: {active_mount}")
        elif saved_mount:
            self.config.clear_mount_state()
            print(f"Active mount: none (cleared stale {saved_mount})")
        else:
            print("Active mount: none")

        return 0


class ConfigCommandHandler:
    """Show and update saved mount settings."""

    VALID_KEYS: ClassVar[dict[str, str]] = {
        "browser": "last_browser",
        "mount-point": "last_mount_point",
    }

    def __init__(self, args: argparse.Namespace, logger: logging.Logger):
        self.args = args
        self.logger = logger
        self.config = ConfigManager(cache_dir=args.cache_dir, logger=logger)

    def execute(self) -> int:
        if self.args.config_action == "show":
            return self.show()
        return self.set_value()

    def show(self) -> int:
        user_config = self.config.load_user_config()
        print(f"mount-point: {user_config.get('last_mount_point', 'not set')}")
        print(f"browser: {user_config.get('last_browser', 'not set')}")
        print(f"config-file: {self.config.config_file}")
        return 0

    def set_value(self) -> int:
        key = self.VALID_KEYS[self.args.key]
        value = self.args.value
        if self.args.key == "mount-point":
            value = str(Path(value).expanduser().resolve())

        user_config = self.config.load_user_config()
        user_config[key] = value
        self.config.save_user_config(user_config)
        print(f"Saved {self.args.key}: {value}")
        return 0


class DoctorCommandHandler:
    """Run local environment checks."""

    def __init__(self, args: argparse.Namespace, logger: logging.Logger):
        self.args = args
        self.logger = logger
        self.config = ConfigManager(cache_dir=args.cache_dir, logger=logger)

    def execute(self) -> int:
        checks = [
            ("fusermount", self.has_unmount_helper()),
            ("python fuse module", importlib.util.find_spec("fuse") is not None),
            ("js runtime", any(shutil.which(runtime) for runtime in JS_RUNTIMES)),
            ("cache dir writable", os.access(self.config.cache_dir, os.W_OK)),
        ]
        failed = False
        for name, ok in checks:
            print(f"{name}: {'ok' if ok else 'missing'}")
            failed = failed or not ok

        return 1 if failed else 0

    @staticmethod
    def has_unmount_helper() -> bool:
        return UnmountCommandHandler.find_unmount_command() is not None


class CacheCommandHandler:
    """Inspect or clear persistent cache files."""

    CACHE_FILES = ("cache.db", "cache.db-wal", "cache.db-shm")
    CACHE_DIRS = ("audio",)

    def __init__(self, args: argparse.Namespace, logger: logging.Logger):
        self.args = args
        self.logger = logger
        self.config = ConfigManager(cache_dir=args.cache_dir, logger=logger)

    def execute(self) -> int:
        if self.args.cache_action == "clear":
            return self.clear()
        if self.args.cache_action == "refresh":
            return self.refresh()
        return self.stats()

    def clear(self) -> int:
        return self.remove_cache_entries("clear", include_audio=True)

    def refresh(self) -> int:
        return self.remove_cache_entries("refresh", include_audio=False)

    def remove_cache_entries(self, action: str, include_audio: bool) -> int:
        active_mount = MountInspector.find_active_mount_point()
        if active_mount:
            print_error(
                f"Cannot {action} cache while mounted at {active_mount}. "
                "Run: ytmusicfs unmount"
            )
            return 1

        removed = 0
        for name in self.CACHE_FILES:
            path = self.config.cache_dir / name
            if path.exists():
                path.unlink()
                removed += 1
        if include_audio:
            for name in self.CACHE_DIRS:
                path = self.config.cache_dir / name
                if path.exists():
                    shutil.rmtree(path)
                    removed += 1

        print(f"Cache {action}: removed {removed} files from {self.config.cache_dir}")
        return 0

    def stats(self) -> int:
        db_path = self.config.cache_dir / "cache.db"
        if not db_path.exists():
            print(f"Cache database: missing ({db_path})")
            return 0

        stats = self.read_database_stats(db_path)
        print(f"Cache database: {db_path}")
        print(f"Size: {db_path.stat().st_size} bytes")
        for key, value in stats.items():
            print(f"{key}: {value}")
        audio_files, audio_size = self.audio_stats()
        print(f"audio_files: {audio_files}")
        print(f"audio_size: {audio_size} bytes")
        return 0

    def audio_stats(self) -> tuple[int, int]:
        audio_dir = self.config.cache_dir / "audio"
        if not audio_dir.exists():
            return 0, 0

        files = [path for path in audio_dir.rglob("*") if path.is_file()]
        return len(files), sum(path.stat().st_size for path in files)

    @staticmethod
    def read_database_stats(db_path: Path) -> dict[str, Any]:
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            stats = {}
            for table in ("cache_entries", "hash_mappings", "refresh_tracker"):
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
                    (table,),
                )
                if cursor.fetchone():
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    stats[table] = cursor.fetchone()[0]
            return stats


class LogsCommandHandler:
    """Print the ytmusicfs log path or tail."""

    def __init__(self, args: argparse.Namespace, logger: logging.Logger):
        self.args = args
        self.logger = logger

    def execute(self) -> int:
        if not LOG_FILE.exists():
            print_error(f"No log file found at {LOG_FILE}")
            return 1

        if self.args.path:
            print(str(LOG_FILE))
            return 0

        lines = LOG_FILE.read_text(encoding="utf-8", errors="replace").splitlines()
        for line in lines[-self.args.tail :]:
            print(line)
        return 0


class ServiceCommandHandler:
    """Manage a simple systemd user service."""

    def __init__(self, args: argparse.Namespace, logger: logging.Logger):
        self.args = args
        self.logger = logger
        self.config = ConfigManager(logger=logger)

    def execute(self) -> int:
        action = self.args.service_action
        if action == "install":
            return self.install()
        return self.run_systemctl(action)

    def install(self) -> int:
        user_config = self.config.load_user_config()
        if not user_config.get("last_mount_point") or not user_config.get(
            "last_browser"
        ):
            print_error(
                "Missing saved mount settings. Run: ytmusicfs mount "
                "--mount-point ~/Music/ytmusic --browser brave"
            )
            return 1

        executable = shutil.which("ytmusicfs") or sys.argv[0]
        SYSTEMD_USER_DIR.mkdir(parents=True, exist_ok=True)
        SYSTEMD_SERVICE_FILE.write_text(
            "\n".join(
                [
                    "[Unit]",
                    "Description=YTMusicFS mount",
                    "",
                    "[Service]",
                    "Type=simple",
                    f"ExecStart={executable} mount --foreground",
                    f"ExecStop={executable} unmount",
                    "Restart=on-failure",
                    "",
                    "[Install]",
                    "WantedBy=default.target",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        print(f"Installed {SYSTEMD_SERVICE_FILE}")
        if self.run_systemctl("daemon-reload") != 0:
            return 1
        return self.run_systemctl("enable")

    def run_systemctl(self, action: str) -> int:
        command = ["systemctl", "--user", action, "ytmusicfs.service"]
        if action == "daemon-reload":
            command = ["systemctl", "--user", "daemon-reload"]

        try:
            subprocess.run(command, check=True, capture_output=True, text=True)
        except FileNotFoundError:
            print_error("Missing systemctl in PATH.")
            return 1
        except subprocess.CalledProcessError as error:
            stderr = error.stderr.strip()
            message = stderr or f"exit code {error.returncode}"
            print_error(f"systemctl {action} failed: {message}")
            return 1

        print(f"systemctl {action}: ok")
        return 0


def main() -> int:
    """Command-line entry point for YTMusicFS."""
    parser = argparse.ArgumentParser(
        description="YTMusicFS - Mount YouTube Music as a filesystem"
    )
    parser.add_argument(
        "--version", "-v", action="version", version=f"YTMusicFS {__version__}"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Mount command
    mount_parser = subparsers.add_parser("mount", help="Mount YouTube Music filesystem")
    mount_parser.add_argument("--mount-point", "-m", help="Mount point directory")
    mount_parser.add_argument("--cache-dir", "-c", help="Cache directory")
    mount_parser.add_argument(
        "--foreground", "-f", action="store_true", help="Run in foreground"
    )
    add_debug_option(mount_parser)
    mount_parser.add_argument(
        "--browser",
        "-b",
        help="Browser to use for cookies (e.g., 'chrome', 'firefox', 'brave')",
    )
    mount_parser.set_defaults(
        func=lambda args: MountCommandHandler(args, setup_logging(args)).execute()
    )

    # Unmount command
    unmount_parser = subparsers.add_parser(
        "unmount", help="Unmount YouTube Music filesystem"
    )
    unmount_parser.add_argument(
        "--mount-point",
        "-m",
        help="Mount point directory. Defaults to the active ytmusicfs mount.",
    )
    add_common_options(unmount_parser)
    unmount_parser.set_defaults(
        func=lambda args: UnmountCommandHandler(args, setup_logging(args)).execute()
    )

    status_parser = subparsers.add_parser("status", help="Show YTMusicFS status")
    add_common_options(status_parser)
    status_parser.set_defaults(
        func=lambda args: StatusCommandHandler(args, setup_logging(args)).execute()
    )

    config_parser = subparsers.add_parser("config", help="Show or update saved config")
    add_common_options(config_parser)
    config_subparsers = config_parser.add_subparsers(
        dest="config_action", required=True
    )
    config_show_parser = config_subparsers.add_parser("show", help="Show saved config")
    add_deferred_common_options(config_show_parser)
    config_show_parser.set_defaults(
        func=lambda args: ConfigCommandHandler(args, setup_logging(args)).execute()
    )
    config_set_parser = config_subparsers.add_parser("set", help="Set saved config")
    add_deferred_common_options(config_set_parser)
    config_set_parser.add_argument(
        "key", choices=sorted(ConfigCommandHandler.VALID_KEYS)
    )
    config_set_parser.add_argument("value")
    config_set_parser.set_defaults(
        func=lambda args: ConfigCommandHandler(args, setup_logging(args)).execute()
    )

    doctor_parser = subparsers.add_parser("doctor", help="Check local setup")
    add_common_options(doctor_parser)
    doctor_parser.set_defaults(
        func=lambda args: DoctorCommandHandler(args, setup_logging(args)).execute()
    )

    cache_parser = subparsers.add_parser("cache", help="Inspect or clear cache")
    add_common_options(cache_parser)
    cache_subparsers = cache_parser.add_subparsers(dest="cache_action", required=True)
    cache_stats_parser = cache_subparsers.add_parser("stats", help="Show cache stats")
    add_deferred_common_options(cache_stats_parser)
    cache_stats_parser.set_defaults(
        func=lambda args: CacheCommandHandler(args, setup_logging(args)).execute()
    )
    cache_clear_parser = cache_subparsers.add_parser("clear", help="Clear cache files")
    add_deferred_common_options(cache_clear_parser)
    cache_clear_parser.set_defaults(
        func=lambda args: CacheCommandHandler(args, setup_logging(args)).execute()
    )
    cache_refresh_parser = cache_subparsers.add_parser(
        "refresh", help="Clear metadata cache but keep cached audio"
    )
    add_deferred_common_options(cache_refresh_parser)
    cache_refresh_parser.set_defaults(
        func=lambda args: CacheCommandHandler(args, setup_logging(args)).execute()
    )

    logs_parser = subparsers.add_parser("logs", help="Show recent logs")
    logs_parser.add_argument(
        "--tail",
        type=positive_int,
        default=50,
        help="Print the last N log lines (default: 50)",
    )
    logs_parser.add_argument(
        "--path", action="store_true", help="Print the log file path"
    )
    logs_parser.add_argument(
        "--debug", "-d", action="store_true", help="Enable debug logging"
    )
    logs_parser.set_defaults(
        skip_log_file=True,
        func=lambda args: LogsCommandHandler(args, setup_logging(args)).execute(),
    )

    service_parser = subparsers.add_parser(
        "service", help="Manage the systemd user service"
    )
    add_debug_option(service_parser)
    service_subparsers = service_parser.add_subparsers(
        dest="service_action", required=True
    )
    service_help = {
        "install": "Install and enable the user service without starting it",
        "start": "Start the user service",
        "stop": "Stop the user service",
        "restart": "Restart the user service",
        "status": "Show user service status",
    }
    for action, help_text in service_help.items():
        service_action_parser = service_subparsers.add_parser(
            action,
            help=help_text,
        )
        add_deferred_debug_option(service_action_parser)
        service_action_parser.set_defaults(
            func=lambda args: ServiceCommandHandler(args, setup_logging(args)).execute()
        )

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
