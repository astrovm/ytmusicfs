#!/usr/bin/env python3

import argparse
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

from ytmusicfs import __version__
from ytmusicfs.config import ConfigManager


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
    if not foreground:
        log_path = Path.home() / ".local" / "share" / "ytmusicfs" / "logs"
        log_path.mkdir(parents=True, exist_ok=True)
        log_file = log_path / "ytmusicfs.log"
        handlers.append(logging.FileHandler(str(log_file)))

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

    def resolve_mount_settings(self) -> Optional[tuple[Path, str]]:
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

    def resolve_mount_point(self) -> Optional[Path]:
        """Resolve the mount point from CLI option or active mount state."""
        if self.args.mount_point:
            return Path(self.args.mount_point).expanduser().resolve()

        mount_state = self.config.load_mount_state()
        mount_point = mount_state.get("mount_point")
        if mount_point:
            return Path(mount_point).expanduser().resolve()

        detected_mount_point = self.find_active_mount_point()
        if detected_mount_point:
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
    def find_unmount_command() -> Optional[str]:
        """Find the FUSE unmount helper."""
        return shutil.which("fusermount") or shutil.which("fusermount3")

    @staticmethod
    def find_active_mount_point() -> Optional[Path]:
        """Find an active ytmusicfs mount from the kernel mount table."""
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
    mount_parser.add_argument(
        "--debug", "-d", action="store_true", help="Enable debug logging"
    )
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
    unmount_parser.add_argument("--cache-dir", "-c", help="Cache directory")
    unmount_parser.add_argument(
        "--debug", "-d", action="store_true", help="Enable debug logging"
    )
    unmount_parser.set_defaults(
        func=lambda args: UnmountCommandHandler(args, setup_logging(args)).execute()
    )

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
