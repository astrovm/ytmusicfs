#!/usr/bin/env python3

import argparse
import logging
import sys
from pathlib import Path

from ytmusicfs import __version__
from ytmusicfs.config import ConfigManager
from ytmusicfs.filesystem import mount_ytmusicfs


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

    def execute(self) -> int:
        """Execute the mount command.

        Returns:
            Exit code (0 for success, 1 for failure).
        """
        self.logger.info(f"YTMusicFS version {__version__}")
        mount_point = Path(self.args.mount_point)
        mount_point.mkdir(parents=True, exist_ok=True)

        if not self.args.browser:
            self.logger.error("Missing required browser. Use '--browser <name>'.")
            return 1

        try:
            self.logger.info(f"Mounting at {mount_point}")
            mount_ytmusicfs(
                mount_point=str(mount_point),
                cache_dir=str(self.config.cache_dir),
                foreground=self.args.foreground,
                browser=self.args.browser,
            )
            return 0
        except Exception as e:
            self.logger.error(f"Mount failed: {e}")
            return 1


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
    mount_parser.add_argument(
        "--mount-point", "-m", required=True, help="Mount point directory"
    )
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
        required=True,
        help="Browser to use for cookies (e.g., 'chrome', 'firefox', 'brave')",
    )
    mount_parser.set_defaults(
        func=lambda args: MountCommandHandler(args, setup_logging(args)).execute()
    )

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
