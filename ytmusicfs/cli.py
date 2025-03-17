#!/usr/bin/env python3

from pathlib import Path
from typing import Tuple, Optional
from ytmusicfs import __version__
from ytmusicfs.config import ConfigManager
from ytmusicfs.filesystem import mount_ytmusicfs
from ytmusicfs.oauth_setup import main as oauth_setup
import argparse
import logging
import os
import sys


def setup_logging(
    args: argparse.Namespace, log_dir: str = "~/.local/share/ytmusicfs/logs"
) -> logging.Logger:
    """Configure logging based on command-line arguments.

    Args:
        args: Parsed command-line arguments.
        log_dir: Directory for log files.

    Returns:
        Configured logger instance.
    """
    log_level = logging.DEBUG if args.debug else logging.INFO
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    handlers = [logging.StreamHandler(sys.stdout)]
    if not args.foreground:
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, "ytmusicfs.log")
        handlers.append(logging.FileHandler(log_file))

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
            auth_file=args.auth_file,
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

        if not self.config.auth_file.exists():
            self.logger.error(f"Authentication file not found: {self.config.auth_file}")
            self.logger.error("Run 'ytmusicfs oauth' to set up authentication.")
            return 1

        client_id, client_secret = self._get_credentials()
        if not client_id or not client_secret:
            self.logger.error("Client ID and Client Secret required.")
            return 1

        try:
            self.logger.info(f"Mounting at {mount_point}")
            mount_ytmusicfs(
                mount_point=str(mount_point),
                auth_file=str(self.config.auth_file),
                client_id=client_id,
                client_secret=client_secret,
                cache_dir=str(self.config.cache_dir),
                foreground=self.args.foreground,
            )
            return 0
        except Exception as e:
            self.logger.error(f"Mount failed: {e}")
            return 1

    def _get_credentials(self) -> Tuple[Optional[str], Optional[str]]:
        """Retrieve or extract client credentials.

        Returns:
            Tuple of (client_id, client_secret).
        """
        client_id, client_secret = self.args.client_id, self.args.client_secret
        if not client_id or not client_secret:
            client_id, client_secret = self.config.get_credentials()
            if client_id and client_secret:
                self.logger.info("Using credentials from config")
        return client_id, client_secret


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
    mount_parser.add_argument("--auth-file", "-a", help="OAuth token file path")
    mount_parser.add_argument("--credentials-file", help="Client credentials file path")
    mount_parser.add_argument("--client-id", "-i", help="OAuth client ID")
    mount_parser.add_argument("--client-secret", "-s", help="OAuth client secret")
    mount_parser.add_argument("--cache-dir", "-c", help="Cache directory")
    mount_parser.add_argument(
        "--foreground", "-f", action="store_true", help="Run in foreground"
    )
    mount_parser.add_argument(
        "--debug", "-d", action="store_true", help="Enable debug logging"
    )
    mount_parser.set_defaults(
        func=lambda args: MountCommandHandler(args, setup_logging(args)).execute()
    )

    # OAuth command
    oauth_parser = subparsers.add_parser("oauth", help="Set up OAuth authentication")
    oauth_parser.add_argument("--client-id", "-i", help="OAuth Client ID")
    oauth_parser.add_argument("--client-secret", "-s", help="OAuth Client Secret")
    oauth_parser.add_argument("--auth-file", "-a", help="OAuth token file path")
    oauth_parser.add_argument("--credentials-file", "-c", help="Credentials file path")
    oauth_parser.add_argument("--open-browser", "-b", action="store_true", default=True)
    oauth_parser.add_argument(
        "--no-open-browser", action="store_false", dest="open_browser"
    )
    oauth_parser.add_argument(
        "--debug", "-d", action="store_true", help="Enable debug output"
    )
    oauth_parser.set_defaults(
        func=lambda args: oauth_with_logger(args, setup_logging(args))
    )

    args = parser.parse_args()
    return args.func(args)


def oauth_with_logger(args, logger):
    """Wrapper function to pass the logger to the oauth_setup function.

    Args:
        args: Command line arguments
        logger: Logger instance

    Returns:
        Exit code
    """
    args.logger = logger
    result, _ = oauth_setup(args)
    return result


if __name__ == "__main__":
    sys.exit(main())
