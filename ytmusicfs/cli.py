#!/usr/bin/env python3

import os
import sys
import argparse
import logging
from pathlib import Path

from ytmusicfs import __version__
from ytmusicfs.filesystem import mount_ytmusicfs


def main():
    """Command-line entry point for YTMusicFS."""
    parser = argparse.ArgumentParser(
        description="YTMusicFS - Mount YouTube Music as a filesystem"
    )

    # Mount point options
    parser.add_argument(
        "--mount-point",
        "-m",
        required=True,
        help="Directory where the filesystem will be mounted",
    )

    # Authentication options
    auth_group = parser.add_argument_group("Authentication Options")
    auth_group.add_argument(
        "--auth-file",
        "-a",
        default=os.path.expanduser("~/.config/ytmusicfs/oauth.json"),
        help="Path to the OAuth token file (default: ~/.config/ytmusicfs/oauth.json)",
    )
    auth_group.add_argument(
        "--client-id",
        "-i",
        help="OAuth client ID (required for OAuth authentication)",
    )
    auth_group.add_argument(
        "--client-secret",
        "-s",
        help="OAuth client secret (required for OAuth authentication)",
    )

    # Cache options
    cache_group = parser.add_argument_group("Cache Options")
    cache_group.add_argument(
        "--cache-dir",
        "-c",
        help="Directory to store cache files (default: ~/.cache/ytmusicfs)",
    )
    cache_group.add_argument(
        "--cache-timeout",
        "-t",
        type=int,
        default=300,
        help="Cache timeout in seconds (default: 300)",
    )

    # Operational options
    op_group = parser.add_argument_group("Operational Options")
    op_group.add_argument(
        "--foreground",
        "-f",
        action="store_true",
        help="Run in the foreground (for debugging)",
    )
    op_group.add_argument(
        "--debug",
        "-d",
        action="store_true",
        help="Enable debug logging",
    )

    # Version
    parser.add_argument(
        "--version",
        "-v",
        action="version",
        version=f"YTMusicFS {__version__}",
        help="Show version and exit",
    )

    args = parser.parse_args()

    # Set up logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    if args.foreground:
        logging.basicConfig(level=log_level, format=log_format)
    else:
        # Set up log file
        log_dir = os.path.expanduser("~/.local/share/ytmusicfs/logs")
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, "ytmusicfs.log")

        # Configure file logging
        logging.basicConfig(
            level=log_level,
            format=log_format,
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout),
            ],
        )

    logger = logging.getLogger("YTMusicFS")
    logger.info(f"YTMusicFS version {__version__}")

    # Check and create mount point
    mount_point = Path(args.mount_point)
    if not mount_point.exists():
        logger.info(f"Creating mount point directory: {mount_point}")
        os.makedirs(mount_point, exist_ok=True)

    # Check for auth file
    auth_file = Path(args.auth_file)
    if not auth_file.exists():
        logger.error(f"Authentication file not found: {auth_file}")
        logger.error("Please run ytmusicfs-oauth to set up authentication")
        return 1

    # Get client credentials from environment or config
    client_id = args.client_id or os.environ.get("YTMUSICFS_CLIENT_ID")
    client_secret = args.client_secret or os.environ.get("YTMUSICFS_CLIENT_SECRET")

    # Check for client credentials in auth file if not provided
    if not client_id or not client_secret:
        try:
            import json

            with open(auth_file, "r") as f:
                auth_data = json.load(f)
                client_id = client_id or auth_data.get("client_id")
                client_secret = client_secret or auth_data.get("client_secret")
        except Exception as e:
            logger.warning(f"Failed to read client credentials from auth file: {e}")

    # Final check for client credentials
    if not client_id or not client_secret:
        logger.error("Client ID and Client Secret are required for authentication")
        logger.error(
            "Provide them with --client-id and --client-secret or set YTMUSICFS_CLIENT_ID and YTMUSICFS_CLIENT_SECRET environment variables"
        )
        return 1

    # Mount the filesystem
    try:
        logger.info(f"Mounting YouTube Music filesystem at {mount_point}")

        mount_ytmusicfs(
            mount_point=str(mount_point),
            auth_file=str(auth_file),
            client_id=client_id,
            client_secret=client_secret,
            foreground=args.foreground,
            debug=args.debug,
            cache_dir=args.cache_dir,
            cache_timeout=args.cache_timeout,
        )

        return 0
    except Exception as e:
        logger.error(f"Failed to mount filesystem: {e}")
        import traceback

        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
