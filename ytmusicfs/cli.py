#!/usr/bin/env python3

import os
import sys
import json
import argparse
import logging
from pathlib import Path

from ytmusicfs import __version__
from ytmusicfs.filesystem import mount_ytmusicfs


def load_credentials(config_dir):
    """Load client credentials from a separate file."""
    cred_file = Path(config_dir) / "credentials.json"

    if not cred_file.exists():
        return None, None

    try:
        with open(cred_file, "r") as f:
            credentials = json.load(f)

        return credentials.get("client_id"), credentials.get("client_secret")
    except Exception:
        return None, None


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
        default=2592000,
        help="Cache timeout in seconds (default: 2592000)",
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
        logger.error("Please run ytmusicfs-oauth to set up authentication:")
        logger.error(
            f"  ytmusicfs-oauth --client-id YOUR_CLIENT_ID --client-secret YOUR_CLIENT_SECRET"
        )
        return 1

    # Get client credentials
    client_id = args.client_id
    client_secret = args.client_secret

    # If not provided, try to load from separate credentials file
    if not client_id or not client_secret:
        config_dir = auth_file.parent
        loaded_id, loaded_secret = load_credentials(config_dir)

        if loaded_id and loaded_secret:
            client_id = loaded_id
            client_secret = loaded_secret
            logger.info("Using client credentials from credentials file")

    # Check for client credentials in auth file if not provided (backward compatibility)
    if not client_id or not client_secret:
        try:
            with open(auth_file, "r") as f:
                auth_data = json.load(f)

                # We should no longer store these in the auth file, but check for backward compatibility
                if "client_id" in auth_data or "client_secret" in auth_data:
                    logger.warning(
                        "Found client credentials in OAuth token file - this is not recommended"
                    )
                    logger.warning(
                        "Credentials should be provided via command line or environment variables"
                    )

                    # Still use them if needed
                    client_id = client_id or auth_data.get("client_id")
                    client_secret = client_secret or auth_data.get("client_secret")

                    # Clean up the file by removing credentials
                    if "client_id" in auth_data:
                        del auth_data["client_id"]
                    if "client_secret" in auth_data:
                        del auth_data["client_secret"]

                    # Save the cleaned file
                    with open(auth_file, "w") as f:
                        json.dump(auth_data, f, indent=2)
                    logger.info(
                        "Removed client credentials from OAuth token file for security"
                    )

                if client_id and client_secret:
                    logger.info("Using client credentials from OAuth token file")
        except Exception as e:
            logger.warning(f"Failed to read client credentials from auth file: {e}")

    # Final check for client credentials
    if not client_id or not client_secret:
        logger.error("Client ID and Client Secret are required for authentication")
        logger.error("Options:")
        logger.error("1. Provide them with --client-id and --client-secret")
        logger.error(
            "2. Set YTMUSICFS_CLIENT_ID and YTMUSICFS_CLIENT_SECRET environment variables"
        )
        logger.error(
            "3. Use ytmusicfs-oauth to set up authentication with your credentials"
        )
        logger.error("")
        logger.error("To regenerate your OAuth token with credentials:")
        logger.error(
            f"  ytmusicfs-oauth --client-id YOUR_CLIENT_ID --client-secret YOUR_CLIENT_SECRET"
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
