#!/usr/bin/env python3

from pathlib import Path
from ytmusicfs import __version__
from ytmusicfs.filesystem import mount_ytmusicfs
from ytmusicfs.oauth_setup import main as oauth_setup
from ytmusicfs.config import ConfigManager
import argparse
import json
import logging
import os
import sys


def mount_command(args):
    """Handle the default mount command."""
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

    # Initialize configuration manager
    config = ConfigManager(
        auth_file=args.auth_file,
        credentials_file=args.credentials_file,
        cache_dir=args.cache_dir,
        cache_timeout=args.cache_timeout,
        logger=logger,
    )

    # Check and create mount point
    mount_point = Path(args.mount_point)
    if not mount_point.exists():
        logger.info(f"Creating mount point directory: {mount_point}")
        os.makedirs(mount_point, exist_ok=True)

    # Check for auth file
    auth_file = config.auth_file
    if not auth_file.exists():
        logger.error(f"Authentication file not found: {auth_file}")
        logger.error("Please run ytmusicfs oauth to set up authentication:")
        logger.error(
            f"  ytmusicfs oauth --client-id YOUR_CLIENT_ID --client-secret YOUR_CLIENT_SECRET"
        )
        return 1

    # Get client credentials
    client_id = args.client_id
    client_secret = args.client_secret

    # If not provided, get from config manager
    if not client_id or not client_secret:
        loaded_id, loaded_secret = config.get_credentials()
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

                    # Save them to the credentials file
                    config.save_credentials(client_id, client_secret)
                    logger.info("Saved extracted credentials to credentials file")

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
            "3. Use ytmusicfs oauth to set up authentication with your credentials"
        )
        logger.error("")
        logger.error("To regenerate your OAuth token with credentials:")
        logger.error(
            f"  ytmusicfs oauth --client-id YOUR_CLIENT_ID --client-secret YOUR_CLIENT_SECRET"
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
            cache_dir=str(config.cache_dir),
            cache_timeout=config.cache_timeout,
            browser=args.browser,
            credentials_file=(
                str(config.credentials_file) if config.credentials_file else None
            ),
        )

        return 0
    except Exception as e:
        logger.error(f"Failed to mount filesystem: {e}")
        import traceback

        logger.error(traceback.format_exc())
        return 1


def oauth_command(args):
    """Handle the oauth subcommand by delegating to oauth_setup"""
    # Parse just the arguments needed for oauth_setup() and pass them through
    return oauth_setup(args)


def main():
    """Command-line entry point for YTMusicFS."""
    parser = argparse.ArgumentParser(
        description="YTMusicFS - Mount YouTube Music as a filesystem"
    )

    # Add version argument to top-level parser
    parser.add_argument(
        "--version",
        "-v",
        action="version",
        version=f"YTMusicFS {__version__}",
        help="Show version and exit",
    )

    # Create subparsers for the different commands
    subparsers = parser.add_subparsers(
        dest="command",
        help="Command to run",
        required=True,  # Require explicit command - no backward compatibility
    )

    # Create the mount command
    mount_parser = subparsers.add_parser(
        "mount", help="Mount YouTube Music as a filesystem"
    )

    # Mount point options
    mount_parser.add_argument(
        "--mount-point",
        "-m",
        required=True,
        help="Directory where the filesystem will be mounted",
    )

    # Authentication options
    auth_group = mount_parser.add_argument_group("Authentication Options")
    auth_group.add_argument(
        "--auth-file",
        "-a",
        help=f"Path to the OAuth token file (default: {ConfigManager.DEFAULT_AUTH_FILE})",
    )
    auth_group.add_argument(
        "--credentials-file",
        help=f"Path to the client credentials file (default: {ConfigManager.DEFAULT_CRED_FILE})",
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
    cache_group = mount_parser.add_argument_group("Cache Options")
    cache_group.add_argument(
        "--cache-dir",
        "-c",
        help=f"Directory to store cache files (default: {ConfigManager.DEFAULT_CACHE_DIR})",
    )
    cache_group.add_argument(
        "--cache-timeout",
        "-t",
        type=int,
        default=ConfigManager.DEFAULT_CACHE_TIMEOUT,
        help=f"Cache timeout in seconds (default: {ConfigManager.DEFAULT_CACHE_TIMEOUT})",
    )

    # Operational options
    op_group = mount_parser.add_argument_group("Operational Options")
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
    op_group.add_argument(
        "--browser",
        "-b",
        help="Browser to use for cookies (e.g., 'chrome', 'firefox', 'brave', etc.). If not specified, no browser cookies will be used",
    )

    mount_parser.set_defaults(func=mount_command)

    # Create the oauth command
    oauth_parser = subparsers.add_parser(
        "oauth", help="Set up OAuth authentication for YTMusicFS"
    )

    # Add OAuth options
    oauth_parser.add_argument(
        "--client-id",
        "-i",
        help="OAuth Client ID from Google Cloud Console",
    )
    oauth_parser.add_argument(
        "--client-secret",
        "-s",
        help="OAuth Client Secret from Google Cloud Console",
    )
    oauth_parser.add_argument(
        "--auth-file",
        "-a",
        help=f"Path to the OAuth token file (default: {ConfigManager.DEFAULT_AUTH_FILE})",
    )
    oauth_parser.add_argument(
        "--credentials-file",
        "-c",
        help=f"Output file for the client credentials (default: {ConfigManager.DEFAULT_CRED_FILE})",
    )
    oauth_parser.add_argument(
        "--open-browser",
        "-b",
        action="store_true",
        default=True,
        help="Automatically open the browser for authentication",
    )
    oauth_parser.add_argument(
        "--no-open-browser",
        action="store_false",
        dest="open_browser",
        help="Do not automatically open the browser for authentication",
    )
    oauth_parser.add_argument(
        "--debug",
        "-d",
        action="store_true",
        help="Enable debug output",
    )

    oauth_parser.set_defaults(func=oauth_command)

    # Parse arguments
    args = parser.parse_args()

    # Execute the appropriate command function
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
