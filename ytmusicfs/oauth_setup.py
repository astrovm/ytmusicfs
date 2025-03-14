#!/usr/bin/env python3

from ytmusicapi import setup_oauth as ytmusic_setup_oauth
from ytmusicapi import YTMusic, OAuthCredentials
from ytmusicfs.config import ConfigManager
import argparse
import json
import logging
import sys
import ytmusicapi


def main(args=None):
    """Command-line entry point for YTMusicFS OAuth setup."""
    # If args are not provided, parse them from command line
    if args is None:
        parser = argparse.ArgumentParser(
            description="Set up OAuth authentication for YTMusicFS"
        )

        parser.add_argument(
            "--client-id",
            "-i",
            help="OAuth Client ID from Google Cloud Console",
        )
        parser.add_argument(
            "--client-secret",
            "-s",
            help="OAuth Client Secret from Google Cloud Console",
        )
        parser.add_argument(
            "--auth-file",
            "-a",
            help="Path to the OAuth token file (default: ~/.config/ytmusicfs/oauth.json)",
        )
        parser.add_argument(
            "--credentials-file",
            "-c",
            help="Output file for the client credentials (default: same directory as auth-file with name 'credentials.json')",
        )
        parser.add_argument(
            "--open-browser",
            "-b",
            action="store_true",
            default=True,
            help="Automatically open the browser for authentication",
        )
        parser.add_argument(
            "--no-open-browser",
            action="store_false",
            dest="open_browser",
            help="Do not automatically open the browser for authentication",
        )
        parser.add_argument(
            "--debug",
            "-d",
            action="store_true",
            help="Enable debug output",
        )

        args = parser.parse_args()

    # Use provided logger or create a new one
    if hasattr(args, "logger") and args.logger:
        logger = args.logger
    else:
        # Configure logging
        log_level = logging.DEBUG if args.debug else logging.INFO
        logging.basicConfig(
            level=log_level,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        logger = logging.getLogger("YTMusicFS OAuth")

    logger.info("YTMusicFS OAuth Setup")
    logger.info("=====================")
    logger.info(f"ytmusicapi version: {ytmusicapi.__version__}")
    logger.info("")

    # Initialize the configuration manager
    config = ConfigManager(
        auth_file=args.auth_file, credentials_file=args.credentials_file, logger=logger
    )

    # Get the OAuth token file path
    output_file = config.auth_file

    # Get client ID and secret
    client_id = args.client_id
    client_secret = args.client_secret

    # If not provided, try to read them from the existing token file
    if (not client_id or not client_secret) and output_file.exists():
        try:
            with open(output_file, "r") as f:
                data = json.load(f)
                client_id = client_id or data.get("client_id")
                client_secret = client_secret or data.get("client_secret")
            logger.info("Read client credentials from existing OAuth file")
        except Exception as e:
            logger.warning(f"Could not read credentials from existing file: {e}")

    # If still not available, try to get them from config manager
    if not client_id or not client_secret:
        loaded_id, loaded_secret = config.get_credentials()
        if loaded_id and loaded_secret:
            client_id = client_id or loaded_id
            client_secret = client_secret or loaded_secret
            logger.info("Loaded client credentials from credentials file")

    # If still not available, prompt for them
    if not client_id or not client_secret:
        logger.info("You need to provide OAuth client credentials.")
        logger.info("Instructions to get them:")
        logger.info("1. Go to https://console.cloud.google.com/")
        logger.info("2. Create a new project or select an existing one")
        logger.info("3. Enable the YouTube Data API v3")
        logger.info("4. Go to 'APIs & Services' > 'Credentials'")
        logger.info("5. Click 'Create Credentials' > 'OAuth client ID'")
        logger.info("6. Application type: 'TV and Limited Input devices'")
        logger.info("7. Copy the Client ID and Client Secret")
        logger.info("")

        client_id = input("Enter Client ID: ")
        client_secret = input("Enter Client Secret: ")

    if not client_id or not client_secret:
        logger.error("Error: Client ID and Client Secret are required.")
        return 1, logger

    # Check if file exists
    if output_file.exists():
        overwrite = input(f"File {output_file} already exists. Overwrite? (y/n): ")
        if overwrite.lower() != "y":
            logger.info("Aborted.")
            return 1, logger

    try:
        logger.info("Starting OAuth setup...")
        logger.info("This will open a browser for you to authorize YouTube Music.")

        # Use the setup_oauth function
        ytmusic_setup_oauth(
            filepath=str(output_file),
            client_id=client_id,
            client_secret=client_secret,
            open_browser=args.open_browser,
        )

        # Make sure the client credentials are included in the output file
        try:
            with open(output_file, "r") as f:
                oauth_data = json.load(f)

            # Remove client credentials from the token file itself to avoid conflicts
            # They should only be passed via the OAuthCredentials object
            if "client_id" in oauth_data:
                del oauth_data["client_id"]
            if "client_secret" in oauth_data:
                del oauth_data["client_secret"]

            # Save the cleaned oauth data file
            with open(output_file, "w") as f:
                json.dump(oauth_data, f, indent=2)

            # Save credentials to the config manager
            config.save_credentials(client_id, client_secret)

            # Keep the client credentials in memory for the OAuthCredentials object
            logger.info(
                "Removed client credentials from OAuth token file to avoid conflicts"
            )
        except Exception as e:
            logger.warning(f"Failed to update OAuth file: {e}")
            # Continue anyway, as the setup was successful

        logger.info(f"OAuth setup completed successfully!")
        logger.info(f"OAuth token saved to: {output_file}")

        # Test connection
        logger.info("Testing OAuth connection...")

        # Create a small test to verify the OAuth token works
        try:
            # Create OAuth credentials object
            oauth_credentials = OAuthCredentials(
                client_id=client_id, client_secret=client_secret
            )

            # Initialize YTMusic with OAuth credentials as a separate parameter
            ytmusic = YTMusic(
                auth=str(output_file), oauth_credentials=oauth_credentials
            )

            playlists = ytmusic.get_library_playlists(limit=3)

            logger.info(f"Success! Found {len(playlists)} playlists in your library")

            if playlists:
                logger.info("First few playlists:")
                for i, playlist in enumerate(playlists[:3], 1):
                    logger.info(f"  {i}. {playlist['title']}")
        except Exception as e:
            logger.error(f"Error testing OAuth connection: {e}")
            logger.error(
                "The OAuth setup completed but there was an error testing the connection."
            )
            logger.error("You might need to re-run the setup.")
            return 1, logger

        logger.info("")
        logger.info("You can now use YTMusicFS with the following command:")
        logger.info(
            f"ytmusicfs mount --mount-point <mount_point> --auth-file {output_file}"
        )

        return 0, logger

    except Exception as e:
        logger.error(f"Error during OAuth setup: {e}")
        import traceback

        logger.error(traceback.format_exc())
        return 1, logger


if __name__ == "__main__":
    sys.exit(main())
