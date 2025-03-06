#!/usr/bin/env python3

import os
import sys
import json
import argparse
import logging
from pathlib import Path

import ytmusicapi
from ytmusicapi import setup_oauth as ytmusic_setup_oauth
from ytmusicapi import YTMusic, OAuthCredentials


def ensure_dir(path):
    """Ensure directory exists, creating it if necessary."""
    path.parent.mkdir(parents=True, exist_ok=True)


def main():
    """Command-line entry point for YTMusicFS OAuth setup."""
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
        "--output",
        "-o",
        help="Output file for the OAuth token (default: ~/.config/ytmusicfs/oauth.json)",
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

    # Configure logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger("YTMusicFS-OAuth")

    logger.info("YTMusicFS OAuth Setup")
    logger.info("=====================")
    logger.info(f"ytmusicapi version: {ytmusicapi.__version__}")
    logger.info("")

    # Determine output file
    if args.output:
        output_file = Path(args.output)
    else:
        output_file = Path(os.path.expanduser("~/.config/ytmusicfs/oauth.json"))

    # Ensure directory exists
    ensure_dir(output_file)

    # Get client ID and secret
    client_id = args.client_id
    client_secret = args.client_secret

    # If not provided, try to read them from the existing file
    if (not client_id or not client_secret) and output_file.exists():
        try:
            with open(output_file, "r") as f:
                data = json.load(f)
                client_id = client_id or data.get("client_id")
                client_secret = client_secret or data.get("client_secret")
            logger.info("Read client credentials from existing OAuth file")
        except Exception as e:
            logger.warning(f"Could not read credentials from existing file: {e}")

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
        return 1

    # Check if file exists
    if output_file.exists():
        overwrite = input(f"File {output_file} already exists. Overwrite? (y/n): ")
        if overwrite.lower() != "y":
            logger.info("Aborted.")
            return 1

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

            # Add client credentials to oauth data if not present
            if "client_id" not in oauth_data or "client_secret" not in oauth_data:
                oauth_data["client_id"] = client_id
                oauth_data["client_secret"] = client_secret

                with open(output_file, "w") as f:
                    json.dump(oauth_data, f, indent=2)
                logger.info("Added client credentials to OAuth token file")
        except Exception as e:
            logger.warning(f"Failed to update OAuth file with client credentials: {e}")
            # Continue anyway, as the setup was successful

        logger.info(f"OAuth setup completed successfully!")
        logger.info(f"OAuth token saved to: {output_file}")

        # Test connection
        logger.info("Testing OAuth connection...")

        # Create a small test to verify the OAuth token works
        try:
            from ytmusicapi import YTMusic, OAuthCredentials

            # Create OAuth credentials object
            oauth_credentials = OAuthCredentials(
                client_id=client_id, client_secret=client_secret
            )

            # Initialize YTMusic with OAuth credentials
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
            return 1

        logger.info("")
        logger.info("You can now use YTMusicFS with the following command:")
        logger.info(f"ytmusicfs --mount-point <mount_point> --auth-file {output_file}")

        return 0

    except Exception as e:
        logger.error(f"Error during OAuth setup: {e}")
        import traceback

        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
