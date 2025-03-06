#!/usr/bin/env python3

import os
import sys
import json
import argparse
import logging
from ytmusicapi import YTMusic, OAuthCredentials

# Import our adapter if available
try:
    from ytmusic_oauth_adapter import YTMusicOAuthAdapter

    ADAPTER_AVAILABLE = True
except ImportError:
    ADAPTER_AVAILABLE = False


def main():
    parser = argparse.ArgumentParser(description="Test OAuth setup for YTMusicFS")
    parser.add_argument("--auth-file", required=True, help="Path to OAuth token file")
    parser.add_argument("--client-id", help="OAuth client ID")
    parser.add_argument("--client-secret", help="OAuth client secret")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger("TestOAuth")

    # Check if auth file exists
    if not os.path.exists(args.auth_file):
        logger.error(f"Auth file not found: {args.auth_file}")
        return 1

    # Print file info
    logger.info(f"Testing OAuth with file: {args.auth_file}")
    try:
        with open(args.auth_file, "r") as f:
            auth_data = json.load(f)
            logger.info(f"Auth file format: {list(auth_data.keys())}")
    except Exception as e:
        logger.error(f"Error reading auth file: {e}")
        return 1

    # Test with adapter if available
    if ADAPTER_AVAILABLE:
        logger.info("Testing with YTMusicOAuthAdapter...")
        try:
            adapter = YTMusicOAuthAdapter(
                auth_file=args.auth_file,
                client_id=args.client_id,
                client_secret=args.client_secret,
                logger=logger,
            )

            # Test getting playlists
            playlists = adapter.get_library_playlists(limit=5)
            logger.info(f"Success with adapter! Found {len(playlists)} playlists")
            for i, playlist in enumerate(playlists[:5], 1):
                logger.info(f"  {i}. {playlist['title']}")
        except Exception as e:
            logger.error(f"Error with adapter: {e}")
            logger.error("Adapter test failed")
    else:
        logger.warning("YTMusicOAuthAdapter not available, skipping adapter test")

    # Test with direct YTMusic
    logger.info("\nTesting with direct YTMusic...")
    try:
        # Create OAuth credentials object if client ID and secret are provided
        oauth_credentials = None
        if args.client_id and args.client_secret:
            oauth_credentials = OAuthCredentials(
                client_id=args.client_id, client_secret=args.client_secret
            )
            logger.info("Using provided OAuth credentials")
        else:
            logger.warning("No client ID/secret provided, token refresh may not work")

        # Initialize YTMusic with OAuth
        ytmusic = YTMusic(auth=args.auth_file, oauth_credentials=oauth_credentials)

        # Test getting playlists
        playlists = ytmusic.get_library_playlists(limit=5)
        logger.info(f"Success with direct YTMusic! Found {len(playlists)} playlists")
        for i, playlist in enumerate(playlists[:5], 1):
            logger.info(f"  {i}. {playlist['title']}")
    except Exception as e:
        logger.error(f"Error with direct YTMusic: {e}")
        logger.error("Direct YTMusic test failed")
        return 1

    logger.info("\nAll tests completed successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
