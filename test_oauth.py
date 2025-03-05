#!/usr/bin/env python3

import json
import sys
from ytmusicapi import YTMusic, OAuthCredentials


def main():
    auth_file = "oauth_auth.json"

    print(f"Testing OAuth authentication with file: {auth_file}")
    print("=" * 50)

    # Load the OAuth file
    with open(auth_file, "r") as f:
        oauth_data = json.load(f)

    print("OAuth file loaded")
    print(
        f"Contents: {json.dumps({k: '***' if 'token' in k or 'secret' in k else v for k, v in oauth_data.items()}, indent=2)}"
    )

    # Test 1: Standard approach
    try:
        print("\nTest 1: Using standard YTMusic initialization with file path")
        ytmusic = YTMusic(auth_file)
        print("Success! Could initialize YTMusic with file path only.")

        # Test a basic API call
        playlists = ytmusic.get_library_playlists(limit=1)
        print(
            f"API call successful. Found playlist: {playlists[0]['title'] if playlists else 'None'}"
        )
    except Exception as e:
        print(f"Error: {e}")

    # Test 2: With OAuthCredentials
    try:
        print("\nTest 2: Using OAuthCredentials")
        client_id = oauth_data.get("client_id")
        client_secret = oauth_data.get("client_secret")

        # Creating OAuth credentials object
        print("Creating OAuthCredentials object")
        oauth_creds = OAuthCredentials(client_id=client_id, client_secret=client_secret)

        print("Initializing YTMusic with OAuth credentials")
        ytmusic = YTMusic(auth_file, oauth_credentials=oauth_creds)
        print("Success!")

        # Test a basic API call
        playlists = ytmusic.get_library_playlists(limit=1)
        print(
            f"API call successful. Found playlist: {playlists[0]['title'] if playlists else 'None'}"
        )
    except Exception as e:
        print(f"Error: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
