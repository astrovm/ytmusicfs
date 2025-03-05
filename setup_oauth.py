#!/usr/bin/env python3

import os
import sys
import json
import argparse
import ytmusicapi


def main():
    parser = argparse.ArgumentParser(
        description="Setup OAuth authentication for YTMusicFS"
    )
    parser.add_argument("--client-id", help="OAuth Client ID from Google Cloud Console")
    parser.add_argument(
        "--client-secret", help="OAuth Client Secret from Google Cloud Console"
    )
    parser.add_argument(
        "--credentials-file",
        help="Path to credentials.json file downloaded from Google Cloud Console",
    )
    parser.add_argument(
        "--output-file",
        default="oauth_auth.json",
        help="Output file for OAuth credentials (default: oauth_auth.json)",
    )

    args = parser.parse_args()

    print("YTMusicFS OAuth Authentication Setup")
    print("====================================")
    print("This script will help you set up OAuth authentication for YouTube Music.")
    print("OAuth authentication lasts longer than cookie-based authentication.")
    print()

    client_id = args.client_id
    client_secret = args.client_secret

    # If credentials file is provided, extract client_id and client_secret from it
    if args.credentials_file and not (client_id and client_secret):
        try:
            with open(args.credentials_file, "r") as f:
                creds = json.load(f)
                if "installed" in creds:
                    client_id = creds["installed"]["client_id"]
                    client_secret = creds["installed"]["client_secret"]
                elif "web" in creds:
                    client_id = creds["web"]["client_id"]
                    client_secret = creds["web"]["client_secret"]
                else:
                    print("Error: Invalid credentials file format")
                    return 1
            print(f"Successfully loaded credentials from {args.credentials_file}")
        except Exception as e:
            print(f"Error loading credentials file: {e}")
            return 1

    # If we still don't have credentials, prompt the user
    if not client_id:
        print(
            "You need to provide OAuth 2.0 credentials to use this authentication method."
        )
        print("To get these credentials:")
        print("1. Go to https://console.cloud.google.com/")
        print("2. Create a new project or select an existing one")
        print("3. Enable the YouTube Data API v3")
        print("4. Go to 'APIs & Services' > 'Credentials'")
        print("5. Click 'Create Credentials' > 'OAuth client ID'")
        print("6. Application type: 'Desktop app'")
        print("7. Copy the Client ID and Client Secret")
        print()
        client_id = input("Enter your Client ID: ")
        client_secret = input("Enter your Client Secret: ")

    if not client_id or not client_secret:
        print("Error: Client ID and Client Secret are required")
        return 1

    output_file = args.output_file

    print(f"OAuth authentication data will be saved to: {output_file}")
    print()

    if os.path.exists(output_file):
        overwrite = input(f"File {output_file} already exists. Overwrite? (y/n): ")
        if overwrite.lower() != "y":
            print("Aborted.")
            return 1

    try:
        # Set up OAuth with the provided credentials
        oauth = ytmusicapi.setup_oauth(
            filepath=output_file,
            client_id=client_id,
            client_secret=client_secret,
            open_browser=True,
        )
        print()
        print(f"OAuth authentication data saved to {output_file}")
        print("You can now use this file with YTMusicFS by running:")
        print(
            f"python ytmusicfs.py --auth-file {output_file} --auth-type oauth --mount-point /path/to/mount"
        )
        return 0
    except Exception as e:
        print(f"Error during OAuth setup: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
