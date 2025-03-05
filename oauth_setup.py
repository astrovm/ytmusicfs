#!/usr/bin/env python3

import os
import sys
import json
import argparse
import subprocess
import ytmusicapi


def main():
    """Comprehensive OAuth setup utility for YTMusicFS with ytmusicapi 1.10.2"""

    parser = argparse.ArgumentParser(
        description="Set up YouTube Music OAuth authentication for YTMusicFS"
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
        default="oauth.json",
        help="Output file for OAuth credentials (default: oauth.json)",
    )

    args = parser.parse_args()

    print("YTMusicFS OAuth Setup Utility")
    print("=============================")
    print("This utility will help you set up OAuth authentication for YouTube Music.")
    print("ytmusicapi version:", ytmusicapi.__version__)
    print()

    # Get client_id and client_secret
    client_id = args.client_id
    client_secret = args.client_secret

    # Try to load from credentials file
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
        except Exception as e:
            print(f"Error loading credentials file: {e}")

    # Prompt for credentials if not provided
    if not client_id:
        print("You need to provide OAuth credentials to use YouTube Music.")
        print("To obtain these credentials:")
        print("1. Go to https://console.cloud.google.com/")
        print("2. Create a new project or select an existing one")
        print("3. Enable the YouTube Data API v3")
        print("4. Go to 'APIs & Services' > 'Credentials'")
        print("5. Click 'Create Credentials' > 'OAuth client ID'")
        print("6. Application type: 'TV and Limited Input devices'")
        print("7. Copy the Client ID and Client Secret")
        print()
        client_id = input("Enter your Client ID: ")
        client_secret = input("Enter your Client Secret: ")

    if not client_id or not client_secret:
        print("Error: Client ID and Client Secret are required")
        return 1

    # Output file
    output_file = args.output_file

    # Check if file exists
    if os.path.exists(output_file):
        overwrite = input(f"File {output_file} already exists. Overwrite? (y/n): ")
        if overwrite.lower() != "y":
            print("Aborted.")
            return 1

    try:
        print("\nStarting OAuth setup...")
        print("This will open a browser for you to authorize YouTube Music.")

        # Create the OAuth token
        oauth_token = ytmusicapi.setup_oauth(
            client_id=client_id,
            client_secret=client_secret,
            filepath=output_file,
            open_browser=True,
        )

        # Read the file to see if it has client_id and client_secret
        with open(output_file, "r") as f:
            oauth_data = json.load(f)

        # Add client ID and secret if not present
        if "client_id" not in oauth_data:
            oauth_data["client_id"] = client_id

        if "client_secret" not in oauth_data:
            oauth_data["client_secret"] = client_secret

        # Write the updated file
        with open(output_file, "w") as f:
            json.dump(oauth_data, f, indent=2)

        print("\nOAuth setup completed successfully!")
        print(f"OAuth token saved to: {output_file}")
        print("\nYou can now use this file with YTMusicFS:")
        print(
            f"python ytmusicfs.py --auth-file {output_file} --auth-type oauth --mount-point /path/to/mount"
        )

        # Test the authentication
        test = input("\nWould you like to test the authentication now? (y/n): ")
        if test.lower() == "y":
            print("\nTesting OAuth authentication...")
            subprocess.run(
                [
                    sys.executable,
                    "test_auth.py",
                    "--auth-file",
                    output_file,
                    "--auth-type",
                    "oauth",
                ]
            )

        return 0

    except Exception as e:
        print(f"Error during OAuth setup: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
