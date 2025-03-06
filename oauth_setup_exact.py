#!/usr/bin/env python3

import os
import sys
import json
import argparse
import ytmusicapi


def main():
    parser = argparse.ArgumentParser(
        description="Set up OAuth for YTMusicFS following the documentation"
    )
    parser.add_argument("--client-id", help="OAuth Client ID from Google Cloud Console")
    parser.add_argument(
        "--client-secret", help="OAuth Client Secret from Google Cloud Console"
    )
    parser.add_argument(
        "--output", default="oauth.json", help="Output file for the OAuth token"
    )

    args = parser.parse_args()

    print("YTMusicFS OAuth Setup (Following Documentation)")
    print("=============================================")
    print(f"ytmusicapi version: {ytmusicapi.__version__}")
    print()

    # Get client ID and secret
    client_id = args.client_id
    client_secret = args.client_secret

    if not client_id or not client_secret:
        print("You need to provide OAuth client credentials.")
        print("Instructions to get them:")
        print("1. Go to https://console.cloud.google.com/")
        print("2. Create a new project or select an existing one")
        print("3. Enable the YouTube Data API v3")
        print("4. Go to 'APIs & Services' > 'Credentials'")
        print("5. Click 'Create Credentials' > 'OAuth client ID'")
        print("6. Application type: 'TV and Limited Input devices'")
        print("7. Copy the Client ID and Client Secret")
        print()

        client_id = input("Enter Client ID: ")
        client_secret = input("Enter Client Secret: ")

    if not client_id or not client_secret:
        print("Error: Client ID and Client Secret are required.")
        return 1

    output_file = args.output

    # Check if file exists
    if os.path.exists(output_file):
        overwrite = input(f"File {output_file} already exists. Overwrite? (y/n): ")
        if overwrite.lower() != "y":
            print("Aborted.")
            return 1

    try:
        print("Starting OAuth setup...")
        print("This will open a browser for you to authorize YouTube Music.")

        # Use the setup_oauth function exactly as documented
        ytmusicapi.setup_oauth(
            client_id=client_id,
            client_secret=client_secret,
            filepath=output_file,
            open_browser=True,
        )

        print(f"\nOAuth setup completed successfully!")
        print(f"OAuth token saved to: {output_file}")

        # Now create a test script to verify it works
        test_script = "test_oauth_exact.py"
        with open(test_script, "w") as f:
            f.write(
                f"""#!/usr/bin/env python3

import sys
from ytmusicapi import YTMusic, OAuthCredentials

def main():
    try:
        # Load client credentials
        client_id = "{client_id}"
        client_secret = "{client_secret}"

        # Create OAuth credentials object
        oauth_creds = OAuthCredentials(
            client_id=client_id,
            client_secret=client_secret
        )

        # Initialize YTMusic with OAuth
        ytmusic = YTMusic("{output_file}", oauth_credentials=oauth_creds)

        # Test getting playlists
        playlists = ytmusic.get_library_playlists(limit=5)
        print(f"Success! Found {{len(playlists)}} playlists")
        for i, playlist in enumerate(playlists[:5], 1):
            print(f"  {{i}}. {{playlist['title']}}")

        return 0
    except Exception as e:
        print(f"Error: {{e}}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
"""
            )

        os.chmod(test_script, 0o755)  # Make executable

        print("\nCreated test script to verify OAuth setup.")
        print(f"Run it with: python {test_script}")

        # Ask if user wants to run the test
        test = input("\nWould you like to test the OAuth setup now? (y/n): ")
        if test.lower() == "y":
            print("\nTesting OAuth setup...")
            import subprocess

            subprocess.run([sys.executable, test_script])

        return 0

    except Exception as e:
        print(f"Error during OAuth setup: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
