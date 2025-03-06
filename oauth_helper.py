#!/usr/bin/env python3

import os
import sys
import json
import argparse
import ytmusicapi
from importlib import import_module
from dotenv import load_dotenv


def main():
    parser = argparse.ArgumentParser(
        description="YTMusicFS OAuth Helper for ytmusicapi 1.10.2"
    )
    parser.add_argument("--client-id", help="OAuth Client ID from Google Cloud Console")
    parser.add_argument(
        "--client-secret", help="OAuth Client Secret from Google Cloud Console"
    )
    parser.add_argument(
        "--output", default="browser_auth.json", help="Output file for the OAuth token"
    )

    args = parser.parse_args()

    print("YTMusicFS OAuth Helper for ytmusicapi 1.10.2")
    print("===========================================")
    print(f"ytmusicapi version: {ytmusicapi.__version__}")
    print()

    client_id = args.client_id
    client_secret = args.client_secret

    # If not provided, try to load from environment or prompt
    if not client_id or not client_secret:
        # Try to load from .env
        load_dotenv()
        client_id = os.environ.get("YTM_CLIENT_ID", client_id)
        client_secret = os.environ.get("YTM_CLIENT_SECRET", client_secret)

    # Still need to prompt?
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

    # Check again
    if not client_id or not client_secret:
        print("Error: Client ID and Client Secret are required.")
        return 1

    output_file = args.output
    oauth_backup = "oauth_token.json"

    # Check if output file exists
    if os.path.exists(output_file):
        overwrite = input(f"File {output_file} already exists. Overwrite? (y/n): ")
        if overwrite.lower() != "y":
            print("Aborted.")
            return 1

    try:
        # First we'll set up the OAuth tokens using ytmusicapi
        print("Starting OAuth flow...")

        # Get the OAuth token, but don't save it directly
        token = ytmusicapi.setup_oauth(
            client_id=client_id, client_secret=client_secret, open_browser=True
        )

        # Save the OAuth token details to a backup file
        oauth_data = {
            "access_token": token.access_token,
            "refresh_token": token.refresh_token,
            "token_type": token.token_type,
            "expires_in": token.expires_in,
            "expires_at": token.expires_at,
            "scope": token.scope,
            "client_id": client_id,
            "client_secret": client_secret,
        }

        with open(oauth_backup, "w") as f:
            json.dump(oauth_data, f, indent=2)

        # Now create an env file for easy reuse
        env_file = ".env"
        with open(env_file, "w") as f:
            f.write(f"YTM_CLIENT_ID={client_id}\n")
            f.write(f"YTM_CLIENT_SECRET={client_secret}\n")

        print(
            f"OAuth token details saved to {oauth_backup} and client details saved to {env_file}"
        )

        # Now, the most important part: create a browser-auth format file
        # This will be what ytmusicapi 1.10.2 can actually use
        print("\nGenerating browser-compatible authentication file...")

        # We'll use ytmusicapi's browser setup to create a template
        try:
            # Use ytmusicapi's browser setup to generate a template
            headers_auth_temp = "headers_auth_temp.json"
            ytmusicapi.setup(
                filepath=headers_auth_temp,
                headers_raw="""
Accept: */*
Accept-Encoding: gzip, deflate, br
Content-Type: application/json
Cookie: CONSENT=YES+;
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.0.0 Safari/537.36
""",
            )
            # Now read the generated file
            with open(headers_auth_temp, "r") as f:
                headers_data = json.load(f)

            # Remove the temp file
            os.remove(headers_auth_temp)

            # Now modify the headers to include our OAuth token
            headers_data["Cookie"] = (
                f"__Secure-3PAPISID={token.access_token}; {headers_data.get('Cookie', '')}"
            )

            # Save as our output file
            with open(output_file, "w") as f:
                json.dump(headers_data, f, indent=2)

            print(f"Browser-compatible auth file created at: {output_file}")

        except Exception as e:
            print(f"Error creating browser template: {e}")
            print("Creating a basic browser auth file instead...")

            # Create a basic browser auth format manually
            browser_format = {
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.5",
                "Content-Type": "application/json",
                "X-Goog-AuthUser": "0",
                "x-origin": "https://music.youtube.com",
                "Cookie": f"__Secure-3PAPISID={token.access_token}; CONSENT=YES+",
            }

            with open(output_file, "w") as f:
                json.dump(browser_format, f, indent=2)

            print(f"Basic browser auth file created at: {output_file}")

        print("\nTo use this with YTMusicFS, run:")
        print(
            f"python ytmusicfs.py --auth-file {output_file} --auth-type browser --mount-point ./mount"
        )
        print(
            "\nNote: This is using browser auth format but with OAuth token for longer sessions"
        )

        test = input("\nWould you like to test the authentication now? (y/n): ")
        if test.lower() == "y":
            print("\nTesting authentication...")
            import subprocess

            subprocess.run(
                [
                    sys.executable,
                    "test_auth.py",
                    "--auth-file",
                    output_file,
                    "--auth-type",
                    "browser",
                ]
            )

        return 0

    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
