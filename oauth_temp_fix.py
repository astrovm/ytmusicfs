#!/usr/bin/env python3

import json
import os
import sys


def main():
    print("YTMusicFS OAuth Temporary Fix")
    print("=============================")

    # Read the OAuth token from oauth_token.json
    oauth_file = "oauth_token.json"

    if not os.path.exists(oauth_file):
        print(f"Error: {oauth_file} not found.")
        print("Please run oauth_helper.py first to generate the OAuth token.")
        return 1

    try:
        with open(oauth_file, "r") as f:
            oauth_data = json.load(f)

        # Extract the access token
        access_token = oauth_data.get("access_token")

        if not access_token:
            print("Error: access_token not found in the OAuth file.")
            return 1

        # Create a basic browser auth header file
        output_file = "working_auth.json"

        # This is a minimal working format that ytmusicapi 1.10.2 accepts
        browser_auth = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.5",
            "Content-Type": "application/json",
            "X-Goog-AuthUser": "0",
            "x-origin": "https://music.youtube.com",
        }

        # Write the file
        with open(output_file, "w") as f:
            json.dump(browser_auth, f, indent=2)

        print(f"Created browser auth file: {output_file}")
        print("\nTo use this with YTMusicFS, run:")
        print(
            f"python ytmusicfs.py --auth-file {output_file} --auth-type browser --mount-point ./mount"
        )

        return 0

    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
