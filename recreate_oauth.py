#!/usr/bin/env python3

import json
import sys
import ytmusicapi


def main():
    print("Recreating YTMusic OAuth Token")
    print("=============================")

    # Load the existing OAuth file to extract client_id and client_secret
    try:
        with open("oauth_auth.json", "r") as f:
            oauth_data = json.load(f)

        client_id = oauth_data.get("client_id")
        client_secret = oauth_data.get("client_secret")

        if not client_id or not client_secret:
            print("Error: client_id or client_secret not found in oauth_auth.json")
            return 1

        print(f"Found client_id: {client_id[:10]}...")
        print(f"Found client_secret: {client_secret[:5]}...")

        # Create a new OAuth token
        print("\nCreating new OAuth token...")
        output_file = "new_oauth_auth.json"

        # Use the setup_oauth function
        token = ytmusicapi.setup_oauth(
            client_id=client_id,
            client_secret=client_secret,
            filepath=output_file,
            open_browser=True,
        )

        print(f"\nOAuth authentication data saved to {output_file}")
        print("You can now use this file with YTMusicFS by running:")
        print(
            f"python ytmusicfs.py --auth-file {output_file} --auth-type oauth --mount-point ./mount"
        )

        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
