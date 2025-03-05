#!/usr/bin/env python3

import json
import sys


def main():
    # Source files
    original_file = "oauth_auth.json"
    new_file = "new_oauth_auth.json"
    output_file = "complete_oauth_auth.json"

    print(f"Updating OAuth token file with client credentials")
    print("===============================================")

    try:
        # Load client ID and secret from original file
        with open(original_file, "r") as f:
            original_data = json.load(f)

        client_id = original_data.get("client_id")
        client_secret = original_data.get("client_secret")

        if not client_id or not client_secret:
            print(
                f"Error: Could not find client_id or client_secret in {original_file}"
            )
            return 1

        # Load the new OAuth token
        with open(new_file, "r") as f:
            oauth_data = json.load(f)

        # Add client ID and secret
        oauth_data["client_id"] = client_id
        oauth_data["client_secret"] = client_secret

        # Save the complete file
        with open(output_file, "w") as f:
            json.dump(oauth_data, f, indent=2)

        print(f"Successfully created {output_file} with token and client credentials")
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
