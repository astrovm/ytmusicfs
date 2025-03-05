#!/usr/bin/env python3

import sys
import ytmusicapi


def main():
    print("Setting up YTMusicFS with Browser Authentication")
    print("===============================================")

    try:
        # Use the standard browser authentication method
        print(
            "This will set up the browser authentication method, which is simpler but needs more frequent renewal."
        )
        print("Follow the instructions to authenticate with YouTube Music:")

        # Generate a headers file
        output_file = "browser_auth.json"
        ytmusicapi.setup(filepath=output_file)

        print(f"\nAuthentication data saved to {output_file}")
        print("You can now use this file with YTMusicFS by running:")
        print(
            f"python ytmusicfs.py --auth-file {output_file} --auth-type browser --mount-point ./mount"
        )

        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
