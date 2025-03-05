#!/usr/bin/env python3

import os
import sys
import ytmusicapi

def main():
    print("YTMusicFS Authentication Setup")
    print("==============================")
    print("This script will help you set up authentication for YouTube Music.")
    print("You will need to log in to YouTube Music in your browser and provide some information.")
    print()

    output_file = "headers_auth.json"
    if len(sys.argv) > 1:
        output_file = sys.argv[1]

    print(f"Authentication data will be saved to: {output_file}")
    print()

    if os.path.exists(output_file):
        overwrite = input(f"File {output_file} already exists. Overwrite? (y/n): ")
        if overwrite.lower() != 'y':
            print("Aborted.")
            return

    try:
        ytmusicapi.setup(filepath=output_file)
        print()
        print(f"Authentication data saved to {output_file}")
        print("You can now use this file with YTMusicFS.")
    except Exception as e:
        print(f"Error during setup: {e}")
        return 1

    return 0

if __name__ == "__main__":
    sys.exit(main())
