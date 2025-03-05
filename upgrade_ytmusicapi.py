#!/usr/bin/env python3

import sys
import subprocess


def main():
    print("Upgrading ytmusicapi to latest version")
    print("====================================")

    try:
        # Install the latest version of ytmusicapi
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--upgrade", "ytmusicapi"]
        )

        # Check the installed version
        subprocess.check_call(
            [
                sys.executable,
                "-c",
                "import ytmusicapi; print(f'Successfully upgraded ytmusicapi to version {ytmusicapi.__version__}')",
            ]
        )

        print("\nUpgrade successful! Please try using your OAuth file again with:")
        print(
            "python ytmusicfs.py --auth-file complete_oauth_auth.json --auth-type oauth --mount-point ./mount --foreground"
        )

        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
