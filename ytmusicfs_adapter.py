#!/usr/bin/env python3

import os
import json
import requests
import time
import ytmusicapi
from ytmusicapi import YTMusic


class YTMusicAdapter:
    """
    Adapter class to help YTMusicFS work with OAuth tokens in ytmusicapi 1.10.2.

    This class works around the limitations of ytmusicapi 1.10.2's OAuth implementation.
    """

    def __init__(self, auth_file, auth_type="browser"):
        self.auth_file = auth_file
        self.auth_type = auth_type
        self.ytmusic = None
        self.last_refresh = 0
        self.refresh_interval = 1800  # 30 minutes in seconds

        # Initialize
        self._initialize()

    def _initialize(self):
        """Initialize the YTMusic instance using the auth file"""
        try:
            # Load the auth file
            with open(self.auth_file, "r") as f:
                auth_data = json.load(f)

            # Initialize the YTMusic instance using browser auth
            # Even with OAuth tokens, we'll package them into a browser auth format
            self.ytmusic = YTMusic(self.auth_file)

            # Store the initialization time
            self.last_refresh = time.time()
            print(f"Successfully initialized YTMusic with {self.auth_type} auth")

            # Test the connection
            self._test_connection()

        except Exception as e:
            print(f"Error initializing YTMusic: {e}")
            # Try to check the auth file format
            self._check_auth_file()
            raise

    def _check_auth_file(self):
        """Check if the auth file has the right format and print diagnostic info"""
        try:
            with open(self.auth_file, "r") as f:
                auth_data = json.load(f)

            # Check for basic browser auth fields
            browser_fields = [
                "User-Agent",
                "Accept",
                "Cookie",
                "Content-Type",
                "x-origin",
            ]

            missing = [field for field in browser_fields if field not in auth_data]
            if missing:
                print(
                    f"Warning: Auth file is missing these browser auth fields: {missing}"
                )
                print("Consider using oauth_helper.py to create a compatible auth file")

            # Check for OAuth tokens
            oauth_fields = ["access_token", "refresh_token", "expires_at"]
            has_oauth = any(field in auth_data for field in oauth_fields)

            if has_oauth and self.auth_type == "browser":
                print(
                    "Warning: Auth file contains OAuth tokens but you're using browser auth"
                )
                print("Consider using --auth-type oauth")

            if not has_oauth and self.auth_type == "oauth":
                print(
                    "Warning: Auth file doesn't contain OAuth tokens but you're using OAuth auth"
                )
                print("Consider using --auth-type browser")

        except Exception as e:
            print(f"Error checking auth file: {e}")

    def _test_connection(self):
        """Test the connection to YouTube Music"""
        try:
            playlists = self.ytmusic.get_library_playlists(limit=1)
            print(f"Connection test successful! Found {len(playlists)} playlists.")
        except Exception as e:
            print(f"Connection test failed: {e}")

    def refresh_if_needed(self):
        """Refresh the YTMusic instance if needed"""
        # Check if we need to refresh
        if time.time() - self.last_refresh > self.refresh_interval:
            try:
                self._initialize()
                return True
            except Exception as e:
                print(f"Error refreshing YTMusic: {e}")
                return False
        return False

    def __getattr__(self, name):
        """Delegate method calls to the YTMusic instance"""
        # Try to refresh if needed
        self.refresh_if_needed()

        # Return the method from the YTMusic instance
        return getattr(self.ytmusic, name)
