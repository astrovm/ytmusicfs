#!/usr/bin/env python3

import sys
import json
from ytmusicapi import YTMusic, OAuthCredentials
import argparse


def main():
    parser = argparse.ArgumentParser(description="Test YTMusicFS authentication")
    parser.add_argument(
        "--auth-file",
        default="headers_auth.json",
        help="Path to the authentication file (default: headers_auth.json)",
    )
    parser.add_argument(
        "--auth-type",
        default="browser",
        choices=["browser", "oauth"],
        help="Type of authentication to use: browser (cookie-based) or oauth (default: browser)",
    )

    args = parser.parse_args()

    print("YTMusicFS Authentication Test")
    print("=============================")

    print(f"Testing authentication with file: {args.auth_file}")
    print(f"Authentication type: {args.auth_type}")
    print()

    try:
        # We no longer need special handling for OAuth - the auth file is already
        # in the correct format and the API will detect it automatically
        ytmusic = YTMusic(args.auth_file)
        print(f"Authentication successful using {args.auth_type} method!")

        # Test getting playlists
        print("Fetching playlists...")
        playlists = ytmusic.get_library_playlists(limit=5)
        print(f"Found {len(playlists)} playlists")
        for i, playlist in enumerate(playlists[:5], 1):
            print(f"  {i}. {playlist['title']}")

        print()

        # Test getting liked songs
        print("Fetching liked songs...")
        try:
            liked = ytmusic.get_liked_songs(limit=5)

            # Handle different response formats
            tracks = []
            if "tracks" in liked:
                tracks = liked["tracks"]
            elif "contents" in liked:
                print("Detected new API response format, trying to extract songs...")
                if "singleColumnBrowseResultsRenderer" in liked["contents"]:
                    # New format handling
                    tab_renderer = liked["contents"][
                        "singleColumnBrowseResultsRenderer"
                    ]["tabs"][0]["tabRenderer"]
                    section_list = tab_renderer["content"]["sectionListRenderer"][
                        "contents"
                    ][0]
                    if "musicShelfRenderer" in section_list:
                        music_shelf = section_list["musicShelfRenderer"]
                        if "contents" in music_shelf:
                            for item in music_shelf["contents"]:
                                if "musicResponsiveListItemRenderer" in item:
                                    renderer = item["musicResponsiveListItemRenderer"]
                                    song = {
                                        "title": renderer["flexColumns"][0][
                                            "musicResponsiveListItemFlexColumnRenderer"
                                        ]["text"]["runs"][0]["text"],
                                        "artists": [
                                            {
                                                "name": renderer["flexColumns"][1][
                                                    "musicResponsiveListItemFlexColumnRenderer"
                                                ]["text"]["runs"][0]["text"]
                                            }
                                        ],
                                    }
                                    tracks.append(song)

            # If we still couldn't get songs, try a different approach
            if not tracks:
                print(
                    "Warning: Could not get liked songs using standard method, using fallback"
                )
                # Use the first playlist (usually "Liked Music") as a fallback
                playlists = ytmusic.get_library_playlists(limit=5)
                for playlist in playlists:
                    if playlist["title"] == "Liked Music":
                        playlist_songs = ytmusic.get_playlist(playlist["playlistId"])
                        if "tracks" in playlist_songs:
                            tracks = playlist_songs["tracks"]
                        break

            print(f"Found {len(tracks)} liked songs")
            for i, song in enumerate(tracks[:5], 1):
                if "artists" in song and song["artists"]:
                    print(f"  {i}. {song['title']} by {song['artists'][0]['name']}")
                else:
                    print(f"  {i}. {song['title']}")
        except Exception as e:
            print(f"Error fetching liked songs: {e}")
            import traceback

            traceback.print_exc()

        print()
        print("Authentication test successful!")
        return 0

    except Exception as e:
        print(f"Error during authentication test: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
