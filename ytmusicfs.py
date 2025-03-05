#!/usr/bin/env python3

import os
import stat
import time
import errno
import json
from fuse import FUSE, Operations
from ytmusicapi import YTMusic, OAuthCredentials
import requests
import subprocess


class YouTubeMusicFS(Operations):
    def __init__(self, auth_file, auth_type="browser"):
        """Initialize the FUSE filesystem with YouTube Music API.

        Args:
            auth_file: Path to authentication file
            auth_type: Type of authentication, either 'browser' (cookie-based) or 'oauth'
        """
        if auth_type == "oauth":
            # Load OAuth credentials from the auth file
            try:
                with open(auth_file, "r") as f:
                    oauth_data = json.load(f)

                # Extract client_id and client_secret from the file
                client_id = oauth_data.get("client_id")
                client_secret = oauth_data.get("client_secret")

                if not client_id or not client_secret:
                    print("Error: client_id or client_secret not found in OAuth file")
                    raise ValueError("Missing OAuth credentials in auth file")

                # Initialize with OAuth credentials
                self.ytmusic = YTMusic(
                    auth_file,
                    oauth_credentials=OAuthCredentials(
                        client_id=client_id, client_secret=client_secret
                    ),
                )
            except Exception as e:
                print(f"Error initializing with OAuth: {e}")
                raise
        else:
            # Browser authentication
            self.ytmusic = YTMusic(auth_file)

        self.cache = {}  # Cache: {path: {'data': ..., 'time': ...}}
        self.cache_timeout = 300  # 5 minutes in seconds
        self.open_files = {}  # Store file handles: {handle: {'stream_url': ...}}
        self.next_fh = 1  # Next file handle to assign
        self.auth_file = auth_file
        self.auth_type = auth_type

    def _refresh_auth_if_needed(self):
        """Attempt to refresh authentication if available"""
        if self.auth_type == "oauth":
            try:
                # Reload OAuth credentials
                with open(self.auth_file, "r") as f:
                    oauth_data = json.load(f)

                client_id = oauth_data.get("client_id")
                client_secret = oauth_data.get("client_secret")

                if not client_id or not client_secret:
                    print("Error: client_id or client_secret not found in OAuth file")
                    return False

                # Re-initialize with OAuth credentials
                self.ytmusic = YTMusic(
                    self.auth_file,
                    oauth_credentials=OAuthCredentials(
                        client_id=client_id, client_secret=client_secret
                    ),
                )
                return True
            except Exception as e:
                print(f"Error refreshing OAuth authentication: {e}")
                return False
        return False

    def _get_from_cache(self, path):
        if (
            path in self.cache
            and time.time() - self.cache[path]["time"] < self.cache_timeout
        ):
            return self.cache[path]["data"]
        return None

    def _set_cache(self, path, data):
        self.cache[path] = {"data": data, "time": time.time()}

    def _sanitize_filename(self, name):
        """Sanitize a string to be used as a filename."""
        # Replace problematic characters
        sanitized = name.replace("/", "-").replace("\\", "-").replace(":", "-")
        sanitized = sanitized.replace("*", "-").replace("?", "-").replace('"', "-")
        sanitized = sanitized.replace("<", "-").replace(">", "-").replace("|", "-")
        return sanitized

    def readdir(self, path, fh):
        # Standard entries for all directories
        dirents = [".", ".."]

        if path == "/":
            dirents.extend(["playlists", "liked_songs", "artists", "albums"])
        elif path == "/playlists":
            playlists = self._get_from_cache("/playlists")
            if not playlists:
                try:
                    playlists = self.ytmusic.get_library_playlists()
                    self._set_cache("/playlists", playlists)
                except Exception as e:
                    print(f"Error fetching playlists: {e}")
                    # Try to refresh auth and retry
                    if self._refresh_auth_if_needed():
                        try:
                            playlists = self.ytmusic.get_library_playlists()
                            self._set_cache("/playlists", playlists)
                        except Exception as e:
                            print(f"Error fetching playlists after auth refresh: {e}")
                            playlists = []
                    else:
                        playlists = []

            if playlists:
                dirents.extend(p["title"] for p in playlists)
        elif path.startswith("/playlists/"):
            playlist_name = path[len("/playlists/") :]
            playlists = self._get_from_cache("/playlists")
            if not playlists:
                try:
                    playlists = self.ytmusic.get_library_playlists()
                    self._set_cache("/playlists", playlists)
                except Exception as e:
                    print(f"Error fetching playlists: {e}")
                    playlists = []

            # Find the playlist ID
            playlist_id = None
            for p in playlists:
                if p["title"] == playlist_name:
                    playlist_id = p["playlistId"]
                    break

            if not playlist_id:
                return dirents  # Empty directory if playlist not found

            songs = self._get_from_cache(path)
            if not songs:
                try:
                    playlist_data = self.ytmusic.get_playlist(playlist_id)
                    if "tracks" in playlist_data:
                        songs_raw = playlist_data["tracks"]

                        # Generate unique file names
                        seen = {}
                        song_files = []
                        for song in songs_raw:
                            if "title" not in song or "videoId" not in song:
                                continue

                            title = song["title"].replace("/", "-")  # Sanitize
                            filename = f"{title}.m4a"
                            if filename in seen:
                                seen[filename] += 1
                                filename = f"{title}_{seen[filename]}.m4a"
                            else:
                                seen[filename] = 0
                            song_files.append(
                                {"filename": filename, "videoId": song["videoId"]}
                            )

                        self._set_cache(path, song_files)
                        songs = song_files
                    else:
                        print(f"Warning: No tracks found in playlist {playlist_name}")
                        songs = []
                except Exception as e:
                    print(f"Error fetching playlist {playlist_name}: {e}")
                    songs = []

            if songs:
                dirents.extend(song["filename"] for song in songs)
        elif path == "/liked_songs":
            songs = self._get_from_cache(path)
            if not songs:
                try:
                    # Get liked songs with error handling for different response formats
                    liked_songs_response = self.ytmusic.get_liked_songs(limit=100)
                    songs_raw = []

                    # Handle different response formats
                    if "tracks" in liked_songs_response:
                        songs_raw = liked_songs_response["tracks"]
                    elif "contents" in liked_songs_response:
                        if (
                            "singleColumnBrowseResultsRenderer"
                            in liked_songs_response["contents"]
                        ):
                            # New format handling
                            tab_renderer = liked_songs_response["contents"][
                                "singleColumnBrowseResultsRenderer"
                            ]["tabs"][0]["tabRenderer"]
                            section_list = tab_renderer["content"][
                                "sectionListRenderer"
                            ]["contents"][0]
                            if "musicShelfRenderer" in section_list:
                                music_shelf = section_list["musicShelfRenderer"]
                                if "contents" in music_shelf:
                                    for item in music_shelf["contents"]:
                                        if "musicResponsiveListItemRenderer" in item:
                                            renderer = item[
                                                "musicResponsiveListItemRenderer"
                                            ]
                                            song = {
                                                "title": renderer["flexColumns"][0][
                                                    "musicResponsiveListItemFlexColumnRenderer"
                                                ]["text"]["runs"][0]["text"],
                                                "videoId": renderer["flexColumns"][0][
                                                    "musicResponsiveListItemFlexColumnRenderer"
                                                ]["text"]["runs"][0]
                                                .get("navigationEndpoint", {})
                                                .get("watchEndpoint", {})
                                                .get("videoId", ""),
                                            }
                                            if song["videoId"]:
                                                songs_raw.append(song)

                    # If we still couldn't get songs, try a different approach
                    if not songs_raw:
                        print(
                            "Warning: Could not get liked songs using standard method, using fallback"
                        )
                        # Use the first playlist (usually "Liked Music") as a fallback
                        playlists = self._get_from_cache("/playlists")
                        if not playlists:
                            playlists = self.ytmusic.get_library_playlists()
                            self._set_cache("/playlists", playlists)

                        for playlist in playlists:
                            if playlist["title"] == "Liked Music":
                                playlist_songs = self.ytmusic.get_playlist(
                                    playlist["playlistId"]
                                )
                                if "tracks" in playlist_songs:
                                    songs_raw = playlist_songs["tracks"]
                                break

                    seen = {}
                    song_files = []
                    for song in songs_raw:
                        if "title" not in song or "videoId" not in song:
                            continue

                        title = song["title"].replace("/", "-")  # Sanitize
                        filename = f"{title}.m4a"
                        if filename in seen:
                            seen[filename] += 1
                            filename = f"{title}_{seen[filename]}.m4a"
                        else:
                            seen[filename] = 0
                        song_files.append(
                            {"filename": filename, "videoId": song["videoId"]}
                        )

                    self._set_cache(path, song_files)
                    songs = song_files
                except Exception as e:
                    print(f"Error fetching liked songs: {e}")
                    songs = []
            dirents.extend(song["filename"] for song in songs)
        elif path == "/artists":
            artists = self._get_from_cache("/artists")
            if not artists:
                try:
                    artists = self.ytmusic.get_library_artists(limit=50)
                    self._set_cache("/artists", artists)
                except Exception as e:
                    print(f"Error fetching artists: {e}")
                    artists = []

            if artists:
                for a in artists:
                    # Check if 'artist' key exists, otherwise try other keys or use a default
                    if "artist" in a:
                        artist_name = self._sanitize_filename(a["artist"])
                        dirents.append(artist_name)
                    elif "name" in a:
                        artist_name = self._sanitize_filename(a["name"])
                        dirents.append(artist_name)
                    else:
                        # Skip this artist if we can't find a name
                        continue
        elif path.startswith("/artists/"):
            artist_name = path[len("/artists/") :]
            songs = self._get_from_cache(path)

            if not songs:
                try:
                    # Fetch the artist ID
                    artists = self._get_from_cache("/artists")
                    if not artists:
                        artists = self.ytmusic.get_library_artists(limit=50)
                        self._set_cache("/artists", artists)

                    artist_id = None
                    for a in artists:
                        # Check different possible keys and find the artist by name
                        artist_key = None
                        if "artist" in a:
                            artist_key = "artist"
                        elif "name" in a:
                            artist_key = "name"

                        if (
                            artist_key
                            and self._sanitize_filename(a[artist_key]) == artist_name
                        ):
                            # Check for different possible ID keys
                            if "artistId" in a:
                                artist_id = a["artistId"]
                            elif "browseId" in a:
                                artist_id = a["browseId"]
                            elif "id" in a:
                                artist_id = a["id"]
                            break

                    if artist_id:
                        # Fetch the artist's songs
                        artist_data = self.ytmusic.get_artist(artist_id)
                        if "songs" in artist_data:
                            songs_raw = artist_data["songs"]

                            # Generate unique file names
                            seen = {}
                            song_files = []
                            for song in songs_raw:
                                if "title" not in song or "videoId" not in song:
                                    continue

                                title = song["title"].replace("/", "-")  # Sanitize
                                filename = f"{title}.m4a"
                                if filename in seen:
                                    seen[filename] += 1
                                    filename = f"{title}_{seen[filename]}.m4a"
                                else:
                                    seen[filename] = 0
                                song_files.append(
                                    {"filename": filename, "videoId": song["videoId"]}
                                )

                            self._set_cache(path, song_files)
                            songs = song_files
                except Exception as e:
                    print(f"Error fetching artist {artist_name}: {e}")
                    songs = []

            if songs:
                dirents.extend(song["filename"] for song in songs)
        elif path == "/albums":
            albums = self._get_from_cache(path)
            if not albums:
                try:
                    albums = self.ytmusic.get_library_albums(limit=50)
                    self._set_cache(path, albums)
                except Exception as e:
                    print(f"Error fetching albums: {e}")
                    albums = []

            if albums:
                dirents.extend(self._sanitize_filename(a["title"]) for a in albums)
        elif path.startswith("/albums/"):
            album_name = path[len("/albums/") :]
            albums = self._get_from_cache("/albums")
            if not albums:
                try:
                    albums = self.ytmusic.get_library_albums(limit=50)
                    self._set_cache("/albums", albums)
                except Exception as e:
                    print(f"Error fetching albums: {e}")
                    return dirents

            # Find the album ID
            album_id = None
            for a in albums:
                if self._sanitize_filename(a["title"]) == album_name:
                    album_id = a["browseId"]
                    break

            if not album_id:
                return dirents  # Empty directory if album not found

            songs = self._get_from_cache(path)
            if not songs:
                try:
                    album_info = self.ytmusic.get_album(album_id)
                    songs_raw = []

                    if "tracks" in album_info:
                        songs_raw = album_info["tracks"]

                    seen = {}
                    song_files = []
                    for song in songs_raw:
                        if "title" not in song or "videoId" not in song:
                            continue

                        title = song["title"].replace("/", "-")  # Sanitize
                        filename = f"{title}.m4a"
                        if filename in seen:
                            seen[filename] += 1
                            filename = f"{title}_{seen[filename]}.m4a"
                        else:
                            seen[filename] = 0
                        song_files.append(
                            {"filename": filename, "videoId": song["videoId"]}
                        )

                    self._set_cache(path, song_files)
                    songs = song_files
                except Exception as e:
                    print(f"Error fetching album songs: {e}")
                    songs = []

            if songs:
                dirents.extend(song["filename"] for song in songs)

        return dirents

    def getattr(self, path, fh=None):
        now = time.time()
        st = {
            "st_atime": now,
            "st_mtime": now,
            "st_ctime": now,
            "st_uid": os.getuid(),
            "st_gid": os.getgid(),
        }

        # Root and top-level directories
        if path == "/" or path in ["/playlists", "/liked_songs", "/artists", "/albums"]:
            st["st_mode"] = stat.S_IFDIR | 0o755
            st["st_nlink"] = 2
            return st

        # Playlist directories
        if path.startswith("/playlists/") and not path.endswith(".m4a"):
            playlists = self._get_from_cache("/playlists")
            if not playlists:
                try:
                    playlists = self.ytmusic.get_library_playlists()
                    self._set_cache("/playlists", playlists)
                except Exception:
                    pass

            if playlists:
                playlist_name = path[len("/playlists/") :]
                for p in playlists:
                    if p["title"] == playlist_name:
                        st["st_mode"] = stat.S_IFDIR | 0o755
                        st["st_nlink"] = 2
                        return st

        # Artist directories
        if path.startswith("/artists/") and not path.endswith(".m4a"):
            artists = self._get_from_cache("/artists")
            if not artists:
                try:
                    artists = self.ytmusic.get_library_artists(limit=50)
                    self._set_cache("/artists", artists)
                except Exception:
                    pass

            if artists:
                artist_name = path[len("/artists/") :]
                for a in artists:
                    if self._sanitize_filename(a["artist"]) == artist_name:
                        st["st_mode"] = stat.S_IFDIR | 0o755
                        st["st_nlink"] = 2
                        return st

        # Album directories
        if path.startswith("/albums/") and not path.endswith(".m4a"):
            albums = self._get_from_cache("/albums")
            if not albums:
                try:
                    albums = self.ytmusic.get_library_albums(limit=50)
                    self._set_cache("/albums", albums)
                except Exception:
                    pass

            if albums:
                album_name = path[len("/albums/") :]
                for a in albums:
                    if self._sanitize_filename(a["title"]) == album_name:
                        st["st_mode"] = stat.S_IFDIR | 0o755
                        st["st_nlink"] = 2
                        return st

        # Music files
        if path.endswith(".m4a"):
            # Check if the file exists in any of our directories
            dir_path = os.path.dirname(path)
            filename = os.path.basename(path)

            songs = self._get_from_cache(dir_path)
            if songs:
                for song in songs:
                    if song.get("filename") == filename:
                        st["st_mode"] = stat.S_IFREG | 0o644
                        st["st_nlink"] = 1
                        st["st_size"] = (
                            10 * 1024 * 1024
                        )  # Placeholder; size unknown for streams, use 10MB
                        return st

        # Path not found
        raise OSError(errno.ENOENT, f"No such file or directory: {path}")

    def open(self, path, flags):
        print(f"open: {path}")
        if path == "/":
            raise OSError(errno.EISDIR, "Is a directory")

        # Extract directory path and filename
        dir_path = os.path.dirname(path)
        filename = os.path.basename(path)
        songs = self._get_from_cache(dir_path)

        if not songs:
            # Re-fetch if not in cache (simplified; assumes readdir was called)
            self.readdir(dir_path, None)
            songs = self._get_from_cache(dir_path)

        video_id = None
        for song in songs:
            if song["filename"] == filename:
                video_id = song["videoId"]
                break

        if not video_id:
            raise OSError(errno.ENOENT, f"File not found: {path}")

        # Fetch stream URL using yt-dlp
        try:
            # Use yt-dlp to get the audio stream URL
            cmd = [
                "yt-dlp",
                "-f",
                "140",
                "-g",
                f"https://www.youtube.com/watch?v={video_id}",
            ]
            print(f"Running command: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            stream_url = result.stdout.strip()

            if not stream_url:
                raise OSError(errno.EIO, "No suitable audio stream found")

            print(f"Successfully got stream URL for {video_id}")

            # Store the stream URL and return a file handle
            fh = self.next_fh
            self.next_fh += 1
            self.open_files[fh] = {"stream_url": stream_url}
            return fh

        except subprocess.SubprocessError as e:
            print(f"Error running yt-dlp: {e}")
            raise OSError(errno.EIO, f"Failed to get stream URL: {str(e)}")
        except Exception as e:
            print(f"Unexpected error getting stream URL: {e}")
            raise OSError(errno.EIO, f"Failed to get stream URL: {str(e)}")

    def read(self, path, size, offset, fh):
        try:
            if fh not in self.open_files:
                raise OSError(errno.EBADF, "Bad file descriptor")

            stream_url = self.open_files[fh]["stream_url"]
            headers = {"Range": f"bytes={offset}-{offset + size - 1}"}
            response = requests.get(stream_url, headers=headers, stream=True)

            if response.status_code not in (200, 206):
                raise RuntimeError(f"Failed to stream: {response.status_code}")

            return response.content
        except Exception as e:
            print(f"Error in read: {e}")
            raise OSError(errno.EIO, f"Failed to read stream: {str(e)}")

    def release(self, path, fh):
        if fh in self.open_files:
            del self.open_files[fh]
        return 0


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Mount YouTube Music as a filesystem")
    parser.add_argument(
        "--mount-point",
        required=True,
        help="Directory where the filesystem will be mounted",
    )
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
    parser.add_argument(
        "--foreground",
        action="store_true",
        help="Run in the foreground (for debugging)",
    )

    args = parser.parse_args()

    if not os.path.exists(args.mount_point):
        os.makedirs(args.mount_point)

    print(f"Mounting YouTube Music filesystem at {args.mount_point}")
    print(f"Using authentication file: {args.auth_file}")
    print(f"Authentication type: {args.auth_type}")

    if args.foreground:
        print("Press Ctrl+C to unmount")

    FUSE(
        YouTubeMusicFS(args.auth_file, args.auth_type),
        args.mount_point,
        foreground=args.foreground,
        nothreads=True,
    )


if __name__ == "__main__":
    main()
