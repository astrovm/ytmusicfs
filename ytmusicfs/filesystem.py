#!/usr/bin/env python3

import os
import stat
import time
import errno
import json
import logging
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional, List

import requests
from fuse import FUSE, Operations

from ytmusicfs.utils.oauth_adapter import YTMusicOAuthAdapter


class YouTubeMusicFS(Operations):
    """YouTube Music FUSE filesystem implementation."""

    def __init__(
        self,
        auth_file: str,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        cache_dir: Optional[str] = None,
        cache_timeout: int = 300,
    ):
        """Initialize the FUSE filesystem with YouTube Music API.

        Args:
            auth_file: Path to authentication file (OAuth token)
            client_id: OAuth client ID (required for OAuth authentication)
            client_secret: OAuth client secret (required for OAuth authentication)
            cache_dir: Directory to store cache files (defaults to ~/.cache/ytmusicfs)
            cache_timeout: Cache timeout in seconds (default: 5 minutes)
        """
        # Get the logger
        self.logger = logging.getLogger("YTMusicFS")

        # Set up cache directory
        if cache_dir is None:
            cache_dir = os.path.expanduser("~/.cache/ytmusicfs")
        self.cache_dir = Path(cache_dir)
        os.makedirs(self.cache_dir, exist_ok=True)
        self.logger.info(f"Using cache directory: {self.cache_dir}")

        self.logger.info(f"Initializing YTMusicFS with OAuth authentication")

        try:
            # Use YTMusicOAuthAdapter for OAuth support
            self.ytmusic = YTMusicOAuthAdapter(
                auth_file=auth_file,
                client_id=client_id,
                client_secret=client_secret,
                logger=self.logger,
            )
            self.logger.info(f"Authentication successful with OAuth method!")
        except Exception as e:
            self.logger.error(f"Error during authentication: {e}")
            self.logger.error(
                "Try regenerating your authentication file with ytmusicfs-oauth"
            )
            raise

        self.cache = {}  # In-memory cache: {path: {'data': ..., 'time': ...}}
        self.cache_timeout = cache_timeout
        self.open_files = {}  # Store file handles: {handle: {'stream_url': ...}}
        self.next_fh = 1  # Next file handle to assign
        self.auth_file = auth_file
        self.client_id = client_id
        self.client_secret = client_secret

    def _get_from_cache(self, path: str) -> Optional[Any]:
        """Get data from cache if it's still valid.

        Args:
            path: The path to retrieve from cache

        Returns:
            The cached data if valid, None otherwise
        """
        # First check memory cache
        if (
            path in self.cache
            and time.time() - self.cache[path]["time"] < self.cache_timeout
        ):
            self.logger.debug(f"Cache hit (memory) for {path}")
            return self.cache[path]["data"]

        # Then check disk cache
        cache_file = self.cache_dir / f"{path.replace('/', '_')}.json"
        if cache_file.exists():
            try:
                with open(cache_file, "r") as f:
                    cache_data = json.load(f)

                if time.time() - cache_data["time"] < self.cache_timeout:
                    self.logger.debug(f"Cache hit (disk) for {path}")
                    # Also update memory cache
                    self.cache[path] = cache_data
                    return cache_data["data"]
            except Exception as e:
                self.logger.debug(f"Failed to read disk cache for {path}: {e}")

        return None

    def _set_cache(self, path: str, data: Any) -> None:
        """Set data in both memory and disk cache.

        Args:
            path: The path to cache
            data: The data to cache
        """
        cache_entry = {"data": data, "time": time.time()}

        # Update memory cache
        self.cache[path] = cache_entry

        # Update disk cache
        try:
            cache_file = self.cache_dir / f"{path.replace('/', '_')}.json"
            with open(cache_file, "w") as f:
                json.dump(cache_entry, f)
        except Exception as e:
            self.logger.warning(f"Failed to write disk cache for {path}: {e}")

    def _sanitize_filename(self, name: str) -> str:
        """Sanitize a string to be used as a filename.

        Args:
            name: The filename to sanitize

        Returns:
            A sanitized filename
        """
        # Replace problematic characters
        sanitized = name.replace("/", "-").replace("\\", "-").replace(":", "-")
        sanitized = sanitized.replace("*", "-").replace("?", "-").replace('"', "-")
        sanitized = sanitized.replace("<", "-").replace(">", "-").replace("|", "-")
        return sanitized

    def readdir(self, path: str, fh: Optional[int] = None) -> List[str]:
        """Read directory contents.

        Args:
            path: The directory path
            fh: File handle (unused)

        Returns:
            List of directory entries
        """
        self.logger.debug(f"readdir: {path}")

        # Standard entries for all directories
        dirents = [".", ".."]

        if path == "/":
            dirents.extend(["playlists", "liked_songs", "artists", "albums"])
            return dirents

        # Handle each directory type
        try:
            if path == "/playlists":
                playlists = self._get_from_cache("/playlists")
                if not playlists:
                    playlists = self.ytmusic.get_library_playlists(limit=100)
                    self._set_cache("/playlists", playlists)

                for playlist in playlists:
                    dirents.append(self._sanitize_filename(playlist["title"]))

            elif path == "/liked_songs":
                liked_songs = self._get_from_cache("/liked_songs")
                if not liked_songs:
                    liked_songs = self.ytmusic.get_liked_songs()
                    self._set_cache("/liked_songs", liked_songs)

                # Process the raw liked songs data
                self.logger.debug(
                    f"Processing raw liked songs data: {type(liked_songs)}"
                )

                # Add the songs to the directory listing
                processed_tracks = []
                for song in liked_songs["tracks"]:
                    title = song.get("title", "Unknown Title")
                    artists = ", ".join(
                        [
                            a.get("name", "Unknown Artist")
                            for a in song.get("artists", [])
                        ]
                    )
                    filename = f"{artists} - {title}.m4a"
                    sanitized_filename = self._sanitize_filename(filename)
                    dirents.append(sanitized_filename)

                    # Create a processed song with the necessary data
                    processed_song = {
                        "title": title,
                        "artists": song.get("artists", []),
                        "videoId": song.get("videoId"),
                        "filename": sanitized_filename,
                        "originalData": song,  # Keep the original data for reference
                    }
                    processed_tracks.append(processed_song)

                # Cache the processed song list with filename mappings
                self.logger.debug(
                    f"Caching {len(processed_tracks)} processed tracks for /liked_songs"
                )
                self._set_cache("/liked_songs_processed", processed_tracks)

            # Extract playlist information
            elif path.startswith("/playlists/"):
                playlist_name = path.split("/")[2]

                # Find the playlist ID
                playlists = self._get_from_cache("/playlists")
                if not playlists:
                    playlists = self.ytmusic.get_library_playlists(limit=100)
                    self._set_cache("/playlists", playlists)

                playlist_id = None
                for playlist in playlists:
                    if self._sanitize_filename(playlist["title"]) == playlist_name:
                        playlist_id = playlist["playlistId"]
                        break

                if not playlist_id:
                    self.logger.error(f"Could not find playlist ID for {playlist_name}")
                    return dirents

                # Get the playlist tracks
                playlist_cache_key = f"/playlist/{playlist_id}"
                playlist_tracks = self._get_from_cache(playlist_cache_key)

                if not playlist_tracks:
                    try:
                        playlist_data = self.ytmusic.get_playlist(
                            playlist_id, limit=500
                        )
                        playlist_tracks = playlist_data.get("tracks", [])
                        self._set_cache(playlist_cache_key, playlist_tracks)
                    except Exception as e:
                        self.logger.error(f"Error fetching playlist {playlist_id}: {e}")
                        return dirents

                # Add the songs to the directory listing
                processed_tracks = []
                for track in playlist_tracks:
                    title = track.get("title", "Unknown Title")
                    artists = ", ".join(
                        [
                            a.get("name", "Unknown Artist")
                            for a in track.get("artists", [])
                        ]
                    )
                    filename = f"{artists} - {title}.m4a"
                    sanitized_filename = self._sanitize_filename(filename)
                    dirents.append(sanitized_filename)

                    # Add filename to track data for lookups
                    track["filename"] = sanitized_filename
                    processed_tracks.append(track)

                # Cache the processed track list for this playlist
                self._set_cache(path, processed_tracks)

            elif path == "/artists":
                artists = self._get_from_cache("/artists")
                if not artists:
                    artists = self.ytmusic.get_library_artists(limit=100)
                    self._set_cache("/artists", artists)

                for artist in artists:
                    dirents.append(self._sanitize_filename(artist["artist"]))

            elif path == "/albums":
                albums = self._get_from_cache("/albums")
                if not albums:
                    albums = self.ytmusic.get_library_albums(limit=100)
                    self._set_cache("/albums", albums)

                for album in albums:
                    dirents.append(self._sanitize_filename(album["title"]))

            # Extract artist information
            elif path.startswith("/artists/"):
                artist_name = path.split("/")[2]

                # Find the artist ID
                artists = self._get_from_cache("/artists")
                if not artists:
                    artists = self.ytmusic.get_library_artists(limit=100)
                    self._set_cache("/artists", artists)

                artist_id = None
                for artist in artists:
                    if self._sanitize_filename(artist["artist"]) == artist_name:
                        artist_id = artist["artistId"]
                        if not artist_id:
                            artist_id = artist["browseId"]
                        break

                if not artist_id:
                    self.logger.error(f"Could not find artist ID for {artist_name}")
                    return dirents

                # Get the artist's albums and singles
                artist_cache_key = f"/artist/{artist_id}"
                artist_albums = self._get_from_cache(artist_cache_key)

                if not artist_albums:
                    try:
                        artist_data = self.ytmusic.get_artist(artist_id)
                        artist_albums = []

                        # Get albums
                        if "albums" in artist_data:
                            for album in artist_data["albums"]["results"]:
                                artist_albums.append(
                                    {
                                        "title": album.get("title", "Unknown Album"),
                                        "year": album.get("year", ""),
                                        "type": "album",
                                        "browseId": album.get("browseId"),
                                    }
                                )

                        # Get singles
                        if "singles" in artist_data:
                            for single in artist_data["singles"]["results"]:
                                artist_albums.append(
                                    {
                                        "title": single.get("title", "Unknown Single"),
                                        "year": single.get("year", ""),
                                        "type": "single",
                                        "browseId": single.get("browseId"),
                                    }
                                )

                        self._set_cache(artist_cache_key, artist_albums)
                    except Exception as e:
                        self.logger.error(f"Error fetching artist {artist_id}: {e}")
                        return dirents

                # Add the albums to the directory listing
                for item in artist_albums:
                    dirents.append(self._sanitize_filename(item["title"]))

            # Extract album information
            elif (
                path.startswith("/albums/")
                or path.startswith("/artists/")
                and path.count("/") >= 3
            ):
                # Determine if this is an artist's album or a library album
                is_artist_album = path.startswith("/artists/")

                if is_artist_album:
                    parts = path.split("/")
                    artist_name = parts[2]
                    album_name = parts[3]

                    # Find the artist ID
                    artists = self._get_from_cache("/artists")
                    if not artists:
                        artists = self.ytmusic.get_library_artists(limit=100)
                        self._set_cache("/artists", artists)

                    artist_id = None
                    for artist in artists:
                        if self._sanitize_filename(artist["artist"]) == artist_name:
                            artist_id = artist["artistId"]
                            if not artist_id:
                                artist_id = artist["browseId"]
                            break

                    if not artist_id:
                        self.logger.error(f"Could not find artist ID for {artist_name}")
                        return dirents

                    # Get the artist's albums
                    artist_cache_key = f"/artist/{artist_id}"
                    artist_albums = self._get_from_cache(artist_cache_key)

                    if not artist_albums:
                        try:
                            artist_data = self.ytmusic.get_artist(artist_id)
                            artist_albums = []

                            # Get albums
                            if "albums" in artist_data:
                                for album in artist_data["albums"]["results"]:
                                    artist_albums.append(
                                        {
                                            "title": album.get(
                                                "title", "Unknown Album"
                                            ),
                                            "year": album.get("year", ""),
                                            "type": "album",
                                            "browseId": album.get("browseId"),
                                        }
                                    )

                            # Get singles
                            if "singles" in artist_data:
                                for single in artist_data["singles"]["results"]:
                                    artist_albums.append(
                                        {
                                            "title": single.get(
                                                "title", "Unknown Single"
                                            ),
                                            "year": single.get("year", ""),
                                            "type": "single",
                                            "browseId": single.get("browseId"),
                                        }
                                    )

                            self._set_cache(artist_cache_key, artist_albums)
                        except Exception as e:
                            self.logger.error(f"Error fetching artist {artist_id}: {e}")
                            return dirents

                    # Find the album ID
                    album_id = None
                    for album in artist_albums:
                        if self._sanitize_filename(album["title"]) == album_name:
                            album_id = album["browseId"]
                            break

                    if not album_id:
                        self.logger.error(f"Could not find album ID for {album_name}")
                        return dirents
                else:
                    # Regular album path
                    album_name = path.split("/")[2]

                    # Find the album ID
                    albums = self._get_from_cache("/albums")
                    if not albums:
                        albums = self.ytmusic.get_library_albums(limit=100)
                        self._set_cache("/albums", albums)

                    album_id = None
                    for album in albums:
                        if self._sanitize_filename(album["title"]) == album_name:
                            album_id = album["browseId"]
                            break

                    if not album_id:
                        self.logger.error(f"Could not find album ID for {album_name}")
                        return dirents

                # Get the album tracks
                album_cache_key = f"/album/{album_id}"
                album_tracks = self._get_from_cache(album_cache_key)

                if not album_tracks:
                    try:
                        album_data = self.ytmusic.get_album(album_id)
                        album_tracks = album_data.get("tracks", [])
                        self._set_cache(album_cache_key, album_tracks)
                    except Exception as e:
                        self.logger.error(f"Error fetching album {album_id}: {e}")
                        return dirents

                # Add the songs to the directory listing
                processed_tracks = []
                for track in album_tracks:
                    title = track.get("title", "Unknown Title")
                    artists = ", ".join(
                        [
                            a.get("name", "Unknown Artist")
                            for a in track.get("artists", [])
                        ]
                    )
                    filename = f"{artists} - {title}.m4a"
                    dirents.append(self._sanitize_filename(filename))

                    # Add filename to track data for lookups
                    track["filename"] = self._sanitize_filename(filename)
                    processed_tracks.append(track)

                # Cache the processed track list for this album
                self._set_cache(path, processed_tracks)

        except Exception as e:
            self.logger.error(f"Error in readdir for {path}: {e}")
            import traceback

            self.logger.error(traceback.format_exc())

        return dirents

    def getattr(self, path: str, fh: Optional[int] = None) -> Dict[str, Any]:
        """Get file attributes.

        Args:
            path: The file or directory path
            fh: File handle (unused)

        Returns:
            File attributes dictionary
        """
        self.logger.debug(f"getattr: {path}")

        now = time.time()
        attr = {
            "st_atime": now,
            "st_ctime": now,
            "st_mtime": now,
            "st_nlink": 2,
        }

        # Root directory
        if path == "/":
            attr["st_mode"] = stat.S_IFDIR | 0o755
            attr["st_size"] = 0
            return attr

        # Main categories
        if path in ["/playlists", "/liked_songs", "/artists", "/albums"]:
            attr["st_mode"] = stat.S_IFDIR | 0o755
            attr["st_size"] = 0
            return attr

        # Check if this is a song file (ends with .m4a)
        if path.lower().endswith(".m4a"):
            # Check if it's a valid song file by examining its parent directory
            parent_dir = os.path.dirname(path)
            filename = os.path.basename(path)

            try:
                # Get directory listing of parent
                dirlist = self.readdir(parent_dir, None)
                if filename in dirlist:
                    # It's a valid song file
                    attr["st_mode"] = stat.S_IFREG | 0o644
                    attr["st_size"] = 10 * 1024 * 1024  # Default size for audio files
                    return attr
            except Exception as e:
                self.logger.debug(f"Error checking file existence: {e}")
                # Continue to other checks
                pass

        # Check if path is a directory by trying to list it
        try:
            if path.endswith("/"):
                path = path[:-1]

            entries = self.readdir(path, None)
            if entries:
                attr["st_mode"] = stat.S_IFDIR | 0o755
                attr["st_size"] = 0
                return attr
        except Exception:
            pass

        # Check if path is a file
        parent_dir = os.path.dirname(path)
        filename = os.path.basename(path)

        if not filename:
            raise OSError(errno.ENOENT, f"No such file or directory: {path}")

        # Check if file exists in parent directory
        try:
            dirlist = self.readdir(parent_dir, None)
            if filename in dirlist:
                # It's a file
                attr["st_mode"] = stat.S_IFREG | 0o644
                attr["st_size"] = 10 * 1024 * 1024  # Default size for audio files
                return attr
        except Exception:
            pass

        raise OSError(errno.ENOENT, f"No such file or directory: {path}")

    def open(self, path: str, flags: int) -> int:
        """Open a file and return a file handle.

        Args:
            path: The file path
            flags: File open flags

        Returns:
            File handle
        """
        self.logger.debug(f"open: {path} with flags {flags}")
        if path == "/":
            raise OSError(errno.EISDIR, "Is a directory")

        # Extract directory path and filename
        dir_path = os.path.dirname(path)
        filename = os.path.basename(path)
        self.logger.debug(f"Looking for {filename} in {dir_path}")

        # Special handling for liked songs which use a different cache key
        if dir_path == "/liked_songs":
            songs = self._get_from_cache("/liked_songs_processed")
            self.logger.debug(f"Liked songs cache: {'Found' if songs else 'Not found'}")
        else:
            songs = self._get_from_cache(dir_path)
            self.logger.debug(
                f"Cache status for {dir_path}: {'Found' if songs else 'Not found'}"
            )

        if not songs:
            # Re-fetch if not in cache
            self.logger.debug(f"Re-fetching directory {dir_path}")
            self.readdir(dir_path, None)

            # Try to get from cache again after refetching
            if dir_path == "/liked_songs":
                songs = self._get_from_cache("/liked_songs_processed")
                self.logger.debug(
                    f"After re-fetch, liked songs cache: {'Found' if songs else 'Not found'}"
                )
            else:
                songs = self._get_from_cache(dir_path)
                self.logger.debug(
                    f"After re-fetch, songs: {'Found' if songs else 'Not found'}"
                )

        if not songs:
            self.logger.error(f"Could not find songs in directory {dir_path}")
            raise OSError(errno.ENOENT, f"File not found: {path}")

        # Handle songs as either list of strings or list of dictionaries
        video_id = None

        # Check if the cache contains song objects (dictionaries) or just filenames (strings)
        if isinstance(songs, list) and songs and isinstance(songs[0], str):
            # Cache contains only filenames
            self.logger.debug(f"Cache contains string filenames")
            if filename in songs:
                # Matched by filename, but we don't have videoId
                self.logger.error(
                    f"File found in cache but no videoId available: {filename}"
                )
                raise OSError(errno.ENOENT, f"No video ID for file: {path}")
        else:
            # Cache contains song dictionaries as expected
            for song in songs:
                # Check if song is a dictionary before trying to use .get()
                if isinstance(song, dict):
                    self.logger.debug(
                        f"Checking song: {song.get('filename', 'Unknown')} for match with {filename}"
                    )
                    if song.get("filename") == filename:
                        video_id = song.get("videoId")
                        self.logger.debug(f"Found matching song, videoId: {video_id}")
                        break
                else:
                    self.logger.debug(
                        f"Skipping non-dictionary song object: {type(song)}"
                    )

        if not video_id:
            self.logger.error(f"Could not find videoId for {filename}")

            # Special debug to see what's in the cache
            self.logger.debug(
                f"Cache contents for debugging: {songs[:5] if isinstance(songs, list) else songs}"
            )

            raise OSError(errno.ENOENT, f"File not found: {path}")

        # Fetch stream URL using yt-dlp
        try:
            # Use yt-dlp to get the audio stream URL
            cmd = [
                "yt-dlp",
                "-f",
                "bestaudio[ext=m4a]/bestaudio",  # Try different format hierarchy, fallback to best available audio
                "-g",
                f"https://www.youtube.com/watch?v={video_id}",
            ]
            self.logger.debug(f"Running command: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            stream_url = result.stdout.strip()

            if not stream_url:
                self.logger.error("No suitable audio stream found")
                raise OSError(errno.EIO, "No suitable audio stream found")

            self.logger.debug(f"Successfully got stream URL for {video_id}")

            # Store the stream URL and return a file handle
            fh = self.next_fh
            self.next_fh += 1
            self.open_files[fh] = {"stream_url": stream_url}
            self.logger.debug(f"Assigned file handle {fh} to {path}")
            return fh

        except subprocess.SubprocessError as e:
            self.logger.error(f"Error running yt-dlp: {e}")
            raise OSError(errno.EIO, f"Failed to get stream URL: {str(e)}")
        except Exception as e:
            self.logger.error(f"Unexpected error getting stream URL: {e}")
            raise OSError(errno.EIO, f"Failed to get stream URL: {str(e)}")

    def read(self, path: str, size: int, offset: int, fh: int) -> bytes:
        """Read data from a file.

        Args:
            path: The file path
            size: Number of bytes to read
            offset: Offset in bytes
            fh: File handle

        Returns:
            File data as bytes
        """
        self.logger.debug(f"read: {path}, size={size}, offset={offset}, fh={fh}")
        try:
            if fh not in self.open_files:
                self.logger.error(f"Bad file descriptor: {fh} not in open_files")
                raise OSError(errno.EBADF, "Bad file descriptor")

            stream_url = self.open_files[fh]["stream_url"]
            self.logger.debug(f"Using stream URL: {stream_url}")
            headers = {"Range": f"bytes={offset}-{offset + size - 1}"}
            self.logger.debug(f"Requesting range: {headers['Range']}")

            response = requests.get(stream_url, headers=headers, stream=True)
            self.logger.debug(f"Response status: {response.status_code}")

            if response.status_code not in (200, 206):
                self.logger.error(f"Failed to stream: {response.status_code}")
                raise RuntimeError(f"Failed to stream: {response.status_code}")

            content = response.content
            self.logger.debug(f"Got {len(content)} bytes of content")
            return content
        except Exception as e:
            self.logger.error(f"Error in read: {e}")
            raise OSError(errno.EIO, f"Failed to read stream: {str(e)}")

    def release(self, path: str, fh: int) -> int:
        """Release (close) a file handle.

        Args:
            path: The file path
            fh: File handle

        Returns:
            0 on success
        """
        if fh in self.open_files:
            del self.open_files[fh]
        return 0


def mount_ytmusicfs(
    mount_point: str,
    auth_file: str,
    client_id: str,
    client_secret: str,
    foreground: bool = False,
    debug: bool = False,
    cache_dir: Optional[str] = None,
    cache_timeout: int = 300,
) -> None:
    """Mount the YouTube Music filesystem.

    Args:
        mount_point: Directory where the filesystem will be mounted
        auth_file: Path to the OAuth token file
        client_id: OAuth client ID
        client_secret: OAuth client secret
        foreground: Run in the foreground (for debugging)
        debug: Enable debug logging
        cache_dir: Directory to store cache files
        cache_timeout: Cache timeout in seconds
    """
    FUSE(
        YouTubeMusicFS(
            auth_file=auth_file,
            client_id=client_id,
            client_secret=client_secret,
            cache_dir=cache_dir,
            cache_timeout=cache_timeout,
        ),
        mount_point,
        foreground=foreground,
        nothreads=True,
    )
