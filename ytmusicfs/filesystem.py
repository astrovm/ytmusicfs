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
import re

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

        try:
            # Dispatch to the appropriate handler based on path
            if path == "/":
                return dirents + ["playlists", "liked_songs", "artists", "albums"]
            elif path == "/playlists":
                return dirents + self._readdir_playlists()
            elif path == "/liked_songs":
                return dirents + self._readdir_liked_songs()
            elif path == "/artists":
                return dirents + self._readdir_artists()
            elif path == "/albums":
                return dirents + self._readdir_albums()
            elif path.startswith("/playlists/"):
                return dirents + self._readdir_playlist_content(path)
            elif path.startswith("/artists/") and path.count("/") == 2:
                return dirents + self._readdir_artist_content(path)
            elif path.startswith("/albums/") or (
                path.startswith("/artists/") and path.count("/") >= 3
            ):
                return dirents + self._readdir_album_content(path)
        except Exception as e:
            self.logger.error(f"Error in readdir for {path}: {e}")
            import traceback

            self.logger.error(traceback.format_exc())

        return dirents

    def _fetch_and_cache(self, cache_key: str, fetch_func, *args, **kwargs) -> Any:
        """Fetch data from cache or API and update cache if needed.

        Args:
            cache_key: The key to use for caching
            fetch_func: The function to call to fetch data
            *args, **kwargs: Arguments to pass to fetch_func

        Returns:
            The fetched or cached data
        """
        data = self._get_from_cache(cache_key)
        if not data:
            data = fetch_func(*args, **kwargs)
            self._set_cache(cache_key, data)
        return data

    def _process_tracks(
        self, tracks: List[Dict], add_filename: bool = True
    ) -> List[Dict]:
        """Process track data into a consistent format with filenames.

        Args:
            tracks: List of track dictionaries
            add_filename: Whether to add filename to the tracks

        Returns:
            List of processed tracks with filenames
        """
        processed = []
        filenames = []

        for track in tracks:
            title = track.get("title", "Unknown Title")
            artists = ", ".join(
                [a.get("name", "Unknown Artist") for a in track.get("artists", [])]
            )
            # Get album information when available
            album = "Unknown Album"
            album_artist = "Unknown Artist"

            # Handle album which could be None, a string, or a dictionary
            album_obj = track.get("album")
            if album_obj is not None:
                if isinstance(album_obj, dict):
                    album = album_obj.get("name", "Unknown Album")
                    album_artist_obj = album_obj.get("artist")
                    if album_artist_obj is not None:
                        if isinstance(album_artist_obj, list) and album_artist_obj:
                            album_artist = album_artist_obj[0].get(
                                "name", "Unknown Artist"
                            )
                        elif isinstance(album_artist_obj, str):
                            album_artist = album_artist_obj
                elif isinstance(album_obj, str):
                    album = album_obj

            # Extract song duration if available
            duration_seconds = None
            if "duration" in track:
                duration_str = track.get("duration", "0:00")
                try:
                    # Convert MM:SS to seconds
                    parts = duration_str.split(":")
                    if len(parts) == 2:
                        duration_seconds = int(parts[0]) * 60 + int(parts[1])
                    elif len(parts) == 3:
                        duration_seconds = (
                            int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
                        )
                except (ValueError, IndexError):
                    pass
            elif "duration_seconds" in track:
                duration_seconds = track.get("duration_seconds")

            # Extract additional metadata
            track_number = track.get("trackNumber", track.get("index", 0))
            year = None
            if "year" in track:
                year = track.get("year")
            elif album_obj and isinstance(album_obj, dict) and "year" in album_obj:
                year = album_obj.get("year")

            # Extract genre information if available
            genre = "Unknown Genre"
            if "genre" in track:
                genre = track.get("genre")

            # Format duration as mm:ss for display
            duration_formatted = "0:00"
            if duration_seconds:
                minutes = duration_seconds // 60
                seconds = duration_seconds % 60
                duration_formatted = f"{minutes}:{seconds:02d}"

            filename = f"{artists} - {title}.m4a"
            sanitized_filename = self._sanitize_filename(filename)
            filenames.append(sanitized_filename)

            if add_filename:
                # Create a shallow copy of the track and add filename and metadata
                processed_track = dict(track)
                processed_track["filename"] = sanitized_filename
                processed_track["artist"] = (
                    artists  # Flattened artist string for metadata
                )
                processed_track["album"] = album
                processed_track["album_artist"] = album_artist
                processed_track["duration_seconds"] = duration_seconds
                processed_track["duration_formatted"] = duration_formatted
                processed_track["track_number"] = track_number
                processed_track["year"] = year
                processed_track["genre"] = genre
                processed.append(processed_track)

        return filenames if not add_filename else processed

    def _readdir_playlists(self) -> List[str]:
        """Handle listing playlist directories.

        Returns:
            List of playlist names
        """
        playlists = self._fetch_and_cache(
            "/playlists", self.ytmusic.get_library_playlists, limit=100
        )

        return [self._sanitize_filename(playlist["title"]) for playlist in playlists]

    def _readdir_liked_songs(self) -> List[str]:
        """Handle listing liked songs.

        Returns:
            List of liked song filenames
        """
        liked_songs = self._fetch_and_cache(
            "/liked_songs", self.ytmusic.get_liked_songs
        )

        # Process the raw liked songs data
        self.logger.debug(f"Processing raw liked songs data: {type(liked_songs)}")

        # Process tracks and create filenames
        processed_tracks = []
        filenames = []

        for song in liked_songs["tracks"]:
            title = song.get("title", "Unknown Title")
            artists = ", ".join(
                [a.get("name", "Unknown Artist") for a in song.get("artists", [])]
            )
            # Get album information when available
            album = "Unknown Album"
            album_artist = "Unknown Artist"

            # Handle album which could be None, a string, or a dictionary
            album_obj = song.get("album")
            if album_obj is not None:
                if isinstance(album_obj, dict):
                    album = album_obj.get("name", "Unknown Album")
                    album_artist_obj = album_obj.get("artist")
                    if album_artist_obj is not None:
                        if isinstance(album_artist_obj, list) and album_artist_obj:
                            album_artist = album_artist_obj[0].get(
                                "name", "Unknown Artist"
                            )
                        elif isinstance(album_artist_obj, str):
                            album_artist = album_artist_obj
                elif isinstance(album_obj, str):
                    album = album_obj

            # Extract song duration if available
            duration_seconds = None
            if "duration" in song:
                duration_str = song.get("duration", "0:00")
                try:
                    # Convert MM:SS to seconds
                    parts = duration_str.split(":")
                    if len(parts) == 2:
                        duration_seconds = int(parts[0]) * 60 + int(parts[1])
                    elif len(parts) == 3:
                        duration_seconds = (
                            int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
                        )
                except (ValueError, IndexError):
                    pass

            # Extract additional metadata
            track_number = song.get("trackNumber", song.get("index", 0))
            year = None
            if "year" in song:
                year = song.get("year")
            elif album_obj and isinstance(album_obj, dict) and "year" in album_obj:
                year = album_obj.get("year")

            # Extract genre information if available
            genre = "Unknown Genre"
            if "genre" in song:
                genre = song.get("genre")

            # Format duration as mm:ss for display
            duration_formatted = "0:00"
            if duration_seconds:
                minutes = duration_seconds // 60
                seconds = duration_seconds % 60
                duration_formatted = f"{minutes}:{seconds:02d}"

            filename = f"{artists} - {title}.m4a"
            sanitized_filename = self._sanitize_filename(filename)
            filenames.append(sanitized_filename)

            # Create a processed song with the necessary data
            processed_song = {
                "title": title,
                "artists": song.get("artists", []),
                "artist": artists,  # Flattened artist string for metadata
                "album": album,
                "album_artist": album_artist,
                "videoId": song.get("videoId"),
                "filename": sanitized_filename,
                "duration_seconds": duration_seconds,
                "duration_formatted": duration_formatted,
                "track_number": track_number,
                "year": year,
                "genre": genre,
                "originalData": song,  # Keep the original data for reference
            }
            processed_tracks.append(processed_song)

        # Cache the processed song list with filename mappings
        self.logger.debug(
            f"Caching {len(processed_tracks)} processed tracks for /liked_songs"
        )
        self._set_cache("/liked_songs_processed", processed_tracks)

        return filenames

    def _readdir_playlist_content(self, path: str) -> List[str]:
        """Handle listing contents of a specific playlist.

        Args:
            path: Playlist path

        Returns:
            List of track filenames in the playlist
        """
        playlist_name = path.split("/")[2]

        # Find the playlist ID
        playlists = self._fetch_and_cache(
            "/playlists", self.ytmusic.get_library_playlists, limit=100
        )

        playlist_id = None
        for playlist in playlists:
            if self._sanitize_filename(playlist["title"]) == playlist_name:
                playlist_id = playlist["playlistId"]
                break

        if not playlist_id:
            self.logger.error(f"Could not find playlist ID for {playlist_name}")
            return []

        # Get the playlist tracks
        playlist_cache_key = f"/playlist/{playlist_id}"
        playlist_tracks = self._fetch_and_cache(
            playlist_cache_key,
            lambda: self.ytmusic.get_playlist(playlist_id, limit=500).get("tracks", []),
        )

        # Process tracks and create filenames
        processed_tracks = []
        filenames = []

        for track in playlist_tracks:
            title = track.get("title", "Unknown Title")
            artists = ", ".join(
                [a.get("name", "Unknown Artist") for a in track.get("artists", [])]
            )

            # Get album information when available
            album = "Unknown Album"
            album_artist = "Unknown Artist"

            # Handle album which could be None, a string, or a dictionary
            album_obj = track.get("album")
            if album_obj is not None:
                if isinstance(album_obj, dict):
                    album = album_obj.get("name", "Unknown Album")
                    # Handle album artist
                    if "artists" in album_obj and album_obj["artists"]:
                        artists_obj = album_obj["artists"]
                        if artists_obj and isinstance(artists_obj[0], dict):
                            album_artist = artists_obj[0].get("name", "Unknown Artist")
                elif isinstance(album_obj, str):
                    album = album_obj

            # Extract song duration if available
            duration_seconds = None
            if "duration" in track:
                duration_str = track.get("duration", "0:00")
                try:
                    # Convert MM:SS to seconds
                    parts = duration_str.split(":")
                    if len(parts) == 2:
                        duration_seconds = int(parts[0]) * 60 + int(parts[1])
                    elif len(parts) == 3:
                        duration_seconds = (
                            int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
                        )
                except (ValueError, IndexError):
                    pass

            # Extract additional metadata
            track_number = track.get("trackNumber", track.get("index", 0))
            year = None
            if "year" in track:
                year = track.get("year")
            elif album_obj and isinstance(album_obj, dict) and "year" in album_obj:
                year = album_obj.get("year")

            # Extract genre information if available
            genre = "Unknown Genre"
            if "genre" in track:
                genre = track.get("genre")

            # Format duration as mm:ss for display
            duration_formatted = "0:00"
            if duration_seconds:
                minutes = duration_seconds // 60
                seconds = duration_seconds % 60
                duration_formatted = f"{minutes}:{seconds:02d}"

            filename = f"{artists} - {title}.m4a"
            sanitized_filename = self._sanitize_filename(filename)
            filenames.append(sanitized_filename)

            # Add enhanced metadata to track data for lookups
            track_copy = dict(track)
            track_copy["filename"] = sanitized_filename
            track_copy["artist"] = artists  # Flattened artist string for metadata
            track_copy["album"] = album
            track_copy["album_artist"] = album_artist
            track_copy["duration_seconds"] = duration_seconds
            track_copy["duration_formatted"] = duration_formatted
            track_copy["track_number"] = track_number
            track_copy["year"] = year
            track_copy["genre"] = genre
            processed_tracks.append(track_copy)

        # Cache the processed track list for this playlist
        self._set_cache(path, processed_tracks)

        return filenames

    def _readdir_artists(self) -> List[str]:
        """Handle listing artist directories.

        Returns:
            List of artist names
        """
        artists = self._fetch_and_cache(
            "/artists", self.ytmusic.get_library_artists, limit=100
        )

        return [self._sanitize_filename(artist["artist"]) for artist in artists]

    def _readdir_albums(self) -> List[str]:
        """Handle listing album directories.

        Returns:
            List of album names
        """
        albums = self._fetch_and_cache(
            "/albums", self.ytmusic.get_library_albums, limit=100
        )

        return [self._sanitize_filename(album["title"]) for album in albums]

    def _readdir_artist_content(self, path: str) -> List[str]:
        """Handle listing contents of a specific artist directory.

        Args:
            path: Artist path

        Returns:
            List of album names by the artist
        """
        artist_name = path.split("/")[2]

        # Find the artist ID
        artists = self._fetch_and_cache(
            "/artists", self.ytmusic.get_library_artists, limit=100
        )

        artist_id = None
        for artist in artists:
            if self._sanitize_filename(artist["artist"]) == artist_name:
                # Safely access ID fields with fallbacks
                artist_id = artist.get("artistId")
                if not artist_id:
                    artist_id = artist.get("browseId")
                if not artist_id:
                    artist_id = artist.get("id")
                break

        if not artist_id:
            self.logger.error(f"Could not find artist ID for {artist_name}")
            return []

        # Get the artist's albums and singles
        artist_cache_key = f"/artist/{artist_id}"

        def fetch_artist_albums():
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

            return artist_albums

        artist_albums = self._fetch_and_cache(artist_cache_key, fetch_artist_albums)

        # Return the albums
        return [self._sanitize_filename(item["title"]) for item in artist_albums]

    def _readdir_album_content(self, path: str) -> List[str]:
        """Handle listing contents of a specific album.

        Args:
            path: Album path

        Returns:
            List of track filenames in the album
        """
        # Determine if this is an artist's album or a library album
        is_artist_album = path.startswith("/artists/")
        album_id = None
        album_title = None  # Store the album title for metadata

        if is_artist_album:
            parts = path.split("/")
            artist_name = parts[2]
            album_name = parts[3]

            # Find the artist ID
            artists = self._fetch_and_cache(
                "/artists", self.ytmusic.get_library_artists, limit=100
            )

            artist_id = None
            for artist in artists:
                if self._sanitize_filename(artist["artist"]) == artist_name:
                    # Safely access ID fields with fallbacks
                    artist_id = artist.get("artistId")
                    if not artist_id:
                        artist_id = artist.get("browseId")
                    if not artist_id:
                        artist_id = artist.get("id")
                    break

            if not artist_id:
                self.logger.error(f"Could not find artist ID for {artist_name}")
                return []

            # Get the artist's albums
            artist_cache_key = f"/artist/{artist_id}"

            def fetch_artist_albums():
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

                return artist_albums

            artist_albums = self._fetch_and_cache(artist_cache_key, fetch_artist_albums)

            # Find the album ID
            for album in artist_albums:
                if self._sanitize_filename(album["title"]) == album_name:
                    album_id = album["browseId"]
                    album_title = album["title"]
                    break
        else:
            # Regular album path
            album_name = path.split("/")[2]

            # Find the album ID
            albums = self._fetch_and_cache(
                "/albums", self.ytmusic.get_library_albums, limit=100
            )

            for album in albums:
                if self._sanitize_filename(album["title"]) == album_name:
                    album_id = album["browseId"]
                    album_title = album["title"]
                    break

        if not album_id:
            self.logger.error(f"Could not find album ID for album in path: {path}")
            return []

        # Get the album tracks
        album_cache_key = f"/album/{album_id}"

        def fetch_album_tracks():
            album_data = self.ytmusic.get_album(album_id)
            return album_data.get("tracks", [])

        album_tracks = self._fetch_and_cache(album_cache_key, fetch_album_tracks)

        # Process tracks and create filenames
        processed_tracks = []
        filenames = []

        # Extract album artist from the first track if available
        album_artist = "Unknown Artist"
        if album_tracks and "artists" in album_tracks[0]:
            artists_data = album_tracks[0].get("artists", [])
            if artists_data and isinstance(artists_data[0], dict):
                album_artist = artists_data[0].get("name", "Unknown Artist")

        for track in album_tracks:
            title = track.get("title", "Unknown Title")
            artists = ", ".join(
                [a.get("name", "Unknown Artist") for a in track.get("artists", [])]
            )

            # Extract song duration if available
            duration_seconds = None
            if "duration" in track:
                duration_str = track.get("duration", "0:00")
                try:
                    # Convert MM:SS to seconds
                    parts = duration_str.split(":")
                    if len(parts) == 2:
                        duration_seconds = int(parts[0]) * 60 + int(parts[1])
                    elif len(parts) == 3:
                        duration_seconds = (
                            int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
                        )
                except (ValueError, IndexError):
                    pass

            # Determine track number from the track's index in the album if available
            track_number = track.get("trackNumber")
            if not track_number and "index" in track:
                track_number = track.get("index")

            # Use album year if available (already fetched when getting the album)
            year = None
            # Try to find the year for this album
            for album in artist_albums:
                if self._sanitize_filename(album["title"]) == album_name:
                    year = album.get("year")
                    break

            # Extract genre information if available
            genre = "Unknown Genre"
            if "genre" in track:
                genre = track.get("genre")

            # Format duration as mm:ss for display
            duration_formatted = "0:00"
            if duration_seconds:
                minutes = duration_seconds // 60
                seconds = duration_seconds % 60
                duration_formatted = f"{minutes}:{seconds:02d}"

            filename = f"{artists} - {title}.m4a"
            sanitized_filename = self._sanitize_filename(filename)
            filenames.append(sanitized_filename)

            # Add enhanced metadata to track data for lookups
            track_copy = dict(track)
            track_copy["filename"] = sanitized_filename
            track_copy["artist"] = artists  # Flattened artist string for metadata
            track_copy["album"] = album_title or "Unknown Album"
            track_copy["album_artist"] = album_artist
            track_copy["duration_seconds"] = duration_seconds
            track_copy["duration_formatted"] = duration_formatted
            track_copy["track_number"] = track_number
            track_copy["year"] = year
            track_copy["genre"] = genre
            processed_tracks.append(track_copy)

        # Cache the processed track list for this album
        self._set_cache(path, processed_tracks)

        return filenames

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

            # Check if we have a cached file size for this path
            file_size_cache_key = f"filesize:{path}"
            cached_size = self._get_from_cache(file_size_cache_key)

            if cached_size is not None:
                self.logger.debug(f"Using cached file size for {path}: {cached_size}")
                attr["st_mode"] = stat.S_IFREG | 0o644
                attr["st_size"] = cached_size
                return attr

            try:
                # Get directory listing of parent
                dirlist = self.readdir(parent_dir, None)
                if filename in dirlist:
                    # It's a valid song file
                    attr["st_mode"] = stat.S_IFREG | 0o644

                    # Get a more accurate file size estimate based on song duration if available
                    songs = self._get_from_cache(parent_dir)
                    if (
                        songs
                        and isinstance(songs, list)
                        and songs
                        and isinstance(songs[0], dict)
                    ):
                        for song in songs:
                            if song.get("filename") == filename:
                                # If we have duration information, use it to estimate file size
                                # Average bit rate for M4A: ~192kbps = 24KB/s
                                if "duration_seconds" in song:
                                    estimated_size = int(
                                        song["duration_seconds"] * 24 * 1024
                                    )
                                    self.logger.debug(
                                        f"Estimated size from duration: {estimated_size}"
                                    )
                                    # Cache this size estimate
                                    self._set_cache(file_size_cache_key, estimated_size)
                                    attr["st_size"] = estimated_size
                                    return attr

                    # Default size if no duration available
                    # For YouTube Music, assume average song length is about 4 minutes = 240 seconds
                    # 240 seconds * 24 KB/s ~= 5.76 MB
                    estimated_size = 6 * 1024 * 1024
                    attr["st_size"] = estimated_size
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

                # Same file size estimation logic as above
                file_size_cache_key = f"filesize:{path}"
                cached_size = self._get_from_cache(file_size_cache_key)

                if cached_size is not None:
                    attr["st_size"] = cached_size
                else:
                    attr["st_size"] = 6 * 1024 * 1024  # Improved default estimate

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

    def _update_file_size(self, path: str, size: int) -> None:
        """Update the file size cache for a given path.

        Args:
            path: The file path
            size: The actual file size in bytes
        """
        file_size_cache_key = f"filesize:{path}"
        self._set_cache(file_size_cache_key, size)
        self.logger.debug(f"Updated file size cache for {path}: {size} bytes")

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

            # Try with specific range first
            headers = {"Range": f"bytes={offset}-{offset + size - 1}"}
            self.logger.debug(f"Requesting range: {headers['Range']}")

            response = requests.get(stream_url, headers=headers, stream=True)
            self.logger.debug(f"Response status: {response.status_code}")

            # If we get a 416 Range Not Satisfiable error, try alternative approaches
            if response.status_code == 416:
                self.logger.debug("Got 416 error, trying alternative range request")

                # Option 1: Try open-ended range (just specifying start position)
                headers = {"Range": f"bytes={offset}-"}
                self.logger.debug(f"Trying open-ended range: {headers['Range']}")
                response = requests.get(stream_url, headers=headers, stream=True)

                # Option 2: If that still fails, try getting whatever data is available
                if response.status_code == 416:
                    self.logger.debug(
                        "Open-ended range also failed, requesting from beginning"
                    )
                    # Try to get content from the beginning and handle offset manually
                    headers = {}  # No range header
                    response = requests.get(stream_url, stream=True)

                    # If we get data but need to apply offset manually
                    if response.status_code == 200:
                        content = response.content

                        # Update the file size cache with the actual size
                        self._update_file_size(path, len(content))

                        if offset < len(content):
                            # Apply offset and size limit manually
                            return content[offset : offset + size]
                        else:
                            # Offset beyond file size
                            self.logger.debug(
                                f"Offset {offset} is beyond actual file size {len(content)}"
                            )
                            return b""

            # For normal successful responses (200 or 206)
            if response.status_code in (200, 206):
                content = response.content

                # If we've received the whole file, update the file size cache
                if response.status_code == 200:
                    self._update_file_size(path, len(content))
                # If we have a Content-Range header, we can extract the total file size
                elif "Content-Range" in response.headers:
                    content_range = response.headers.get("Content-Range", "")
                    match = re.search(r"bytes \d+-\d+/(\d+)", content_range)
                    if match:
                        total_size = int(match.group(1))
                        self._update_file_size(path, total_size)

                self.logger.debug(f"Got {len(content)} bytes of content")
                return content

            # If we get here, all attempts failed
            self.logger.error(f"Failed to stream: {response.status_code}")
            raise RuntimeError(f"Failed to stream: {response.status_code}")

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

    def getxattr(self, path: str, name: str, position: int = 0) -> bytes:
        """Get extended attribute value.

        Args:
            path: The file path
            name: The attribute name
            position: The attribute position (unused)

        Returns:
            Attribute value
        """
        self.logger.debug(f"getxattr: {path}, {name}")

        # Skip if this is not a music file
        if not path.lower().endswith(".m4a"):
            raise OSError(errno.ENOATTR, "No such attribute")

        # Get file metadata based on path
        parent_dir = os.path.dirname(path)
        filename = os.path.basename(path)

        # Skip if not in a music directory
        if parent_dir == "/":
            raise OSError(errno.ENOATTR, "No such attribute")

        # Get metadata from cache
        cache_key = parent_dir
        if parent_dir == "/liked_songs":
            cache_key = "/liked_songs_processed"

        songs = self._get_from_cache(cache_key)
        if not songs:
            raise OSError(errno.ENOATTR, "No such attribute")

        # Find the song in the cached data
        song = None
        for s in songs:
            if isinstance(s, dict) and s.get("filename") == filename:
                song = s
                break

        if not song:
            raise OSError(errno.ENOATTR, "No such attribute")

        # Map the xattr name to the appropriate song field
        # Common xattr namespaces for media metadata:
        # - user.xdg.tags (freedesktop)
        # - user.dublincore (Dublin Core)
        # - user.metadata (generic)

        xattr_map = {
            # XDG/Freedesktop attributes
            "user.xdg.tags.title": "title",
            "user.xdg.tags.artist": "artist",
            "user.xdg.tags.album": "album",
            "user.xdg.tags.album_artist": "album_artist",
            "user.xdg.tags.track": "track_number",
            "user.xdg.tags.genre": "genre",
            "user.xdg.tags.date": "year",
            "user.xdg.tags.duration": "duration_formatted",
            # Dublin Core attributes
            "user.dublincore.title": "title",
            "user.dublincore.creator": "artist",
            "user.dublincore.publisher": "album_artist",
            "user.dublincore.date": "year",
            "user.dublincore.format.duration": "duration_formatted",
            # Generic metadata
            "user.metadata.title": "title",
            "user.metadata.artist": "artist",
            "user.metadata.album": "album",
            "user.metadata.album_artist": "album_artist",
            "user.metadata.track_number": "track_number",
            "user.metadata.year": "year",
            "user.metadata.date": "year",
            "user.metadata.genre": "genre",
            "user.metadata.duration": "duration_formatted",
            "user.metadata.length": "duration_seconds",
            # Audacious specific
            "user.metadata.audacious.title": "title",
            "user.metadata.audacious.artist": "artist",
            "user.metadata.audacious.album": "album",
            "user.metadata.audacious.track_number": "track_number",
            "user.metadata.audacious.year": "year",
            "user.metadata.audacious.genre": "genre",
            "user.metadata.audacious.length": "duration_seconds",
        }

        # Map the attribute name to the song field
        field = xattr_map.get(name)
        if not field or field not in song:
            raise OSError(errno.ENOATTR, "No such attribute")

        # Return the attribute value as bytes
        value = song[field]
        if isinstance(value, list):
            if all(isinstance(item, dict) for item in value):
                value = ", ".join([item.get("name", "") for item in value])
            else:
                value = ", ".join(value)

        return str(value).encode("utf-8")

    def listxattr(self, path: str) -> List[str]:
        """List extended attributes available for the path.

        Args:
            path: The file path

        Returns:
            List of attribute names
        """
        self.logger.debug(f"listxattr: {path}")

        # Skip if this is not a music file
        if not path.lower().endswith(".m4a"):
            return []

        # Get file metadata based on path
        parent_dir = os.path.dirname(path)
        filename = os.path.basename(path)

        # Skip if not in a music directory
        if parent_dir == "/":
            return []

        # Get metadata from cache
        cache_key = parent_dir
        if parent_dir == "/liked_songs":
            cache_key = "/liked_songs_processed"

        songs = self._get_from_cache(cache_key)
        if not songs:
            return []

        # Find the song in the cached data
        song = None
        for s in songs:
            if isinstance(s, dict) and s.get("filename") == filename:
                song = s
                break

        if not song:
            return []

        # Return the available attributes for this file
        attributes = []

        # Check which metadata fields are available
        metadata_map = {
            "title": [
                "user.xdg.tags.title",
                "user.dublincore.title",
                "user.metadata.title",
                "user.metadata.audacious.title",
            ],
            "artist": [
                "user.xdg.tags.artist",
                "user.dublincore.creator",
                "user.metadata.artist",
                "user.metadata.audacious.artist",
            ],
            "album": [
                "user.xdg.tags.album",
                "user.metadata.album",
                "user.metadata.audacious.album",
            ],
            "album_artist": [
                "user.xdg.tags.album_artist",
                "user.dublincore.publisher",
                "user.metadata.album_artist",
            ],
            "track_number": [
                "user.xdg.tags.track",
                "user.metadata.track_number",
                "user.metadata.audacious.track_number",
            ],
            "year": [
                "user.xdg.tags.date",
                "user.dublincore.date",
                "user.metadata.year",
                "user.metadata.date",
                "user.metadata.audacious.year",
            ],
            "genre": [
                "user.xdg.tags.genre",
                "user.metadata.genre",
                "user.metadata.audacious.genre",
            ],
            "duration_seconds": [
                "user.metadata.length",
                "user.metadata.audacious.length",
            ],
            "duration_formatted": [
                "user.xdg.tags.duration",
                "user.dublincore.format.duration",
                "user.metadata.duration",
            ],
        }

        # Add available attributes based on song data
        for field, attrs in metadata_map.items():
            if field in song and song[field]:
                attributes.extend(attrs)

        return attributes


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
