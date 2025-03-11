#!/usr/bin/env python3

from typing import Dict, Optional, List, Tuple
import logging


class TrackProcessor:
    """Processor for handling track metadata."""

    def __init__(self, logger: Optional[logging.Logger] = None):
        """Initialize the track processor.

        Args:
            logger: Logger instance to use
        """
        self.logger = logger or logging.getLogger("TrackProcessor")

    def sanitize_filename(self, name: str) -> str:
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

    def clean_artists(self, raw_artists: List[Dict]) -> str:
        """Clean and format artist names from a list of artist dictionaries.

        Args:
            raw_artists: List of artist dictionaries with 'name' keys.

        Returns:
            A comma-separated string of cleaned artist names.
        """
        clean_artists = []
        for artist in raw_artists:
            name = artist.get("name", "Unknown Artist")
            # Remove "- Topic" suffix from artist names
            if name.endswith(" - Topic"):
                name = name[:-8]  # Remove "- Topic" (8 characters)
            clean_artists.append(name)
        return ", ".join(clean_artists)

    def parse_duration(self, track: Dict) -> Tuple[Optional[int], str]:
        """Parse track duration into seconds and formatted string.

        Args:
            track: Track dictionary that may contain duration information

        Returns:
            Tuple of (duration_seconds, duration_formatted)
        """
        duration_seconds = None
        duration_formatted = "0:00"

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

        # Format duration as mm:ss for display
        if duration_seconds:
            minutes = duration_seconds // 60
            seconds = duration_seconds % 60
            duration_formatted = f"{minutes}:{seconds:02d}"

        return duration_seconds, duration_formatted

    def extract_album_info(self, track: Dict) -> Tuple[str, str]:
        """Extract album name and album artist from track data.

        Args:
            track: Track dictionary that may contain album information

        Returns:
            Tuple of (album_name, album_artist)
        """
        album = "Unknown Album"
        album_artist = "Unknown Artist"

        # Handle album which could be None, a string, or a dictionary
        album_obj = track.get("album")
        if album_obj is not None:
            if isinstance(album_obj, dict):
                album = album_obj.get("name", "Unknown Album")
                # Handle album artist - could be direct 'artist' or in 'artists' list
                album_artist_obj = album_obj.get("artist")
                if album_artist_obj is not None:
                    if isinstance(album_artist_obj, list) and album_artist_obj:
                        album_artist_name = album_artist_obj[0].get(
                            "name", "Unknown Artist"
                        )
                        # Remove "- Topic" from album artist
                        if album_artist_name.endswith(" - Topic"):
                            album_artist = album_artist_name[:-8]
                        else:
                            album_artist = album_artist_name
                    elif isinstance(album_artist_obj, str):
                        # Remove "- Topic" from album artist if it's a string
                        if album_artist_obj.endswith(" - Topic"):
                            album_artist = album_artist_obj[:-8]
                        else:
                            album_artist = album_artist_obj
                # Try the 'artists' field if 'artist' wasn't found
                elif "artists" in album_obj and album_obj["artists"]:
                    artists_obj = album_obj["artists"]
                    if artists_obj and isinstance(artists_obj[0], dict):
                        album_artist_name = artists_obj[0].get("name", "Unknown Artist")
                        # Remove "- Topic" from album artist
                        if album_artist_name.endswith(" - Topic"):
                            album_artist = album_artist_name[:-8]
                        else:
                            album_artist = album_artist_name
            elif isinstance(album_obj, str):
                album = album_obj

        return album, album_artist

    def extract_year(self, track: Dict) -> Optional[int]:
        """Extract the year from track or album data.

        Args:
            track: Track dictionary that may contain year information

        Returns:
            Year as int or None if not available
        """
        if "year" in track:
            return track.get("year")
        elif (
            track.get("album")
            and isinstance(track.get("album"), dict)
            and "year" in track.get("album")
        ):
            return track.get("album").get("year")
        return None

    def extract_track_info(self, track: Dict) -> Dict:
        """Extract and format track information.

        Args:
            track: Track dictionary containing raw track data

        Returns:
            Dictionary with extracted and formatted track information
        """
        title = track.get("title", "Unknown Title")

        # Process artists
        raw_artists = track.get("artists", [])
        artists = self.clean_artists(raw_artists)

        # Get album information
        album, album_artist = self.extract_album_info(track)

        # Extract song duration
        duration_seconds, duration_formatted = self.parse_duration(track)

        # Extract additional metadata
        track_number = track.get("trackNumber", track.get("index", 0))
        year = self.extract_year(track)

        # Extract genre information if available
        genre = track.get("genre", "Unknown Genre")

        return {
            "title": title,
            "artist": artists,  # Flattened artist string for metadata
            "album": album,
            "album_artist": album_artist,
            "duration_seconds": duration_seconds,
            "duration_formatted": duration_formatted,
            "track_number": track_number,
            "year": year,
            "genre": genre,
        }

    def process_tracks(
        self, tracks: List[Dict], add_filename: bool = True
    ) -> Tuple[List[Dict], List[str]]:
        """Process track data into a consistent format with filenames.

        Args:
            tracks: List of track dictionaries
            add_filename: Whether to add filename to the tracks

        Returns:
            List of processed tracks with filenames and a list of just the filenames
        """
        processed = []
        filenames = []

        for track in tracks:
            # Extract all track information
            track_info = self.extract_track_info(track)

            # Create filename from artist and title
            filename = f"{track_info['artist']} - {track_info['title']}.m4a"
            sanitized_filename = self.sanitize_filename(filename)
            filenames.append(sanitized_filename)

            if add_filename:
                # Create a shallow copy of the track and add track info
                processed_track = dict(track)
                processed_track.update(track_info)
                processed_track["filename"] = sanitized_filename
                processed.append(processed_track)

        return processed, filenames
