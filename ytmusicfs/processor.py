#!/usr/bin/env python3

from typing import Dict, Optional, List, Tuple
import logging
import re


class TrackProcessor:
    """Processor for handling track metadata."""

    def __init__(self, logger: Optional[logging.Logger] = None, cache_manager=None):
        """Initialize the track processor.

        Args:
            logger: Optional logger instance. Defaults to a new logger if None.
            cache_manager: Optional cache manager for retrieving cached durations
        """
        self.logger = logger or logging.getLogger("TrackProcessor")
        self.cache_manager = cache_manager

    def sanitize_filename(self, name: str) -> str:
        """Sanitize a string to be used as a filename.

        Args:
            name: The filename to sanitize.

        Returns:
            A sanitized filename with problematic characters replaced or preserved safely.
        """
        # Preserve dots and some special characters, replace only truly invalid ones
        invalid_chars = ["/", "\\", ":", "*", "?", "<", ">", "|"]
        # Replace invalid chars with hyphen, trim leading/trailing spaces and dots
        sanitized = "".join("-" if c in invalid_chars else c for c in name.strip())
        # Remove leading/trailing dots or multiple consecutive hyphens
        sanitized = re.sub(r"^\.+|\.+$", "", sanitized)
        sanitized = re.sub(r"-+", "-", sanitized)
        return sanitized

    def clean_artists(self, raw_artists: List[Dict]) -> str:
        """Format artist names from a list of artist dictionaries.

        Args:
            raw_artists: List of artist dictionaries with 'name' keys.

        Returns:
            A comma-separated string of cleaned artist names.
        """
        artists = [
            self._clean_artist_name(artist.get("name", "Unknown Artist"))
            for artist in raw_artists
        ]
        return ", ".join(artists)

    def _clean_artist_name(self, name: str) -> str:
        """Clean a single artist name by removing '- Topic' suffix.

        Args:
            name: The artist name to clean.

        Returns:
            The cleaned artist name.
        """
        return name[:-8] if name.endswith(" - Topic") else name

    def parse_duration(self, track: Dict) -> Tuple[Optional[int], str]:
        """Parse track duration into seconds and formatted string.

        Args:
            track: Track dictionary with duration info.

        Returns:
            Tuple of (duration in seconds or None, formatted duration as 'mm:ss').
        """
        duration_seconds = track.get("duration_seconds")
        duration_str = track.get("duration", "0:00") if not duration_seconds else None

        if duration_str:
            duration_seconds = self._parse_duration_str(duration_str)

        duration_formatted = self._format_duration(duration_seconds or 0)
        return duration_seconds, duration_formatted

    def _parse_duration_str(self, duration_str: str) -> Optional[int]:
        """Parse a duration string (e.g., 'MM:SS' or 'HH:MM:SS') into seconds.

        Args:
            duration_str: Duration string to parse.

        Returns:
            Duration in seconds or None if parsing fails.
        """
        try:
            parts = [int(p) for p in duration_str.split(":")]
            if len(parts) == 2:
                return parts[0] * 60 + parts[1]
            if len(parts) == 3:
                return parts[0] * 3600 + parts[1] * 60 + parts[2]
        except (ValueError, IndexError):
            return None
        return None

    def _format_duration(self, seconds: int) -> str:
        """Format duration in seconds to 'mm:ss'.

        Args:
            seconds: Duration in seconds.

        Returns:
            Formatted string in 'mm:ss' format.
        """
        minutes, secs = divmod(seconds, 60)
        return f"{minutes}:{secs:02d}"

    def extract_album_info(self, track: Dict) -> Tuple[str, str]:
        """Extract album name and artist from track data.

        Args:
            track: Track dictionary with potential album info.

        Returns:
            Tuple of (album name, album artist).
        """
        album_obj = track.get("album")
        if not album_obj:
            return "Unknown Album", "Unknown Artist"

        if isinstance(album_obj, str):
            return album_obj, "Unknown Artist"

        album_name = album_obj.get("name", "Unknown Album")
        artist = self._extract_album_artist(album_obj)
        return album_name, artist

    def _extract_album_artist(self, album_obj: Dict) -> str:
        """Extract album artist from album object.

        Args:
            album_obj: Album dictionary.

        Returns:
            Cleaned album artist name.
        """
        artist_obj = album_obj.get("artist") or (album_obj.get("artists") or [{}])[0]
        if isinstance(artist_obj, list) and artist_obj:
            artist_obj = artist_obj[0]
        name = (
            artist_obj.get("name") if isinstance(artist_obj, dict) else artist_obj
        ) or "Unknown Artist"
        return self._clean_artist_name(name)

    def extract_year(self, track: Dict) -> Optional[int]:
        """Extract year from track or album data.

        Args:
            track: Track dictionary with potential year info.

        Returns:
            Year as integer or None if not found.
        """
        return track.get("year") or (
            isinstance(track.get("album"), dict) and track["album"].get("year")
        )

    def extract_track_info(self, track: Dict) -> Dict:
        """Extract and format track information from yt-dlp metadata.

        Args:
            track: Raw track dictionary which could be from yt-dlp or ytmusicapi.

        Returns:
            Dictionary with formatted track metadata.
        """
        # Try to get the video ID from the track
        video_id = track.get("videoId")

        # Handle duration
        duration_seconds = None
        duration_formatted = "0:00"
        is_new_duration = (
            False  # Flag to indicate if this is a newly discovered duration
        )

        # First priority: Use track's provided duration_seconds if available (from yt-dlp)
        if "duration_seconds" in track and track["duration_seconds"] is not None:
            duration_seconds = track["duration_seconds"]
            duration_formatted = self._format_duration(duration_seconds)
            is_new_duration = True  # Mark as new duration to be included in batch
        # Second priority: Check cache for duration
        elif video_id and self.cache_manager:
            cached_duration = self.cache_manager.get_duration(video_id)
            if cached_duration is not None:
                self.logger.debug(
                    f"Using cached duration for {video_id}: {cached_duration}s"
                )
                duration_seconds = cached_duration
                duration_formatted = self._format_duration(duration_seconds)
        # Third priority: Parse from track data if not in cache
        else:
            duration_seconds, duration_formatted = self.parse_duration(track)
            if duration_seconds is not None:
                is_new_duration = True  # Mark as new duration to be included in batch

        # Handle artist information
        # yt-dlp flat extraction provides only uploader, not detailed artist info
        if "artist" in track and isinstance(track["artist"], str):
            # Already processed artist string from yt-dlp, but still need to clean potential "- Topic" suffix
            artist = self._clean_artist_name(track["artist"])
        elif "artists" in track and isinstance(track["artists"], list):
            # ytmusicapi provides a list of artist objects
            artist = self.clean_artists(track["artists"])
        else:
            # Fallback - clean uploader name to remove "- Topic" suffix
            artist = self._clean_artist_name(track.get("uploader", "Unknown Artist"))

        # Get album info - try for existing data or use defaults
        if "album" in track and isinstance(track["album"], str):
            album = track["album"]
            album_artist = self._clean_artist_name(track.get("album_artist", artist))
        else:
            # Try to extract from track data or use default
            album, album_artist = self.extract_album_info(track)

        # Handle year - might not be available in yt-dlp flat extraction
        if "year" in track and track["year"] is not None:
            year = track["year"]
        else:
            year = self.extract_year(track)

        return {
            "title": track.get("title", "Unknown Title"),
            "artist": artist,
            "album": album,
            "album_artist": album_artist,
            "duration_seconds": duration_seconds,
            "duration_formatted": duration_formatted,
            "track_number": track.get("trackNumber", track.get("index", 0)),
            "year": year,
            "genre": track.get("genre", "Unknown Genre"),
            "videoId": video_id,  # Include the video ID in the track info for reference
            "is_new_duration": is_new_duration,  # Flag for batch processing
        }

    def process_tracks(
        self, tracks: List[Dict], add_filename: bool = True
    ) -> List[Dict]:
        """Process track data into a consistent format with filenames.

        Args:
            tracks: List of raw track dictionaries.
            add_filename: Whether to include filenames in processed tracks.

        Returns:
            List of processed track dictionaries with metadata and filenames.
        """
        processed = []
        # Collect durations for batch processing
        durations_batch = {}

        for track in tracks:
            # Extract track info
            track_info = self.extract_track_info(track)

            # Collect newly discovered durations for batch processing
            if (
                track_info.get("is_new_duration")
                and track_info.get("videoId")
                and track_info.get("duration_seconds") is not None
            ):
                durations_batch[track_info["videoId"]] = track_info["duration_seconds"]

            # Remove the temporary flag
            if "is_new_duration" in track_info:
                del track_info["is_new_duration"]

            # Generate filename
            filename = self.sanitize_filename(
                f"{track_info['artist']} - {track_info['title']}.m4a"
            )

            if add_filename:
                processed_track = dict(track)
                processed_track.update(track_info)
                processed_track["filename"] = filename
                processed.append(processed_track)

        # Batch update all durations at once
        if durations_batch and self.cache_manager:
            self.logger.debug(f"Batch updating {len(durations_batch)} track durations")
            self.cache_manager.set_durations_batch(durations_batch)

        return processed
