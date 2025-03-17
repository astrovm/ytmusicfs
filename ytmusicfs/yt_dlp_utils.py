#!/usr/bin/env python3

from yt_dlp import YoutubeDL
import logging

logger = logging.getLogger("YTDLPUtils")


def extract_playlist_content(playlist_id, limit=10000, browser=None):
    """
    Fetch playlist content using yt-dlp.

    Args:
        playlist_id (str): Playlist ID (e.g., 'PL123', 'LM').
        limit (int): Maximum number of tracks to fetch (default: 10000).
        browser (str, optional): Browser name for cookie extraction.

    Returns:
        list: List of track entries from yt-dlp.
    """
    playlist_url = f"https://music.youtube.com/playlist?list={playlist_id}"
    ydl_opts = {
        "extract_flat": True,
        "quiet": True,
        "no_warnings": True,
        "ignoreerrors": True,
        "playlistend": limit,
        "extractor_args": {"youtubetab": {"skip": ["authcheck"]}},
    }
    if browser:
        ydl_opts["cookiesfrombrowser"] = (browser,)

    with YoutubeDL(ydl_opts) as ydl:
        result = ydl.extract_info(playlist_url, download=False)
        if not result or "entries" not in result:
            logger.warning(f"No tracks found for playlist ID: {playlist_id}")
            return []
        return result["entries"][:limit]


def extract_stream_url(video_id, browser=None):
    """
    Extract stream URL and duration for a video using yt-dlp.

    Args:
        video_id (str): YouTube video ID.
        browser (str, optional): Browser name for cookie extraction.

    Returns:
        dict: Dictionary with stream URL and duration (if available).
    """
    url = f"https://music.youtube.com/watch?v={video_id}"
    ydl_opts = {
        "format": "141/bestaudio[ext=m4a]",
        "extractor_args": {"youtube": {"formats": ["missing_pot"]}},
    }
    if browser:
        ydl_opts["cookiesfrombrowser"] = (browser,)

    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)
        result = {"stream_url": info["url"]}

        # Extract and include duration if available
        if "duration" in info and info["duration"] is not None:
            try:
                result["duration"] = int(info["duration"])
            except (ValueError, TypeError):
                pass

        return result


def extract_stream_url_process(video_id, browser, queue):
    """
    Extract stream URL in a separate process and put result in queue.
    Used for background processing in file handler.

    Args:
        video_id (str): YouTube video ID.
        browser (str, optional): Browser name for cookie extraction.
        queue (multiprocessing.Queue): Queue to put result in.
    """
    try:
        result = extract_stream_url(video_id, browser)
        queue.put({"status": "success", **result})
    except Exception as e:
        queue.put({"status": "error", "error": str(e)})
