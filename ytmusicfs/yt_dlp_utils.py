#!/usr/bin/env python3

from yt_dlp import YoutubeDL
import logging
import re

logger = logging.getLogger("YTDLPUtils")


def extract_playlist_content(playlist_id, limit=10000, browser=None):
    """
    Fetch playlist or album content using yt-dlp, handling YouTube Music's redirects.

    Args:
        playlist_id (str): Playlist ID (e.g., 'PL123', 'LM') or Album ID (e.g., 'MPREb_123').
        limit (int): Maximum number of tracks to fetch (default: 10000).
        browser (str, optional): Browser name for cookie extraction.

    Returns:
        list: List of track entries from yt-dlp.
    """
    # Determine if this is an album ID
    is_album = playlist_id.startswith("MPREb_")
    url = None

    if is_album:
        # For albums, start with browse URL
        url = f"https://music.youtube.com/browse/{playlist_id}"
        logger.debug(f"Handling album ID: {playlist_id}")

        # Try to follow any redirects to get OLAK5uy playlist ID
        try:
            redirect_opts = {
                "quiet": True,
                "skip_download": True,
                "extract_flat": True,
                "ignoreerrors": True,
            }
            if browser:
                redirect_opts["cookiesfrombrowser"] = (browser,)

            with YoutubeDL(redirect_opts) as ydl:
                info = ydl.extract_info(url, download=False, process=False)

                # Check if we got a redirect
                if info and info.get("_type") == "url" and "url" in info:
                    redirect_url = info["url"]
                    logger.debug(f"Album redirect detected: {redirect_url}")

                    # Extract OLAK5uy ID from redirect URL
                    match = re.search(r"list=(OLAK5uy_[a-zA-Z0-9_-]+)", redirect_url)
                    if match:
                        olak5_id = match.group(1)
                        url = f"https://music.youtube.com/playlist?list={olak5_id}"
                        logger.debug(f"Using playlist URL with OLAK5uy ID: {olak5_id}")
        except Exception as e:
            logger.warning(f"Error detecting album redirect: {e}")

    # If URL wasn't set by album logic or there was an error, use default playlist URL
    if not url:
        url = f"https://music.youtube.com/playlist?list={playlist_id}"

    # Common extraction options
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

    try:
        with YoutubeDL(ydl_opts) as ydl:
            result = ydl.extract_info(url, download=False)
            if not result or "entries" not in result:
                logger.warning(
                    f"No tracks found for {'album' if is_album else 'playlist'} ID: {playlist_id}"
                )
                return []

            tracks = result.get("entries", [])[:limit]
            logger.debug(
                f"Found {len(tracks)} tracks from {'album' if is_album else 'playlist'}"
            )
            return tracks
    except Exception as e:
        logger.warning(f"Error extracting content: {e}")
        return []


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
