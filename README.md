# YTMusicFS - YouTube Music FUSE Filesystem

YTMusicFS mounts your YouTube Music library as a standard filesystem, allowing you to browse and play your music with any traditional audio player.

![Audacious with Winamp skin](https://github.com/user-attachments/assets/f148ef9e-90b1-4eca-86fd-02973209ff88)

## Features

- **Filesystem Interface**: Access your YouTube Music library through a standard filesystem
- **Traditional Player Support**: Play songs with any audio player that can read files
- **Complete Library Access**: Browse playlists, liked songs, and albums
- **Persistent Authentication**: Uses your browser session headers for long-lasting access
- **Disk Caching**: Caches metadata and audio to improve browsing performance and enable offline playback of previously streamed songs
- **On-Demand Streaming**: Streams audio directly from YouTube Music servers
- **Smart Auto-Refresh**: Automatically refreshes your library cache every hour using an intelligent merging approach that preserves existing data and only updates what has changed
- **Browser Cookies**: Uses browser cookies for authentication to access higher quality audio streams (up to 256kbps) and private playlists

## Requirements

- Python 3.10+
- FUSE (Filesystem in Userspace)
- YouTube Music account
- Ability to copy request headers from an authenticated YouTube Music browser session
- yt-dlp (for audio streaming)

## Installation

### From Source

```bash
git clone https://github.com/astrovm/ytmusicfs
cd ytmusicfs
pip install -e .
```

### Dependencies

On Debian/Ubuntu, install required system dependencies:

```bash
sudo apt install libfuse-dev python3-dev
```

On Fedora/RHEL:

```bash
sudo dnf install fuse fuse-devel python3-devel
```

On Arch Linux:

```bash
sudo pacman -S fuse2 python
```

## Authentication Setup

YTMusicFS reuses the same browser headers that the official YouTube Music site
uses. Grab the headers once and they typically remain valid for up to two
years.

1. Sign in to [https://music.youtube.com](https://music.youtube.com) in your
   preferred browser.
2. Open the browser developer tools and switch to the **Network** tab.
3. Reload the page, then filter for requests containing `/browse` and select any
   authenticated `POST` entry.
4. Copy the entire **Request headers** section starting from `accept: */*`.
5. Run the helper to store those headers for YTMusicFS:

   ```bash
   ytmusicfs browser
   ```

   Paste the copied headers when prompted, or supply them via
   `--headers-file` to automate the step. The command writes
   `browser.json` (by default to `~/.config/ytmusicfs/browser.json`).

## Usage

### Command Structure

YTMusicFS uses a command-based structure with the following format:

```bash
ytmusicfs <command> [options]
```

Available commands:

- `mount`: Mount YouTube Music as a filesystem
- `browser`: Parse browser headers for authentication

### Mount the Filesystem

Create a mount point (if it doesn't exist) and mount the filesystem:

```bash
mkdir -p ~/Music/ytmusic
ytmusicfs mount --mount-point ~/Music/ytmusic
```

Or with custom options:

```bash
ytmusicfs mount \
  --mount-point ~/Music/ytmusic \
  --auth-file /path/to/browser.json \
  --cache-dir ~/.cache/ytmusicfs \
  --foreground \
  --debug
```

You can also specify a browser for cookie retrieval:

```bash
ytmusicfs mount --mount-point ~/Music/ytmusic --browser brave
```

Supported browsers include: chrome, firefox, brave, and others supported by yt-dlp.

The `--browser` option allows YouTube Premium subscribers to access higher quality audio streams (up to 256kbps) and private playlists by using your browser's cookies for authentication. Without this option, audio will stream at standard quality, even for Premium subscribers, and private playlists will not be accessible.

### Browse and Play Music

Once mounted, you can browse the filesystem with your file manager:

```bash
ls ~/Music/ytmusic
ls ~/Music/ytmusic/playlists
ls ~/Music/ytmusic/liked_songs
```

Play music with any audio player:

```bash
audacious ~/Music/ytmusic/playlists/MyFavorites/Song.m4a
mpv ~/Music/ytmusic/liked_songs/Artist\ -\ Song.m4a
```

### Unmount

When you're done, unmount the filesystem:

```bash
fusermount -u ~/Music/ytmusic
```

## Filesystem Structure

- `/playlists/` - Your YouTube Music playlists
- `/liked_songs/` - Your liked songs
- `/albums/` - Albums in your library

## Command Line Options

### ytmusicfs mount

```
usage: ytmusicfs mount [-h] --mount-point MOUNT_POINT [--auth-file AUTH_FILE]
                       [--cache-dir CACHE_DIR] [--foreground] [--debug]
                       [--browser BROWSER]

Mount YouTube Music as a filesystem

Options:
  -h, --help            Show this help message and exit
  --mount-point, -m MOUNT_POINT
                        Directory where the filesystem will be mounted
  --auth-file, -a AUTH_FILE
                        Path to the browser authentication file
                        (default: ~/.config/ytmusicfs/browser.json)
  --cache-dir, -c CACHE_DIR
                        Directory to store cache files
                        (default: ~/.cache/ytmusicfs)
  --foreground, -f      Run in the foreground (for debugging)
  --debug, -d           Enable debug logging
  --browser, -b BROWSER Browser to use for cookies (e.g., 'chrome', 'firefox', 'brave')
```

### ytmusicfs browser

```
usage: ytmusicfs browser [-h] [--auth-file AUTH_FILE] [--headers-file HEADERS_FILE]
                         [--debug]

Set up browser authentication for YTMusicFS

Options:
  -h, --help            Show this help message and exit
  --auth-file, -a AUTH_FILE
                        Output path for the generated browser header JSON
  --headers-file HEADERS_FILE
                        Read raw request headers from this file
  --debug, -d           Enable debug logging
```

## Limitations

- Stream URLs from YouTube Music expire after some time
- Seeking may not be perfectly smooth in all players
- Metadata like album art may be limited depending on your player

## Troubleshooting

### Authentication Issues

- Verify that `browser.json` still matches an active browser session (log in to
  https://music.youtube.com and regenerate headers if needed).
- Re-run `ytmusicfs browser` to refresh the stored headers when your browser
  session changes or expires.
- Confirm that the authentication file path passed to `ytmusicfs mount`
  matches the location printed by the setup command.

### Playback Issues

- Make sure yt-dlp is installed and up-to-date
- Some players may not handle streaming URLs well; try different players
- If audio stops, the stream URL may have expired; simply restart playback

### Performance Issues

- Keep the cache directory on a fast disk for quicker metadata lookups
- Reduce network calls by browsing directories fully before playing
