# YTMusicFS - YouTube Music FUSE Filesystem

YTMusicFS mounts your YouTube Music library as a standard filesystem, allowing you to browse and play your music with any traditional audio player.

![Audacious with Winamp skin](https://github.com/user-attachments/assets/f148ef9e-90b1-4eca-86fd-02973209ff88)

## Features

- **Filesystem Interface**: Access your YouTube Music library through a standard filesystem
- **Traditional Player Support**: Play songs with any audio player that can read files
- **Complete Library Access**: Browse playlists, liked songs, artists, and albums
- **Persistent Authentication**: Uses OAuth for reliable, long-lasting sessions
- **Disk Caching**: Caches metadata to improve browsing performance
- **On-Demand Streaming**: Streams audio directly from YouTube Music servers
- **Auto-Refresh**: Automatically refreshes your liked songs cache hourly to show new additions
- **Browser Cookies**: Uses browser cookies for authentication to access higher quality audio streams (up to 256kbps)

## Requirements

- Python 3.9+
- FUSE (Filesystem in Userspace)
- YouTube Music account
- Google Cloud Console account (for OAuth credentials)
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
sudo apt-get install libfuse-dev python3-dev
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

YTMusicFS uses OAuth for authentication, which provides better reliability and longer session lifetimes.

### Step 1: Get OAuth Credentials

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Enable the YouTube Data API v3:
   - Go to "APIs & Services" > "Library"
   - Search for "YouTube Data API v3"
   - Click "Enable"
4. Create OAuth credentials:
   - Go to "APIs & Services" > "Credentials"
   - Click "Create Credentials" > "OAuth client ID"
   - Application type: "TV and Limited Input devices"
   - Name: "YTMusicFS" (or any name you prefer)
   - Note your Client ID and Client Secret

### Step 2: Generate OAuth Token

Run the setup utility with your OAuth credentials:

```bash
ytmusicfs-oauth --client-id YOUR_CLIENT_ID --client-secret YOUR_CLIENT_SECRET
```

This will:

1. Open a browser window for you to authorize the application
2. Generate and store an OAuth token in `~/.config/ytmusicfs/oauth.json`

## Usage

### Mount the Filesystem

Create a mount point (if it doesn't exist) and mount the filesystem:

```bash
mkdir -p ~/Music/ytmusic
ytmusicfs --mount-point ~/Music/ytmusic
```

Or with custom options:

```bash
ytmusicfs \
  --mount-point ~/Music/ytmusic \
  --auth-file /path/to/oauth.json \
  --cache-timeout 600 \
  --foreground \
  --debug
```

You can also specify a browser for cookie retrieval:

```bash
ytmusicfs --mount-point ~/Music/ytmusic --browser brave
```

Supported browsers include: chrome, firefox, brave, edge, safari, opera, and others supported by yt-dlp.

The `--browser` option allows YouTube Premium subscribers to access higher quality audio streams (up to 256kbps) by using your browser's cookies for authentication. Without this option, audio will stream at standard quality, even for Premium subscribers.

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
- `/artists/` - Artists in your library
- `/albums/` - Albums in your library

## Command Line Options

### ytmusicfs

```
usage: ytmusicfs [-h] --mount-point MOUNT_POINT [--auth-file AUTH_FILE]
                 [--client-id CLIENT_ID] [--client-secret CLIENT_SECRET]
                 [--cache-dir CACHE_DIR] [--cache-timeout CACHE_TIMEOUT]
                 [--foreground] [--debug] [--browser BROWSER] [--version]

Mount YouTube Music as a filesystem

Options:
  -h, --help            Show this help message and exit
  --mount-point, -m MOUNT_POINT
                        Directory where the filesystem will be mounted
  --version, -v         Show version and exit

Authentication Options:
  --auth-file, -a AUTH_FILE
                        Path to the OAuth token file
                        (default: ~/.config/ytmusicfs/oauth.json)
  --client-id, -i CLIENT_ID
                        OAuth client ID (required for OAuth authentication)
  --client-secret, -s CLIENT_SECRET
                        OAuth client secret (required for OAuth authentication)

Cache Options:
  --cache-dir, -c CACHE_DIR
                        Directory to store cache files
                        (default: ~/.cache/ytmusicfs)
  --cache-timeout, -t CACHE_TIMEOUT
                        Cache timeout in seconds (default: 300)

Operational Options:
  --foreground, -f      Run in the foreground (for debugging)
  --debug, -d           Enable debug logging
  --browser, -b BROWSER Browser to use for cookies (e.g., 'chrome', 'firefox', 'brave').
                        If not specified, no browser cookies will be used
```

### ytmusicfs-oauth

```
usage: ytmusicfs-oauth [-h] [--client-id CLIENT_ID]
                       [--client-secret CLIENT_SECRET] [--output OUTPUT]
                       [--open-browser] [--no-open-browser] [--debug]

Set up OAuth authentication for YTMusicFS

Options:
  -h, --help            Show this help message and exit
  --client-id, -i CLIENT_ID
                        OAuth Client ID from Google Cloud Console
  --client-secret, -s CLIENT_SECRET
                        OAuth Client Secret from Google Cloud Console
  --output, -o OUTPUT   Output file for the OAuth token
                        (default: ~/.config/ytmusicfs/oauth.json)
  --open-browser, -b    Automatically open the browser for authentication
  --no-open-browser     Do not automatically open the browser for authentication
  --debug, -d           Enable debug output
```

## Limitations

- Stream URLs from YouTube Music expire after some time
- Seeking may not be perfectly smooth in all players
- Metadata like album art may be limited depending on your player

## Troubleshooting

### Authentication Issues

- Ensure your OAuth token is valid and refresh tokens are working
- Try regenerating your token with `ytmusicfs-oauth`
- Check if your Google Cloud project has YouTube Data API enabled

### Playback Issues

- Make sure yt-dlp is installed and up-to-date
- Some players may not handle streaming URLs well; try different players
- If audio stops, the stream URL may have expired; simply restart playback

### Performance Issues

- Increase cache timeout with `--cache-timeout` for better performance
- Reduce network calls by browsing directories fully before playing
