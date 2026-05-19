# YTMusicFS - YouTube Music FUSE Filesystem

YTMusicFS mounts your YouTube Music library as a standard filesystem, allowing you to browse and play your music with any traditional audio player.

![Audacious with Winamp skin](https://github.com/user-attachments/assets/f148ef9e-90b1-4eca-86fd-02973209ff88)

## Features

- **Filesystem Interface**: Access your YouTube Music library through a standard filesystem
- **Traditional Player Support**: Play songs with any audio player that can read files
- **Complete Library Access**: Browse playlists, liked songs, and albums
- **Persistent Authentication**: Uses your local browser cookies
- **Disk Caching**: Caches metadata and audio to improve browsing performance and enable offline playback of previously streamed songs
- **On-Demand Streaming**: Streams audio directly from YouTube Music servers
- **Smart Auto-Refresh**: Automatically refreshes your library cache every hour using an intelligent merging approach that preserves existing data and only updates what has changed
- **Browser Cookies**: Uses browser cookies for authentication to access higher quality audio streams (up to 256kbps) and private playlists

## Requirements

- Python 3.10+
- FUSE (Filesystem in Userspace)
- YouTube Music account
- An authenticated YouTube Music browser session
- A browser supported by yt-dlp for cookies, such as Brave, Chrome, or Firefox
- `pipx` for isolated CLI installation

## Installation

### System Dependencies

On Debian/Ubuntu, install required system dependencies:

```bash
sudo apt install fuse libfuse-dev python3-dev pipx
pipx ensurepath
```

On Fedora/RHEL:

```bash
sudo dnf install fuse fuse-devel python3-devel pipx
pipx ensurepath
```

On Arch Linux:

```bash
sudo pacman -S fuse2 python python-pipx
pipx ensurepath
```

Restart your shell after `pipx ensurepath` if `ytmusicfs` is not found.

High quality YouTube Music extraction needs one supported JavaScript runtime on
the `ytmusicfs` process `PATH`: `node`, `bun`, `deno`, or `quickjs`. Use your
preferred install method; it does not need to come from the system package
manager.

### Install YTMusicFS

Install YTMusicFS as an isolated command-line app:

```bash
git clone https://github.com/astrovm/ytmusicfs
cd ytmusicfs
pipx install .
```

Upgrade after pulling new changes:

```bash
git pull
pipx install --force .
```

## Authentication Setup

YTMusicFS reads cookies from your browser when you mount with `--browser`.
Log in to YouTube Music in that browser before mounting.

## Usage

### Command Structure

YTMusicFS uses a command-based structure with the following format:

```bash
ytmusicfs <command> [options]
```

Available commands:

- `mount`: Mount YouTube Music as a filesystem

### Mount the Filesystem

Create a mount point and mount with browser cookies. This is the normal way to
run YTMusicFS because it enables high quality streams and private library
access.

```bash
mkdir -p ~/Music/ytmusic
ytmusicfs mount --mount-point ~/Music/ytmusic --browser brave
```

`--browser brave` tells `yt-dlp` which local browser profile to read cookies
from. Replace `brave` with your browser if needed. Supported browsers include
`brave`, `chrome`, `firefox`, and others supported by yt-dlp.

For debugging or custom paths:

```bash
ytmusicfs mount \
  --mount-point ~/Music/ytmusic \
  --browser brave \
  --cache-dir ~/.cache/ytmusicfs \
  --foreground \
  --debug
```

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
usage: ytmusicfs mount [-h] --mount-point MOUNT_POINT [--cache-dir CACHE_DIR]
                       [--foreground] [--debug] --browser BROWSER

Mount YouTube Music as a filesystem

Options:
  -h, --help            Show this help message and exit
  --mount-point, -m MOUNT_POINT
                        Directory where the filesystem will be mounted
  --cache-dir, -c CACHE_DIR
                        Directory to store cache files
                        (default: ~/.cache/ytmusicfs)
  --foreground, -f      Run in the foreground (for debugging)
  --debug, -d           Enable debug logging
  --browser, -b BROWSER Browser to use for cookies (e.g., 'chrome', 'firefox', 'brave')
```

## Limitations

- Stream URLs from YouTube Music expire after some time
- Seeking may not be perfectly smooth in all players
- Metadata like album art may be limited depending on your player

## Troubleshooting

### Authentication Issues

- Log in to https://music.youtube.com in the browser passed to `--browser`.
- Keep that browser installed and available to `yt-dlp`.

### Playback Issues

- Refresh the local install after pulling changes: `pipx install --force .`
- Some players may not handle streaming URLs well; try different players
- If audio stops, the stream URL may have expired; simply restart playback

### Performance Issues

- Keep the cache directory on a fast disk for quicker metadata lookups
- Reduce network calls by browsing directories fully before playing
