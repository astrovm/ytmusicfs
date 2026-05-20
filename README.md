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
- `unmount`: Unmount the active YouTube Music filesystem
- `status`: Show saved settings and active mount state
- `doctor`: Check local dependencies
- `config`: Show or update saved mount settings
- `cache`: Inspect or clear the persistent cache
- `refresh`: Clear cache so the next browse fetches fresh data
- `logs`: Show recent log lines (default last 50)
- `service`: Manage an optional systemd user service

### Mount the Filesystem

Create a mount point and mount with browser cookies. This is the normal way to
run YTMusicFS because it enables high quality streams and private library
access.

```bash
mkdir -p ~/Music/ytmusic
ytmusicfs mount --mount-point ~/Music/ytmusic --browser brave
```

YTMusicFS remembers the last mount point and browser. After the first successful
mount, you can use:

```bash
ytmusicfs mount
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
ytmusicfs unmount
```

To unmount a specific path:

```bash
ytmusicfs unmount --mount-point ~/Music/ytmusic
```

### Status and Config

Inspect the current setup:

```bash
ytmusicfs status
ytmusicfs config show
ytmusicfs doctor
```

`doctor` checks the local FUSE helper, Python FUSE module, JavaScript runtime,
and cache directory permissions.

Set saved defaults without mounting:

```bash
ytmusicfs config set mount-point ~/Music/ytmusic
ytmusicfs config set browser brave
```

### Cache, Refresh, and Logs

Inspect or clear the local metadata and audio cache:

```bash
ytmusicfs cache stats
ytmusicfs cache clear
ytmusicfs refresh
```

`cache clear` and `refresh` refuse to run while YTMusicFS is mounted.

Show logs:

```bash
ytmusicfs logs           # last 50 lines
ytmusicfs logs --tail 20 # last 20 lines
ytmusicfs logs --path    # print log file path
```

### Systemd User Service

Install and manage an optional user service. It uses the saved mount settings,
so run one successful `ytmusicfs mount --mount-point ... --browser ...` first.

```bash
ytmusicfs service install
ytmusicfs service start
ytmusicfs service stop
ytmusicfs service status
```

## Filesystem Structure

- `/playlists/` - Your YouTube Music playlists
- `/liked_songs/` - Your liked songs
- `/albums/` - Albums in your library
- `/.ytmusicfs/status.json` - Lightweight mount status for debugging

## Command Line Options

### ytmusicfs mount

```
usage: ytmusicfs mount [-h] [--mount-point MOUNT_POINT] [--cache-dir CACHE_DIR]
                       [--foreground] [--debug] [--browser BROWSER]

Mount YouTube Music as a filesystem

Options:
  -h, --help            Show this help message and exit
  --mount-point, -m MOUNT_POINT
                        Directory where the filesystem will be mounted
  --cache-dir, -c CACHE_DIR
                        Cache directory
  --foreground, -f      Run in foreground
  --debug, -d           Enable debug logging
  --browser, -b BROWSER Browser to use for cookies (e.g., 'chrome', 'firefox', 'brave')
```

### ytmusicfs unmount

```
usage: ytmusicfs unmount [-h] [--mount-point MOUNT_POINT] [--cache-dir CACHE_DIR]
                         [--debug]

Unmount YouTube Music filesystem

Options:
  -h, --help            Show this help message and exit
  --mount-point, -m MOUNT_POINT
                        Mount point directory. Defaults to the active ytmusicfs mount.
  --cache-dir, -c CACHE_DIR
                        Cache directory
  --debug, -d           Enable debug logging
```

### Other Commands

```
ytmusicfs status
ytmusicfs doctor
ytmusicfs config show
ytmusicfs config set {browser,mount-point} VALUE
ytmusicfs cache stats
ytmusicfs cache clear
ytmusicfs refresh [--cache-dir CACHE_DIR] [--debug]
ytmusicfs logs [--tail N] [--path] [--debug]
ytmusicfs service {install,start,stop,restart,status} [--debug]
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
