# YTMusicFS

**Your YouTube Music library as a folder.**

YTMusicFS is a YouTube Music FUSE filesystem. It mounts your library as a standard filesystem, so you can browse and play your music with any traditional audio player.

![Audacious with Winamp skin](https://github.com/user-attachments/assets/f148ef9e-90b1-4eca-86fd-02973209ff88)

## Requirements

- Python 3.14+
- FUSE (Filesystem in Userspace)
- YouTube Music account
- An authenticated YouTube Music browser session
- A browser supported by yt-dlp for cookies, such as Brave, Chrome, or Firefox
- `pipx` for isolated CLI installation
- For high quality extraction, one supported JavaScript runtime (see below)

## ⬇️ Install

**1. Install system dependencies:**

| Distro | Command |
| --- | --- |
| Debian/Ubuntu | `sudo apt install fuse libfuse-dev python3-dev pipx` |
| Fedora/RHEL | `sudo dnf install fuse fuse-devel python3-devel pipx` |
| Arch Linux | `sudo pacman -S fuse2 python python-pipx` |

Then run:

```bash
pipx ensurepath
```

Restart your shell after `pipx ensurepath` if `ytmusicfs` is not found.

**2. Add a JavaScript runtime.**
High quality YouTube Music extraction needs one supported JavaScript runtime on the `ytmusicfs` process `PATH`: `node`, `bun`, `deno`, or `quickjs`.
Use your preferred install method; it does not need to come from the system package manager.

**3. Install YTMusicFS** as an isolated command-line app:

```bash
git clone https://github.com/astrovm/ytmusicfs
cd ytmusicfs
pipx install .
```

To upgrade after pulling new changes:

```bash
git pull
pipx install --force .
```

## 🚀 Use

### Log in

YTMusicFS reads cookies from your browser when you mount with `--browser`.
Log in to YouTube Music in that browser before mounting.

### Mount

Create a mount point and mount with browser cookies.
This is the normal way to run YTMusicFS because it enables high quality streams and private library access.

```bash
mkdir -p ~/Music/ytmusic
ytmusicfs mount --mount-point ~/Music/ytmusic --browser brave
```

YTMusicFS remembers the last mount point and browser. After the first successful mount, you can just use:

```bash
ytmusicfs mount
```

- **`--browser brave`** tells `yt-dlp` which local browser profile to read cookies from.
- **Other browsers.** Replace `brave` with yours. Supported browsers include `brave`, `chrome`, `firefox`, and others supported by yt-dlp.

For debugging or custom paths:

```bash
ytmusicfs mount \
  --mount-point ~/Music/ytmusic \
  --browser brave \
  --cache-dir ~/.cache/ytmusicfs \
  --foreground \
  --debug
```

### Browse and play

Browse with your file manager or the terminal:

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

| Folder | What's in it |
| --- | --- |
| `/playlists/` | Your YouTube Music playlists |
| `/liked_songs/` | Your liked songs |
| `/albums/` | Albums in your library |
| `/.ytmusicfs/status.json` | Lightweight mount status for debugging, including current refresh state |

### Unmount

```bash
ytmusicfs unmount                               # the active mount
ytmusicfs unmount --mount-point ~/Music/ytmusic # a specific path
```

## Commands

Every command looks like `ytmusicfs <command> [options]`.

| Command | What it does |
| --- | --- |
| `mount` | Mount YouTube Music as a filesystem |
| `unmount` | Unmount the active YouTube Music filesystem |
| `status` | Show saved settings and active mount state |
| `doctor` | Check local dependencies: the FUSE helper, Python FUSE module, JavaScript runtime, and cache directory permissions |
| `config` | Show or update saved mount settings |
| `cache` | Inspect or clear the persistent cache |
| `repair` | Replace unavailable liked-song IDs with playable matches |
| `logs` | Show recent log lines (default last 50) |
| `service` | Manage an optional systemd user service |

### Status and config

```bash
ytmusicfs status
ytmusicfs config show
ytmusicfs doctor
```

Set saved defaults without mounting:

```bash
ytmusicfs config set mount-point ~/Music/ytmusic
ytmusicfs config set browser brave
```

### Cache

```bash
ytmusicfs cache stats
ytmusicfs cache clear
ytmusicfs cache refresh
```

- **`cache clear`** removes metadata and cached audio.
- **`cache refresh`** removes metadata only and keeps cached audio.
- **Works while mounted.** The mount process detects and applies the change automatically within a few seconds.

### Repair dead liked songs

When `/liked_songs` contains a dead backing video ID, YTMusicFS tries to repair the local cached path automatically by searching for a verified playable replacement.
That automatic repair does not change your YouTube Music account.

To also fix the liked state in your account, run:

```bash
ytmusicfs repair
```

- **Works while mounted.** The mount process detects and applies the change automatically within a few seconds without needing a remount.
- **Only unavailable tracks.** It only handles tracks already marked unavailable in `/liked_songs`.
- **Careful matching.** For each one, it searches YouTube Music by artist and title and verifies the replacement can stream in the preferred high quality format.
- **Asks first.** It prints the exact account changes before asking for confirmation.
- **What it changes.** If confirmed, it likes the replacement video, removes the like from the unavailable video, and updates the local cache.
- **Safe on failure.** It skips weak matches and reports failed API calls instead of changing your account.

### Logs

```bash
ytmusicfs logs           # last 50 lines
ytmusicfs logs --tail 20 # last 20 lines
ytmusicfs logs --path    # print log file path
```

### Systemd user service

Install and manage an optional user service.
It uses the saved mount settings, so run one successful `ytmusicfs mount --mount-point ... --browser ...` first.

```bash
ytmusicfs service install
ytmusicfs service start
ytmusicfs service stop
ytmusicfs service status
```

<details>
<summary><b>Full command line options</b></summary>

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

### Other commands

```
ytmusicfs status
ytmusicfs doctor
ytmusicfs config show
ytmusicfs config set {browser,mount-point} VALUE
ytmusicfs cache stats
ytmusicfs cache clear
ytmusicfs cache refresh [--cache-dir CACHE_DIR] [--debug]
ytmusicfs repair [--cache-dir CACHE_DIR] [--debug]
ytmusicfs logs [--tail N] [--path] [--debug]
ytmusicfs service {install,start,stop,restart,status} [--debug]
```

</details>

## Features

- **Filesystem interface.** Access your YouTube Music library through a standard filesystem.
- **Traditional player support.** Play songs with any audio player that can read files.
- **Complete library access.** Browse playlists, liked songs, and albums.
- **Persistent authentication.** Uses your local browser cookies.
- **Better audio and private playlists.** Browser cookies give access to higher quality audio streams (up to 256kbps) and private playlists.
- **Disk caching.** Caches metadata and audio to improve browsing performance and enable offline playback of previously streamed songs.
- **On-demand streaming.** Streams audio directly from YouTube Music servers.
- **Smart auto-refresh.** Refreshes your library cache every hour by merging: it keeps existing data and only updates what has changed.

## How mounting and refresh work

- **Instant library.** On mount, YTMusicFS shows your saved library right away.
- **Background refresh.** It refreshes the playlist and album lists from YouTube Music in the background, so new playlists appear a few seconds after mounting.
- **First mount waits.** With nothing saved yet, the first mount waits for that fetch.
- **Liked songs later.** The expensive liked-song refresh runs later in a delayed background worker that waits for the mounted filesystem to be idle, so file managers do not wait on YouTube Music.
- **Check progress.** The `/.ytmusicfs/status.json` file shows current refresh state.
- **No Premium, faster starts.** When an account does not receive the highest quality stream (format 141, which needs YouTube Music Premium), YTMusicFS retries extraction for a few tracks and then stops retrying, so songs start faster on accounts without Premium.

## Limitations

- **Links expire.** Stream URLs from YouTube Music expire after some time.
- **Estimated sizes.** File sizes are estimated from track duration until a song is first streamed or cached; reads still end at the real end of the audio.
- **Seeking.** May not be perfectly smooth in all players.
- **Metadata.** Things like album art may be limited depending on your player.

## 🛠️ Troubleshooting

**Authentication issues.**

- Log in to https://music.youtube.com in the browser passed to `--browser`.
- Keep that browser installed and available to `yt-dlp`.

**Playback issues.**

- Refresh the local install after pulling changes: `pipx install --force .`
- Some players may not handle streaming URLs well; try different players.
- If audio stops, the stream URL may have expired; simply restart playback.
- If a liked song exists in YouTube Music but ytmusicfs reports `No such file or directory`, its cached video ID may be unavailable. Browse `/liked_songs` again to let YTMusicFS repair the local cache automatically. Run `ytmusicfs repair` only when you also want to update your account likes.

**Audacious is slow with big folders.**

- Enable `Settings` > `Advanced` > `Do not load metadata for songs until played` before adding large directories such as `/liked_songs`. Otherwise Audacious may probe many uncached audio files while building the playlist, which is much slower than loading cached paths and streaming songs when they are played.
- Also disable `Settings` > `Song Info` > `Show popup information` so Audacious does not request extra song details while browsing.

**Performance issues.**

- Keep the cache directory on a fast disk for quicker metadata lookups.
- Reduce network calls by browsing directories fully before playing.

<details>
<summary><b>Development</b></summary>

Development and CI use Python 3.14.7, pinned in `.python-version`.
Create a virtual environment with that interpreter and install the development tools:

```bash
python3.14 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
black --check .
ruff check .
mypy ytmusicfs
pytest -q --cov
python -m build
python -m benchmarks.benchmark_hot_paths
```

Tests live in `tests/test_<module>.py`, one file per module, with classes named
`Test<ClassUnderTest>` and methods named `test_<subject>_<expected_behavior>`.
Warnings fail the run, and coverage must stay at or above 95%.
`tests/test_end_to_end.py` mounts the filesystem through FUSE against a local
fake of YouTube; it is skipped when `/dev/fuse` or `fusermount` is unavailable.

</details>
