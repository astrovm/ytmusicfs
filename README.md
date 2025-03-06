# YTMusicFS - YouTube Music FUSE Filesystem

YTMusicFS is a FUSE filesystem that integrates YouTube Music into your Linux environment, allowing traditional music players like Audacious to access and play your songs and playlists as if they were local files.

## Features

- Access your YouTube Music library through a standard filesystem interface
- Browse and play your playlists, liked songs, artists, and albums
- Compatible with any music player that can read files from the filesystem
- Streams audio on-demand from YouTube Music servers
- Caches metadata to improve performance
- Uses OAuth authentication for reliable, long-lasting sessions

## Requirements

- Python 3.6+
- FUSE (Filesystem in Userspace)
- YouTube Music account
- Google Cloud Console account

## Installation

1. Clone this repository:

   ```
   git clone https://github.com/yourusername/ytmusicfs.git
   cd ytmusicfs
   ```

2. Install the required dependencies:

   ```
   pip install -r requirements.txt
   ```

## OAuth Authentication Setup

Setting up OAuth authentication requires a few steps but provides a more reliable and longer-lasting authentication method.

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
5. Note down your Client ID and Client Secret

### Step 2: Generate OAuth Token

Run the `setup_oauth.py` script to generate an OAuth token:

```
python setup_oauth.py --client-id YOUR_CLIENT_ID --client-secret YOUR_CLIENT_SECRET --output oauth.json
```

This will:

1. Open a browser window for you to authorize the application
2. Generate an OAuth token file (`oauth.json`)

## Usage

1. Create a mount point:

   ```
   sudo mkdir -p /mnt/ytmusic
   sudo chown $USER:$USER /mnt/ytmusic
   ```

2. Mount the filesystem:

   ```
   python ytmusicfs.py --auth-file oauth.json --client-id YOUR_CLIENT_ID --client-secret YOUR_CLIENT_SECRET --mount-point /mnt/ytmusic
   ```

   For debugging, add the `--foreground` and `--debug` flags:

   ```
   python ytmusicfs.py --auth-file oauth.json --client-id YOUR_CLIENT_ID --client-secret YOUR_CLIENT_SECRET --mount-point /mnt/ytmusic --foreground --debug
   ```

3. Browse your music:

   ```
   ls /mnt/ytmusic
   ls /mnt/ytmusic/playlists
   ls /mnt/ytmusic/liked_songs
   ```

4. Play music with your favorite player:

   ```
   audacious /mnt/ytmusic/playlists/MyFavorites/Song.m4a
   ```

5. To unmount:
   ```
   fusermount -u /mnt/ytmusic
   ```

## Filesystem Structure

- `/playlists/` - Contains subdirectories for each of your playlists
- `/liked_songs/` - Contains your liked songs
- `/artists/` - Contains subdirectories for each artist in your library
- `/albums/` - Contains subdirectories for each album in your library

## Limitations

- Stream URLs from YouTube Music expire after some time
- Seeking within tracks may not be perfectly smooth
- Metadata (like album art) may not be available to all players

## Troubleshooting

### Token Refresh Issues

If you encounter token refresh issues, make sure:

- You're providing the correct Client ID and Client Secret
- Your OAuth token file is valid
- The YouTube Data API v3 is enabled in your Google Cloud Console project

### Authentication Errors

If you see authentication errors:

1. Try regenerating your OAuth token with `setup_oauth.py`
2. Check if your token file has the correct format
3. Verify your Client ID and Client Secret are correct

### OAuth Token File Format

The OAuth token file should contain:

- `access_token`: Your access token
- `refresh_token`: Your refresh token
- `token_expiry`: Expiration timestamp
- `client_id`: Your client ID
- `client_secret`: Your client secret

### Other Issues

- If you encounter permission issues, make sure your user has write access to the mount point
- For debugging, run with the `--foreground` flag to see log messages

## Command Line Arguments

### ytmusicfs.py

- `--mount-point`: Directory where the filesystem will be mounted
- `--auth-file`: Path to the OAuth token file
- `--client-id`: OAuth client ID
- `--client-secret`: OAuth client secret
- `--foreground`: Run in the foreground (for debugging)
- `--debug`: Enable debug logging

### setup_oauth.py

- `--client-id`: OAuth client ID
- `--client-secret`: OAuth client secret
- `--output`: Output file for the OAuth token

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [ytmusicapi](https://github.com/sigma67/ytmusicapi) - Python library for the unofficial YouTube Music API
- [fusepy](https://github.com/fusepy/fusepy) - Python bindings for FUSE
