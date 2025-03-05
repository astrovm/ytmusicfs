# YTMusicFS - YouTube Music FUSE Filesystem

YTMusicFS is a FUSE filesystem that integrates YouTube Music into your Linux environment, allowing traditional music players like Audacious to access and play your songs and playlists as if they were local files.

## Features

- Access your YouTube Music library through a standard filesystem interface
- Browse and play your playlists, liked songs, artists, and albums
- Compatible with any music player that can read files from the filesystem
- Streams audio on-demand from YouTube Music servers
- Caches metadata to improve performance
- Supports two authentication methods: browser-based (cookies) and OAuth (longer lasting)

## Requirements

- Python 3.6+
- FUSE (Filesystem in Userspace)
- YouTube Music account

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

3. Set up YouTube Music authentication:

   **Option 1: Cookie-based authentication (standard method):**

   ```
   python setup_auth.py
   ```

   Follow the prompts to log into YouTube Music via a browser and save the resulting JSON file as `headers_auth.json` in the project directory.

   **Option 2: OAuth authentication (recommended for longer sessions):**

   First, you need to obtain OAuth 2.0 credentials from Google:

   1. Go to the [Google Cloud Console](https://console.cloud.google.com/)
   2. Create a new project (or select an existing one)
   3. Enable the YouTube Data API v3:
      - Go to "APIs & Services" > "Library"
      - Search for "YouTube Data API v3"
      - Click "Enable"
   4. Create OAuth credentials:
      - Go to "APIs & Services" > "Credentials"
      - Click "Create Credentials" > "OAuth client ID"
      - Application type: "Desktop app"
      - Name: "YTMusicFS" (or any name you prefer)
      - Click "Create"
      - Download the credentials JSON file

   Then run the OAuth setup script:

   ```
   # Using the downloaded credentials file
   python setup_oauth.py --credentials-file path/to/credentials.json

   # OR specifying client ID and secret directly
   python setup_oauth.py --client-id YOUR_CLIENT_ID --client-secret YOUR_CLIENT_SECRET
   ```

   Follow the prompts to authorize the application in your browser. This will create an `oauth_auth.json` file with your OAuth tokens.

## Usage

1. Create a mount point:

   ```
   sudo mkdir -p /mnt/ytmusic
   sudo chown $USER:$USER /mnt/ytmusic
   ```

2. Mount the filesystem:

   **Using cookie-based authentication:**

   ```
   python ytmusicfs.py --auth-file headers_auth.json --mount-point /mnt/ytmusic
   ```

   **Using OAuth authentication (recommended for longer sessions):**

   ```
   python ytmusicfs.py --auth-file oauth_auth.json --auth-type oauth --mount-point /mnt/ytmusic
   ```

   For debugging, add the `--foreground` flag:

   ```
   python ytmusicfs.py --auth-file oauth_auth.json --auth-type oauth --mount-point /mnt/ytmusic --foreground
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

## Authentication Methods

YTMusicFS supports two types of authentication:

1. **Browser-based (cookie) authentication** - The standard method using browser cookies, but requires more frequent re-authentication (typically every few hours to days).
2. **OAuth authentication** - A more persistent authentication method that typically lasts much longer (weeks to months) and can refresh automatically.

If you find your authentication expiring too quickly with the browser-based method, we recommend switching to OAuth authentication.

## Limitations

- Stream URLs from YouTube Music expire after some time
- Seeking within tracks may not be perfectly smooth
- Metadata (like album art) may not be available to all players

## Troubleshooting

- If you encounter permission issues, make sure your user has write access to the mount point
- If authentication fails, regenerate the auth file using the appropriate setup script
- If using cookie-based authentication and it expires too quickly, switch to OAuth authentication
- For debugging, run with the `--foreground` flag to see log messages
- If you have issues with OAuth setup, make sure YouTube Data API v3 is enabled in your Google Cloud project

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [ytmusicapi](https://github.com/sigma67/ytmusicapi) - Python library for the unofficial YouTube Music API
- [fusepy](https://github.com/fusepy/fusepy) - Python bindings for FUSE
