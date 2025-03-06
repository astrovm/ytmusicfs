# YTMusicFS Contributions

This directory contains additional files that may be useful for running YTMusicFS.

## Systemd Service Files

### User Service (ytmusicfs.service)

This service file allows you to run YTMusicFS as a user service.

#### Installation

1. Copy the service file to your systemd user directory:

   ```bash
   mkdir -p ~/.config/systemd/user/
   cp ytmusicfs.service ~/.config/systemd/user/
   ```

2. Enable and start the service:

   ```bash
   systemctl --user daemon-reload
   systemctl --user enable ytmusicfs.service
   systemctl --user start ytmusicfs.service
   ```

3. Check the status:

   ```bash
   systemctl --user status ytmusicfs.service
   ```

### System Service (ytmusicfs@.service)

This service file allows you to run YTMusicFS as a system service for a specific user.

#### Installation

1. Copy the service file to the systemd directory:

   ```bash
   sudo cp ytmusicfs@.service /etc/systemd/system/
   ```

2. Enable and start the service for a specific user (replace `username` with the actual username):

   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable ytmusicfs@username.service
   sudo systemctl start ytmusicfs@username.service
   ```

3. Check the status:

   ```bash
   sudo systemctl status ytmusicfs@username.service
   ```

## Environment Variables

You can configure the service by setting environment variables. Create a file at `/etc/default/ytmusicfs` for system services or `~/.config/ytmusicfs/env` for user services:

```bash
# OAuth credentials
YTMUSICFS_CLIENT_ID="your_client_id"
YTMUSICFS_CLIENT_SECRET="your_client_secret"

# Cache settings
YTMUSICFS_CACHE_TIMEOUT=600
```
