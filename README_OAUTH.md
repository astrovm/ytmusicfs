# YTMusicFS OAuth Setup Guide

This guide explains how to set up YTMusicFS with OAuth authentication, which is more reliable than browser-based authentication.

## Prerequisites

- Python 3.6 or higher
- `ytmusicapi` version 1.10.2 or higher
- Google Cloud Console account

## Step 1: Get OAuth Credentials

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

## Step 2: Generate OAuth Token

Run the `oauth_setup_exact.py` script to generate an OAuth token:

```bash
python oauth_setup_exact.py --client-id YOUR_CLIENT_ID --client-secret YOUR_CLIENT_SECRET --output oauth.json
```

This will:

1. Open a browser window for you to authorize the application
2. Generate an OAuth token file (`oauth.json`)
3. Create a test script to verify the token works

## Step 3: Test OAuth Setup

Test your OAuth setup with the test script:

```bash
python test_oauth_setup.py --auth-file oauth.json --client-id YOUR_CLIENT_ID --client-secret YOUR_CLIENT_SECRET
```

If successful, you should see a list of your playlists.

## Step 4: Mount YTMusicFS with OAuth

Mount YTMusicFS using your OAuth token:

```bash
python ytmusicfs.py --mount-point ./mount --auth-file oauth.json --auth-type oauth --client-id YOUR_CLIENT_ID --client-secret YOUR_CLIENT_SECRET
```

## Troubleshooting

### Token Refresh Issues

If you encounter token refresh issues, make sure:

- You're providing the correct Client ID and Client Secret
- Your OAuth token file is valid
- The YouTube Data API v3 is enabled in your Google Cloud Console project

### Authentication Errors

If you see authentication errors:

1. Try regenerating your OAuth token with `oauth_setup_exact.py`
2. Check if your token file has the correct format
3. Verify your Client ID and Client Secret are correct

### File Format Issues

The OAuth token file should contain:

- `access_token`: Your access token
- `refresh_token`: Your refresh token
- `token_expiry`: Expiration timestamp
- `client_id`: Your client ID
- `client_secret`: Your client secret

## Files Overview

- `oauth_setup_exact.py`: Script to generate OAuth token
- `ytmusic_oauth_adapter.py`: Adapter class for YTMusic with OAuth support
- `test_oauth_setup.py`: Script to test OAuth setup
- `ytmusicfs.py`: Main YTMusicFS script with OAuth support

## Command Line Arguments

### ytmusicfs.py

- `--mount-point`: Directory where the filesystem will be mounted
- `--auth-file`: Path to the OAuth token file
- `--auth-type`: Type of authentication (`oauth` or `browser`)
- `--client-id`: OAuth client ID
- `--client-secret`: OAuth client secret
- `--foreground`: Run in the foreground (for debugging)
- `--debug`: Enable debug logging

### oauth_setup_exact.py

- `--client-id`: OAuth client ID
- `--client-secret`: OAuth client secret
- `--output`: Output file for the OAuth token

### test_oauth_setup.py

- `--auth-file`: Path to OAuth token file
- `--client-id`: OAuth client ID
- `--client-secret`: OAuth client secret
- `--debug`: Enable debug logging
