# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

YTMusicFS is a FUSE filesystem that mounts YouTube Music as a browsable filesystem. It streams audio from YouTube Music through a virtual filesystem interface, allowing traditional media players to access your music library.

## Common Development Commands

### Installation and Setup
```bash
# Install in development mode
pip install -e .

# Install with development dependencies
pip install -e ".[dev]"
```

### Testing
```bash
# Run all tests
pytest

# Run tests with verbose output
pytest -v

# Run specific test file
pytest tests/test_cache.py

# Run tests with coverage
pytest --cov=ytmusicfs
```

### Code Quality
```bash
# Format code with black
black ytmusicfs/

# Sort imports with isort
isort ytmusicfs/

# Check code style with flake8
flake8 ytmusicfs/
```

### Running the Application
```bash
# Set up OAuth authentication
ytmusicfs oauth --client-id YOUR_CLIENT_ID --client-secret YOUR_CLIENT_SECRET

# Mount the filesystem
ytmusicfs mount --mount-point ~/Music/ytmusic

# Mount with debugging enabled
ytmusicfs mount --mount-point ~/Music/ytmusic --foreground --debug

# Mount with browser cookies for higher quality audio
ytmusicfs mount --mount-point ~/Music/ytmusic --browser chrome

# Unmount the filesystem
fusermount -u ~/Music/ytmusic
```

## Architecture Overview

YTMusicFS follows a modular architecture with clear separation of concerns:

### Core Components

1. **cli.py**: Command-line interface and entry point
   - Handles argument parsing for `mount` and `oauth` commands
   - Sets up logging configuration
   - Manages filesystem mounting process

2. **filesystem.py**: Main FUSE filesystem implementation
   - `YouTubeMusicFS` class implements FUSE operations
   - Coordinates between all other components
   - Handles filesystem operations: `readdir`, `getattr`, `open`, `read`

3. **client.py**: YouTube Music API client
   - Wraps ytmusicapi calls
   - Handles authentication through OAuth adapter
   - Provides methods for fetching playlists, albums, liked songs

4. **oauth_adapter.py**: OAuth authentication wrapper
   - Extends ytmusicapi with OAuth2 support
   - Handles token refresh and browser cookie integration
   - Manages authentication state

### Data Processing Layer

5. **content_fetcher.py**: Content retrieval and caching
   - Fetches playlists, albums, and tracks from YouTube Music
   - Maintains playlist registry for ID/name mapping
   - Coordinates with cache and processor components

6. **processor.py**: Track data processing
   - Sanitizes filenames and metadata
   - Extracts video IDs from paths
   - Processes track information for filesystem representation

7. **cache.py**: Multi-level caching system
   - SQLite-based persistent cache with in-memory LRU cache
   - Stores metadata, file attributes, and directory listings
   - Thread-safe operations with connection pooling

### File Operations

8. **file_handler.py**: File-level operations
   - Manages file handles and streaming
   - Coordinates audio streaming with yt-dlp
   - Handles file size updates and caching

9. **yt_dlp_utils.py**: YouTube audio streaming
   - Extracts stream URLs using yt-dlp
   - Manages stream caching and URL refresh
   - Handles different quality preferences

### Supporting Components

10. **path_router.py**: URL routing system
    - Maps filesystem paths to appropriate handlers
    - Supports exact matches and wildcard patterns
    - Validates path access

11. **metadata.py**: Metadata management
    - Handles track metadata extraction and caching
    - Maps between filesystem paths and video IDs

12. **thread_manager.py**: Concurrent operations
    - Manages thread pools for I/O and network operations
    - Provides thread-safe primitives

13. **config.py**: Configuration management
    - Handles default paths and credentials
    - Manages OAuth token and cache directories

## Key Design Patterns

### Component Communication
- Components communicate through well-defined interfaces
- Cache is shared across components for data consistency
- ThreadManager provides thread-safe operations

### Caching Strategy
- Three-tier caching: in-memory LRU, SQLite persistent, filesystem attributes
- Batch operations for performance optimization
- Cache invalidation based on timestamps

### Error Handling
- Each component has comprehensive error handling
- FUSE errors (errno) are properly mapped
- Graceful degradation when services are unavailable

### Authentication Flow
- OAuth2 with refresh token support
- Browser cookie integration for premium features
- Fallback to standard quality without cookies

## Development Tips

### Adding New Filesystem Paths
1. Register path handlers in `filesystem.py` constructor
2. Implement handler logic in `content_fetcher.py` if needed
3. Add path validation in `path_router.py`

### Extending API Support
- Add new methods to `client.py` for YouTube Music API calls
- Update `content_fetcher.py` to process new data types
- Extend `processor.py` for new metadata formats

### Testing Components
- Use pytest fixtures for component initialization
- Mock external dependencies (YouTube Music API, yt-dlp)
- Test error conditions and edge cases

### Performance Optimization
- Cache frequently accessed data
- Use batch operations for database updates
- Minimize API calls through intelligent caching

## Configuration Files

- `pyproject.toml`: Project metadata, dependencies, and tool configuration
- `pytest.ini`: Test configuration and discovery
- Authentication files stored in `~/.config/ytmusicfs/`
- Cache stored in `~/.cache/ytmusicfs/`