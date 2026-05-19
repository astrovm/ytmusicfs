# Repository Guidelines

## Project Overview
- `ytmusicfs` is a Python FUSE filesystem for browsing and streaming YouTube Music as local files.
- It captures YouTube Music browser auth, reads library data through `ytmusicapi`, and streams audio through `yt-dlp`.
- Keep behavior practical for media players: stable paths, predictable metadata, cached listings, and graceful failures.

## Project Structure & Modules
- `ytmusicfs/`: Core package.
  - `cli.py`: CLI entry point, argument parsing, logging, mount orchestration.
  - `filesystem.py`: Main FUSE implementation and filesystem operations.
  - `client.py`: YouTube Music API wrapper.
  - `auth_adapter.py` and `browser_setup.py`: Browser auth capture and `ytmusicapi.YTMusic` setup.
  - `content_fetcher.py`: Library retrieval, playlist registry, cache coordination.
  - `processor.py`: Filename sanitizing, path/video ID extraction, track shaping.
  - `cache.py`: SQLite persistent cache plus in-memory caches.
  - `file_handler.py`, `downloader.py`, `yt_dlp_utils.py`: File handles and audio stream extraction.
  - `path_router.py`: Filesystem path routing and validation.
  - `metadata.py`: Track metadata mapping and cache integration.
  - `thread_manager.py`: Thread pools and concurrent work helpers.
  - `config.py`: Default paths for auth, cache, and logs.
- `tests/`: Pytest suite for cache, routing, filesystem, downloader, and integration behavior.
- Root configs: `pyproject.toml`, `pytest.ini`, `.editorconfig`.
- Extras: `.github/`, `.devcontainer/`, `build/`.

## Build, Test, and Development Commands
- Install editable: `pip install -e .`
- Install editable with dev deps: `pip install -e .[dev]`
- Check CLI: `ytmusicfs --version`
- Capture browser auth: `ytmusicfs browser`
- Mount: `ytmusicfs mount --mount-point ~/Music/ytmusic`
- Mount shorthand: `ytmusicfs mount -m ~/Music/ytmusic`
- Debug mount: `ytmusicfs mount -m ~/Music/ytmusic --foreground --debug`
- Mount with browser cookies for higher quality streams: `ytmusicfs mount -m ~/Music/ytmusic --browser chrome`
- Unmount: `fusermount -u ~/Music/ytmusic`
- Tests: `pytest`
- Verbose tests: `pytest -v`
- Focused tests: `pytest -k path_router -q`
- Coverage: `pytest --cov=ytmusicfs`
- Lint/format check: `black --check . && isort --check-only . && flake8 ytmusicfs tests`
- Auto-format: `black . && isort .`
- Build wheel/sdist: `python -m build` after `pip install build`

## Coding Style & Naming
- Python 3.10+ project; prefer type hints and explicit return types.
- Formatter: Black, line length 88.
- Imports: Isort with `profile = black`.
- Indentation: 4 spaces.
- Names: modules/functions `snake_case`, classes `PascalCase`, constants `UPPER_SNAKE_CASE`.
- Use `logging` instead of prints. Follow `ytmusicfs/cli.py` logging setup.
- Keep functions focused. Prefer small component-level changes over cross-cutting rewrites.

## Architecture Notes
- Components communicate through narrow interfaces; avoid reaching into another component's internals.
- `YouTubeMusicFS` coordinates routing, metadata, content fetches, cache, and file handling.
- Cache is shared for consistency across directory listings, file attributes, metadata, and path validation.
- Caching is multi-tier: in-memory hot data plus SQLite persistence.
- External dependencies (`ytmusicapi`, `yt-dlp`, browser cookies, network calls) should be mocked in unit tests.
- FUSE-facing errors must map cleanly to errno-style failures where appropriate.
- Prefer graceful degradation when YouTube Music, auth, or stream extraction is unavailable.

## Common Development Tasks
- Adding filesystem paths:
  1. Register the route in `filesystem.py`.
  2. Add fetch/transform logic in `content_fetcher.py` or `processor.py` as needed.
  3. Update path validation in `path_router.py`.
  4. Add tests in `tests/`.
- Extending API support:
  1. Add wrapper methods in `client.py`.
  2. Process returned data in `content_fetcher.py`.
  3. Normalize metadata and filenames in `processor.py`.
- Performance work:
  - Reduce API calls first.
  - Use batch cache operations where available.
  - Preserve cache invalidation rules and timestamps.

## Testing Guidelines
- Framework: Pytest.
- Discovery: files `test_*.py`, classes `Test*`, functions `test_*`.
- Add tests alongside the changed behavior in `tests/`.
- Cover critical paths: filesystem operations, path routing, cache updates, downloader behavior, auth failure modes.
- For bug fixes, include a regression test when the failure can be reproduced without live YouTube Music access.

## Commit & Pull Request Guidelines
- Commits: imperative, concise, scoped to one change.
- Example: `Refactor logging setup in cli.py`
- Reference issues when relevant, e.g. `Fixes #123`.
- PRs must include purpose/scope, test commands and results, risk notes, and logs/screenshots for CLI or mount behavior changes.
- CI should pass formatting, lint, and tests before merge.

## Security & Configuration
- Never commit credentials, browser headers, cookies, or local auth captures.
- Auth file: `~/.config/ytmusicfs/browser.json`.
- Cache: `~/.cache/ytmusicfs`.
- Logs: `~/.local/share/ytmusicfs/logs`.
- Use `--foreground` and `--debug` when diagnosing mount issues.
