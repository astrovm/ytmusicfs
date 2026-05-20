# Repository Guidelines

## Project Overview

- `ytmusicfs` is a Python FUSE filesystem for browsing and streaming YouTube Music as local files.
- It captures YouTube Music browser auth, reads library data through `ytmusicapi`, and streams audio through `yt-dlp`.
- Keep behavior practical for media players: stable paths, predictable metadata, cached listings, and graceful failures.

## Approach

- Read before editing. Test before declaring done.
- Prefer small edits over rewrites.
- Reproduce before fixing runtime or external issues.
- Unproven concerns are risks, not bugs. Say so if not reproduced.
- Use the simplest working solution. No over-engineering, speculative features, or single-use abstractions.

## Output

- Code first. Explain only non-obvious logic.
- No filler, boilerplate, or out-of-scope suggestions.
- Use plain hyphens and straight quotes only. No decorative Unicode. Keep code output copy-paste safe.

## Project Structure & Modules

- `ytmusicfs/`: Core package.
  - `cli.py`: CLI entry point, argument parsing, logging, mount orchestration.
  - `filesystem.py`: Main FUSE implementation and filesystem operations.
  - `client.py`: YouTube Music API wrapper.
  - `auth_adapter.py`: Browser cookie auth for `ytmusicapi.YTMusic`.
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

- Install local app: `pipx install .`
- Refresh local app after changes: `pipx install --force .`
- Check CLI: `ytmusicfs --version`
- Mount: `ytmusicfs mount --mount-point ~/Music/ytmusic --browser brave`
- Mount with saved settings after first successful mount: `ytmusicfs mount`
- Debug mount with browser cookies: `ytmusicfs mount --mount-point ~/Music/ytmusic --browser brave --foreground --debug`
- Unmount active mount: `ytmusicfs unmount`
- Unmount explicit path: `ytmusicfs unmount --mount-point ~/Music/ytmusic`
- Status: `ytmusicfs status`
- Doctor: `ytmusicfs doctor`
- Saved config: `ytmusicfs config show`, `ytmusicfs config set browser brave`, `ytmusicfs config set mount-point ~/Music/ytmusic`
- Cache: `ytmusicfs cache stats`, `ytmusicfs cache clear`, `ytmusicfs refresh`
- Logs: `ytmusicfs logs` (last 50 lines), `ytmusicfs logs --tail N`, `ytmusicfs logs --path`
- Systemd user service: `ytmusicfs service install`, `ytmusicfs service start`, `ytmusicfs service stop`
- Mounted debug status: `/.ytmusicfs/status.json`.
- Tests: `pipx run --spec '.[dev]' pytest -q`
- Verbose tests: `pipx run --spec '.[dev]' pytest -v`
- Focused tests: `pipx run --spec '.[dev]' pytest -k path_router -q`
- Coverage: `pipx run --spec '.[dev]' pytest --cov=ytmusicfs`
- Lint/type/format check: `pipx run --spec '.[dev]' black --check . && pipx run --spec '.[dev]' ruff check . && pipx run --spec '.[dev]' mypy ytmusicfs`
- Auto-format: `pipx run --spec '.[dev]' black . && pipx run --spec '.[dev]' ruff check --fix .`
- Build wheel/sdist: `pipx run --spec build pyproject-build`

## Coding Style & Naming

- Python 3.10+ project; prefer type hints and explicit return types.
- Formatter: Black, line length 88.
- Imports: Ruff import sorting (`I` rules), formatted with Black.
- Indentation: 4 spaces.
- Names: modules/functions `snake_case`, classes `PascalCase`, constants `UPPER_SNAKE_CASE`.
- Use `logging` instead of prints. Follow `ytmusicfs/cli.py` logging setup.
- Keep functions focused. Prefer small component-level changes over cross-cutting rewrites.
- Remove unused imports, variables, parameters, dead branches, and dead functions from edited files.
- Do not add error handling for impossible scenarios.
- Keep all imports at the top of the file. Use local imports only when strictly required to break circular dependencies.
- Code and comments must be in English. User-facing strings stay in their existing language.
- Remove old code when introducing replacements. Do not add backward compatibility shims without explicit authorization.
- Do not preserve feature flags for shipped features or abstractions that serve a single caller.

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

## Debugging

- Read code before explaining.
- Prove findings with direct evidence: failing test, reproduced run, or concrete probe.
- State what you found, where, and the fix.
- If unclear, say so.
- For runtime or external failures, reproduce first when possible.

## Verification

- Smallest proof first, then broader checks.
- Use the standard toolchain.
- Default checks: format, lint with warnings as errors, and tests.
- Skip a check only with a stated reason.
- Do not claim fixed, safe, ready, or done without fresh command output.
- Fix every issue encountered. Do not ignore failures as pre-existing.

## Testing Guidelines

- Framework: Pytest.
- Discovery: files `test_*.py`, classes `Test*`, functions `test_*`.
- Add tests alongside the changed behavior in `tests/`.
- Cover critical paths: filesystem operations, path routing, cache updates, downloader behavior, auth failure modes.
- For bug fixes, include a regression test when the failure can be reproduced without live YouTube Music access.
- If the project has tests, run them before committing or declaring work complete. No exceptions.
- A failing test is blocking. Fix it before moving on.

## Commit & Pull Request Guidelines

- Commits: imperative, concise, scoped to one change.
- Example: `Refactor logging setup in cli.py`
- Reference issues when relevant, e.g. `Fixes #123`.
- PRs must include purpose/scope, test commands and results, risk notes, and logs/screenshots for CLI or mount behavior changes.
- CI should pass formatting, lint, and tests before merge.
- Ask before pushing every time, even if previously approved.
- No batch commit and push.
- No force push or hard reset without approval.
- Never use `git commit --amend` unless explicitly asked.
- Merge to `main` with a single squashed commit.
- Commit messages must be in English.

## Security & Configuration

- Never commit credentials, browser cookies, or local auth captures.
- Cache: `~/.cache/ytmusicfs`.
- Saved mount defaults: `~/.config/ytmusicfs/config.json`.
- Active mount state: `~/.cache/ytmusicfs/mount.json`.
- Logs: `~/.local/share/ytmusicfs/logs`.
- Use `--foreground` and `--debug` when diagnosing mount issues.
- Environment variables are only for secrets and external credentials.
- Prefer sane defaults, zero-config behavior, and easy maintenance.
- Hardcode sensible defaults for internal URLs, ports, and feature flags.
- When adding a dependency, verify the actual latest version from the registry or official source. Never rely on model memory.
