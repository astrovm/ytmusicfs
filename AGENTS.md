# Repository Guidelines

## Project Structure & Modules
- `ytmusicfs/`: Core package (CLI, filesystem, cache, auth, downloader).
- `tests/`: Pytest suite covering routing, filesystem, cache, and integrations.
- Root configs: `pyproject.toml` (packaging, Black/Isort), `pytest.ini`, `.editorconfig`.
- Extras: `.github/` (CI/issue templates), `.devcontainer/` (VS Code dev), `build/` (artifacts).

## Build, Test, and Development Commands
- Install (editable + dev): `pip install -e .[dev]`
- Run CLI locally: `ytmusicfs --version`, `ytmusicfs browser`, `ytmusicfs mount -m ~/Music/ytmusic`
- Tests (verbose): `pytest`
- Coverage: `pytest --cov=ytmusicfs`
- Lint/format (check): `black --check . && isort --check-only . && flake8 ytmusicfs tests`
- Auto-format: `black . && isort .`
- Build wheel/sdist (optional): `python -m build` (requires `pip install build`)

## Coding Style & Naming
- Formatter: Black (line length 88); imports: Isort (`profile = black`).
- Indentation: 4 spaces for Python (`.editorconfig`).
- Naming: modules/functions `snake_case`, classes `PascalCase`, constants `UPPER_SNAKE_CASE`.
- Typing: prefer type hints and explicit return types; keep functions focused.
- Logging: use `logging` (see `ytmusicfs/cli.py` setup) instead of prints.

## Testing Guidelines
- Framework: Pytest.
- Discovery (see `pytest.ini`): files `test_*.py`, classes `Test*`, functions `test_*`.
- Run fast, focused tests: `pytest -k path_router -q`.
- Add tests alongside features in `tests/`; aim for coverage on critical paths (filesystem ops, caching, routing, downloader).

## Commit & Pull Request Guidelines
- Commits: imperative, concise, scoped to one change (e.g., "Refactor logging setup in cli.py").
- Reference issues when relevant (e.g., "Fixes #123").
- PRs must include: purpose and scope, how to test (commands/output), risk notes, and screenshots/log snippets if UI/CLI behavior changes.
- Pass CI: formatting, lint, and tests must be green.

## Security & Configuration Tips
- Never commit credentials, browser headers, or cookies.
- Auth file: `~/.config/ytmusicfs/browser.json` (created via `ytmusicfs browser`).
- Cache: `~/.cache/ytmusicfs`; Logs: `~/.local/share/ytmusicfs/logs`.
- Use `--foreground` and `--debug` when diagnosing mount issues.
