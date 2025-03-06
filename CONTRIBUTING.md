# Contributing to YTMusicFS

Thank you for considering contributing to YTMusicFS! This document provides guidelines and instructions for contributing.

## Code of Conduct

Please be respectful and considerate of others when contributing to this project.

## Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally
3. Create a virtual environment and install development dependencies:

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -e ".[dev]"
   ```

4. Create a branch for your changes:

   ```bash
   git checkout -b feature/your-feature-name
   ```

## Development Workflow

### Running Tests

```bash
# Run all tests
pytest

# Run tests with coverage
pytest --cov=ytmusicfs tests/
```

### Code Style

We use the following tools to maintain code quality:

- Black for code formatting
- isort for import ordering
- flake8 for linting

You can check and format your code with:

```bash
# Check code style
make lint

# Format code
make format
```

## Pull Request Process

1. Update the README.md and documentation with details of your changes, if applicable
2. Add or update tests for your changes
3. Ensure all tests pass and the code style checks pass
4. Submit a pull request to the `main` branch

## Release Process

Releases are handled by the maintainers. The release process:

1. Update version number in `ytmusicfs/__init__.py`
2. Update CHANGELOG.md with the new version and changes
3. Create a new GitHub release with a tag matching the version

## Getting Help

If you need help with contributing, please open an issue with your question or problem.

## License

By contributing to YTMusicFS, you agree that your contributions will be licensed under the project's MIT License.
