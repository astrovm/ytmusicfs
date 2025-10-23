#!/usr/bin/env python3

"""Browser-based authentication helper for YTMusicFS."""

from pathlib import Path
from typing import Optional
import argparse
import logging

from ytmusicapi import setup as ytmusic_setup

from ytmusicfs.config import ConfigManager


def _configure_logger(args: argparse.Namespace) -> logging.Logger:
    """Create a logger when the CLI wrapper did not provide one."""
    if getattr(args, "logger", None):
        return args.logger

    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=log_level)
    return logging.getLogger("YTMusicFS Browser Setup")


def _read_headers_file(path: Optional[str]) -> Optional[str]:
    if not path:
        return None

    headers_path = Path(path)
    if not headers_path.exists():
        raise FileNotFoundError(f"Headers file not found: {headers_path}")
    return headers_path.read_text(encoding="utf-8")


def main(args: Optional[argparse.Namespace] = None) -> int:
    """Collect browser headers and persist them for later use."""
    if args is None:
        parser = argparse.ArgumentParser(
            description="Generate browser authentication headers for YTMusicFS",
        )
        parser.add_argument(
            "--auth-file",
            "-a",
            help="Output path for the generated browser header JSON",
        )
        parser.add_argument(
            "--headers-file",
            help="Read request headers from this file instead of prompting",
        )
        parser.add_argument(
            "--debug", "-d", action="store_true", help="Enable debug logging"
        )
        args = parser.parse_args()

    logger = _configure_logger(args)
    config = ConfigManager(auth_file=args.auth_file, logger=logger)
    output_path = config.auth_file
    output_path.parent.mkdir(parents=True, exist_ok=True)

    headers_raw: Optional[str] = None
    try:
        headers_raw = _read_headers_file(getattr(args, "headers_file", None))
    except FileNotFoundError as exc:
        logger.error(str(exc))
        return 1

    if output_path.exists():
        overwrite = input(f"File {output_path} already exists. Overwrite? (y/n): ")
        if overwrite.strip().lower() != "y":
            logger.info("Aborted.")
            return 1

    if headers_raw is None:
        logger.info("Paste the request headers from an authenticated browser session.")
        logger.info(
            "Follow the instructions at https://ytmusicapi.readthedocs.io/en/latest/setup/browser.html"
        )
        logger.info("Press Ctrl+D (Linux/macOS) or Ctrl+Z (Windows) when finished.")

    try:
        ytmusic_setup(filepath=str(output_path), headers_raw=headers_raw)
    except Exception as exc:  # pragma: no cover - defensive logging path
        logger.error(f"Failed to save browser headers: {exc}")
        return 1

    logger.info("Browser authentication headers saved to %s", output_path)
    logger.info("You can now mount your library with 'ytmusicfs mount'.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
