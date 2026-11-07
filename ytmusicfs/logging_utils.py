"""Logging helpers that keep request credentials out of log output."""

from __future__ import annotations

import logging
import re

REDACTED = "<redacted>"

_URL_QUERY = re.compile(
    r"(?P<base>https?://[^\s?'\"<>]+)\?[^\s'\"<>]+",
    re.IGNORECASE,
)
_COOKIE_FILE = re.compile(
    r"\S*ytmusicfs-\S*\.cookies",
    re.IGNORECASE,
)
_SECRET_MAPPING = re.compile(
    r"\b(?P<label>http_headers|headers|cookies)['\"]?\s*[:=]\s*\{[^}\n]*\}",
    re.IGNORECASE,
)
_COOKIE_HEADER = re.compile(
    r"\bcookie(?:\s+header)?(?:\s+for\s+[^:\n]+)?\s*:\s*[^\n]+",
    re.IGNORECASE,
)
_SECRET_VALUE = re.compile(
    r"\b(?P<label>authorization|proxy-authorization|set-cookie|cookiefile|"
    r"cookie_file|auth|access[_-]?token|refresh[_-]?token|id[_-]?token|"
    r"api[_-]?key|x-goog-api-key|x-youtube-identity-token|signature|sig)"
    r"['\"]?\s*[:=]\s*(?:'[^'\n]*'|\"[^\"\n]*\"|[^,;\n}\]]+)",
    re.IGNORECASE,
)


def redact_log_text(text: str) -> str:
    """Return formatted log text with request and cookie secrets removed."""
    text = _URL_QUERY.sub(rf"\g<base>?{REDACTED}", text)
    text = _COOKIE_FILE.sub("<redacted-cookie-file>", text)
    text = _SECRET_MAPPING.sub(rf"\g<label>={REDACTED}", text)
    text = _COOKIE_HEADER.sub(f"cookie: {REDACTED}", text)
    return _SECRET_VALUE.sub(rf"\g<label>={REDACTED}", text)


class SecretRedactingFilter(logging.Filter):
    """Redact secrets after lazy log formatting and before output handlers."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact_log_text(record.getMessage())
        record.args = ()
        if record.exc_info:
            formatter = logging.Formatter()
            record.exc_text = redact_log_text(
                formatter.formatException(record.exc_info)
            )
            record.exc_info = None
        if record.stack_info:
            record.stack_info = redact_log_text(record.stack_info)
        return True


_SECRET_REDACTING_FILTER = SecretRedactingFilter()


def add_secret_redaction[T: (logging.Handler, logging.Logger)](target: T) -> T:
    """Install the shared secret filter and return *target* unchanged."""
    target.addFilter(_SECRET_REDACTING_FILTER)
    return target
