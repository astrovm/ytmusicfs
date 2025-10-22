"""Utility helpers for working with :class:`ytmusicapi.YTMusic`."""

from __future__ import annotations

import json
import logging
import re
from typing import Optional

from ytmusicapi import YTMusic
from ytmusicapi.constants import YTM_DOMAIN

INNERTUBE_VERSION_KEY = "INNERTUBE_CLIENT_VERSION"
CLIENT_VERSION_HEADER = "X-YouTube-Client-Version"
_YTCFG_PATTERN = re.compile(r"ytcfg\.set\s*\(\s*(\{.*?\})\s*\)\s*;?", re.DOTALL)


def apply_server_client_version(
    ytmusic: YTMusic, logger: Optional[logging.Logger] = None
) -> Optional[str]:
    """Synchronize the client version of a :class:`YTMusic` instance with the server."""

    log = logger or logging.getLogger(__name__)

    try:
        response = ytmusic._send_get_request(YTM_DOMAIN, use_base_headers=True)  # type: ignore[attr-defined]
    except Exception as error:  # noqa: BLE001 - network errors should not be fatal here
        log.debug(
            "Unable to fetch YouTube Music homepage for client version discovery: %s",
            error,
        )
        return None

    matches = _YTCFG_PATTERN.findall(response.text)
    for match in matches:
        try:
            ytcfg = json.loads(match)
        except json.JSONDecodeError:
            continue

        version = ytcfg.get(INNERTUBE_VERSION_KEY)
        if not version:
            continue

        ytmusic.context["context"]["client"]["clientVersion"] = version

        if hasattr(ytmusic, "_auth_headers"):
            try:
                ytmusic._auth_headers[CLIENT_VERSION_HEADER] = version  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001 - best-effort update
                log.debug("Unable to update OAuth headers with client version", exc_info=True)

        cached_headers = getattr(ytmusic, "__dict__", {}).get("base_headers")
        if cached_headers is not None:
            cached_headers[CLIENT_VERSION_HEADER] = version
        else:
            try:
                headers = ytmusic.headers
            except Exception as error:  # noqa: BLE001 - optional best effort
                log.debug(
                    "Unable to initialize base headers for client version update: %s",
                    error,
                )
            else:
                headers[CLIENT_VERSION_HEADER] = version

        log.debug("Discovered YouTube Music client version: %s", version)
        return version

    log.debug(
        "No %s value found in YouTube Music homepage configuration", INNERTUBE_VERSION_KEY
    )
    return None

