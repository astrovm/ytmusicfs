"""Utility helpers for working with :class:`ytmusicapi.YTMusic`."""

from __future__ import annotations

import json
import logging
import re
from typing import Optional

from ytmusicapi import YTMusic
from ytmusicapi.constants import YTM_DOMAIN

INNERTUBE_VERSION_KEY = "INNERTUBE_CLIENT_VERSION"
VISITOR_DATA_KEY = "VISITOR_DATA"
CLIENT_VERSION_HEADER = "X-YouTube-Client-Version"
VISITOR_ID_HEADER = "X-Goog-Visitor-Id"
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
        visitor_data = ytcfg.get(VISITOR_DATA_KEY)

        if not version:
            continue

        context = getattr(ytmusic, "context", {})
        client_context = None

        client_parent: Optional[dict[str, object]] = None
        client_parent_key = "client"

        if isinstance(context, dict):
            nested_context = context.get("context")
            if isinstance(nested_context, dict):
                nested_client = nested_context.get("client")
                if isinstance(nested_client, dict):
                    client_context = nested_client
                elif nested_client is None:
                    client_parent = nested_context
            if client_context is None:
                direct_client = context.get("client")
                if isinstance(direct_client, dict):
                    client_context = direct_client
                elif direct_client is None and client_parent is None:
                    client_parent = context

        if isinstance(client_context, dict):
            client_name = client_context.get("clientName")
            if client_name and client_name not in {"WEB_REMIX", "WEB"}:
                log.debug(
                    "Skipping client version synchronization for unsupported client: %s",
                    client_name,
                )
                return None
        elif client_context is None and client_parent is not None:
            client_context = {}
            client_parent[client_parent_key] = client_context

        if isinstance(client_context, dict):
            client_context["clientVersion"] = version
            if visitor_data is not None:
                client_context["visitorData"] = visitor_data

        headers_updated = False

        if hasattr(ytmusic, "_auth_headers"):
            try:
                ytmusic._auth_headers[CLIENT_VERSION_HEADER] = version  # type: ignore[attr-defined]
                if visitor_data is not None:
                    ytmusic._auth_headers[VISITOR_ID_HEADER] = visitor_data  # type: ignore[attr-defined]
                headers_updated = True
            except Exception:  # noqa: BLE001 - best-effort update
                log.debug("Unable to update OAuth headers with client version", exc_info=True)

        cached_headers = getattr(ytmusic, "__dict__", {}).get("base_headers")
        if cached_headers is not None:
            cached_headers[CLIENT_VERSION_HEADER] = version
            if visitor_data is not None:
                cached_headers[VISITOR_ID_HEADER] = visitor_data
            headers_updated = True
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
                if visitor_data is not None:
                    headers[VISITOR_ID_HEADER] = visitor_data
                headers_updated = True

        log.debug("Discovered YouTube Music client version: %s", version)
        if visitor_data is not None and headers_updated:
            log.debug("Synchronized YouTube Music visitor data: %s", visitor_data)
        return version

    log.debug(
        "No %s value found in YouTube Music homepage configuration", INNERTUBE_VERSION_KEY
    )
    return None

