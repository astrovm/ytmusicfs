import argparse
import io
import logging
import threading
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from ytmusicfs.cli import setup_logging
from ytmusicfs.dependencies import FileHandlerDependencies
from ytmusicfs.file_handler import FileHandler
from ytmusicfs.models import DownloadRequest
from ytmusicfs.yt_dlp_utils import YTDLPUtils

SIGNED_URL_SECRET = "signed-url-secret"
HEADER_COOKIE_SECRET = "header-cookie-secret"
HEADER_AUTH_SECRET = "header-auth-secret"
COOKIE_MAP_SECRET = "cookie-map-secret"
COOKIE_FILE_SECRET = "cookie-file-secret"
SIGNED_URL = (
    "https://googlevideo.com/videoplayback" f"?expire=123&sig={SIGNED_URL_SECRET}"
)
SECRETS = (
    SIGNED_URL_SECRET,
    HEADER_COOKIE_SECRET,
    HEADER_AUTH_SECRET,
    COOKIE_MAP_SECRET,
    COOKIE_FILE_SECRET,
)


def _capturing_logger(name: str) -> tuple[logging.Logger, io.StringIO]:
    output = io.StringIO()
    logger = logging.getLogger(name)
    logger.handlers = [logging.StreamHandler(output)]
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    return logger, output


def _file_handler(tmp_path: Path, logger: logging.Logger) -> FileHandler:
    (tmp_path / "audio").mkdir(exist_ok=True)
    thread_manager = Mock()
    thread_manager.create_lock.return_value = threading.Lock()
    thread_manager.is_shutdown.return_value = False
    cache = Mock()
    cache.get_unavailable_track.return_value = None
    cache.is_track_unavailable.return_value = False
    return FileHandler(
        FileHandlerDependencies(
            thread_manager=thread_manager,
            cache_dir=tmp_path,
            cache=cache,
            logger=logger,
            update_file_size=Mock(),
            yt_dlp=Mock(),
            browser="brave",
        )
    )


def _secret_error(prefix: str) -> RuntimeError:
    return RuntimeError(
        f"{prefix}: {SIGNED_URL}; "
        f"Authorization: Bearer {HEADER_AUTH_SECRET}; "
        f"Cookie: session={HEADER_COOKIE_SECRET}; "
        f"cookies={{'SID': '{COOKIE_MAP_SECRET}'}}"
    )


def _assert_no_secrets(output: str) -> None:
    for secret in SECRETS:
        assert secret not in output


def test_stream_info_logs_no_request_secrets(tmp_path):
    initial_logger, _ = _capturing_logger("security-stream-info-initial")
    handler = _file_handler(tmp_path, initial_logger)
    logger, output = _capturing_logger("security-stream-info-replaced")
    handler.logger = logger

    handler._apply_stream_info(
        {"video_id": "video"},
        {
            "format_id": "141",
            "stream_url": SIGNED_URL,
            "http_headers": {
                "Authorization": f"Bearer {HEADER_AUTH_SECRET}",
                "Cookie": f"session={HEADER_COOKIE_SECRET}",
            },
            "cookies": {"SID": COOKIE_MAP_SECRET},
        },
    )

    logged = output.getvalue()
    assert "<redacted>" in logged
    assert "video" in logged
    _assert_no_secrets(logged)


def test_read_error_logs_no_request_secrets(tmp_path):
    logger, output = _capturing_logger("security-read")
    handler = _file_handler(tmp_path, logger)
    file_handle = handler.open("/song.m4a", "video")
    handler.open_files[file_handle].update(
        stream_url=SIGNED_URL,
        format_id="141",
        headers={"Authorization": f"Bearer {HEADER_AUTH_SECRET}"},
        cookies={"SID": COOKIE_MAP_SECRET},
    )

    with (
        patch("ytmusicfs.file_handler.http_get", side_effect=_secret_error("read")),
        pytest.raises(OSError),
    ):
        handler.read("/song.m4a", 32, 0, file_handle)

    _assert_no_secrets(output.getvalue())


def test_download_error_logs_no_request_secrets(tmp_path):
    logger, output = _capturing_logger("security-download")
    handler = _file_handler(tmp_path, logger)

    with patch("ytmusicfs.downloader.http_head", side_effect=_secret_error("download")):
        result = handler.downloader.download_file_now(
            DownloadRequest(
                video_id="video",
                stream_url=SIGNED_URL,
                path="/song.m4a",
                format_id="141",
                headers={"Authorization": f"Bearer {HEADER_AUTH_SECRET}"},
                cookies={"SID": COOKIE_MAP_SECRET},
                retries=1,
            )
        )

    assert not result
    _assert_no_secrets(output.getvalue())


def test_yt_dlp_error_logs_no_request_secrets():
    logger, output = _capturing_logger("security-yt-dlp")
    utils = YTDLPUtils(logger=logger)

    with patch.object(utils, "extract_stream_url", side_effect=_secret_error("yt-dlp")):
        result = utils._extract_stream_url_worker("video", "brave")

    assert result["status"] == "error"
    _assert_no_secrets(output.getvalue())


def test_cookie_cleanup_error_logs_no_cookie_file(tmp_path):
    logger, output = _capturing_logger("security-cookie-file")
    utils = YTDLPUtils(logger=logger)
    cookie_file = tmp_path / f"ytmusicfs-{COOKIE_FILE_SECRET}.cookies"
    utils._browser_cookie_files["brave"] = str(cookie_file)

    with patch.object(
        Path, "unlink", side_effect=OSError(f"cannot remove {cookie_file}")
    ):
        utils.cleanup()

    _assert_no_secrets(output.getvalue())


def test_exception_traceback_and_stack_info_are_redacted():
    logger, output = _capturing_logger("security-traceback")
    YTDLPUtils(logger=logger)

    try:
        raise _secret_error("traceback")
    except RuntimeError:
        logger.exception("request failed", stack_info=True)

    logged = output.getvalue()
    assert "Traceback" in logged
    assert "Stack" in logged
    assert "<redacted>" in logged
    _assert_no_secrets(logged)


def test_main_read_and_download_logs_exclude_secrets(tmp_path):
    logger, output = _capturing_logger("security-main-paths")
    handler = _file_handler(tmp_path, logger)
    file_handle = handler.open("/song.m4a", "video")
    handler.open_files[file_handle].update(
        stream_url=SIGNED_URL,
        format_id="141",
        headers={"Authorization": f"Bearer {HEADER_AUTH_SECRET}"},
        cookies={"SID": COOKIE_MAP_SECRET},
    )
    request = DownloadRequest(
        video_id="download-video",
        stream_url=SIGNED_URL,
        path="/download.m4a",
        format_id="141",
        headers={"Authorization": f"Bearer {HEADER_AUTH_SECRET}"},
        cookies={"SID": COOKIE_MAP_SECRET},
        retries=1,
    )

    with patch("ytmusicfs.file_handler.http_get", side_effect=_secret_error("read")):
        with pytest.raises(OSError):
            handler.read("/song.m4a", 32, 0, file_handle)
    with patch("ytmusicfs.downloader.http_head", side_effect=_secret_error("download")):
        assert not handler.downloader.download_file_now(request)

    _assert_no_secrets(output.getvalue())


@pytest.mark.parametrize(
    "level",
    [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL],
)
def test_configured_logging_redacts_request_secrets_at_every_level(
    level, capsys, tmp_path, monkeypatch
):
    monkeypatch.setattr("ytmusicfs.cli.LOG_DIR", tmp_path / "logs")
    monkeypatch.setattr("ytmusicfs.cli.LOG_FILE", tmp_path / "logs" / "ytmusicfs.log")
    logger = setup_logging(
        argparse.Namespace(debug=True, foreground=True, skip_log_file=True)
    )

    logger.log(
        level,
        "request url=%s headers=%s cookies=%s cookiefile=%s",
        SIGNED_URL,
        {
            "Authorization": f"Bearer {HEADER_AUTH_SECRET}",
            "Cookie": f"session={HEADER_COOKIE_SECRET}",
        },
        {"SID": COOKIE_MAP_SECRET},
        tmp_path / f"ytmusicfs-{COOKIE_FILE_SECRET}.cookies",
    )

    _assert_no_secrets(capsys.readouterr().out)
