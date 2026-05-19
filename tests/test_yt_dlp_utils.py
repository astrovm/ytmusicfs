#!/usr/bin/env python3

import unittest
from unittest.mock import MagicMock, patch

from ytmusicfs.yt_dlp_utils import YTDLPUtils


class TestYTDLPUtils(unittest.TestCase):
    @patch("ytmusicfs.yt_dlp_utils.YoutubeDL")
    def test_stream_extraction_enables_ejs_runtime(self, mock_youtube_dl):
        info = {
            "url": "https://example.com/audio.m4a",
            "http_headers": {"User-Agent": "UnitTest"},
            "format_id": "141",
        }

        ydl = MagicMock()
        ydl.extract_info.return_value = info
        mock_youtube_dl.return_value.__enter__.return_value = ydl

        result = YTDLPUtils().extract_stream_url("abc123", browser="brave")

        self.assertEqual(result["stream_url"], "https://example.com/audio.m4a")
        opts = mock_youtube_dl.call_args.args[0]
        self.assertEqual(opts["format"], "141/140/bestaudio[ext=m4a]")
        self.assertEqual(opts["cookiesfrombrowser"], ("brave",))
        self.assertEqual(
            opts["extractor_args"], {"youtube": {"formats": ["missing_pot"]}}
        )
        self.assertIn("node", opts["js_runtimes"])
        self.assertIn("deno", opts["js_runtimes"])


if __name__ == "__main__":
    unittest.main()
