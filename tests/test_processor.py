from unittest.mock import Mock

import pytest

from ytmusicfs.processor import TrackProcessor


class TestTrackProcessor:
    def test_process_tracks_adds_stable_suffix_for_duplicate_filenames(self):
        processor = TrackProcessor()
        tracks = [
            {
                "title": "Same Song",
                "videoId": "video-one",
                "artists": [{"name": "Artist"}],
            },
            {
                "title": "Same Song",
                "videoId": "video-two",
                "artists": [{"name": "Artist"}],
            },
        ]

        result = processor.process_tracks(tracks)

        assert result[0]["filename"] == "Artist - Same Song [video-one].m4a"
        assert result[1]["filename"] == "Artist - Same Song [video-two].m4a"

    def test_process_tracks_uses_index_suffix_for_duplicates_without_video_id(self):
        processor = TrackProcessor()
        tracks = [
            {"title": "Song", "artist": "Artist"},
            {"title": "Song", "artist": "Artist"},
        ]

        result = processor.process_tracks(tracks)

        assert [track["filename"] for track in result] == [
            "Artist - Song [1].m4a",
            "Artist - Song [2].m4a",
        ]

    def test_process_tracks_without_filenames_returns_empty_list(self):
        processor = TrackProcessor()

        assert processor.process_tracks([{"title": "Song"}], add_filename=False) == []

    def test_process_tracks_batches_new_durations_into_cache(self):
        cache = Mock()
        cache.get_duration.return_value = None
        processor = TrackProcessor(cache_manager=cache)
        tracks = [
            {"title": "A", "videoId": "a", "duration_seconds": 61},
            {"title": "B", "videoId": "b", "duration_seconds": None},
        ]

        result = processor.process_tracks(tracks)

        cache.set_durations_batch.assert_called_once_with({"a": 61})
        assert "is_new_duration" not in result[0]
        assert result[0]["duration_formatted"] == "1:01"

    def test_process_tracks_skips_cache_write_without_new_durations(self):
        cache = Mock()
        cache.get_duration.return_value = 200
        processor = TrackProcessor(cache_manager=cache)

        result = processor.process_tracks([{"title": "A", "videoId": "a"}])

        cache.set_durations_batch.assert_not_called()
        assert result[0]["duration_seconds"] == 200
        assert result[0]["duration_formatted"] == "3:20"

    def test_extract_track_info_handles_missing_uploader(self):
        processor = TrackProcessor()

        result = processor.extract_track_info(
            {"title": "Song", "videoId": "video-one", "uploader": None}
        )

        assert result["artist"] == "Unknown Artist"

    def test_extract_track_info_parses_duration_text_without_cache(self):
        processor = TrackProcessor()

        result = processor.extract_track_info(
            {"title": "Song", "videoId": "v", "duration": "1:02:03"}
        )

        assert result["duration_seconds"] == 3723
        assert result["duration_formatted"] == "62:03"
        assert result["is_new_duration"] is True

    def test_extract_track_info_cache_miss_leaves_duration_unknown(self):
        cache = Mock()
        cache.get_duration.return_value = None
        processor = TrackProcessor(cache_manager=cache)

        result = processor.extract_track_info(
            {"title": "Song", "videoId": "v", "duration": "3:00"}
        )

        assert result["duration_seconds"] is None
        assert result["duration_formatted"] == "0:00"
        assert result["is_new_duration"] is False

    def test_extract_track_info_uses_flat_album_string_and_album_artist(self):
        processor = TrackProcessor()

        result = processor.extract_track_info(
            {
                "title": "Song",
                "artist": "Band - Topic",
                "album": "Record",
                "album_artist": "Label - Topic",
                "year": 1999,
            }
        )

        assert result["artist"] == "Band"
        assert result["album"] == "Record"
        assert result["album_artist"] == "Label"
        assert result["year"] == 1999

    def test_extract_track_info_reads_year_from_album_object(self):
        processor = TrackProcessor()

        result = processor.extract_track_info(
            {
                "title": "Song",
                "artists": [{"name": "A"}, {}],
                "album": {"name": "Record", "year": 2001, "artist": {"name": "AA"}},
            }
        )

        assert result["artist"] == "A, Unknown Artist"
        assert result["album"] == "Record"
        assert result["album_artist"] == "AA"
        assert result["year"] == 2001

    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("  AC/DC: Live?  ", "AC-DC- Live-"),
            ("...hidden...", "hidden"),
            ("a//b", "a-b"),
        ],
    )
    def test_sanitize_filename_replaces_invalid_characters(self, name, expected):
        assert TrackProcessor().sanitize_filename(name) == expected

    @pytest.mark.parametrize(
        ("track", "expected"),
        [
            ({"duration_seconds": 125}, (125, "2:05")),
            ({"duration": "4:05"}, (245, "4:05")),
            ({"duration": "bad"}, (None, "0:00")),
            ({"duration": "1:2:3:4"}, (None, "0:00")),
            ({}, (0, "0:00")),
        ],
    )
    def test_parse_duration_handles_seconds_and_text_formats(self, track, expected):
        assert TrackProcessor().parse_duration(track) == expected

    @pytest.mark.parametrize(
        ("track", "expected"),
        [
            ({}, ("Unknown Album", "Unknown Artist")),
            ({"album": "Loose"}, ("Loose", "Unknown Artist")),
            ({"album": {"name": "X", "artists": [{"name": "Y"}]}}, ("X", "Y")),
            ({"album": {"artist": [{"name": "Z - Topic"}]}}, ("Unknown Album", "Z")),
            ({"album": {"name": "X", "artist": "Plain"}}, ("X", "Plain")),
            ({"album": {"name": "X"}}, ("X", "Unknown Artist")),
        ],
    )
    def test_extract_album_info_handles_album_shapes(self, track, expected):
        assert TrackProcessor().extract_album_info(track) == expected

    def test_extract_year_prefers_track_year(self):
        processor = TrackProcessor()

        assert processor.extract_year({"year": 2020, "album": {"year": 1990}}) == 2020
        assert not processor.extract_year({"album": "Loose"})
