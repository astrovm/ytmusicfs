from ytmusicfs.processor import TrackProcessor


def test_process_tracks_adds_stable_suffix_for_duplicate_filenames():
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


def test_extract_track_info_handles_missing_uploader():
    processor = TrackProcessor()

    result = processor.extract_track_info(
        {"title": "Song", "videoId": "video-one", "uploader": None}
    )

    assert result["artist"] == "Unknown Artist"
