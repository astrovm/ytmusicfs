#!/usr/bin/env python3

import concurrent.futures
import json
import logging
import shutil
import sqlite3
import stat
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import Mock, patch

from ytmusicfs.cache import CacheManager

FILE_ATTRS = {"st_mode": stat.S_IFREG | 0o644, "st_size": 10}
DIR_ATTRS = {"st_mode": stat.S_IFDIR | 0o755}


def _thread_manager():
    thread_manager = Mock()
    thread_manager.create_lock.side_effect = threading.RLock
    return thread_manager


class TestCacheManager(unittest.TestCase):
    """CacheManager behavior against a real SQLite database in a temp dir."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir)
        self.logger = logging.getLogger("test")
        self._extra_caches = []
        self.cache = CacheManager(
            thread_manager=_thread_manager(),
            cache_dir=self.temp_dir,
            logger=self.logger,
        )

    def tearDown(self):
        for cache in self._extra_caches:
            cache.close()
        self.cache.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _reopen(self):
        """Close the cache and open a fresh instance on the same directory."""
        self.cache.close()
        self.cache = CacheManager(
            thread_manager=_thread_manager(),
            cache_dir=self.temp_dir,
            logger=self.logger,
        )
        return self.cache

    def _new_cache(self):
        cache = CacheManager(
            thread_manager=_thread_manager(),
            cache_dir=self.temp_dir,
            logger=self.logger,
        )
        self._extra_caches.append(cache)
        return cache

    def _row_count(self, table="cache_entries"):
        return self.cache.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def _insert_raw(self, key, entry):
        self.cache.conn.execute(
            "INSERT OR REPLACE INTO cache_entries (key, entry) VALUES (?, ?)",
            (self.cache.path_to_key(key), entry),
        )
        self.cache.conn.commit()

    def _forget_memory(self):
        self.cache.hotcache.clear()
        self.cache.directory_listings_cache.clear()
        self.cache.path_validation_cache.clear()
        self.cache.attrs_cache.clear()
        self.cache.valid_paths.clear()
        self.cache.path_types.clear()

    @contextmanager
    def _broken_db(self):
        """Swap the connection for one whose every query raises sqlite3.Error."""
        real_conn = self.cache.conn
        broken = Mock()
        broken.cursor.side_effect = sqlite3.OperationalError("db down")
        broken.execute.side_effect = sqlite3.OperationalError("db down")
        broken.commit.side_effect = sqlite3.OperationalError("db down")
        self.cache.conn = broken
        try:
            with self.assertLogs(self.logger, level="WARNING"):
                yield
        finally:
            self.cache.conn = real_conn

    # --- get / set / delete ---------------------------------------------

    def test_default_cache_dir_is_created_under_home(self):
        home = self.cache_dir / "home"
        with patch("ytmusicfs.cache.Path.home", return_value=home):
            cache = CacheManager(thread_manager=_thread_manager(), logger=self.logger)
        self._extra_caches.append(cache)

        self.assertEqual(cache.cache_dir, home / ".cache" / "ytmusicfs")
        self.assertTrue(cache.cache_dir.is_dir())
        cache.set("/k", 1)
        self.assertEqual(cache.get("/k"), 1)

    def test_get_returns_value_written_by_set(self):
        self.cache.set("key", {"name": "Test", "id": 123})
        self.cache.hotcache.clear()

        self.assertEqual(self.cache.get("key"), {"name": "Test", "id": 123})
        self.assertEqual(self.cache.stats["db_hits"], 1)
        self.assertEqual(self.cache.get("key"), {"name": "Test", "id": 123})
        self.assertEqual(self.cache.stats["hits"], 1)

    def test_get_returns_none_for_missing_key(self):
        self.assertIsNone(self.cache.get("missing"))
        self.assertEqual(self.cache.stats["db_misses"], 1)

    def test_get_returns_none_for_expired_entry(self):
        self.cache.set("key", "value")
        later = time.time() + self.cache.cache_timeout + 1
        with patch("ytmusicfs.cache.time.time", return_value=later):
            self.assertIsNone(self.cache.get("key"))
        self.assertEqual(self.cache.stats["db_misses"], 1)

    def test_get_returns_none_when_database_errors(self):
        with self._broken_db():
            self.assertIsNone(self.cache.get("key"))
        self.assertEqual(self.cache.stats["db_misses"], 1)

    def test_set_logs_and_skips_unserializable_value(self):
        with self.assertLogs(self.logger, level="ERROR"):
            self.cache.set("key", object())
        self._assert_missing_after_hotcache_clear("key")

    def _assert_missing_after_hotcache_clear(self, key):
        self.cache.hotcache.clear()
        self.assertIsNone(self.cache.get(key))

    def test_set_batch_values_readable_after_hotcache_cleared(self):
        entries = {"key1": "value1", "key2": {"name": "value2"}, "key3": [1, 2, 3]}
        self.cache.set_batch(entries)
        self.cache.hotcache.clear()

        for key, expected in entries.items():
            self.assertEqual(self.cache.get(key), expected)

    def test_set_batch_ignores_empty_mapping(self):
        rows_before = self._row_count()
        self.cache.set_batch({})
        self.assertEqual(self._row_count(), rows_before)

    def test_set_batch_logs_and_skips_unserializable_value(self):
        rows_before = self._row_count()
        with self.assertLogs(self.logger, level="ERROR"):
            self.cache.set_batch({"ok": 1, "bad": object()})
        self.assertEqual(self._row_count(), rows_before)

    def test_delete_removes_hot_and_persisted_value(self):
        self.cache.set("key", "value")
        self.cache.delete("key")
        self.assertNotIn("hotcache:key", self.cache.hotcache)
        self.assertIsNone(self.cache.get("key"))

    def test_delete_logs_database_error(self):
        self.cache.set("key", "value")
        with self._broken_db():
            self.cache.delete("key")
        self.assertNotIn("hotcache:key", self.cache.hotcache)

    # --- get_many ----------------------------------------------------------

    def test_get_many_returns_hot_entries_without_database_read(self):
        self.cache.set("a", 1)
        with patch.object(self.cache, "conn") as conn:
            self.assertEqual(self.cache.get_many(["a"]), {"a": 1})
        conn.execute.assert_not_called()

    def test_get_many_reads_database_in_chunks(self):
        self.cache.GET_MANY_CHUNK_SIZE = 2
        self.cache.set_batch({f"k{i}": i for i in range(5)})
        self.cache.hotcache.clear()

        self.assertEqual(
            self.cache.get_many([f"k{i}" for i in range(5)]),
            {f"k{i}": i for i in range(5)},
        )

    def test_get_many_skips_expired_and_corrupt_rows(self):
        self.cache.set("old", 1)
        self._insert_raw("corrupt", "not json")
        self.cache.hotcache.clear()
        later = time.time() + self.cache.cache_timeout + 1
        with patch("ytmusicfs.cache.time.time", return_value=later):
            self.cache.set("fresh", 2)
            self.cache.hotcache.clear()
            result = self.cache.get_many(["old", "corrupt", "fresh"])
        self.assertEqual(result, {"fresh": 2})

    def test_get_many_returns_hot_results_when_database_errors(self):
        self.cache.set("hot", 1)
        with self._broken_db():
            self.assertEqual(self.cache.get_many(["hot", "cold"]), {"hot": 1})

    # --- write batching ----------------------------------------------------

    def test_pending_writes_commit_at_fixed_interval(self):
        self.cache.flush()
        for i in range(self.cache.WRITE_COMMIT_INTERVAL - 1):
            self.cache.set(f"key{i}", i)
        self.assertTrue(self.cache.conn.in_transaction)

        self.cache.set("last", 0)

        self.assertFalse(self.cache.conn.in_transaction)
        self.assertEqual(self.cache._pending_writes, 0)

    def test_forced_write_commits_immediately(self):
        self.cache.set("valid_files:/albums/X", ["a.m4a"])
        self.assertFalse(self.cache.conn.in_transaction)
        self.assertEqual(self.cache._pending_writes, 0)

    def test_flush_makes_pending_write_visible_to_new_connection(self):
        self.cache.set("pending-key", {"value": 1})
        self.cache.flush()
        self.assertEqual(self._new_cache().get("pending-key"), {"value": 1})

    def test_flush_without_pending_writes_is_noop(self):
        self.cache.flush()
        with patch.object(self.cache, "conn") as conn:
            self.cache.flush()
        conn.commit.assert_not_called()
        self.assertEqual(self.cache._pending_writes, 0)
        self.assertFalse(self.cache.conn.in_transaction)

    # --- keys ----------------------------------------------------------------

    def test_path_to_key_hashes_long_paths_reversibly(self):
        long_path = "/playlists/" + "x" * 300
        key = self.cache.path_to_key(long_path)

        self.assertLessEqual(len(key), 200)
        self.assertEqual(self.cache.key_to_path(key), long_path)
        self.assertEqual(self._row_count("hash_mappings"), 1)

    def test_key_to_path_returns_hashed_key_without_mapping(self):
        key = "a" * 30 + "_" + "0" * 32
        with self.assertLogs(self.logger, level="WARNING"):
            self.assertEqual(self.cache.key_to_path(key), key)

    def test_key_to_path_reverses_simple_keys(self):
        self.assertEqual(self.cache.key_to_path("_albums_X"), "/albums/X")

    def test_path_to_key_still_returns_key_when_mapping_store_fails(self):
        long_path = "/" + "y" * 300
        with self._broken_db():
            key = self.cache.path_to_key(long_path)
        self.assertLessEqual(len(key), 200)

    def test_get_original_path_returns_none_on_database_error(self):
        with self._broken_db():
            self.assertIsNone(self.cache.get_original_path("anything"))

    # --- durations ------------------------------------------------------------

    def test_set_durations_batch_persists_durations(self):
        self.cache.set_durations_batch({"v1": 100, "v2": 200})
        self.cache.hotcache.clear()
        self.assertEqual(self.cache.get_duration("v1"), 100)
        self.assertEqual(self.cache.get_duration("v2"), 200)
        self.assertIsNone(self.cache.get_duration("v3"))

    def test_set_durations_batch_ignores_empty_mapping(self):
        rows_before = self._row_count()
        self.cache.set_durations_batch({})
        self.assertEqual(self._row_count(), rows_before)

    # --- valid paths ---------------------------------------------------------

    def test_mark_valid_ignores_root(self):
        rows_before = self._row_count()
        self.cache.mark_valid("/", is_directory=True)
        self.assertEqual(self._row_count(), rows_before)

    def test_mark_valid_file_removes_stale_directory_entry(self):
        path = "/playlists/my_playlist/song.m4a"
        self.cache.mark_valid(path, is_directory=True)
        self.assertEqual(self.cache.get_entry_type(path), "directory")

        self.cache.mark_valid(path, is_directory=False)

        self.assertEqual(self.cache.get_entry_type(path), "file")
        self.assertFalse(self.cache.is_directory(path))

    def test_mark_valid_keeps_memory_state_when_database_errors(self):
        with self._broken_db():
            self.cache.mark_valid("/albums/X", is_directory=True)
        self.assertIn("/albums/X", self.cache.valid_paths)
        self.assertEqual(self.cache.path_types["/albums/X"], "directory")

    def test_reopen_loads_valid_paths_and_types(self):
        self.cache.mark_valid("/albums/Album", is_directory=True)
        self.cache.mark_valid("/albums/Album/Song.m4a", is_directory=False)
        self.cache.set_directory_listing_with_attrs(
            "/playlists/Mix", {"Track.m4a": FILE_ATTRS}
        )

        cache = self._reopen()

        self.assertIn("/albums/Album", cache.valid_paths)
        self.assertEqual(cache.path_types["/albums/Album"], "directory")
        self.assertEqual(cache.path_types["/albums/Album/Song.m4a"], "file")
        # Listing children are persisted without an entry_type column.
        self.assertIn("/playlists/Mix/Track.m4a", cache.valid_paths)
        self.assertNotIn("/playlists/Mix/Track.m4a", cache.path_types)

    def test_load_valid_paths_restores_paths_with_underscores_and_spaces(self):
        self.cache.mark_valid("/liked_songs/My Song_1.m4a", is_directory=False)
        self.cache.set_directory_listing_with_attrs(
            "/playlists/Road Trip_2", {"Best_Track.m4a": FILE_ATTRS}
        )

        cache = self._reopen()

        self.assertIn("/liked_songs/My Song_1.m4a", cache.valid_paths)
        self.assertIn("/playlists/Road Trip_2/Best_Track.m4a", cache.valid_paths)
        self.assertNotIn("/liked/songs/My/Song/1.m4a", cache.valid_paths)

    def test_load_valid_paths_skips_rows_without_stored_path(self):
        self.cache.conn.executemany(
            "INSERT INTO cache_entries (key, entry, entry_type) VALUES (?, ?, ?)",
            [
                ("exact_path:_liked_songs_a.m4a", '{"data": true}', "file"),
                ("valid_dir:_albums_X", "[]", "directory"),
            ],
        )
        self.cache.conn.commit()

        cache = self._reopen()

        self.assertNotIn("/liked/songs/a.m4a", cache.valid_paths)
        self.assertNotIn("/albums/X", cache.valid_paths)

    def test_load_valid_paths_skips_rows_with_non_string_path(self):
        self._insert_raw("exact_path:/a.m4a", json.dumps({"path": 42}))
        self._insert_raw("exact_path:/b.m4a", json.dumps({"path": "/b.m4a"}))

        cache = self._reopen()

        self.assertIn("/b.m4a", cache.valid_paths)
        self.assertNotIn(42, cache.valid_paths)
        self.assertNotIn("/a.m4a", cache.valid_paths)

    def test_load_valid_paths_survives_database_error(self):
        with self._broken_db():
            self.cache._load_valid_paths()

    def test_is_valid_path_accepts_static_directories(self):
        for path in CacheManager.STATIC_DIRECTORIES:
            self.assertTrue(self.cache.is_valid_path(path))

    def test_is_valid_path_uses_known_valid_paths(self):
        self.cache.valid_paths.add("/albums/X")
        self.cache.path_types["/albums/X"] = "directory"
        self.assertTrue(self.cache.is_valid_path("/albums/X"))
        self.assertTrue(self.cache.path_validation_cache["/albums/X"]["is_directory"])

    def test_is_valid_path_finds_child_in_memory_listing(self):
        self.cache.set_directory_listing_with_attrs(
            "/playlists/Mix", {"a.m4a": FILE_ATTRS, "Sub": DIR_ATTRS}
        )
        self.assertTrue(self.cache.is_valid_path("/playlists/Mix/a.m4a"))
        self.assertTrue(self.cache.is_valid_path("/playlists/Mix/Sub"))
        self.assertEqual(self.cache.path_types["/playlists/Mix/a.m4a"], "file")
        self.assertEqual(self.cache.path_types["/playlists/Mix/Sub"], "directory")

    def test_is_valid_path_finds_child_in_persisted_listing(self):
        self.cache.set_directory_listing_with_attrs(
            "/playlists/Mix", {"a.m4a": FILE_ATTRS}
        )
        self._forget_memory()

        self.assertTrue(self.cache.is_valid_path("/playlists/Mix/a.m4a"))
        self.assertGreaterEqual(self.cache.stats["db_hits"], 1)

    def test_is_valid_path_falls_back_to_valid_files_when_listing_lacks_child(self):
        self.cache.set_directory_listing_with_attrs(
            "/playlists/Mix", {"a.m4a": FILE_ATTRS}
        )
        self.cache.set("valid_files:/playlists/Mix", ["a.m4a", "b.m4a"])

        self.assertTrue(self.cache.is_valid_path("/playlists/Mix/b.m4a"))
        self.assertFalse(self.cache.is_valid_path("/playlists/Mix/c.m4a"))

    def test_is_valid_path_finds_child_in_valid_files(self):
        self.cache.mark_valid("/albums/X", is_directory=True)
        self.cache.set("valid_files:/albums/X", ["t.m4a"])

        self.assertTrue(self.cache.is_valid_path("/albums/X/t.m4a"))
        self.assertEqual(self.cache.path_types["/albums/X/t.m4a"], "file")

    def test_is_valid_path_finds_prefixed_database_entry(self):
        self.cache.mark_valid("/x/file", is_directory=False)
        self._forget_memory()

        self.assertTrue(self.cache.is_valid_path("/x/file"))
        self.assertEqual(self.cache.path_types["/x/file"], "file")

    def test_is_valid_path_caches_negative_result(self):
        self.assertFalse(self.cache.is_valid_path("/nope/file"))
        self.assertFalse(self.cache.path_validation_cache["/nope/file"]["valid"])
        hits = self.cache.stats["hits"]

        self.assertFalse(self.cache.is_valid_path("/nope/file"))
        self.assertEqual(self.cache.stats["hits"], hits + 1)

    def test_is_valid_path_rejects_relative_path(self):
        self.assertFalse(self.cache.is_valid_path("relative"))

    def test_is_valid_path_returns_false_when_database_errors(self):
        self.cache.valid_paths.add("/x")
        self.cache.path_validation_cache["/x"] = {"valid": True, "time": 1e18}
        with self._broken_db():
            self.assertFalse(self.cache.is_valid_path("/x/file"))

    # --- entry types -----------------------------------------------------------

    def test_get_entry_type_reports_static_directories(self):
        self.assertEqual(self.cache.get_entry_type("/albums"), "directory")

    def test_get_entry_type_reads_persisted_type(self):
        self.cache.mark_valid("/albums/X", is_directory=True)
        self.cache.path_types.clear()
        self.assertEqual(self.cache.get_entry_type("/albums/X"), "directory")
        self.assertEqual(self.cache.path_types["/albums/X"], "directory")

    def test_get_entry_type_infers_type_from_parent_listing(self):
        self.cache.set_directory_listing_with_attrs(
            "/liked_songs", {"a.m4a": FILE_ATTRS, "Dir": DIR_ATTRS}
        )
        self.assertEqual(self.cache.get_entry_type("/liked_songs/a.m4a"), "file")
        self.assertEqual(self.cache.get_entry_type("/liked_songs/Dir"), "directory")

    def test_get_entry_type_returns_none_for_unknown_path(self):
        self.assertIsNone(self.cache.get_entry_type("/nowhere/x"))
        self.assertIsNone(self.cache.is_directory("/nowhere/x"))

    def test_get_entry_type_returns_none_when_database_errors(self):
        with self._broken_db():
            self.assertIsNone(self.cache.get_entry_type("/albums/X"))

    # --- directory listings ---------------------------------------------------

    def test_directory_listing_round_trips_through_database(self):
        listing = {
            ".": DIR_ATTRS,
            "..": DIR_ATTRS,
            "song1.m4a": {**FILE_ATTRS, "st_size": 5242880},
            "Sub": DIR_ATTRS,
        }
        self.cache.set_directory_listing_with_attrs("/playlists/Mix", listing)
        self.assertEqual(
            self.cache.get_directory_listing_with_attrs("/playlists/Mix"), listing
        )

        self.cache.directory_listings_cache.clear()

        self.assertEqual(
            self.cache.get_directory_listing_with_attrs("/playlists/Mix"), listing
        )
        self.assertIn("/playlists/Mix", self.cache.directory_listings_cache)
        self.assertNotIn("/playlists/Mix/.", self.cache.attrs_cache)
        keys = {
            row[0] for row in self.cache.conn.execute("SELECT key FROM cache_entries")
        }
        self.assertIn(self.cache.path_to_key("valid_dir:/playlists/Mix/Sub"), keys)
        self.assertIn(
            self.cache.path_to_key("exact_path:/playlists/Mix/song1.m4a"), keys
        )

    def test_directory_listing_persists_across_instances(self):
        listing = {"file1.txt": FILE_ATTRS, "file2.txt": FILE_ATTRS}
        self.cache.set_directory_listing_with_attrs("/test/dir", listing)

        cache = self._reopen()

        self.assertEqual(cache.get_directory_listing_with_attrs("/test/dir"), listing)

    def test_set_directory_listing_skips_empty_listing(self):
        rows_before = self._row_count()
        self.cache.set_directory_listing_with_attrs("/empty", {})
        self.assertEqual(self._row_count(), rows_before)
        self.assertNotIn("/empty", self.cache.directory_listings_cache)

    def test_set_directory_listing_skips_child_rows_when_listing_unserializable(self):
        rows_before = self._row_count()
        with self.assertLogs(self.logger, level="WARNING"):
            self.cache.set_directory_listing_with_attrs(
                "/bad", {"a": {"st_mode": 0, "obj": object()}}
            )
        self.assertEqual(self._row_count(), rows_before)

    def test_get_directory_listing_returns_none_for_missing_listing(self):
        self.assertIsNone(self.cache.get_directory_listing_with_attrs("/none"))
        self.assertEqual(self.cache.stats["db_misses"], 1)

    def test_get_directory_listing_returns_none_when_expired(self):
        self.cache.set_directory_listing_with_attrs("/d", {"a": FILE_ATTRS})
        later = time.time() + self.cache.cache_timeout + 1
        with patch("ytmusicfs.cache.time.time", return_value=later):
            self.assertIsNone(self.cache.get_directory_listing_with_attrs("/d"))

    def test_get_directory_listing_returns_none_for_corrupt_row(self):
        self._insert_raw("/bad_listing_with_attrs", "not json")
        with self.assertLogs(self.logger, level="WARNING"):
            self.assertIsNone(self.cache.get_directory_listing_with_attrs("/bad"))

    def test_get_directory_listing_returns_none_for_non_dict_data(self):
        self.cache.set("/odd_listing_with_attrs", [1, 2])
        self.assertIsNone(self.cache.get_directory_listing_with_attrs("/odd"))
        self.assertNotIn("/odd", self.cache.directory_listings_cache)

    def test_get_directory_listing_returns_none_when_database_errors(self):
        with self._broken_db():
            self.assertIsNone(self.cache.get_directory_listing_with_attrs("/d"))

    # --- file attributes -----------------------------------------------------

    def test_get_file_attrs_from_parent_dir_reads_listing_attrs(self):
        listing = {"file.txt": {**FILE_ATTRS, "st_size": 1024}}
        self.cache.set_directory_listing_with_attrs("/test/dir", listing)
        self.cache.attrs_cache.clear()

        result = self.cache.get_file_attrs_from_parent_dir("/test/dir/file.txt")

        self.assertEqual(result, listing["file.txt"])
        self.assertIn("/test/dir/file.txt", self.cache.attrs_cache)

    def test_get_file_attrs_from_parent_dir_prefers_attrs_cache(self):
        self.cache.attrs_cache["/a/b"] = {"st_size": 5}
        self.assertEqual(
            self.cache.get_file_attrs_from_parent_dir("/a/b"), {"st_size": 5}
        )

    def test_get_file_attrs_from_parent_dir_returns_none_without_parent(self):
        self.assertIsNone(self.cache.get_file_attrs_from_parent_dir("song.m4a"))
        self.assertIsNone(self.cache.get_file_attrs_from_parent_dir("/"))

    def test_get_file_attrs_from_parent_dir_returns_none_for_file_without_listing(self):
        path = "/liked_songs/song.m4a"
        self.cache.mark_valid(path, is_directory=False)
        self.assertIsNone(self.cache.get_file_attrs_from_parent_dir(path))

    def test_get_file_attrs_from_parent_dir_builds_registry_directory_attrs(self):
        self.cache.mark_valid("/playlists/Mix", is_directory=True)
        attrs = self.cache.get_file_attrs_from_parent_dir("/playlists/Mix")
        self.assertTrue(stat.S_ISDIR(attrs["st_mode"]))

    def test_get_file_attrs_from_parent_dir_rejects_unknown_registry_entry(self):
        self.assertIsNone(self.cache.get_file_attrs_from_parent_dir("/albums/Ghost"))

    def test_get_file_attrs_from_parent_dir_infers_file_from_grandparent(self):
        self.cache.set_directory_listing_with_attrs("/playlists", {"Mix": DIR_ATTRS})
        self.cache.mark_valid("/playlists/Mix/song.m4a", is_directory=False)

        attrs = self.cache.get_file_attrs_from_parent_dir("/playlists/Mix/song.m4a")

        self.assertTrue(stat.S_ISREG(attrs["st_mode"]))

    def test_get_file_attrs_from_parent_dir_infers_directory_from_grandparent(self):
        self.cache.set_directory_listing_with_attrs("/playlists", {"Mix": DIR_ATTRS})
        self.cache.mark_valid("/playlists/Mix/Sub", is_directory=True)

        attrs = self.cache.get_file_attrs_from_parent_dir("/playlists/Mix/Sub")

        self.assertTrue(stat.S_ISDIR(attrs["st_mode"]))

    def test_get_file_attrs_from_parent_dir_ignores_unknown_grandchild(self):
        self.cache.set_directory_listing_with_attrs("/playlists", {"Mix": DIR_ATTRS})
        self.assertIsNone(
            self.cache.get_file_attrs_from_parent_dir("/playlists/Mix/unknown.m4a")
        )

    def test_get_file_attrs_from_parent_dir_ignores_non_directory_parent(self):
        self.cache.set_directory_listing_with_attrs("/playlists", {"Mix": FILE_ATTRS})
        self.cache.mark_valid("/playlists/Mix/song.m4a", is_directory=False)
        self.assertIsNone(
            self.cache.get_file_attrs_from_parent_dir("/playlists/Mix/song.m4a")
        )

    def test_update_file_attrs_without_listing_caches_attrs_only(self):
        rows_before = self._row_count()
        self.cache.update_file_attrs_in_parent_dir("/none/a.m4a", {"st_size": 7})
        self.assertEqual(self.cache.attrs_cache["/none/a.m4a"], {"st_size": 7})
        self.assertEqual(self._row_count(), rows_before)

    # --- unavailable tracks --------------------------------------------------

    def test_mark_unavailable_track_records_video_id_and_path(self):
        path = "/liked_songs/song.m4a"
        self.cache.mark_unavailable_track("abc123", path, "Video unavailable")
        self.cache.hotcache.clear()

        track = self.cache.get_unavailable_track("abc123")
        self.assertEqual(track["videoId"], "abc123")
        self.assertEqual(track["reason"], "Video unavailable")
        self.assertTrue(self.cache.is_track_unavailable("abc123"))
        self.assertTrue(self.cache.is_path_unavailable(path))
        self.assertEqual(self.cache.get_unavailable_video_ids(), {"abc123"})

    def test_mark_unavailable_track_ignores_empty_video_id(self):
        self.cache.mark_unavailable_track("", "/liked_songs/a.m4a", "gone")
        self.assertEqual(self.cache.get_unavailable_video_ids(), set())
        self.assertFalse(self.cache.is_path_unavailable("/liked_songs/a.m4a"))

    def test_mark_unavailable_track_without_path_records_only_video_id(self):
        self.cache.mark_unavailable_track("abc", None, "gone")
        self.cache.mark_unavailable_track("def", "song.m4a", "gone")
        self.assertEqual(self.cache.get_unavailable_video_ids(), {"abc", "def"})
        self.assertEqual(self.cache.unavailable_paths, {"song.m4a"})

    def test_mark_unavailable_track_invalidates_stale_path_caches(self):
        path = "/liked_songs/song.m4a"
        parent_dir = "/liked_songs"
        self.cache.set_directory_listing_with_attrs(
            parent_dir, {"song.m4a": FILE_ATTRS}
        )
        self.cache.mark_valid(path, is_directory=False)
        self.cache.set(f"video_id:{path}", "abc123")
        self.cache.set(f"valid_files:{parent_dir}", ["song.m4a"])
        self.assertTrue(self.cache.is_valid_path(path))

        self.cache.mark_unavailable_track("abc123", path, "Video unavailable")

        self.assertNotIn(path, self.cache.valid_paths)
        self.assertNotIn(path, self.cache.path_types)
        self.assertNotIn(path, self.cache.path_validation_cache)
        self.assertNotIn(path, self.cache.attrs_cache)
        self.assertNotIn(parent_dir, self.cache.directory_listings_cache)
        self._assert_missing_after_hotcache_clear(f"video_id:{path}")
        self.assertIsNone(self.cache.get(f"valid_files:{parent_dir}"))
        self.assertIsNone(self.cache.get_directory_listing_with_attrs(parent_dir))
        self.assertIsNone(self.cache.get(f"exact_path:{path}"))

    def test_get_unavailable_track_returns_none_for_empty_or_non_dict(self):
        self.assertIsNone(self.cache.get_unavailable_track(""))
        self.cache.set("unavailable:weird", "not a dict")
        self.assertIsNone(self.cache.get_unavailable_track("weird"))

    def test_get_unavailable_tracks_lists_valid_entries_only(self):
        self.cache.mark_unavailable_track("a", "/liked_songs/a.m4a", "gone")
        self.cache.mark_unavailable_track("b", None, "gone")
        self.cache.set("unavailable:list", [1])
        self._insert_raw("unavailable:corrupt", "garbage")

        tracks = self.cache.get_unavailable_tracks()

        self.assertEqual(sorted(t["videoId"] for t in tracks), ["a", "b"])

    def test_get_unavailable_tracks_returns_empty_on_database_error(self):
        with self._broken_db():
            self.assertEqual(self.cache.get_unavailable_tracks(), [])

    def test_clear_unavailable_track_removes_state(self):
        path = "/liked_songs/a.m4a"
        self.cache.mark_unavailable_track("a", path, "gone")

        self.cache.clear_unavailable_track("a", path)
        self.cache.clear_unavailable_track("")

        self.assertFalse(self.cache.is_track_unavailable("a"))
        self.assertFalse(self.cache.is_path_unavailable(path))
        self.assertIsNone(self.cache.get_unavailable_track("a"))

    def test_clear_unavailable_track_without_path_keeps_paths(self):
        self.cache.mark_unavailable_track("a", "/liked_songs/a.m4a", "gone")
        self.cache.clear_unavailable_track("a")
        self.assertFalse(self.cache.is_track_unavailable("a"))
        self.assertTrue(self.cache.is_path_unavailable("/liked_songs/a.m4a"))

    def test_reopen_loads_unavailable_tracks(self):
        # YouTube video IDs routinely contain "_" which path keys can't round-trip.
        self.cache.mark_unavailable_track("ab_c-1", "/liked_songs/a.m4a", "gone")
        self.cache.mark_unavailable_track("noPath", None, "gone")
        self._insert_raw("unavailable:corrupt", "garbage")
        self._insert_raw("unavailable:listy", json.dumps([1]))

        cache = self._reopen()

        self.assertTrue(cache.is_track_unavailable("ab_c-1"))
        self.assertTrue(cache.is_track_unavailable("noPath"))
        self.assertTrue(cache.is_track_unavailable("corrupt"))
        self.assertEqual(cache.unavailable_paths, {"/liked_songs/a.m4a"})

    def test_reopen_uses_key_id_when_unavailable_row_lacks_video_id(self):
        self._insert_raw(
            "unavailable:keyonly", json.dumps({"data": {"path": "/albums/A/x.m4a"}})
        )
        self._insert_raw(
            "unavailable:blankid",
            json.dumps({"data": {"videoId": "", "path": 7}}),
        )
        self._insert_raw("unavailable:scalar", json.dumps({"data": "gone"}))

        cache = self._reopen()

        self.assertTrue(cache.is_track_unavailable("keyonly"))
        self.assertTrue(cache.is_track_unavailable("blankid"))
        self.assertFalse(cache.is_track_unavailable(""))
        self.assertTrue(cache.is_track_unavailable("scalar"))
        self.assertEqual(cache.unavailable_paths, {"/albums/A/x.m4a"})

    def test_load_unavailable_tracks_survives_database_error(self):
        with self._broken_db():
            self.cache._load_unavailable_tracks()

    # --- no-replacement markers ----------------------------------------------

    def test_is_no_replacement_expires_after_ttl(self):
        now = time.time()
        with patch("ytmusicfs.cache.time.time", return_value=now):
            self.cache.mark_no_replacement("dead123", "/liked_songs/a.m4a", ttl=10)
            self.assertTrue(self.cache.is_no_replacement("dead123"))
        with patch("ytmusicfs.cache.time.time", return_value=now + 11):
            self.assertFalse(self.cache.is_no_replacement("dead123"))
        self.assertIsNone(self.cache.get("no_replacement:dead123"))

    def test_is_no_replacement_tracks_video_ids_independently(self):
        self.cache.mark_no_replacement("dead1", "/liked_songs/song1.m4a")
        self.cache.mark_no_replacement("dead2", "/liked_songs/song2.m4a")

        self.assertTrue(self.cache.is_no_replacement("dead1"))
        self.assertTrue(self.cache.is_no_replacement("dead2"))
        self.assertFalse(self.cache.is_no_replacement("live3"))

    def test_is_no_replacement_ignores_non_dict_value(self):
        self.cache.set("no_replacement:v", "yes")
        self.assertFalse(self.cache.is_no_replacement("v"))

    # --- refresh metadata ----------------------------------------------------

    def test_refresh_metadata_round_trips(self):
        self.cache.set_refresh_metadata("playlist_registry", 123.0, "pending")
        self.assertEqual(
            self.cache.get_refresh_metadata("playlist_registry"), (123.0, "pending")
        )

    def test_set_refresh_metadata_coerces_invalid_status_to_fresh(self):
        with self.assertLogs(self.logger, level="WARNING"):
            self.cache.set_refresh_metadata("k", 1.0, "bogus")
        self.assertEqual(self.cache.get_refresh_metadata("k"), (1.0, "fresh"))

    def test_get_refresh_metadata_returns_none_for_unknown_key(self):
        self.assertEqual(self.cache.get_refresh_metadata("unknown"), (None, None))

    def test_refresh_metadata_handles_database_errors(self):
        with self._broken_db():
            self.cache.set_refresh_metadata("k", 1.0)
            self.assertEqual(self.cache.get_refresh_metadata("k"), (None, None))

    # --- trigger files -------------------------------------------------------

    def test_record_cache_trigger_writes_timestamp_file(self):
        before = time.time()
        self.cache.record_cache_trigger("refresh")
        after = time.time()
        trigger = self.cache_dir / ".refresh_trigger"
        self.assertTrue(before <= float(trigger.read_text(encoding="utf-8")) <= after)

    def test_record_cache_trigger_rejects_unknown_action(self):
        with self.assertRaises(ValueError):
            self.cache.record_cache_trigger("explode")

    def test_record_cache_trigger_logs_write_failure(self):
        with (
            patch.object(Path, "write_text", side_effect=OSError("disk full")),
            self.assertLogs(self.logger, level="WARNING"),
        ):
            self.cache.record_cache_trigger("clear")
        self.assertIsNone(self.cache.get_pending_cache_trigger())

    def test_get_pending_cache_trigger_detects_trigger_files(self):
        self.assertIsNone(self.cache.get_pending_cache_trigger())
        (self.cache_dir / ".refresh_trigger").write_text("1", encoding="utf-8")
        self.assertEqual(self.cache.get_pending_cache_trigger(), "refresh")
        (self.cache_dir / ".clear_trigger").write_text("1", encoding="utf-8")
        self.assertEqual(self.cache.get_pending_cache_trigger(), "clear")

    def test_clear_cache_trigger_removes_file(self):
        trigger = self.cache_dir / ".refresh_trigger"
        trigger.write_text("123", encoding="utf-8")
        self.cache.clear_cache_trigger("refresh")
        self.assertFalse(trigger.exists())
        self.cache.clear_cache_trigger("refresh")

    def test_clear_cache_trigger_logs_unlink_failure(self):
        trigger = self.cache_dir / ".clear_trigger"
        trigger.write_text("1", encoding="utf-8")
        with (
            patch.object(Path, "unlink", side_effect=OSError("busy")),
            self.assertLogs(self.logger, level="WARNING"),
        ):
            self.cache.clear_cache_trigger("clear")
        self.assertTrue(trigger.exists())

    def test_record_repair_trigger_writes_json_file(self):
        repairs = [{"old_video_id": "old1", "path": "/liked_songs/a.m4a"}]
        self.cache.record_repair_trigger(repairs)
        data = json.loads(
            (self.cache_dir / ".repair_trigger").read_text(encoding="utf-8")
        )
        self.assertEqual(data["repairs"], repairs)

    def test_record_repair_trigger_ignores_empty_repairs(self):
        self.cache.record_repair_trigger([])
        self.assertFalse((self.cache_dir / ".repair_trigger").exists())

    def test_record_repair_trigger_removes_temp_file_on_failure(self):
        with (
            patch.object(Path, "rename", side_effect=OSError("cross-device")),
            self.assertLogs(self.logger, level="WARNING"),
        ):
            self.cache.record_repair_trigger([{"old_video_id": "x"}])
        self.assertEqual(list(self.cache_dir.glob(".repair_trigger*")), [])

    def test_get_pending_repair_trigger_parses_file(self):
        (self.cache_dir / ".repair_trigger").write_text(
            json.dumps({"timestamp": 123, "repairs": [{"old_video_id": "old1"}]}),
            encoding="utf-8",
        )
        result = self.cache.get_pending_repair_trigger()
        self.assertEqual(result["repairs"][0]["old_video_id"], "old1")

    def test_get_pending_repair_trigger_returns_none_for_missing_or_invalid(self):
        self.assertIsNone(self.cache.get_pending_repair_trigger())
        trigger = self.cache_dir / ".repair_trigger"
        trigger.write_text("[1, 2]", encoding="utf-8")
        self.assertIsNone(self.cache.get_pending_repair_trigger())
        trigger.write_text("not json", encoding="utf-8")
        with self.assertLogs(self.logger, level="WARNING"):
            self.assertIsNone(self.cache.get_pending_repair_trigger())

    def test_clear_repair_trigger_removes_file(self):
        trigger = self.cache_dir / ".repair_trigger"
        trigger.write_text("{}", encoding="utf-8")
        self.cache.clear_repair_trigger()
        self.assertFalse(trigger.exists())
        self.cache.clear_repair_trigger()

    def test_clear_repair_trigger_logs_unlink_failure(self):
        trigger = self.cache_dir / ".repair_trigger"
        trigger.write_text("{}", encoding="utf-8")
        with (
            patch.object(Path, "unlink", side_effect=OSError("busy")),
            self.assertLogs(self.logger, level="WARNING"),
        ):
            self.cache.clear_repair_trigger()
        self.assertTrue(trigger.exists())

    # --- repair invalidation -------------------------------------------------

    def test_invalidate_repaired_paths_clears_path_state(self):
        path = "/playlists/Mix/song.m4a"
        parent = "/playlists/Mix"
        self.cache.set_directory_listing_with_attrs(parent, {"song.m4a": FILE_ATTRS})
        self.cache.mark_unavailable_track("old1", path, "gone")
        self.cache.mark_valid(path, is_directory=False)
        self.cache.set(f"video_id:{path}", "old1")
        self.cache.set(f"{parent}_listing", ["song.m4a"])
        self.cache.hotcache[f"hotcache:{parent}_processed"] = {"data": []}

        self.cache.invalidate_repaired_paths([{"old_video_id": "old1", "path": path}])

        self.assertNotIn("old1", self.cache.unavailable_video_ids)
        self.assertNotIn(path, self.cache.unavailable_paths)
        self.assertNotIn(path, self.cache.valid_paths)
        self.assertNotIn(path, self.cache.path_validation_cache)
        self.assertNotIn(path, self.cache.attrs_cache)
        self.assertNotIn(parent, self.cache.directory_listings_cache)
        self.assertNotIn(f"hotcache:{parent}_processed", self.cache.hotcache)
        self.assertIsNone(self.cache.get(f"video_id:{path}"))
        self.assertIsNone(self.cache.get(f"{parent}_listing"))
        self.assertIsNone(self.cache.get_directory_listing_with_attrs(parent))

    def test_invalidate_repaired_paths_skips_incomplete_repairs(self):
        self.cache.unavailable_video_ids.add("old1")
        self.cache.directory_listings_cache["/p"] = {"data": {}, "time": time.time()}

        self.cache.invalidate_repaired_paths([])
        self.cache.invalidate_repaired_paths(
            [{"old_video_id": "old1"}, {"path": "/p/a"}]
        )

        self.assertIn("old1", self.cache.unavailable_video_ids)
        self.assertIn("/p", self.cache.directory_listings_cache)

    def test_invalidate_repaired_paths_handles_path_without_parent(self):
        self.cache.unavailable_video_ids.add("old1")
        self.cache.invalidate_repaired_paths([{"old_video_id": "old1", "path": "a"}])
        self.assertNotIn("old1", self.cache.unavailable_video_ids)

    # --- clearing / closing --------------------------------------------------

    def test_clear_metadata_empties_database_and_memory(self):
        self.cache.set("key", "value")
        self.cache.set_directory_listing_with_attrs("/liked_songs", {"a": FILE_ATTRS})
        self.cache.mark_unavailable_track("old1", "/liked_songs/a", "gone")
        self.cache.set_refresh_metadata("k", 1.0)
        self.cache.path_to_key("/" + "z" * 300)

        self.cache.clear_metadata()

        for table in ("cache_entries", "hash_mappings", "refresh_tracker"):
            self.assertEqual(self._row_count(table), 0)
        self.assertEqual(len(self.cache.hotcache), 0)
        self.assertEqual(len(self.cache.directory_listings_cache), 0)
        self.assertEqual(len(self.cache.valid_paths), 0)
        self.assertEqual(self.cache.unavailable_video_ids, set())
        self.assertEqual(self.cache.unavailable_paths, set())
        self.assertIsNone(self.cache.get("key"))

    def test_clear_metadata_keeps_memory_state_when_database_errors(self):
        self.cache.set("key", "value")
        with self._broken_db():
            self.cache.clear_metadata()
        self.assertIn("hotcache:key", self.cache.hotcache)

    def test_clear_all_removes_audio_and_ranges_dirs(self):
        audio_dir = self.cache_dir / "audio"
        audio_dir.mkdir()
        (audio_dir / "song.m4a").write_bytes(b"audio")
        ranges_dir = self.cache_dir / "ranges"
        ranges_dir.mkdir()

        self.cache.clear_all()

        self.assertFalse(audio_dir.exists())
        self.assertFalse(ranges_dir.exists())
        self.cache.clear_all()

    def test_clear_all_logs_directory_removal_failure(self):
        (self.cache_dir / "audio").mkdir()
        with (
            patch("ytmusicfs.cache.shutil.rmtree", side_effect=OSError("busy")),
            self.assertLogs(self.logger, level="WARNING"),
        ):
            self.cache.clear_all()
        self.assertTrue((self.cache_dir / "audio").exists())

    def test_close_is_idempotent(self):
        self.cache.close()
        self.cache.close()
        self.assertTrue(self.cache._closed)

    def test_close_logs_error_and_stays_open_on_failure(self):
        real_conn = self.cache.conn
        broken = Mock()
        broken.commit.side_effect = sqlite3.OperationalError("locked")
        self.cache.conn = broken
        try:
            with self.assertLogs(self.logger, level="ERROR"):
                self.cache.close()
            self.assertFalse(self.cache._closed)
        finally:
            self.cache.conn = real_conn

    # --- stats -----------------------------------------------------------------

    def test_get_cache_stats_reports_zero_rates_without_lookups(self):
        stats = self.cache.get_cache_stats()
        self.assertEqual(stats["memory_hit_rate"], 0.0)
        self.assertEqual(stats["db_hit_rate"], 0.0)

    def test_get_cache_stats_computes_hit_rates(self):
        self.cache.stats.update(hits=3, misses=1, db_hits=1, db_misses=1)
        stats = self.cache.get_cache_stats()
        self.assertEqual(stats["memory_hit_rate"], 75.0)
        self.assertEqual(stats["db_hit_rate"], 50.0)
        self.assertEqual(
            stats["path_validation_cache_size"], len(self.cache.path_validation_cache)
        )

    # --- concurrency -----------------------------------------------------------

    def test_concurrent_listing_writes_keep_all_paths_valid(self):
        errors = []
        valid = []

        def worker(thread_id):
            try:
                for i in range(3):
                    path = f"/test/concurrent/thread{thread_id}/iter{i}"
                    self.cache.mark_valid(path)
                    self.cache.set_directory_listing_with_attrs(
                        path, {f"file{i}.txt": FILE_ATTRS}
                    )
                    valid.append(self.cache.is_valid_path(path))
            except Exception as error:  # pragma: no cover - reported below
                errors.append(error)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(3)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])
        self.assertEqual(valid, [True] * 9)

    def test_concurrent_set_and_get_do_not_raise(self):
        def writer(n):
            for i in range(50):
                self.cache.set(f"key_{n}_{i}", {"data": i})
            return n

        def reader(n):
            for i in range(50):
                self.cache.get(f"key_{n}_{i}")
            return n

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(writer, i) for i in range(4)]
            futures += [pool.submit(reader, i) for i in range(4)]
            results = [f.result() for f in futures]

        self.assertEqual(sorted(results), [0, 0, 1, 1, 2, 2, 3, 3])


class TestCacheManagerHotPaths(unittest.TestCase):
    """Hot-path cache behavior against a real SQLite database."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        thread_manager = Mock()
        thread_manager.create_lock.side_effect = threading.RLock
        self.cache = CacheManager(
            thread_manager=thread_manager,
            cache_dir=self.temp_dir,
            logger=logging.getLogger("test"),
        )

    def tearDown(self):
        self.cache.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _row_count(self) -> int:
        return int(
            self.cache.conn.execute("SELECT COUNT(*) FROM cache_entries").fetchone()[0]
        )

    def test_mark_valid_skips_known_paths(self):
        self.cache.mark_valid("/playlists/Mix/a.m4a", is_directory=False)
        with patch.object(self.cache, "_record_write") as record_write:
            self.cache.mark_valid("/playlists/Mix/a.m4a", is_directory=False)
            self.cache.mark_valid("/playlists/Mix/a.m4a")
        record_write.assert_not_called()

    def test_mark_valid_persists_type_changes(self):
        self.cache.mark_valid("/playlists/Mix", is_directory=False)
        self.cache.mark_valid("/playlists/Mix", is_directory=True)
        self.assertEqual(self.cache.get_entry_type("/playlists/Mix"), "directory")
        keys = {
            row[0] for row in self.cache.conn.execute("SELECT key FROM cache_entries")
        }
        self.assertIn(self.cache.path_to_key("valid_dir:/playlists/Mix"), keys)
        self.assertNotIn(self.cache.path_to_key("exact_path:/playlists/Mix"), keys)

    def test_get_many_reads_hot_and_database_entries(self):
        self.cache.set("filesize:/a", 1)
        self.cache.set("filesize:/b", 2)
        self.cache.hotcache.clear()
        self.cache.set("filesize:/c", 3)

        values = self.cache.get_many(["filesize:/a", "filesize:/b", "filesize:/c", "x"])

        self.assertEqual(values, {"filesize:/a": 1, "filesize:/b": 2, "filesize:/c": 3})

    def test_update_file_attrs_merges_and_writes_only_listing(self):
        listing = {
            "a.m4a": {"st_mode": 0o100644, "st_size": 10, "videoId": "vid_a"},
            "b.m4a": {"st_mode": 0o100644, "st_size": 20, "videoId": "vid_b"},
        }
        self.cache.set_directory_listing_with_attrs("/liked_songs", listing)
        rows_before = self._row_count()

        with patch.object(self.cache, "set_batch") as set_batch:
            self.cache.update_file_attrs_in_parent_dir(
                "/liked_songs/a.m4a", {"st_size": 999}
            )

        set_batch.assert_not_called()
        self.assertEqual(self._row_count(), rows_before)
        attrs = self.cache.get_file_attrs_from_parent_dir("/liked_songs/a.m4a")
        self.assertEqual(attrs["st_size"], 999)
        self.assertEqual(attrs["videoId"], "vid_a")
        self.cache.directory_listings_cache.clear()
        self.cache.hotcache.clear()
        persisted = self.cache.get_directory_listing_with_attrs("/liked_songs")
        self.assertEqual(persisted["a.m4a"]["st_size"], 999)
        self.assertEqual(persisted["a.m4a"]["videoId"], "vid_a")


if __name__ == "__main__":
    unittest.main()
