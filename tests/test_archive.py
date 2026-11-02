import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tick_futures.archive import create, decode, encode, restore, seal
from tick_futures.audit import verify_db
from tick_futures.github import upload
from tick_futures.runtime import atomic_json
from tick_futures.store import Store
from test_collection import row


class Archive(unittest.TestCase):
    def test_all_standard_codecs_strict_original_bytes(self):
        raw = b"id,datetime,p\r\n1,1793600000000000001,-0.0\r\n2,2,NaN\n" + bytes(range(256))
        for codec, level in (("gzip", 9), ("zstd", 3), ("zstd", 9), ("zstd", 19), ("xz", 6), ("xz", 9), ("parquet-zstd", 9)):
            with self.subTest(codec=codec, level=level):
                self.assertEqual(decode(encode(raw, codec, level), codec, expected_size=len(raw)), raw)

    def test_empty_files_binary_shards_and_order_restore(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root/"raw").write_bytes(bytes(range(256))*4)
            (root/"empty").write_bytes(b"")
            m = create({"x/raw": root/"raw", "empty": root/"empty"}, root/"archive", shard_bytes=300, data_kind="synthetic")
            self.assertEqual(len(m["part_order"]), 5)
            r = restore(root/"archive", root/"recovered")
            self.assertEqual(r["status"], "all_original_bytes_match")
            self.assertEqual((root/"raw").read_bytes(), (root/"recovered/x/raw").read_bytes())

    def test_missing_corrupt_order_dictionary_version_and_traversal_fail(self):
        for mutation in ("missing", "corrupt", "order", "dictionary", "version", "traversal"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                (root/"raw").write_bytes(b"a"*500 + b"b"*500)
                m = create({"raw": root/"raw"}, root/"archive", shard_bytes=300, data_kind="synthetic")
                part = root/"archive"/m["part_order"][0]
                if mutation == "missing":
                    part.unlink()
                elif mutation == "corrupt":
                    part.write_bytes(b"bad")
                elif mutation == "order":
                    m["part_order"].reverse()
                elif mutation == "dictionary":
                    m["files"][0]["parts"][0]["dictionary"] = "wrong-dictionary"
                elif mutation == "version":
                    m["record_format"] = "tfrec-999"
                elif mutation == "traversal":
                    m["files"][0]["path"] = "../outside"
                atomic_json(root/"archive/manifest.json", m)
                with self.assertRaises(ValueError):
                    restore(root/"archive", root/"recovered")

    def test_incremental_seal_keeps_raw_bits_and_global_order(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            db = Store(root/"hot", min_free_bytes=0)
            db.ingest("TEST.x", [row(1), row(2)], "run", "initial_backfill")
            first = seal(root/"hot", root/"archive1", shard_bytes=20000)
            db.ingest("TEST.x", [row(2), row(3)], "run", "live")
            second = seal(root/"hot", root/"archive2", shard_bytes=20000)
            self.assertEqual(second["parent"], first["archive_id"])
            self.assertEqual((second["scope"]["first_seq"], second["scope"]["last_seq"]), (3,3))
            restore(root/"archive2", root/"out")
            self.assertEqual(verify_db(root/"out/journal/journal.sqlite3")["rows"], 1)
            db.close()

    def test_market_upload_without_rights_stops_before_release_mutation(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root/"raw").write_bytes(b"fixture")
            create({"raw": root/"raw"}, root/"archive")
            with patch("tick_futures.github.verify_owner_private"), patch("tick_futures.github.release_info") as release:
                with self.assertRaises(PermissionError):
                    upload(root/"archive", "owner/repo", "tag")
                release.assert_not_called()

    def test_interrupted_upload_reuses_verified_assets(self):
        from tick_futures.codec import sha256
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root/"raw").write_bytes(b"fixture"*20)
            m = create({"raw": root/"raw"}, root/"archive", data_kind="synthetic", shard_bytes=100)
            remote = {}
            uploads = []
            fail = [True]
            def run(args, output=None):
                if args[:2] == ["release", "upload"]:
                    path = Path(args[3])
                    if path.name.startswith("part-000001") and fail[0]:
                        fail[0] = False
                        raise RuntimeError("injected upload interruption")
                    if path.name in remote:
                        raise AssertionError("already uploaded asset must not be repeated")
                    remote[path.name] = path.read_bytes()
                    uploads.append(path.name)
                return b""
            def assets(*args):
                return [{"name": k, "id": i, "size": len(v)} for i,(k,v) in enumerate(remote.items())]
            def download(repo, asset, target):
                Path(target).write_bytes(remote[asset["name"]])
            with patch("tick_futures.github.verify_owner_private"), patch("tick_futures.github.release_info", return_value={"id":1,"html_url":"fixture","draft":False}), patch("tick_futures.github.asset_list", side_effect=assets), patch("tick_futures.github.command", side_effect=run), patch("tick_futures.github.download", side_effect=download):
                with self.assertRaisesRegex(RuntimeError, "interruption"):
                    upload(root/"archive", "owner/repo", "tag")
                self.assertFalse(any(k.startswith("complete-") for k in remote))
                result = upload(root/"archive", "owner/repo", "tag")
                self.assertEqual(uploads.count(m["part_order"][0]), 1)
                self.assertTrue(uploads[-1].startswith("complete-"))
                self.assertEqual(result["downloaded_decoded_parts"], 2)


if __name__ == "__main__":
    unittest.main()
