"""
Tests. Run:  python -m unittest test_cloudbox.py -v
Starts 3 real storage nodes on ports 9201-9203 with a temporary data folder.
"""
import concurrent.futures
import logging
import os
import re
import shutil
import tempfile
import unittest

logging.getLogger("werkzeug").setLevel(logging.ERROR)

from metrics import MetricsDB
from node import StorageNode
from storage import CHUNK_SIZE, CloudStorage, StorageError

CHUNK_ID_PAT = re.compile(r"^[0-9a-f]{32}$")


def count_chunks(node):
    """Count chunk files on a node's disk."""
    return len([f for f in os.listdir(node.folder) if CHUNK_ID_PAT.match(f)])


class TestCloudBox(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.nodes = {f"node-{i}": StorageNode(f"node-{i}", 9200 + i, os.path.join(self.tmp, f"n{i}")) for i in (1, 2, 3)}
        for n in self.nodes.values():
            n.start()
        self.metrics_db = MetricsDB(os.path.join(self.tmp, "test_metrics.db"))
        self.s = CloudStorage(self.nodes, self.tmp, metrics_db=self.metrics_db)
        self.data = os.urandom(CHUNK_SIZE * 2 + 1000)        # 3 chunks

    def tearDown(self):
        self.s.stop_health_monitor()
        for n in self.nodes.values():
            n.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    # ---- original tests ----
    def test_upload_download_roundtrip(self):
        entry, _ = self.s.upload("a.bin", self.data)
        self.assertEqual(len(entry["chunks"]), 3)
        self.assertEqual(self.s.download(entry["id"])[1], self.data)

    def test_every_chunk_has_two_copies(self):
        entry, _ = self.s.upload("a.bin", self.data)
        for c in entry["chunks"]:
            self.assertEqual(len(set(c["nodes"])), 2)

    def test_chunks_are_encrypted_on_disk(self):
        entry, _ = self.s.upload("a.txt", b"secret marks sheet " * 100)
        c = entry["chunks"][0]
        raw = (self.nodes[c["nodes"][0]].folder / c["id"]).read_bytes()
        self.assertNotIn(b"secret marks sheet", raw)

    def test_download_survives_one_node_down(self):
        entry, _ = self.s.upload("a.bin", self.data)
        self.nodes["node-1"].stop()
        self.assertEqual(self.s.download(entry["id"])[1], self.data)

    def test_repair_restores_copies(self):
        entry, _ = self.s.upload("a.bin", self.data)
        self.nodes["node-1"].stop()
        self.assertLess(self.s.health(entry), 2)
        self.s.repair()
        self.assertEqual(self.s.health(entry), 2)
        self.nodes["node-2"].stop()                    # after repair even a second failure is survivable
        self.assertEqual(self.s.download(entry["id"])[1], self.data)

    def test_two_nodes_down_without_repair_fails(self):
        entry, _ = self.s.upload("a.bin", self.data)
        self.nodes["node-1"].stop()
        self.nodes["node-2"].stop()
        with self.assertRaises(StorageError):
            self.s.download(entry["id"])

    def test_corrupted_copy_is_skipped(self):
        entry, _ = self.s.upload("a.bin", self.data)
        c = entry["chunks"][0]
        (self.nodes[c["nodes"][0]].folder / c["id"]).write_bytes(b"garbage")
        self.assertEqual(self.s.download(entry["id"])[1], self.data)

    def test_delete_removes_chunks(self):
        entry, _ = self.s.upload("a.bin", self.data)
        self.s.delete(entry["id"])
        left = sum(len(os.listdir(n.folder)) for n in self.nodes.values())
        self.assertEqual(left, 0)

    # ---- Phase 1: storage reliability tests ----

    def test_partial_upload_cleanup(self):
        """Failed upload should clean up already-stored chunks and metadata."""
        original_put = self.s._put
        call_count = [0]

        def failing_put(node, chunk_id, blob):
            call_count[0] += 1
            if call_count[0] > 2:           # fail after first chunk (2 replicas stored)
                return False
            return original_put(node, chunk_id, blob)

        self.s._put = failing_put
        with self.assertRaises(StorageError):
            self.s.upload("fail.bin", self.data)
        self.s._put = original_put

        # No incomplete files in metadata
        self.assertEqual(len(self.s.files()), 0)
        self.assertEqual(len(self.s.meta["files"]), 0)

        # Orphaned chunks should be cleaned up from disk
        total = sum(count_chunks(n) for n in self.nodes.values())
        self.assertEqual(total, 0)

    def test_incomplete_file_not_listed(self):
        """Files with status 'uploading' should not appear in file listings."""
        entry, _ = self.s.upload("a.bin", self.data)
        self.assertEqual(len(self.s.files()), 1)

        # Simulate interrupted upload by changing status
        with self.s.lock:
            self.s.meta["files"][entry["id"]]["status"] = "uploading"
            self.s._save()
        self.assertEqual(len(self.s.files()), 0)

        # Restore for tearDown cleanup
        with self.s.lock:
            self.s.meta["files"][entry["id"]]["status"] = "complete"

    def test_upload_marks_complete(self):
        """Successful upload should have status 'complete'."""
        entry, _ = self.s.upload("a.bin", self.data)
        self.assertEqual(entry["status"], "complete")
        self.assertEqual(self.s.meta["files"][entry["id"]]["status"], "complete")

    def test_repair_verifies_source_before_copying(self):
        """Repair should not propagate corrupted data."""
        entry, _ = self.s.upload("a.bin", self.data)
        self.nodes["node-1"].stop()

        # Corrupt all remaining copies on live nodes
        for c in entry["chunks"]:
            for node_name in c["nodes"]:
                if node_name != "node-1" and self.nodes[node_name].up:
                    path = self.nodes[node_name].folder / c["id"]
                    if path.exists():
                        path.write_bytes(b"corrupted-data")

        copied = self.s.repair()
        self.assertEqual(copied, 0, "Repair should not copy corrupted replicas")

    def test_pending_deletes_cleanup_on_node_restart(self):
        """Chunks should be cleaned up when a down node comes back online."""
        entry, _ = self.s.upload("a.bin", self.data)

        # Track chunks that are on node-1
        node1_chunks = [c["id"] for c in entry["chunks"] if "node-1" in c["nodes"]]
        self.assertTrue(len(node1_chunks) > 0, "Expected some chunks on node-1")

        # Stop node-1, then delete the file
        self.nodes["node-1"].stop()
        self.s.delete(entry["id"])

        # Pending deletes should exist for node-1
        pending = self.s.meta.get("pending_deletes", {})
        self.assertIn("node-1", pending)
        self.assertTrue(len(pending["node-1"]) > 0)

        # Chunks still on node-1's disk (node was down during delete)
        for cid in node1_chunks:
            self.assertTrue((self.nodes["node-1"].folder / cid).exists())

        # Start node-1 and process pending deletes
        self.nodes["node-1"].start()
        cleaned = self.s.process_pending_deletes()
        self.assertGreater(cleaned, 0)

        # Chunks now deleted from node-1
        for cid in node1_chunks:
            self.assertFalse((self.nodes["node-1"].folder / cid).exists())

        # Pending deletes should be cleared
        pending = self.s.meta.get("pending_deletes", {})
        self.assertNotIn("node-1", pending)

    def test_concurrent_uploads_metadata_consistency(self):
        """Multiple concurrent uploads should not corrupt metadata."""
        results = []

        def do_upload(i):
            data = os.urandom(1000)
            entry, _ = self.s.upload(f"file_{i}.bin", data)
            return entry["id"], data

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(do_upload, i) for i in range(8)]
            for f in concurrent.futures.as_completed(futures):
                results.append(f.result())

        # All 8 files should be in metadata
        self.assertEqual(len(self.s.files()), 8)

        # Each file should be downloadable and match original data
        for fid, original_data in results:
            downloaded = self.s.download(fid)[1]
            self.assertEqual(downloaded, original_data)

    def test_cleanup_orphans_on_startup(self):
        """cleanup_orphans should remove entries stuck in 'uploading' status."""
        entry, _ = self.s.upload("a.bin", self.data)

        # Simulate an interrupted upload entry
        with self.s.lock:
            self.s.meta["files"]["orphan123456"] = {
                "id": "orphan123456", "name": "ghost.bin", "size": 100,
                "sha256": "abc", "uploaded": "2024-01-01 00:00",
                "status": "uploading", "chunks": []}
            self.s._save()

        self.assertEqual(len(self.s.meta["files"]), 2)
        cleaned = self.s.cleanup_orphans()
        self.assertEqual(cleaned, 1)
        self.assertNotIn("orphan123456", self.s.meta["files"])
        # Original complete file should remain
        self.assertEqual(len(self.s.files()), 1)

    def test_delete_with_all_nodes_up_no_pending(self):
        """Delete should remove all chunks with no pending deletes when all nodes are up."""
        entry, _ = self.s.upload("a.bin", self.data)
        self.s.delete(entry["id"])
        total = sum(count_chunks(n) for n in self.nodes.values())
        self.assertEqual(total, 0)
        # No pending deletes when all nodes were available
        self.assertFalse(self.s.meta.get("pending_deletes"))

    # ---- Phase 2: intelligent load balancing tests ----

    def test_node_load_score_calculation(self):
        """Verify load_score calculates relative load and flags down nodes."""
        # Initial empty state: all alive nodes should have 0.0 or low score
        score_1 = self.s.load_score("node-1")
        self.assertIsInstance(score_1, (int, float))
        self.assertLessEqual(score_1, 1.0)

        # Stop node-1: load score should be float('inf') to indicate unavailability
        self.nodes["node-1"].stop()
        self.assertEqual(self.s.load_score("node-1"), float("inf"))
        self.nodes["node-1"].start()

    def test_load_aware_placement_directs_to_least_loaded_nodes(self):
        """Pre-loaded node should be avoided in favor of less loaded nodes."""
        # Pre-populate node-1 with 10 dummy chunks
        for i in range(10):
            cid = f"{i:032x}"
            (self.nodes["node-1"].folder / cid).write_bytes(b"x" * 1024)

        # Upload a 1-chunk file using load-aware strategy
        self.s.strategy = "load-aware"
        entry, _ = self.s.upload("small.bin", b"test load-aware placement")

        # Replicas (2 copies) should be placed on node-2 and node-3, avoiding node-1
        chunk_nodes = entry["chunks"][0]["nodes"]
        self.assertEqual(len(chunk_nodes), 2)
        self.assertNotIn("node-1", chunk_nodes)
        self.assertIn("node-2", chunk_nodes)
        self.assertIn("node-3", chunk_nodes)

    def test_placement_decision_recorded_in_metadata(self):
        """Metadata for each chunk should record the strategy used for placement."""
        self.s.strategy = "load-aware"
        entry_la, _ = self.s.upload("la.bin", self.data)
        for c in entry_la["chunks"]:
            self.assertEqual(c.get("placement"), "load-aware")

        self.s.strategy = "round-robin"
        entry_rr, _ = self.s.upload("rr.bin", self.data)
        for c in entry_rr["chunks"]:
            self.assertEqual(c.get("placement"), "round-robin")

    def test_replica_placement_avoids_same_node(self):
        """Load-aware placement must never place two copies of the same chunk on the same node."""
        self.s.strategy = "load-aware"
        entry, _ = self.s.upload("rep_check.bin", self.data)
        for c in entry["chunks"]:
            self.assertEqual(len(c["nodes"]), 2)
            self.assertEqual(len(set(c["nodes"])), 2)

    def test_mid_upload_node_failure_handling(self):
        """If a node stops during a multi-chunk upload, upload continues using alive nodes."""
        data = os.urandom(CHUNK_SIZE * 3)  # 3 chunks
        original_put = self.s._put
        call_count = [0]

        def put_with_node_kill(node, chunk_id, blob):
            call_count[0] += 1
            # After first chunk is stored, stop node-1
            if call_count[0] == 2:
                self.nodes["node-1"].stop()
            return original_put(node, chunk_id, blob)

        self.s._put = put_with_node_kill
        entry, _ = self.s.upload("survive.bin", data)
        self.s._put = original_put

        self.assertEqual(entry["status"], "complete")
        # Download should still succeed with data integrity
        _, downloaded, _, _ = self.s.download(entry["id"])
        self.assertEqual(downloaded, data)

    def test_round_robin_vs_load_aware_benchmark(self):
        """Compare placement strategies: load-aware should achieve lower variance than round-robin."""
        res = self.s.compare_placement_strategies(num_chunks=30, initial_skew={"node-1": 15, "node-2": 0, "node-3": 0})
        rr_var = res["round_robin"]["variance"]
        la_var = res["load_aware"]["variance"]
        self.assertLess(la_var, rr_var, f"Load-aware variance ({la_var}) should be less than round-robin ({rr_var})")

    # ---- Phase 3: failure detection & self-healing tests ----

    def test_node_health_endpoint(self):
        """Verify storage node exposes /health with status and capacity details."""
        res = self.s.check_node_health("node-1")
        self.assertTrue(res["up"])
        self.assertEqual(res["status"], "healthy")
        self.assertEqual(res["node"], "node-1")

        self.nodes["node-1"].stop()
        down_res = self.s.check_node_health("node-1")
        self.assertFalse(down_res["up"])
        self.assertEqual(down_res["status"], "unreachable")
        self.nodes["node-1"].start()

    def test_file_health_status_detection(self):
        """Verify file health status transition from healthy to at_risk on node failure."""
        entry, _ = self.s.upload("health_test.bin", self.data)
        self.assertEqual(self.s.file_health_status(entry), "healthy")

        # Stop node-1: file should now be at_risk (reduced to 1 copy)
        self.nodes["node-1"].stop()
        self.assertEqual(self.s.file_health_status(entry), "at_risk")

        # Repair file: should return to healthy on live nodes
        self.s.repair()
        self.assertEqual(self.s.file_health_status(entry), "healthy")

    def test_auto_repair_and_recovery_events(self):
        """Verify auto-repair creates new replicas and records recovery events."""
        entry, _ = self.s.upload("auto_test.bin", self.data)
        self.nodes["node-1"].stop()

        initial_event_count = len(self.s.recovery_events)
        copied = self.s.repair(auto=True)
        self.assertGreater(copied, 0)
        self.assertEqual(len(self.s.recovery_events), initial_event_count + copied)

        event = self.s.recovery_events[-1]
        self.assertEqual(event["action"], "auto_repair")
        self.assertIn("target", event)
        self.assertIn("source", event)

    def test_prevent_overlapping_repairs(self):
        """Verify overlapping repair attempts are rejected by the lock."""
        # Acquire the repair lock externally
        self.assertTrue(self.s._repair_lock.acquire(blocking=False))
        try:
            # Calling repair while lock is held should exit cleanly returning 0
            copied = self.s.repair()
            self.assertEqual(copied, 0)
        finally:
            self.s._repair_lock.release()

    def test_background_health_monitor_triggers_repair(self):
        """Background health monitor should automatically repair degraded files."""
        import time
        entry, _ = self.s.upload("monitor_test.bin", self.data)
        self.nodes["node-1"].stop()
        self.assertEqual(self.s.file_health_status(entry), "at_risk")

        repaired_flag = []
        self.s.start_health_monitor(interval=0.2, on_repair=lambda n: repaired_flag.append(n))

        # Wait up to 2 seconds for background thread to run self-healing
        for _ in range(20):
            time.sleep(0.1)
            if self.s.file_health_status(entry) == "healthy":
                break

        self.s.stop_health_monitor()
        self.assertEqual(self.s.file_health_status(entry), "healthy")

    # ---- Phase 4: performance & storage analytics tests ----

    def test_metrics_db_record_transfer_and_summary(self):
        """Verify transfer metrics calculation, persistence, and summary stats."""
        # Transfer a file through storage (triggers MetricsDB recording)
        entry, _ = self.s.upload("m_test.bin", self.data)
        self.s.download(entry["id"])

        summary = self.metrics_db.get_metrics()
        self.assertGreaterEqual(summary["upload"]["count"], 1)
        self.assertGreaterEqual(summary["download"]["count"], 1)
        self.assertGreater(summary["upload"]["avg_throughput_mbps"], 0)
        self.assertGreater(summary["download"]["avg_throughput_mbps"], 0)
        self.assertTrue(len(summary["timeline"]) >= 2)

    def test_metrics_csv_export(self):
        """Verify CSV export contains header and recorded rows."""
        self.s.upload("csv_test.bin", self.data)
        csv_str = self.metrics_db.export_csv()
        lines = csv_str.strip().splitlines()
        self.assertGreaterEqual(len(lines), 2)
        self.assertIn("Timestamp", lines[0])
        self.assertIn("Throughput_MBps", lines[0])
        self.assertIn("csv_test.bin", csv_str)

    def test_system_snapshot_replication_overhead(self):
        """Verify system snapshot accurately calculates replication overhead (~2.0x - 2.7x with Fernet encryption)."""
        entry, _ = self.s.upload("snap.bin", self.data)
        self.s.record_system_snapshot()
        summary = self.metrics_db.get_metrics()
        latest = summary["latest_snapshot"]
        self.assertIsNotNone(latest)
        self.assertGreater(latest["replication_overhead"], 1.5)
        self.assertLessEqual(latest["replication_overhead"], 3.5)

    # ---- Phase 5: parallel chunk transfer tests ----

    def test_parallel_chunk_upload_and_download(self):
        """Multi-chunk file upload and download succeed in parallel mode with data integrity."""
        multi_data = os.urandom(CHUNK_SIZE * 4)  # 4 chunks
        entry, ms_up = self.s.upload("par.bin", multi_data, parallel=True)
        self.assertEqual(len(entry["chunks"]), 4)
        for c in entry["chunks"]:
            self.assertEqual(len(c["nodes"]), 2)

        _, downloaded, _, ms_down = self.s.download(entry["id"], parallel=True)
        self.assertEqual(downloaded, multi_data)

    def test_parallel_chunk_ordering_preserved(self):
        """Parallel chunk transfers strictly preserve sequential chunk ordering."""
        # Create patterned data where each chunk has exactly CHUNK_SIZE bytes
        hdr_len = len("CHUNK_HEADER_0000_")
        chunks_data = [f"CHUNK_HEADER_{i:04d}_".encode() + os.urandom(CHUNK_SIZE - hdr_len) for i in range(4)]
        combined_data = b"".join(chunks_data)

        entry, _ = self.s.upload("pattern.bin", combined_data, parallel=True)
        _, downloaded, _, _ = self.s.download(entry["id"], parallel=True)
        self.assertEqual(downloaded, combined_data)

        # Verify each segment matches its expected chunk
        for i in range(4):
            start = i * CHUNK_SIZE
            end = start + CHUNK_SIZE
            self.assertEqual(downloaded[start:end], chunks_data[i])

    def test_sequential_vs_parallel_benchmark(self):
        """Verify transfer benchmark executes both modes and confirms bit-for-bit identical output."""
        res = self.s.benchmark_transfers(size_bytes=CHUNK_SIZE * 3)
        self.assertTrue(res["data_identical"])
        self.assertGreater(res["sequential_upload_ms"], 0)
        self.assertGreater(res["parallel_upload_ms"], 0)
        self.assertGreater(res["sequential_download_ms"], 0)
        self.assertGreater(res["parallel_download_ms"], 0)

    def test_transient_retry_backoff(self):
        """Upload should succeed through transient network failures via bounded retry backoff."""
        data = os.urandom(CHUNK_SIZE * 2)
        real_put = self.s._put
        fails_remaining = [1]

        def flaky_put(node, chunk_id, blob):
            if fails_remaining[0] > 0:
                fails_remaining[0] -= 1
                return False  # Simulate transient error
            return real_put(node, chunk_id, blob)

        self.s._put = flaky_put
        entry, _ = self.s.upload("flaky.bin", data)
        self.s._put = real_put

        self.assertEqual(entry["status"], "complete")
        _, downloaded, _, _ = self.s.download(entry["id"])
        self.assertEqual(downloaded, data)


if __name__ == "__main__":
    unittest.main(verbosity=2)
