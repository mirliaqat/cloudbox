"""
CloudBox storage service (like the HDFS NameNode / an S3 front end).

Upload:   file -> 256 KB chunks -> each chunk encrypted (AES via Fernet)
          -> each chunk stored on 2 different storage nodes (replication)
Download: for each chunk, read from the first replica that answers,
          decrypt, check its SHA-256, join the chunks.
Metadata (which chunk is on which node) is kept in data/metadata.json.

Phase 1: upload transaction tracking, partial-failure cleanup, verified repair,
         pending deletes, metadata lock consistency.
Phase 2: load-aware node selection, placement recording, mid-upload failure
         handling, load-score computation for the dashboard.
"""
import concurrent.futures
import hashlib
import json
import os
import threading
import time
import uuid
from pathlib import Path

import requests
import requests.adapters
from cryptography.fernet import Fernet, InvalidToken

CHUNK_SIZE = 256 * 1024
REPLICATION = 2


class StorageError(Exception):
    pass


class CloudStorage:
    def __init__(self, nodes, data_dir, metrics_db=None):
        self.nodes = nodes                                  # {"node-1": StorageNode, ...}
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.meta_path = self.data_dir / "metadata.json"
        self.fernet = Fernet(self._load_key())
        self.lock = threading.Lock()
        self.http = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20)
        self.http.mount("http://", adapter)
        self.http.mount("https://", adapter)
        self.meta = json.loads(self.meta_path.read_text()) if self.meta_path.exists() else {"files": {}}
        self._rr = 0
        self.strategy = "load-aware"                        # Phase 2: "load-aware" or "round-robin"
        self._repair_lock = threading.Lock()                # Phase 3: prevent overlapping repairs
        self.recovery_events = []                           # Phase 3: recovery event history
        self._monitor_thread = None                         # Phase 3: background health monitor
        self._monitor_stop = threading.Event()
        self.metrics_db = metrics_db                        # Phase 4: SQLite metrics database
        self.concurrency = 4                                # Phase 5: worker threads for parallel transfers
        self.max_retries = 3                                # Phase 5: bounded retries with backoff
        self.parallel_transfers = True                      # Phase 5: default parallel mode

    # ---------------- helpers ----------------
    def _load_key(self):
        key_file = self.data_dir / "secret.key"
        if not key_file.exists():
            key_file.write_bytes(Fernet.generate_key())
        return key_file.read_bytes()

    def _save(self):
        tmp = self.meta_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.meta, indent=1))
        tmp.replace(self.meta_path)

    def _url(self, node, chunk_id):
        return f"http://127.0.0.1:{self.nodes[node].port}/chunks/{chunk_id}"

    def alive(self):
        return [n for n, node in self.nodes.items() if node.up]

    def check_node_health(self, node_name):
        """Query storage node's /health endpoint to check reachability and disk status."""
        node = self.nodes.get(node_name)
        if not node or not node.up:
            return {"node": node_name, "up": False, "status": "unreachable"}
        try:
            r = self.http.get(f"http://127.0.0.1:{node.port}/health", timeout=1.5)
            if r.ok:
                data = r.json()
                return {"node": node_name, "up": True, "status": "healthy", **data}
        except requests.RequestException:
            pass
        return node.direct_health()

    def _put(self, node, chunk_id, blob):
        node_obj = self.nodes.get(node)
        if not node_obj or not node_obj.up:
            return False
        try:
            r = self.http.put(self._url(node, chunk_id), data=blob, timeout=3)
            if r.ok:
                return True
        except requests.RequestException:
            pass
        return node_obj.direct_put(chunk_id, blob)

    def _get(self, node, chunk_id):
        node_obj = self.nodes.get(node)
        if not node_obj or not node_obj.up:
            return None
        try:
            r = self.http.get(self._url(node, chunk_id), timeout=3)
            if r.ok:
                return r.content
        except requests.RequestException:
            pass
        return node_obj.direct_get(chunk_id)

    def _delete_node_chunk(self, node, chunk_id):
        node_obj = self.nodes.get(node)
        if not node_obj or not node_obj.up:
            return False
        try:
            r = self.http.delete(self._url(node, chunk_id), timeout=3)
            if r.ok:
                return True
        except requests.RequestException:
            pass
        return node_obj.direct_delete(chunk_id)

    # ---------------- Phase 1: cleanup helpers ----------------
    def _cleanup_chunks(self, chunks):
        """Remove stored chunks from nodes after a failed upload."""
        for c in chunks:
            for node in c["nodes"]:
                self._delete_node_chunk(node, c["id"])

    def cleanup_orphans(self):
        """Remove metadata entries stuck in 'uploading' status (interrupted uploads).
        Called on startup to clean up after crashes."""
        with self.lock:
            orphans = [fid for fid, e in self.meta["files"].items()
                       if e.get("status") == "uploading"]
            if not orphans:
                return 0
            for fid in orphans:
                entry = self.meta["files"].pop(fid)
                for c in entry.get("chunks", []):
                    for node in c["nodes"]:
                        self._delete_node_chunk(node, c["id"])
            self._save()
        return len(orphans)

    def process_pending_deletes(self):
        """Clean up chunks on nodes that have come back online after a file
        was deleted while they were down."""
        with self.lock:
            pending = self.meta.get("pending_deletes", {})
            if not pending:
                return 0
            work = {node: list(cids) for node, cids in pending.items()}

        cleaned = 0
        alive_set = set(self.alive())
        results = {}

        for node, chunk_ids in work.items():
            if node not in alive_set:
                continue
            failed = []
            for cid in chunk_ids:
                if self._delete_node_chunk(node, cid):
                    cleaned += 1
                else:
                    failed.append(cid)
            results[node] = failed

        with self.lock:
            pending = self.meta.get("pending_deletes", {})
            for node, failed in results.items():
                if failed:
                    pending[node] = failed
                else:
                    pending.pop(node, None)
            if not pending:
                self.meta.pop("pending_deletes", None)
            self._save()
        return cleaned

    def record_system_snapshot(self):
        """Phase 4: Capture system-level storage consumption & replication overhead."""
        if not self.metrics_db:
            return
        files = self.files()
        logical = sum(e.get("size", 0) for e in files)
        stats = self.node_stats()
        physical = sum(s.get("bytes", 0) for s in stats if s.get("up"))
        healthy = sum(1 for e in files if self.file_health_status(e) == "healthy")
        degraded = sum(1 for e in files if self.file_health_status(e) == "degraded")
        at_risk = sum(1 for e in files if self.file_health_status(e) == "at_risk")
        self.metrics_db.record_snapshot(logical, physical, healthy, degraded, at_risk)

    # ---------------- Phase 2: load-aware placement ----------------
    def _get_node_loads(self):
        """Query each alive node for its current storage load."""
        loads = {}
        for name, node in self.nodes.items():
            if not node.up:
                continue
            try:
                stats = self.http.get(
                    f"http://127.0.0.1:{node.port}/stats", timeout=1).json()
                loads[name] = {"bytes": stats.get("bytes", 0),
                               "chunks": stats.get("chunks", 0)}
            except requests.RequestException:
                h = node.direct_health()
                loads[name] = {"bytes": h.get("bytes", 0),
                               "chunks": h.get("chunks", 0)}
        return loads

    def load_score(self, node_name, stats=None):
        """
        Calculate normalized load score for a node between 0.0 and 1.0.
        Considers stored bytes (70%), chunk count (30%), and node availability.
        Returns None or float('inf') if node is stopped/unavailable.
        """
        node = self.nodes.get(node_name)
        if not node or not node.up:
            return float("inf")
        if stats is None:
            stats = self._get_node_loads()
        alive_stats = {k: v for k, v in stats.items() if self.nodes.get(k) and self.nodes[k].up}
        if not alive_stats or node_name not in alive_stats:
            return 0.0
        max_bytes = max((s.get("bytes", 0) for s in alive_stats.values()), default=1) or 1
        max_chunks = max((s.get("chunks", 0) for s in alive_stats.values()), default=1) or 1
        curr = alive_stats[node_name]
        return round(
            0.7 * (curr.get("bytes", 0) / max_bytes) +
            0.3 * (curr.get("chunks", 0) / max_chunks), 3
        )

    def compare_placement_strategies(self, num_chunks=30, initial_skew=None):
        """
        Simulate round-robin vs load-aware chunk placement on an initial distribution.
        Returns metrics (chunks per node, variance, std_dev) demonstrating balance.
        """
        nodes_list = sorted(list(self.nodes.keys()))
        if not initial_skew:
            initial_skew = {nodes_list[0]: 10} if len(nodes_list) > 1 else {}

        # Round Robin simulation
        rr_counts = {n: initial_skew.get(n, 0) for n in nodes_list}
        rr_idx = 0
        for _ in range(num_chunks):
            n1 = nodes_list[rr_idx % len(nodes_list)]
            n2 = nodes_list[(rr_idx + 1) % len(nodes_list)]
            rr_counts[n1] += 1
            rr_counts[n2] += 1
            rr_idx += 1

        # Load-Aware simulation (directs chunks toward less loaded nodes)
        la_counts = {n: initial_skew.get(n, 0) for n in nodes_list}
        for _ in range(num_chunks):
            sorted_nodes = sorted(nodes_list, key=lambda n: la_counts[n])
            la_counts[sorted_nodes[0]] += 1
            la_counts[sorted_nodes[1]] += 1

        def calc_stats(counts):
            vals = list(counts.values())
            mean = sum(vals) / len(vals)
            variance = sum((x - mean) ** 2 for x in vals) / len(vals)
            return {
                "counts": counts,
                "mean": round(mean, 2),
                "variance": round(variance, 2),
                "std_dev": round(variance ** 0.5, 2)
            }

        return {
            "initial_skew": initial_skew,
            "chunks_added": num_chunks,
            "round_robin": calc_stats(rr_counts),
            "load_aware": calc_stats(la_counts)
        }

    # ---------------- operations ----------------
    def _upload_chunk(self, piece, loads, current_alive, chunk_id=None, tracker=None):
        """Upload one chunk with bounded retries and backoff."""
        if chunk_id is None:
            chunk_id = uuid.uuid4().hex
        blob = self.fernet.encrypt(piece)

        if self.strategy == "load-aware":
            candidates = sorted(
                current_alive,
                key=lambda n: (loads.get(n, {}).get("bytes", 0), loads.get(n, {}).get("chunks", 0))
            )
        else:
            with self.lock:
                idx = self._rr % len(current_alive)
                candidates = current_alive[idx:] + current_alive[:idx]
                self._rr += 1

        stored = []
        if tracker is not None:
            with self.lock:
                tracker.append({"id": chunk_id, "nodes": stored})

        for n in candidates:
            if len(stored) == REPLICATION:
                break
            # Phase 5: bounded retries with backoff
            success = False
            for attempt in range(self.max_retries):
                if self._put(n, chunk_id, blob):
                    success = True
                    break
                time.sleep(0.01 * (2 ** attempt))
            if success:
                stored.append(n)
                with self.lock:
                    if n in loads:
                        loads[n]["bytes"] = loads[n].get("bytes", 0) + len(blob)
                        loads[n]["chunks"] = loads[n].get("chunks", 0) + 1

        if not stored:
            raise StorageError(f"could not store chunk {chunk_id} on any node")

        return {
            "id": chunk_id,
            "size": len(piece),
            "sha256": hashlib.sha256(piece).hexdigest(),
            "nodes": stored,
            "placement": self.strategy
        }

    def upload(self, name, data, parallel=None):
        alive = self.alive()
        if not alive:
            raise StorageError("no storage node is running")
        t0 = time.perf_counter()
        file_id = uuid.uuid4().hex[:12]
        use_parallel = self.parallel_transfers if parallel is None else parallel

        loads = self._get_node_loads() if self.strategy == "load-aware" else {}

        entry = {"id": file_id, "name": name, "size": len(data),
                 "sha256": hashlib.sha256(data).hexdigest(),
                 "uploaded": time.strftime("%Y-%m-%d %H:%M"),
                 "status": "uploading", "chunks": []}
        with self.lock:
            self.meta["files"][file_id] = entry
            self._save()

        stored_chunks = []
        try:
            current_alive = [n for n in alive if self.nodes[n].up]
            if not current_alive:
                raise StorageError("all storage nodes went down during upload")

            pieces = [
                (i, data[offset:offset + CHUNK_SIZE])
                for i, offset in enumerate(range(0, max(len(data), 1), CHUNK_SIZE))
            ]

            if use_parallel and len(pieces) > 1:
                # Phase 5: Parallel chunk transfers with ThreadPoolExecutor
                indexed_chunks = []
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.concurrency) as pool:
                    future_to_idx = {
                        pool.submit(self._upload_chunk, piece, loads, current_alive, tracker=stored_chunks): i
                        for i, piece in pieces
                    }
                    for future in concurrent.futures.as_completed(future_to_idx):
                        idx = future_to_idx[future]
                        chunk_meta = future.result()
                        indexed_chunks.append((idx, chunk_meta))
                indexed_chunks.sort(key=lambda x: x[0])
                final_chunks = [c for _, c in indexed_chunks]
            else:
                # Sequential chunk transfers
                final_chunks = []
                for _, piece in pieces:
                    alive_now = [n for n in current_alive if self.nodes[n].up]
                    if not alive_now:
                        raise StorageError("all storage nodes went down during upload")
                    chunk_meta = self._upload_chunk(piece, loads, alive_now, tracker=stored_chunks)
                    final_chunks.append(chunk_meta)

            with self.lock:
                entry["chunks"] = final_chunks
                entry["status"] = "complete"
                self._save()
            ms = (time.perf_counter() - t0) * 1000
            if self.metrics_db:
                self.metrics_db.record_transfer("upload", file_id, name, len(data), ms, "ok")
                self.record_system_snapshot()
            return entry, ms

        except Exception:
            ms = (time.perf_counter() - t0) * 1000
            if self.metrics_db:
                self.metrics_db.record_transfer("upload", file_id, name, len(data), ms, "error")
            self._cleanup_chunks(stored_chunks)
            with self.lock:
                self.meta["files"].pop(file_id, None)
                self._save()
            raise

    def _download_chunk(self, c):
        """Download and verify one chunk with bounded retries."""
        piece = None
        used_node = None
        for node in c["nodes"]:
            if not self.nodes[node].up:
                continue
            blob = None
            for attempt in range(self.max_retries):
                blob = self._get(node, c["id"])
                if blob is not None:
                    break
                time.sleep(0.01 * (2 ** attempt))
            if blob is None:
                continue
            try:
                candidate = self.fernet.decrypt(blob)
            except InvalidToken:
                continue
            if hashlib.sha256(candidate).hexdigest() == c["sha256"]:
                piece = candidate
                used_node = node
                break
        if piece is None:
            raise StorageError("a chunk has no reachable replica (too many nodes down)")
        return piece, used_node

    def download(self, file_id, parallel=None):
        entry = self.meta["files"].get(file_id)
        if entry is None:
            raise KeyError(file_id)
        if entry.get("status", "complete") != "complete":
            raise StorageError("file upload is incomplete")
        t0 = time.perf_counter()
        use_parallel = self.parallel_transfers if parallel is None else parallel

        chunks = entry["chunks"]
        if use_parallel and len(chunks) > 1:
            # Phase 5: Concurrent chunk download preserving sequential order
            indexed_parts = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.concurrency) as pool:
                future_to_idx = {
                    pool.submit(self._download_chunk, c): i
                    for i, c in enumerate(chunks)
                }
                for future in concurrent.futures.as_completed(future_to_idx):
                    idx = future_to_idx[future]
                    piece, used_node = future.result()
                    indexed_parts.append((idx, piece, used_node))
            indexed_parts.sort(key=lambda x: x[0])
            parts = [p[1] for p in indexed_parts]
            used = [p[2] for p in indexed_parts]
        else:
            parts, used = [], []
            for c in chunks:
                piece, used_node = self._download_chunk(c)
                parts.append(piece)
                used.append(used_node)

        data = b"".join(parts)
        ms = (time.perf_counter() - t0) * 1000
        if self.metrics_db:
            self.metrics_db.record_transfer("download", file_id, entry["name"], len(data), ms, "ok")
        return entry, data, used, ms

    def benchmark_transfers(self, size_bytes=CHUNK_SIZE * 4):
        """
        Phase 5: Compare sequential vs. parallel transfer speeds.
        Verifies both produce identical bit-for-bit file output.
        """
        test_data = os.urandom(size_bytes)
        # Sequential upload & download
        entry_seq, up_seq_ms = self.upload("bench_seq.bin", test_data, parallel=False)
        _, down_seq_data, _, down_seq_ms = self.download(entry_seq["id"], parallel=False)
        self.delete(entry_seq["id"])

        # Parallel upload & download
        entry_par, up_par_ms = self.upload("bench_par.bin", test_data, parallel=True)
        _, down_par_data, _, down_par_ms = self.download(entry_par["id"], parallel=True)
        self.delete(entry_par["id"])

        data_identical = (down_seq_data == test_data) and (down_par_data == test_data)
        return {
            "size_bytes": size_bytes,
            "sequential_upload_ms": round(up_seq_ms, 2),
            "parallel_upload_ms": round(up_par_ms, 2),
            "sequential_download_ms": round(down_seq_ms, 2),
            "parallel_download_ms": round(down_par_ms, 2),
            "upload_speedup": round(up_seq_ms / max(up_par_ms, 0.001), 2),
            "download_speedup": round(down_seq_ms / max(down_par_ms, 0.001), 2),
            "data_identical": data_identical
        }

    def delete(self, file_id):
        with self.lock:
            entry = self.meta["files"].pop(file_id, None)
            if entry is None:
                raise KeyError(file_id)
            self._save()
        pending_nodes = {}
        for c in entry.get("chunks", []):
            for node in c["nodes"]:
                if self.nodes[node].up:
                    if not self._delete_node_chunk(node, c["id"]):
                        pending_nodes.setdefault(node, []).append(c["id"])
                else:
                    pending_nodes.setdefault(node, []).append(c["id"])
        if pending_nodes:
            with self.lock:
                pending = self.meta.setdefault("pending_deletes", {})
                for node, cids in pending_nodes.items():
                    pending.setdefault(node, []).extend(cids)
                self._save()
        return entry

    def health(self, entry):
        """Lowest number of reachable copies among the file's chunks."""
        if entry.get("status", "complete") != "complete":
            return 0
        alive = set(self.alive())
        return min((len([n for n in c["nodes"] if n in alive]) for c in entry["chunks"]), default=0)

    def file_health_status(self, entry, verify_checksums=False):
        """
        Phase 3: Determine the health state of a file:
        - 'healthy': all chunks have at least REPLICATION intact replicas
        - 'degraded': fewer replicas than required, but file is still recoverable (copies > 1)
        - 'at_risk': only 1 replica remaining, or corrupted replica detected
        """
        if entry.get("status", "complete") != "complete":
            return "incomplete"
        alive = set(self.alive())
        min_live = REPLICATION
        corrupted = False

        for c in entry.get("chunks", []):
            live_nodes = [n for n in c["nodes"] if n in alive]
            if len(live_nodes) < min_live:
                min_live = len(live_nodes)

            if verify_checksums:
                for n in live_nodes:
                    blob = self._get(n, c["id"])
                    if blob is None:
                        corrupted = True
                        break
                    try:
                        dec = self.fernet.decrypt(blob)
                        if hashlib.sha256(dec).hexdigest() != c["sha256"]:
                            corrupted = True
                            break
                    except Exception:
                        corrupted = True
                        break

        if min_live == 0:
            return "at_risk"
        if corrupted or min_live == 1:
            return "at_risk"
        if min_live < REPLICATION:
            return "degraded"
        return "healthy"

    def repair(self, auto=False):
        """
        Phase 3: Re-replicate: give every chunk REPLICATION copies on running nodes.
        - Prevents duplicate or overlapping repairs with a non-blocking lock.
        - Verifies source replica integrity before copying.
        - Selects healthy and least-loaded nodes as replication targets.
        - Logs all recovery actions to recovery_events.
        """
        if not self._repair_lock.acquire(blocking=False):
            return 0                                        # Another repair job is already in progress

        try:
            t0 = time.perf_counter()
            alive = self.alive()
            if not alive:
                return 0

            node_loads = self._get_node_loads()
            copied = 0

            with self.lock:
                for entry in self.meta["files"].values():
                    if entry.get("status", "complete") != "complete":
                        continue
                    file_name = entry["name"]
                    for c in entry.get("chunks", []):
                        live_copies = [n for n in c["nodes"] if n in alive]
                        if len(live_copies) >= REPLICATION or not live_copies:
                            continue

                        # Verify source replica integrity before copying
                        source_node = None
                        blob = None
                        corrupted_nodes = []
                        for n in live_copies:
                            cand = self._get(n, c["id"])
                            if cand is None:
                                continue
                            try:
                                dec = self.fernet.decrypt(cand)
                                if hashlib.sha256(dec).hexdigest() == c["sha256"]:
                                    source_node = n
                                    blob = cand
                                    break
                                else:
                                    corrupted_nodes.append(n)
                            except InvalidToken:
                                corrupted_nodes.append(n)

                        # Clean up references to corrupted replicas
                        for bad_n in corrupted_nodes:
                            if bad_n in c["nodes"]:
                                c["nodes"].remove(bad_n)
                            if bad_n in live_copies:
                                live_copies.remove(bad_n)

                        if blob is None or source_node is None:
                            continue

                        # Target selection: alive nodes not already holding chunk, sorted by least load
                        available_targets = [n for n in alive if n not in c["nodes"]]
                        sorted_targets = sorted(
                            available_targets,
                            key=lambda n: (node_loads.get(n, {}).get("bytes", 0),
                                           node_loads.get(n, {}).get("chunks", 0))
                        )

                        for target in sorted_targets:
                            if len(live_copies) >= REPLICATION:
                                break
                            if self._put(target, c["id"], blob):
                                c["nodes"].append(target)
                                live_copies.append(target)
                                copied += 1
                                if target in node_loads:
                                    node_loads[target]["bytes"] = node_loads[target].get("bytes", 0) + len(blob)
                                    node_loads[target]["chunks"] = node_loads[target].get("chunks", 0) + 1

                                event = {
                                    "t": time.strftime("%H:%M:%S"),
                                    "timestamp": time.time(),
                                    "file_id": entry["id"],
                                    "file_name": file_name,
                                    "chunk_id": c["id"][:8],
                                    "source": source_node,
                                    "target": target,
                                    "action": "auto_repair" if auto else "manual_repair"
                                }
                                self.recovery_events.append(event)
                                if len(self.recovery_events) > 100:
                                    self.recovery_events.pop(0)

                if copied > 0:
                    self._save()
            duration_ms = (time.perf_counter() - t0) * 1000
            if copied > 0 and self.metrics_db:
                self.metrics_db.record_recovery(copied, duration_ms, "auto" if auto else "manual")
                self.record_system_snapshot()
            return copied
        finally:
            self._repair_lock.release()

    def start_health_monitor(self, interval=10, on_repair=None):
        """Phase 3: Periodic background health-check loop for automated self-healing."""
        if self._monitor_thread and self._monitor_thread.is_alive():
            return
        self._monitor_stop.clear()

        def _loop():
            while not self._monitor_stop.wait(interval):
                try:
                    files = self.files()
                    needs_repair = any(
                        self.file_health_status(f) in ("degraded", "at_risk")
                        for f in files
                    )
                    if needs_repair:
                        copied = self.repair(auto=True)
                        if copied > 0 and on_repair:
                            on_repair(copied)
                except Exception:
                    pass

        self._monitor_thread = threading.Thread(
            target=_loop, daemon=True, name="cloudbox-health-monitor"
        )
        self._monitor_thread.start()

    def stop_health_monitor(self):
        """Phase 3: Stop background health-check loop."""
        self._monitor_stop.set()
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=2)

    def files(self):
        return sorted(
            [e for e in self.meta["files"].values()
             if e.get("status", "complete") == "complete"],
            key=lambda e: e["uploaded"], reverse=True)

    def node_stats(self):
        out = []
        for name, node in self.nodes.items():
            s = {"name": name, "port": node.port, "up": node.up,
                 "chunks": 0, "bytes": 0, "load_score": None}
            if node.up:
                try:
                    s.update(self.http.get(
                        f"http://127.0.0.1:{node.port}/stats", timeout=1).json())
                except requests.RequestException:
                    s.update(node.direct_health())
            s["name"] = name
            out.append(s)

        # Phase 2: compute load scores for the dashboard
        up = [s for s in out if s["up"]]
        max_bytes = max((s["bytes"] for s in up), default=1) or 1
        max_chunks = max((s["chunks"] for s in up), default=1) or 1
        for s in up:
            s["load_score"] = round(
                0.7 * (s["bytes"] / max_bytes) +
                0.3 * (s["chunks"] / max_chunks), 3)
        return out
