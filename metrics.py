"""
CloudBox metrics and performance analytics persistence using SQLite (metrics.db).
Tracks transfer durations, throughput (MB/s), storage consumption, replica health,
replication overhead, and self-healing recovery time.
"""
from contextlib import contextmanager
import csv
import io
import sqlite3
import threading
import time
from pathlib import Path


class MetricsDB:
    def __init__(self, db_path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self._init_db()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def _init_db(self):
        with self.lock, self._conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS transfers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL,
                    time_str TEXT,
                    operation TEXT,
                    file_id TEXT,
                    file_name TEXT,
                    size_bytes INTEGER,
                    duration_ms REAL,
                    throughput_mbps REAL,
                    status TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS recoveries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL,
                    time_str TEXT,
                    chunks_repaired INTEGER,
                    duration_ms REAL,
                    trigger_type TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL,
                    time_str TEXT,
                    logical_bytes INTEGER,
                    physical_bytes INTEGER,
                    replication_overhead REAL,
                    healthy_files INTEGER,
                    degraded_files INTEGER,
                    at_risk_files INTEGER
                )
            """)

    def record_transfer(self, operation, file_id, file_name, size_bytes, duration_ms, status="ok"):
        """Record an upload or download transfer event and calculate throughput in MB/s."""
        sec = max(duration_ms / 1000.0, 0.001)
        throughput_mbps = round((size_bytes / (1024 * 1024)) / sec, 3) if size_bytes > 0 else 0.0
        now = time.time()
        time_str = time.strftime("%H:%M:%S", time.localtime(now))
        with self.lock, self._conn() as conn:
            conn.execute("""
                INSERT INTO transfers
                (timestamp, time_str, operation, file_id, file_name, size_bytes, duration_ms, throughput_mbps, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (now, time_str, operation, file_id, file_name, size_bytes, round(duration_ms, 2), throughput_mbps, status))
        return throughput_mbps

    def record_recovery(self, chunks_repaired, duration_ms, trigger_type="auto"):
        """Record a repair/recovery event duration."""
        now = time.time()
        time_str = time.strftime("%H:%M:%S", time.localtime(now))
        with self.lock, self._conn() as conn:
            conn.execute("""
                INSERT INTO recoveries
                (timestamp, time_str, chunks_repaired, duration_ms, trigger_type)
                VALUES (?, ?, ?, ?, ?)
            """, (now, time_str, chunks_repaired, round(duration_ms, 2), trigger_type))

    def record_snapshot(self, logical_bytes, physical_bytes, healthy, degraded, at_risk):
        """Record a system-level snapshot with replication overhead."""
        now = time.time()
        time_str = time.strftime("%H:%M:%S", time.localtime(now))
        overhead = round(physical_bytes / max(logical_bytes, 1), 2) if logical_bytes > 0 else 0.0
        with self.lock, self._conn() as conn:
            conn.execute("""
                INSERT INTO snapshots
                (timestamp, time_str, logical_bytes, physical_bytes, replication_overhead, healthy_files, degraded_files, at_risk_files)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (now, time_str, logical_bytes, physical_bytes, overhead, healthy, degraded, at_risk))
        return overhead

    def get_metrics(self):
        """Aggregate performance and system metrics for dashboard visualization."""
        with self.lock, self._conn() as conn:
            rows = conn.execute("""
                SELECT time_str, operation, throughput_mbps, duration_ms, size_bytes, file_name
                FROM transfers
                ORDER BY id DESC LIMIT 30
            """).fetchall()
            timeline = [dict(r) for r in reversed(rows)]

            up_stats = conn.execute("""
                SELECT COUNT(*) as count, AVG(throughput_mbps) as avg_mbps, AVG(duration_ms) as avg_ms
                FROM transfers WHERE operation = 'upload' AND status = 'ok'
            """).fetchone()

            down_stats = conn.execute("""
                SELECT COUNT(*) as count, AVG(throughput_mbps) as avg_mbps, AVG(duration_ms) as avg_ms
                FROM transfers WHERE operation = 'download' AND status = 'ok'
            """).fetchone()

            rec_stats = conn.execute("""
                SELECT COUNT(*) as count, SUM(chunks_repaired) as total_chunks, AVG(duration_ms) as avg_ms
                FROM recoveries
            """).fetchone()

            latest_snap = conn.execute("""
                SELECT * FROM snapshots ORDER BY id DESC LIMIT 1
            """).fetchone()

        return {
            "timeline": timeline,
            "upload": {
                "count": up_stats["count"] or 0,
                "avg_throughput_mbps": round(up_stats["avg_mbps"] or 0.0, 2),
                "avg_duration_ms": round(up_stats["avg_ms"] or 0.0, 1),
            },
            "download": {
                "count": down_stats["count"] or 0,
                "avg_throughput_mbps": round(down_stats["avg_mbps"] or 0.0, 2),
                "avg_duration_ms": round(down_stats["avg_ms"] or 0.0, 1),
            },
            "recovery": {
                "count": rec_stats["count"] or 0,
                "chunks_repaired": rec_stats["total_chunks"] or 0,
                "avg_duration_ms": round(rec_stats["avg_ms"] or 0.0, 1),
            },
            "latest_snapshot": dict(latest_snap) if latest_snap else None
        }

    def export_csv(self):
        """Export transfer and performance metrics to CSV format."""
        out = io.StringIO()
        writer = csv.writer(out)
        writer.writerow(["ID", "Timestamp", "Time", "Operation", "File_ID", "File_Name", "Size_Bytes", "Duration_MS", "Throughput_MBps", "Status"])
        with self.lock, self._conn() as conn:
            for r in conn.execute("SELECT * FROM transfers ORDER BY id ASC"):
                writer.writerow([r["id"], r["timestamp"], r["time_str"], r["operation"], r["file_id"], r["file_name"], r["size_bytes"], r["duration_ms"], r["throughput_mbps"], r["status"]])
        return out.getvalue()
