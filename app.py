"""
CloudBox - a mini cloud storage service (like Google Drive / Dropbox / S3)

Run:   pip install -r requirements.txt
       python app.py
Open:  http://localhost:8000
"""
import hashlib
import hmac
import logging
import os
import time
from collections import deque
from pathlib import Path
from urllib.parse import quote

import flask.cli
from flask import Flask, Response, jsonify, request, send_from_directory

from metrics import MetricsDB
from node import StorageNode
from storage import CHUNK_SIZE, REPLICATION, CloudStorage, StorageError

PORT = int(os.environ.get("PORT", 8000))
HOST = os.environ.get("HOST", "0.0.0.0")
BASE = Path(__file__).parent
DATA = BASE / "data"
MAX_MB = 50

logging.getLogger("werkzeug").setLevel(logging.ERROR)
flask.cli.show_server_banner = lambda *a, **k: None

metrics = MetricsDB(DATA / "metrics.db")
nodes = {f"node-{i}": StorageNode(f"node-{i}", 8000 + i, DATA / f"node-{i}") for i in (1, 2, 3)}
store = CloudStorage(nodes, DATA, metrics_db=metrics)
SHARE_SECRET = hashlib.sha256((DATA / "secret.key").read_bytes() + b"share-links").digest()
events = deque(maxlen=80)

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = MAX_MB * 1024 * 1024
_initialized = False


def log(msg, kind="info"):
    events.appendleft({"t": time.strftime("%H:%M:%S"), "msg": msg, "kind": kind})


def init_app():
    global _initialized
    if _initialized:
        return
    _initialized = True
    for n in nodes.values():
        n.start()
    orphans = store.cleanup_orphans()
    if orphans:
        log(f"Cleaned up {orphans} incomplete upload(s) from previous session", "warn")
    store.process_pending_deletes()

    def on_auto_repair(copied):
        log(f"Self-healing: automatically restored {copied} chunk cop{'y' if copied == 1 else 'ies'}", "ok")

    store.start_health_monitor(interval=10, on_repair=on_auto_repair)
    log(f"Started 3 storage nodes; {len(store.files())} file(s) in storage; self-healing active (10s)", "ok")


init_app()


@app.before_request
def ensure_initialized():
    init_app()


def human(n):
    for unit in ("B", "KB", "MB"):
        if n < 1024 or unit == "MB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024


def file_view(e):
    health_status = store.file_health_status(e)
    return {"id": e["id"], "name": e["name"], "size": e["size"], "size_h": human(e["size"]),
            "uploaded": e["uploaded"], "chunks": len(e["chunks"]), "copies": store.health(e),
            "health_status": health_status,
            "nodes": sorted({n for c in e["chunks"] for n in c["nodes"]}),
            "placement": e["chunks"][0].get("placement", "round-robin") if e.get("chunks") else "unknown"}


def send_file_bytes(name, data):
    return Response(data, mimetype="application/octet-stream", headers={
        "Content-Disposition": f"attachment; filename*=UTF-8''{quote(name)}"})


# ---------------- signed share links (security control) ----------------
def sign(file_id, exp):
    return hmac.new(SHARE_SECRET, f"{file_id}|{exp}".encode(), hashlib.sha256).hexdigest()[:32]


# ---------------- pages ----------------
@app.get("/")
def index():
    return send_from_directory(BASE / "static", "index.html")


# ---------------- files ----------------
@app.get("/api/files")
def list_files():
    return jsonify([file_view(e) for e in store.files()])


@app.post("/api/files")
def upload():
    f = request.files.get("file")
    if f is None or not f.filename:
        return jsonify(error="choose a file to upload"), 400
    data = f.read()
    try:
        entry, ms = store.upload(f.filename, data)
    except StorageError as e:
        log(f"Upload of {f.filename} failed: {e}", "error")
        return jsonify(error=str(e)), 503
    v = file_view(entry)
    log(f"Uploaded {entry['name']} ({v['size_h']}) as {v['chunks']} encrypted chunk(s) "
        f"on {', '.join(v['nodes'])} in {ms:.0f} ms", "ok")
    if v["copies"] < REPLICATION:
        log(f"{entry['name']} has only {v['copies']} copy: start another node and press Repair", "warn")
    return jsonify(file=v, ms=round(ms, 1))


@app.get("/api/files/<file_id>/download")
def download(file_id):
    try:
        entry, data, used, ms = store.download(file_id)
    except KeyError:
        return jsonify(error="file not found"), 404
    except StorageError as e:
        log(f"Download failed: {e}", "error")
        return jsonify(error=str(e)), 503
    log(f"Downloaded {entry['name']} from {', '.join(sorted(set(used)))} in {ms:.0f} ms "
        f"(decrypted and checksum verified)", "ok")
    return send_file_bytes(entry["name"], data)


@app.delete("/api/files/<file_id>")
def delete(file_id):
    try:
        entry = store.delete(file_id)
    except KeyError:
        return jsonify(error="file not found"), 404
    log(f"Deleted {entry['name']}")
    return jsonify(ok=True)


@app.post("/api/files/<file_id>/share")
def share(file_id):
    if file_id not in store.meta["files"]:
        return jsonify(error="file not found"), 404
    minutes = max(1, min(int((request.get_json(silent=True) or {}).get("minutes", 10)), 1440))
    exp = int(time.time()) + minutes * 60
    link = f"/s/{file_id}?exp={exp}&sig={sign(file_id, exp)}"
    log(f"Share link created for {store.meta['files'][file_id]['name']} (valid {minutes} min)")
    return jsonify(link=link, minutes=minutes)


@app.get("/s/<file_id>")
def shared(file_id):
    exp, sig = request.args.get("exp", ""), request.args.get("sig", "")
    if not exp.isdigit() or not hmac.compare_digest(sign(file_id, int(exp)), sig):
        log(f"Share link rejected: bad signature", "error")
        return Response(DENIED.format(reason="this link has been changed or is not valid"), 403, mimetype="text/html")
    if time.time() > int(exp):
        log(f"Share link rejected: expired", "error")
        return Response(DENIED.format(reason="this link has expired"), 403, mimetype="text/html")
    return download(file_id)


# ---------------- nodes ----------------
@app.get("/api/nodes")
def node_list():
    return jsonify(store.node_stats())


@app.post("/api/nodes/<name>/<action>")
def node_action(name, action):
    node = nodes.get(name)
    if node is None or action not in ("stop", "start"):
        return jsonify(error="unknown node or action"), 404
    if action == "stop":
        node.stop()
        log(f"{name} STOPPED (simulated crash)", "error")
    else:
        node.start()
        log(f"{name} started again", "ok")
        # Phase 1: clean up chunks from files deleted while this node was down
        cleaned = store.process_pending_deletes()
        if cleaned:
            log(f"Cleaned up {cleaned} orphaned chunk(s) from {name}", "ok")
    return jsonify(ok=True)


@app.post("/api/repair")
def repair():
    copied = store.repair()
    log(f"Repair finished: {copied} chunk cop{'y' if copied == 1 else 'ies'} made", "ok")
    return jsonify(copied=copied)


@app.get("/api/summary")
def summary():
    files = store.files()
    healthy_count = sum(1 for e in files if store.file_health_status(e) == "healthy")
    degraded_count = sum(1 for e in files if store.file_health_status(e) == "degraded")
    at_risk_count = sum(1 for e in files if store.file_health_status(e) == "at_risk")
    return jsonify(files=len(files), bytes=human(sum(e["size"] for e in files)),
                   nodes_up=len(store.alive()), nodes=len(nodes),
                   at_risk=at_risk_count, healthy=healthy_count, degraded=degraded_count,
                   chunk_kb=CHUNK_SIZE // 1024, replication=REPLICATION,
                   strategy=store.strategy, recovery_events=store.recovery_events[-10:],
                   events=list(events))


@app.get("/api/strategy")
def get_strategy():
    return jsonify(strategy=store.strategy)


@app.post("/api/strategy")
def set_strategy():
    strat = (request.get_json(silent=True) or {}).get("strategy", "load-aware")
    if strat not in ("load-aware", "round-robin"):
        return jsonify(error="invalid strategy, must be 'load-aware' or 'round-robin'"), 400
    store.strategy = strat
    log(f"Switched placement strategy to '{strat}'")
    return jsonify(strategy=store.strategy)


@app.get("/api/benchmark/load-balancing")
def benchmark_load_balancing():
    num = request.args.get("chunks", default=30, type=int)
    res = store.compare_placement_strategies(num_chunks=num)
    return jsonify(res)


@app.get("/api/recovery-events")
def get_recovery_events():
    return jsonify(store.recovery_events)


@app.get("/api/health-check")
def manual_health_check():
    results = {name: store.check_node_health(name) for name in nodes}
    files = store.files()
    file_states = {e["id"]: store.file_health_status(e) for e in files}
    return jsonify(nodes=results, files=file_states)


@app.get("/api/metrics")
def get_metrics():
    store.record_system_snapshot()
    data = metrics.get_metrics()
    data["node_utilization"] = store.node_stats()
    return jsonify(data)


@app.get("/api/metrics/export")
def export_metrics():
    csv_data = metrics.export_csv()
    return Response(
        csv_data,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=cloudbox_metrics.csv"}
    )


@app.get("/api/benchmark/transfers")
def benchmark_transfers():
    res = store.benchmark_transfers(size_bytes=CHUNK_SIZE * 4)
    return jsonify(res)


@app.errorhandler(413)
def too_big(_):
    return jsonify(error=f"file is larger than {MAX_MB} MB"), 413


DENIED = """<!doctype html><html><head><meta charset="utf-8"><title>Link not valid</title>
<style>body{{font-family:Segoe UI,Arial,sans-serif;background:#f8fafc;padding:60px 20px}}
.c{{max-width:460px;margin:auto;background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:28px}}
h1{{margin:0 0 8px;color:#b91c1c;font-size:22px}}</style></head><body><div class="c">
<h1>403: link not valid</h1><p>Sorry, {reason}.</p><p style="color:#64748b;font-size:13px">
CloudBox share links are signed (HMAC-SHA256) and expire. Ask the owner for a new link.</p></div></body></html>"""


if __name__ == "__main__":
    init_app()
    print("\n  CloudBox is running")
    print(f"  Open:   http://localhost:{PORT} (bound to {HOST})")
    print("  Nodes:  node-1 :8001, node-2 :8002, node-3 :8003")
    print("  Stop:   Ctrl + C\n")
    app.run(host=HOST, port=PORT, threaded=True, use_reloader=False)
