"""
Storage node (like an HDFS DataNode or an S3 storage server).
Each node is its own HTTP server on its own port and keeps chunks in its own
folder on disk. A node can be stopped (crash) and started again.
"""
import os
import re
import threading
from pathlib import Path

from flask import Flask, Response, jsonify, request
from werkzeug.serving import make_server

CHUNK_ID = re.compile(r"^[0-9a-f]{32}$")


class StorageNode:
    def __init__(self, name, port, folder):
        self.name, self.port = name, port
        self.folder = Path(folder)
        self.folder.mkdir(parents=True, exist_ok=True)
        self.server = None

    @property
    def up(self):
        return self.server is not None

    def _path(self, chunk_id):
        if not CHUNK_ID.match(chunk_id):
            raise ValueError("bad chunk id")
        return self.folder / chunk_id

    def _app(self):
        app = Flask(f"node-{self.name}")
        node = self

        @app.put("/chunks/<chunk_id>")
        def put_chunk(chunk_id):
            try:
                path = node._path(chunk_id)
            except ValueError:
                return jsonify(error="bad chunk id"), 400
            tmp = path.with_suffix(".tmp")
            tmp.write_bytes(request.get_data())
            os.replace(tmp, path)                 # atomic: never a half-written chunk
            return jsonify(node=node.name, ok=True)

        @app.get("/chunks/<chunk_id>")
        def get_chunk(chunk_id):
            try:
                path = node._path(chunk_id)
            except ValueError:
                return jsonify(error="bad chunk id"), 400
            if not path.exists():
                return jsonify(error="chunk not found"), 404
            return Response(path.read_bytes(), mimetype="application/octet-stream")

        @app.delete("/chunks/<chunk_id>")
        def delete_chunk(chunk_id):
            try:
                node._path(chunk_id).unlink(missing_ok=True)
            except ValueError:
                return jsonify(error="bad chunk id"), 400
            return jsonify(ok=True)

        @app.get("/health")
        def health_check():
            files = [f for f in node.folder.iterdir() if CHUNK_ID.match(f.name)]
            return jsonify(
                status="healthy",
                node=node.name,
                port=node.port,
                chunks=len(files),
                bytes=sum(f.stat().st_size for f in files)
            )

        @app.get("/stats")
        def stats():
            files = [f for f in node.folder.iterdir() if CHUNK_ID.match(f.name)]
            return jsonify(node=node.name, chunks=len(files), bytes=sum(f.stat().st_size for f in files))

        return app

    def start(self):
        if self.server:
            return
        self.server = make_server("127.0.0.1", self.port, self._app(), threaded=True)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def stop(self):
        """Simulates a crash: the server stops answering, but its disk is kept."""
        if not self.server:
            return
        server, self.server = self.server, None
        server.shutdown()
        server.server_close()
