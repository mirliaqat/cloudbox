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
        self._running = False

    @property
    def up(self):
        return self._running

    def _path(self, chunk_id):
        if not CHUNK_ID.match(chunk_id):
            raise ValueError("bad chunk id")
        return self.folder / chunk_id

    def direct_put(self, chunk_id, data):
        """Direct disk write for environments where local HTTP sockets are blocked."""
        if not self._running:
            return False
        try:
            path = self._path(chunk_id)
        except ValueError:
            return False
        tmp = path.with_suffix(".tmp")
        tmp.write_bytes(data)
        os.replace(tmp, path)
        return True

    def direct_get(self, chunk_id):
        """Direct disk read for environments where local HTTP sockets are blocked."""
        if not self._running:
            return None
        try:
            path = self._path(chunk_id)
        except ValueError:
            return None
        if not path.exists():
            return None
        return path.read_bytes()

    def direct_delete(self, chunk_id):
        """Direct disk deletion for environments where local HTTP sockets are blocked."""
        if not self._running:
            return False
        try:
            self._path(chunk_id).unlink(missing_ok=True)
            return True
        except Exception:
            return False

    def direct_health(self):
        """Direct disk health statistics."""
        if not self._running:
            return {"node": self.name, "up": False, "status": "unreachable", "chunks": 0, "bytes": 0}
        files = [f for f in self.folder.iterdir() if CHUNK_ID.match(f.name)]
        return {
            "status": "healthy",
            "node": self.name,
            "port": self.port,
            "up": True,
            "chunks": len(files),
            "bytes": sum(f.stat().st_size for f in files)
        }

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
        if self._running:
            return
        self._running = True
        try:
            self.server = make_server("127.0.0.1", self.port, self._app(), threaded=True)
            threading.Thread(target=self.server.serve_forever, daemon=True).start()
        except Exception:
            # Platform socket restriction (e.g. PythonAnywhere restricts custom port binding)
            self.server = None

    def stop(self):
        """Simulates a crash: the node stops answering, but its disk is kept."""
        if not self._running:
            return
        self._running = False
        if self.server:
            server, self.server = self.server, None
            try:
                server.shutdown()
                server.server_close()
            except Exception:
                pass
