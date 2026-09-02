"""Lokální server pro map.html + snapshoty z flights.sqlite."""

from __future__ import annotations

import json
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from flight_db import fetch_snapshot, fetch_snapshot_times

ROOT = Path(__file__).resolve().parent
PORT = 8765


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def end_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.end_headers()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/snapshots":
            self._json(200, fetch_snapshot_times())
            return
        if parsed.path == "/api/snapshot":
            times = parse_qs(parsed.query).get("time", [])
            if not times or not times[0].strip():
                self._json(400, {"error": "missing time"})
                return
            self._json(200, fetch_snapshot(times[0]))
            return
        super().do_GET()

    def _json(self, code: int, payload: object) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:
        print(f"{self.address_string()} - {format % args}")


if __name__ == "__main__":
    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    print(f"Mapa: http://127.0.0.1:{PORT}/map.html")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nZastaveno.")
