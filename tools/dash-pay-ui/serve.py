#!/usr/bin/env python3
"""Serve the Dash pay demo UI and proxy API calls to devapi.v4v.app."""

from __future__ import annotations

import argparse
import json
import os
import urllib.error
import urllib.request
import webbrowser
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from urllib.parse import parse_qs, urlparse

from qrcode import QRCode
from qrcode.constants import ERROR_CORRECT_H

HERE = os.path.dirname(os.path.abspath(__file__))
UPSTREAM = "https://devapi.v4v.app"
QR_MAX_CHARS = 4096


def make_qr_png(data: str) -> bytes:
    """PNG QR using the same qrcode settings as v4vapp-api-ext."""
    qr = QRCode(
        version=1,
        error_correction=ERROR_CORRECT_H,
        box_size=10,
        border=1,
    )
    qr.add_data(data)
    image = qr.make_image()
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=HERE, **kwargs)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/qr":
            self._qr(parsed.query)
            return
        if self._is_api():
            self._proxy()
            return
        super().do_GET()

    def do_POST(self) -> None:
        if self._is_api():
            self._proxy()
            return
        self.send_error(405)

    def do_OPTIONS(self) -> None:
        if self._is_api():
            self._proxy()
            return
        self.send_error(405)

    def _qr(self, query: str) -> None:
        data = parse_qs(query).get("data", [""])[0]
        if not data or len(data) > QR_MAX_CHARS:
            payload = b'{"detail":"missing or oversized data"}'
            self.send_response(400)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        try:
            png = make_qr_png(data)
        except Exception as exc:
            payload = json.dumps({"detail": f"qr error: {exc}"}).encode()
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        self.send_response(200)
        self.send_header("Content-Type", "image/png")
        self.send_header("Content-Length", str(len(png)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(png)

    def _is_api(self) -> bool:
        path = self.path.split("?", 1)[0]
        return path == "/v1" or path.startswith("/v1/") or path.startswith("/v2/")

    def _proxy(self) -> None:
        length = int(self.headers.get("Content-Length", "0") or 0)
        body = self.rfile.read(length) if length else None
        headers = {
            "User-Agent": "dash-pay-ui/1.0",
            "Accept": self.headers.get("Accept", "application/json"),
        }
        content_type = self.headers.get("Content-Type")
        if content_type:
            headers["Content-Type"] = content_type

        request = urllib.request.Request(
            UPSTREAM + self.path,
            data=body,
            method=self.command,
            headers=headers,
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                self._write_upstream(response.status, response.headers, response.read())
        except urllib.error.HTTPError as exc:
            self._write_upstream(exc.code, exc.headers, exc.read())
        except Exception as exc:
            payload = json.dumps({"detail": f"proxy error: {exc}"}).encode()
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    def _write_upstream(self, status: int, headers, body: bytes) -> None:
        self.send_response(status)
        content_type = headers.get("Content-Type", "application/json")
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        super().log_message(format, *args)


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve the Dash pay demo UI")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--no-open", action="store_true")
    args = parser.parse_args()

    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    url = f"http://{args.host}:{args.port}/"
    print(f"Dash pay UI → {url}")
    print(f"API proxy   → {UPSTREAM}")
    if not args.no_open:
        webbrowser.open(url)
    httpd.serve_forever()


if __name__ == "__main__":
    main()
