"""
HTTP API Server — Lightweight async HTTP for dashboard and metrics.

This module provides a minimal HTTP server built on top of asyncio's
raw TCP server. It serves:
    - GET /              -> Dashboard HTML page
    - GET /metrics       -> JSON metrics snapshot
    - GET /metrics/dlq   -> JSON DLQ task listing
    - GET /health        -> Simple health check

Why not use Flask, FastAPI, or aiohttp?
    DistroSync is zero-dependency by design. This server handles only
    the 4 routes above, so a full framework would be overkill. Raw HTTP
    parsing for simple GET requests is ~50 lines of code.

How it works:
    asyncio.start_server gives us raw TCP connections. We read the HTTP
    request line and headers, extract the method and path, and dispatch
    to the right handler. The response is a standard HTTP/1.1 response
    with Content-Type and CORS headers.

Why CORS headers?
    The dashboard HTML may be served from a file:// URL during development,
    or from a different port. Without Access-Control-Allow-Origin: *,
    the browser would block the fetch() calls to /metrics.

Thread safety:
    This server runs on the SAME asyncio event loop as the broker's TCP
    server. No threading issues — all handlers are async coroutines that
    share the broker's state safely through the event loop.
"""

import asyncio
import json
import logging
import os
import sys
from typing import Optional, Callable

logger = logging.getLogger(__name__)

# Path to the dashboard HTML file (relative to project root)
DASHBOARD_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "dashboard"
)


class HTTPAPIServer:
    """
    Minimal async HTTP server for the DistroSync dashboard and metrics.

    Usage:
        http_server = HTTPAPIServer(
            host="0.0.0.0",
            port=8000,
            metrics_handler=broker.get_metrics_snapshot,
            dlq_handler=broker.get_dlq_listing,
        )
        await http_server.start()
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8000,
        metrics_handler: Optional[Callable] = None,
        dlq_handler: Optional[Callable] = None,
        stats_handler: Optional[Callable] = None,
        crash_worker_handler: Optional[Callable] = None,
        replay_dlq_handler: Optional[Callable] = None,
        reset_handler: Optional[Callable] = None,
        dashboard_dir: str = DASHBOARD_DIR,
    ):
        self.host = host
        self.port = port
        self._metrics_handler = metrics_handler
        self._dlq_handler = dlq_handler
        self._stats_handler = stats_handler
        self._crash_worker_handler = crash_worker_handler
        self._replay_dlq_handler = replay_dlq_handler
        self._reset_handler = reset_handler
        self._dashboard_dir = dashboard_dir
        self._server: Optional[asyncio.Server] = None
        self._active_loads = []

    async def handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        """Handle a single HTTP connection."""
        addr = writer.get_extra_info("peername")
        try:
            # Read the full HTTP request (up to 8KB — more than enough for GET)
            data = await asyncio.wait_for(reader.read(8192), timeout=5.0)
            if not data:
                return

            request_text = data.decode("utf-8", errors="replace")
            # Parse the request line: "GET /path HTTP/1.1"
            request_line = request_text.split("\r\n")[0]
            parts = request_line.split(" ")
            if len(parts) < 2:
                await self._send_response(writer, 400, {"error": "Bad request"})
                return

            method = parts[0]
            path = parts[1].split("?")[0]  # Strip query string

            # Only support GET and POST
            if method not in ("GET", "POST"):
                await self._send_response(
                    writer, 405, {"error": "Method not allowed"}
                )
                return

            # Route dispatch
            if method == "GET" and (path == "/" or path == "/dashboard"):
                await self._serve_dashboard(writer)
            elif method == "GET" and (path == "/metrics" or path == "/metrics/"):
                await self._serve_metrics(writer)
            elif method == "POST" and path == "/admin/load-test":
                await self._trigger_load_test(writer)
            elif method == "POST" and path == "/admin/flood":
                await self._trigger_flood(writer)
            elif method == "POST" and path == "/admin/crash-worker":
                await self._crash_worker(writer)
            elif method == "POST" and path == "/admin/dlq/replay-all":
                await self._replay_dlq(writer)
            elif method == "POST" and path == "/admin/reset":
                await self._handle_reset(writer)
            elif path == "/metrics/dlq":
                await self._serve_dlq(writer)
            elif path == "/stats":
                await self._serve_stats(writer)
            elif path == "/health":
                await self._send_response(writer, 200, {
                    "status": "healthy",
                    "service": "distrosync-broker",
                })
            elif path.startswith("/static/"):
                await self._serve_static(writer, path)
            else:
                await self._send_response(
                    writer, 404, {"error": f"Not found: {path}"}
                )

        except asyncio.TimeoutError:
            pass
        except Exception as e:
            logger.error(f"HTTP error from {addr}: {e}")
            try:
                await self._send_response(
                    writer, 500, {"error": "Internal server error"}
                )
            except Exception:
                pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _send_response(
        self,
        writer: asyncio.StreamWriter,
        status_code: int,
        body: dict,
        content_type: str = "application/json",
    ) -> None:
        """Send an HTTP response with JSON body."""
        status_text = {
            200: "OK",
            400: "Bad Request",
            404: "Not Found",
            405: "Method Not Allowed",
            500: "Internal Server Error",
        }.get(status_code, "Unknown")

        body_bytes = json.dumps(body, indent=2).encode("utf-8")

        response = (
            f"HTTP/1.1 {status_code} {status_text}\r\n"
            f"Content-Type: {content_type}\r\n"
            f"Content-Length: {len(body_bytes)}\r\n"
            f"Access-Control-Allow-Origin: *\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        ).encode("utf-8") + body_bytes

        writer.write(response)
        await writer.drain()

    async def _send_html_response(
        self, writer: asyncio.StreamWriter, html: str
    ) -> None:
        """Send an HTTP response with HTML body."""
        body_bytes = html.encode("utf-8")
        response = (
            f"HTTP/1.1 200 OK\r\n"
            f"Content-Type: text/html; charset=utf-8\r\n"
            f"Content-Length: {len(body_bytes)}\r\n"
            f"Access-Control-Allow-Origin: *\r\n"
            f"Cache-Control: no-cache\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        ).encode("utf-8") + body_bytes

        writer.write(response)
        await writer.drain()

    async def _serve_dashboard(self, writer: asyncio.StreamWriter) -> None:
        """Serve the dashboard HTML page."""
        html_path = os.path.join(self._dashboard_dir, "index.html")
        if not os.path.exists(html_path):
            await self._send_response(
                writer, 404,
                {"error": "Dashboard not found. Create dashboard/index.html"},
            )
            return

        with open(html_path, "r", encoding="utf-8") as f:
            html = f.read()

        await self._send_html_response(writer, html)

    async def _serve_static(self, writer: asyncio.StreamWriter, path: str) -> None:
        """Serve a static file from the dashboard directory."""
        # Sanitize path to prevent directory traversal
        relative = path.lstrip("/static/")
        if ".." in relative or relative.startswith("/"):
            await self._send_response(writer, 404, {"error": "Not found"})
            return

        file_path = os.path.join(self._dashboard_dir, relative)
        if not os.path.exists(file_path) or not os.path.isfile(file_path):
            await self._send_response(writer, 404, {"error": "Not found"})
            return

        # Determine content type
        ext = os.path.splitext(file_path)[1].lower()
        content_types = {
            ".css": "text/css",
            ".js": "application/javascript",
            ".png": "image/png",
            ".svg": "image/svg+xml",
            ".ico": "image/x-icon",
        }
        content_type = content_types.get(ext, "application/octet-stream")

        with open(file_path, "rb") as f:
            body = f.read()

        response = (
            f"HTTP/1.1 200 OK\r\n"
            f"Content-Type: {content_type}\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Access-Control-Allow-Origin: *\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        ).encode("utf-8") + body

        writer.write(response)
        await writer.drain()

    async def _serve_metrics(self, writer: asyncio.StreamWriter) -> None:
        """Serve the metrics snapshot as JSON."""
        if self._metrics_handler:
            metrics = await self._metrics_handler()
        else:
            metrics = {"error": "Metrics handler not configured"}
        await self._send_response(writer, 200, metrics)

    async def _serve_dlq(self, writer: asyncio.StreamWriter) -> None:
        """Serve the DLQ task listing as JSON."""
        if self._dlq_handler:
            dlq_data = await self._dlq_handler()
        else:
            dlq_data = {"error": "DLQ handler not configured"}
        await self._send_response(writer, 200, dlq_data)

    async def _serve_stats(self, writer: asyncio.StreamWriter) -> None:
        """Serve internal broker stats."""
        if self._stats_handler:
            stats = self._stats_handler()
        else:
            stats = {"error": "Stats handler not registered"}
        await self._send_response(writer, 200, stats)

    async def _handle_reset(self, writer: asyncio.StreamWriter):
        """Completely restart the broker metrics and queues from scratch."""
        # Kill any actively running load generators
        for p in self._active_loads:
            try:
                p.kill()
            except Exception:
                pass
        self._active_loads.clear()

        if self._reset_handler:
            result = await self._reset_handler()
            await self._send_response(writer, 200, result)
        else:
            await self._send_response(writer, 500, {"error": "Handler not configured"})

    async def _trigger_load_test(self, writer: asyncio.StreamWriter) -> None:
        """Trigger the load simulator asynchronously."""
        try:
            import subprocess
            load_script = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "run_load.py"
            )
            # Port defaults to 5555
            p = subprocess.Popen([sys.executable, load_script, "--port", "5555"])
            self._active_loads.append(p)
            await self._send_response(writer, 200, {"status": "ok", "message": "Load test started"})
        except Exception as e:
            logger.error(f"Failed to start load test: {e}")
            await self._send_response(writer, 500, {"status": "error", "error": str(e)})

    async def _trigger_flood(self, writer: asyncio.StreamWriter) -> None:
        """Trigger the load simulator in flood mode asynchronously."""
        try:
            import subprocess
            load_script = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "run_load.py"
            )
            p = subprocess.Popen([sys.executable, load_script, "--producers", "200", "--tasks", "500", "--port", "5555"])
            self._active_loads.append(p)
            await self._send_response(writer, 200, {"status": "ok", "message": "Flood test started"})
        except Exception as e:
            logger.error(f"Failed to start flood test: {e}")
            await self._send_response(writer, 500, {"status": "error", "error": str(e)})

    async def _crash_worker(self, writer: asyncio.StreamWriter) -> None:
        if self._crash_worker_handler:
            result = await self._crash_worker_handler()
            await self._send_response(writer, 200, result)
        else:
            await self._send_response(writer, 500, {"error": "Handler not configured"})

    async def _replay_dlq(self, writer: asyncio.StreamWriter) -> None:
        if self._replay_dlq_handler:
            result = await self._replay_dlq_handler()
            await self._send_response(writer, 200, result)
        else:
            await self._send_response(writer, 500, {"error": "Handler not configured"})

    async def start(self) -> None:
        """Start the HTTP server."""
        self._server = await asyncio.start_server(
            self.handle_connection, self.host, self.port
        )
        logger.info(
            f"HTTP API server listening on http://{self.host}:{self.port}"
        )

    async def stop(self) -> None:
        """Stop the HTTP server."""
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            logger.info("HTTP API server stopped")
