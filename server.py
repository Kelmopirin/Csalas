import json
import os
import queue
import sqlite3
import sys
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

# Render (and most PaaS providers) capture stdout/stderr for their log
# viewer, but Python block-buffers stdout when it isn't attached to a
# terminal. Without this, print() output can sit in the buffer and never
# show up in the platform's logs. stderr (used for request logging by
# BaseHTTPRequestHandler) is unbuffered by default, so this mainly affects
# our own print() calls.
sys.stdout.reconfigure(line_buffering=True)


DATABASE_PATH = Path(__file__).with_name("qr_codes.db")
MAX_REQUEST_SIZE = 64 * 1024

# Thread-safe registry of all currently connected SSE subscribers, so a new
# scanned QR code can be broadcast to every connected client. Maps each
# subscriber's queue to a human-readable label (its address) for logging.
subscribers = {}
subscribers_lock = threading.Lock()


def broadcast(event):
	with subscribers_lock:
		recipients = dict(subscribers)
		dead_subscribers = []
		for subscriber, label in recipients.items():
			try:
				subscriber.put_nowait(event)
			except queue.Full:
				dead_subscribers.append(subscriber)
		for subscriber in dead_subscribers:
			subscribers.pop(subscriber, None)

	if recipients:
		print(f"Broadcasting {event['qr_code']!r} to {len(recipients)} client(s): "
			f"{', '.join(recipients.values())}")
	else:
		print(f"Broadcasting {event['qr_code']!r} to 0 clients (none connected)")

@contextmanager
def database_connection():
	connection = sqlite3.connect(DATABASE_PATH)
	try:
		with connection:
			yield connection
	finally:
		connection.close()


def initialize_database():
	with database_connection() as connection:
		connection.execute(
			"CREATE TABLE IF NOT EXISTS scanned_qr_codes "
			"(id INTEGER PRIMARY KEY, value TEXT NOT NULL, scanned_at TEXT NOT NULL)"
		)


class QRCodeHandler(BaseHTTPRequestHandler):
	# HTTP/1.1 lets us use chunked transfer-encoding for the SSE stream
	# below, which reverse proxies (like Render's) need to forward each
	# event as it's written instead of buffering the whole response.
	protocol_version = "HTTP/1.1"

	def send_json(self, status, payload, write_body=True):
		body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
		self.send_response(status)
		self.send_header("Content-Type", "application/json; charset=utf-8")
		self.send_header("Content-Length", str(len(body)))
		self.end_headers()
		if write_body:
			self.wfile.write(body)

	def do_POST(self):

		try:
			content_length = int(self.headers.get("Content-Length", "0"))
		except ValueError:
			self.send_json(400, {"error": "Invalid Content-Length"})
			return

		if content_length <= 0 or content_length > MAX_REQUEST_SIZE:
			self.send_json(413, {"error": "Request must be between 1 byte and 64 KB"})
			return

		try:
			payload = json.loads(self.rfile.read(content_length))
		except (json.JSONDecodeError, UnicodeDecodeError):
			self.send_json(400, {"error": "Expected a JSON request body"})
			return

		qr_code = payload.get("qr_code") if isinstance(payload, dict) else None
		if not isinstance(qr_code, str) or not qr_code.strip():
			self.send_json(400, {"error": "qr_code must be a non-empty string"})
			return

		if "bme" not in qr_code.lower():
			self.send_json(422, {"error": "qr_code must contain 'bme'", "qr_code": qr_code})
			return

		scanned_at = datetime.now(timezone.utc).isoformat()
		with database_connection() as connection:
			cursor = connection.execute(
				"INSERT INTO scanned_qr_codes (value, scanned_at) VALUES (?, ?)",
				(qr_code, scanned_at),
			)

		result = {"id": cursor.lastrowid, "qr_code": qr_code, "scanned_at": scanned_at}
		broadcast(result)
		self.send_json(201, result)

	def do_GET(self):
		self.handle_get(write_body=True)

	def do_HEAD(self):
		# Hosting platforms (e.g. Render) send HEAD requests for health
		# checks; without this they'd get a 501 "Unsupported method" and
		# might consider the service unhealthy. Reuses the GET handling but
		# only sends headers, no body.
		self.handle_get(write_body=False)

	def handle_get(self, write_body):
		if self.path == "/":
			# Lightweight health check endpoint for uptime monitors and the
			# hosting platform (e.g. Render pings "/" to confirm the
			# service is up).
			self.send_json(200, {"status": "ok"}, write_body=write_body)
			return

		if self.path == "/events":
			if write_body:
				self.handle_events()
			else:
				# /events is a never-ending stream, so a HEAD request can
				# only sensibly confirm the endpoint exists.
				self.send_json(200, {}, write_body=False)
			return

		if self.path != "/qr/latest":
			self.send_json(404, {"error": "Not found"}, write_body=write_body)
			return

		with database_connection() as connection:
			row = connection.execute(
				"SELECT id, value, scanned_at FROM scanned_qr_codes ORDER BY id DESC LIMIT 1"
			).fetchone()

		if row is None:
			self.send_json(404, {"error": "No QR code has been scanned"}, write_body=write_body)
			return

		self.send_json(200, {"id": row[0], "qr_code": row[1], "scanned_at": row[2]}, write_body=write_body)

	def handle_events(self):
		# Server-Sent Events endpoint: keeps the connection open so every
		# connected client receives each newly scanned QR code as it arrives.
		self.send_response(200)
		self.send_header("Content-Type", "text/event-stream; charset=utf-8")
		self.send_header("Cache-Control", "no-cache")
		self.send_header("Transfer-Encoding", "chunked")
		# Hints to common reverse proxies (nginx/Render/etc.) not to buffer
		# the response, which would otherwise delay or block the broadcast.
		self.send_header("X-Accel-Buffering", "no")
		self.end_headers()

		subscriber_queue = queue.Queue(maxsize=100)
		client_label = f"{self.client_address[0]}:{self.client_address[1]}"
		with subscribers_lock:
			subscribers[subscriber_queue] = client_label
		print(f"Client connected: {client_label} ({len(subscribers)} total)")

		try:
			while True:
				try:
					event = subscriber_queue.get(timeout=15)
				except queue.Empty:
					# Periodic comment keeps the connection alive and lets us
					# detect that the client has disconnected.
					self._write_sse_chunk(b": keep-alive\n\n")
					continue

				data = json.dumps(event, ensure_ascii=False)
				self._write_sse_chunk(f"data: {data}\n\n".encode("utf-8"))
		except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
			pass
		finally:
			with subscribers_lock:
				subscribers.pop(subscriber_queue, None)
			print(f"Client disconnected: {client_label} ({len(subscribers)} total)")

	def _write_sse_chunk(self, data):
		# Manually frame each write as an HTTP chunked-encoding chunk so
		# proxies forward it immediately instead of waiting for the
		# response to finish (which, for a streaming endpoint, never
		# happens).
		self.wfile.write(f"{len(data):x}\r\n".encode("ascii"))
		self.wfile.write(data)
		self.wfile.write(b"\r\n")
		self.wfile.flush()


if __name__ == "__main__":
	initialize_database()
	# HOST/PORT environment variables let you deploy without hardcoding an
	# IP address in the source; by default the server listens on every
	# network interface (0.0.0.0), so it's reachable regardless of which
	# IP the deployment host ends up with.
	host = os.environ.get("HOST", "0.0.0.0")
	port = int(os.environ.get("PORT", "8000"))
	print(f"QR server listening on http://{host}:{port}")
	server = ThreadingHTTPServer((host, port), QRCodeHandler)
	# Each connected client (including long-lived /events subscribers) is
	# handled on its own daemon thread, so many clients can stay connected
	# simultaneously and the process can still exit cleanly.
	server.daemon_threads = True
	server.serve_forever()
