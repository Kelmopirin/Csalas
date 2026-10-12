import json
import os
import queue
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


DATABASE_PATH = Path(__file__).with_name("qr_codes.db")
MAX_REQUEST_SIZE = 64 * 1024

# Thread-safe registry of all currently connected SSE subscribers, so a new
# scanned QR code can be broadcast to every connected client.
subscribers = set()
subscribers_lock = threading.Lock()


def broadcast(event):
	with subscribers_lock:
		dead_subscribers = []
		for subscriber in subscribers:
			try:
				subscriber.put_nowait(event)
			except queue.Full:
				dead_subscribers.append(subscriber)
		for subscriber in dead_subscribers:
			subscribers.discard(subscriber)

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
	def send_json(self, status, payload):
		body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
		self.send_response(status)
		self.send_header("Content-Type", "application/json; charset=utf-8")
		self.send_header("Content-Length", str(len(body)))
		self.end_headers()
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
		if self.path == "/events":
			self.handle_events()
			return

		if self.path != "/qr/latest":
			self.send_json(404, {"error": "Not found"})
			return

		with database_connection() as connection:
			row = connection.execute(
				"SELECT id, value, scanned_at FROM scanned_qr_codes ORDER BY id DESC LIMIT 1"
			).fetchone()

		if row is None:
			self.send_json(404, {"error": "No QR code has been scanned"})
			return

		self.send_json(200, {"id": row[0], "qr_code": row[1], "scanned_at": row[2]})

	def handle_events(self):
		# Server-Sent Events endpoint: keeps the connection open so every
		# connected client receives each newly scanned QR code as it arrives.
		self.send_response(200)
		self.send_header("Content-Type", "text/event-stream; charset=utf-8")
		self.send_header("Cache-Control", "no-cache")
		self.send_header("Connection", "keep-alive")
		self.end_headers()

		subscriber_queue = queue.Queue(maxsize=100)
		with subscribers_lock:
			subscribers.add(subscriber_queue)

		try:
			while True:
				try:
					event = subscriber_queue.get(timeout=15)
				except queue.Empty:
					# Periodic comment keeps the connection alive and lets us
					# detect that the client has disconnected.
					self.wfile.write(b": keep-alive\n\n")
					self.wfile.flush()
					continue

				data = json.dumps(event, ensure_ascii=False)
				self.wfile.write(f"data: {data}\n\n".encode("utf-8"))
				self.wfile.flush()
		except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
			pass
		finally:
			with subscribers_lock:
				subscribers.discard(subscriber_queue)


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
