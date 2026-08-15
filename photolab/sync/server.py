import argparse
import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

from photolab.hash_collector import HashCollector
from photolab.sync import immich_api_client
from photolab.sync.file_manipulator import FileManipulator
from photolab.sync.immich_api_client import ImmichAPIClient
from photolab.sync.unexpected_server_state import UnexpectedServerStateError

from algorithm_builder import extract_album_name

# Simulated in-memory database
# mock_db = [
#     {"id": 1, "name": "Alice", "role": "Developer"},
#     {"id": 2, "name": "Bob", "role": "Designer"},
# ]

class Server:
    def __init__(self, photolab_root: str,
                 serving_host: str,
                 serving_port: int,
                 immich_host: str,
                 immich_port: int,
                 immich_api_key: str):
        self.photolab_root = photolab_root
        self.host = serving_host
        self.port = serving_port

        self.immich_client = ImmichAPIClient(immich_host, immich_port, immich_api_key)
        self.file_manipulator = FileManipulator(photolab_root)

    def run(self):
        photolab_root = self.photolab_root
        host = self.host
        port = self.port
        file_manipulator = self.file_manipulator
        immich_client = self.immich_client

        class JSONRequestHandler(BaseHTTPRequestHandler):
            def _set_headers(self, status=200):
                """Helper to set standard JSON headers."""
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()

            def do_GET(self):
                """Handles GET requests to fetch data."""
                if self.path == "/api/collect_hashes":
                    self._set_headers(200)

                    # Collect the data
                    phc = HashCollector(photolab_root)
                    phc.collect()

                    # Serialize the data and send it back
                    response_data = json.dumps(phc.get_map(), ensure_ascii=False)
                    self.wfile.write(response_data.encode("utf-8"))
                else:
                    # Handle unknown routes
                    self._set_headers(404)
                    self.wfile.write(json.dumps({"error": "Not Found"}).encode("utf-8"))

            def do_POST(self):
                """Handles POST requests to create/process JSON data."""
                if self.path == "/api/create_album":
                    content_length = int(self.headers.get("Content-Length", 0))
                    contents = self.rfile.read(content_length)

                    json_request = json.loads(contents.decode("utf-8"))
                    album_path = json_request.get("album_path")
                    if not album_path:
                        self._set_headers(400)
                        self.wfile.write(json.dumps({"error": "Missing album_path in JSON request"}).encode("utf-8"))
                        return

                    try:
                        # Extracting the album name
                        album_name = extract_album_name(album_path)
                    except ValueError as e:
                        self._set_headers(400)
                        self.wfile.write(json.dumps({"error": f"Invalid album path: {album_path}"}).encode("utf-8"))
                        return

                    try:
                        # Creating the directory
                        file_manipulator.create_dir(album_path)

                        # Creating the album
                        immich_client.create_album(album_name)

                    except UnexpectedServerStateError as e:
                        self._set_headers(500)
                        self.wfile.write(json.dumps({"error": str(e)}).encode("utf-8"))
                        return
                    except Exception as e:
                        self._set_headers(500)
                        self.wfile.write(json.dumps({"error": str(e)}).encode("utf-8"))
                        return

                if self.path == "/api/upload":
                    file_path = self.headers.get("x-file-path")
                    if not file_path:
                        self._set_headers(400)
                        self.wfile.write(json.dumps({"error": "Missing x-file-path header"}).encode("utf-8"))
                        return

                    content_length = int(self.headers.get("Content-Length", 0))
                    contents = self.rfile.read(content_length)

                    try:
                        file_manipulator.create_file(file_path, contents)
                    except UnexpectedServerStateError as e:
                        self._set_headers(409)
                        self.wfile.write(json.dumps({"error": str(e)}).encode("utf-8"))
                        return

                    try:
                        # Extracting the album name
                        album_name = extract_album_name(file_path)
                    except ValueError as e:
                        self._set_headers(400)
                        self.wfile.write(json.dumps({"error": f"Invalid file path: {file_path}"}).encode("utf-8"))
                        return

                    try:
                        # Adding the new file to the album
                        immich_client.add_image_to_album(album_name, file_path)
                    except UnexpectedServerStateError as e:
                        self._set_headers(500)
                        self.wfile.write(json.dumps({"error": str(e)}).encode("utf-8"))
                        return

                    self._set_headers(201)
                    self.wfile.write(json.dumps({"status": "ok"}).encode("utf-8"))
                else:
                    self._set_headers(404)
                    self.wfile.write(json.dumps({"error": "Not Found"}).encode("utf-8"))

        server_address = (host, port)
        httpd = HTTPServer(server_address, JSONRequestHandler)
        print(f"Server listening on {host}:{port}...")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nStopping server...")
            httpd.server_close()

if __name__ == "__main__":
    # Make 3 arguments. Host, port and photolab-root
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("photolab_root", help="Path to photolab root directory")
    arg_parser.add_argument("--host", default="127.0.0.1", help="Hosts to listen from")
    arg_parser.add_argument("-p", "--port", default=8080, help="Port to listen on")
    arg_parser.add_argument("--immich-host", default="127.0.0.1", help="Immich instance to control")
    arg_parser.add_argument("--immich-port", default=2283, help="Immich instance port")
    arg_parser.add_argument("--immich-api-key", help="API key to sync to")
    args = arg_parser.parse_args()

    server = Server(photolab_root=args.photolab_root, serving_host=args.host, serving_port=int(args.port),
                    immich_host=args.immich_host, immich_port=args.immich_port, immich_api_key=args.immich_api_key)
    server.run()
