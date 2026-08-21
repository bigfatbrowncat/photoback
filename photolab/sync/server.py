import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlsplit

from photolab.hash_collector import HashCollector
from photolab.sync.algorithm_builder import extract_album_name
from photolab.sync.file_manipulator import FileManipulator
from photolab.sync.immich_api_client import ImmichAPIClient
from photolab.sync.unexpected_server_state import UnexpectedServerStateError


class ServerRequestHandler(BaseHTTPRequestHandler):
    server_version = "PhotolabSync/1.0"
    protocol_version = "HTTP/1.1"

    def _send_json(self, status: int, payload: dict):
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self):
        content_length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(content_length)

    def do_GET(self):
        if urlsplit(self.path).path == "/api/collect_hashes":
            try:
                collector = HashCollector(self.server.photolab_root)
                collector.collect()
            except Exception as e:
                self._send_json(500, {"error": str(e)})
                return

            self._send_json(200, collector.get_map())
        else:
            self._send_json(404, {"error": "Not Found"})

    def do_POST(self):
        try:
            # Read the body up front so that an early error response (400/409)
            # does not leave unread request data on the keep-alive connection
            self.body = self._read_body()
            self._route_post()
        except (ValueError, json.JSONDecodeError) as e:
            self._send_json(400, {"error": str(e)})
        except UnexpectedServerStateError as e:
            self._send_json(500, {"error": str(e)})
        except Exception as e:
            self._send_json(500, {"error": str(e)})

    def _route_post(self):
        if urlsplit(self.path).path == "/api/create_album":
            self._create_album()
        elif urlsplit(self.path).path == "/api/upload":
            self._upload()
        else:
            self._send_json(404, {"error": "Not Found"})

    def _create_album(self):
        try:
            json_request = json.loads(self.body.decode("utf-8"))
        except json.JSONDecodeError:
            raise ValueError("Request body is not valid JSON")

        album_path = json_request.get("album_path")
        if not album_path:
            raise ValueError("Missing album_path in JSON request")
        # The client percent-encodes non-ASCII characters in the path
        album_path = unquote(album_path)

        try:
            album_name = extract_album_name(album_path)
        except (ValueError, IndexError):
            raise ValueError(f"Invalid album path: {album_path}") from None

        self.server.file_manipulator.create_dir(album_path)
        self.server.immich_client.create_album(album_name)
        self._send_json(201, {"status": "ok"})

    def _upload(self):
        file_path = self.headers.get("x-file-path")
        if not file_path:
            raise ValueError("Missing x-file-path header")
        # Header values are latin-1 only (RFC 7230), so the client percent-encodes
        # non-ASCII characters; decode them back to UTF-8
        file_path = unquote(file_path)

        contents = self.body

        try:
            self.server.file_manipulator.create_file(file_path, contents)
        except UnexpectedServerStateError as e:
            # Conflicting local state (e.g. the file already exists), not a failure
            self._send_json(409, {"error": str(e)})
            return

        try:
            album_name = extract_album_name(file_path)
        except (ValueError, IndexError):
            raise ValueError(f"Invalid file path: {file_path}") from None

        self.server.immich_client.add_image_to_album(album_name, file_path)
        self._send_json(201, {"status": "ok"})


class Server:
    def __init__(self, photolab_root: str,
                 serving_host: str,
                 serving_port: int,
                 immich_host: str,
                 immich_port: int,
                 immich_api_key: str):
        self.photolab_root = photolab_root
        self.serving_host = serving_host
        self.serving_port = serving_port

        self.immich_client = ImmichAPIClient(immich_host, immich_port, immich_api_key)
        self.file_manipulator = FileManipulator(photolab_root)

    def create_http_server(self):
        server_address = (self.serving_host, self.serving_port)
        # One thread per connection, so an idle keep-alive client cannot
        # block the whole server (ThreadingHTTPServer is daemon_threads=True)
        httpd = ThreadingHTTPServer(server_address, ServerRequestHandler)
        # Expose the application state to the request handler (self.server inside it)
        httpd.photolab_root = self.photolab_root
        httpd.file_manipulator = self.file_manipulator
        httpd.immich_client = self.immich_client
        return httpd

    def run(self):
        httpd = self.create_http_server()
        print(f"Server listening on {self.serving_host}:{self.serving_port}...")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nStopping server...")
            httpd.server_close()


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--photolab-root", help="Path to photolab root directory")
    arg_parser.add_argument("--host", default="127.0.0.1", help="Host to listen on")
    arg_parser.add_argument("-p", "--port", type=int, default=8080, help="Port to listen on")
    arg_parser.add_argument("--immich-host", default="127.0.0.1", help="Immich instance to control")
    arg_parser.add_argument("--immich-port", type=int, default=2283, help="Immich instance port")
    arg_parser.add_argument("--immich-api-key", required=True, help="API key for the Immich instance")
    args = arg_parser.parse_args()

    server = Server(
        photolab_root=args.photolab_root,
        serving_host=args.host,
        serving_port=args.port,
        immich_host=args.immich_host,
        immich_port=args.immich_port,
        immich_api_key=args.immich_api_key)
    server.run()


if __name__ == "__main__":
    main()
