import argparse
import json
from http.server import BaseHTTPRequestHandler, HTTPServer

from photolab.hash_collector import HashCollector

# Simulated in-memory database
# mock_db = [
#     {"id": 1, "name": "Alice", "role": "Developer"},
#     {"id": 2, "name": "Bob", "role": "Designer"},
# ]

class PhotolabSyncServer:
    def __init__(self, photolab_root: str,
                 serving_host: str = "127.0.0.1",
                 serving_port: int = 8080):
        self.photolab_root = photolab_root
        self.host = serving_host
        self.port = serving_port

    def run(self):
        photolab_root = self.photolab_root
        host = self.host
        port = self.port

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

            # def do_POST(self):
            #     """Handles POST requests to create/process JSON data."""
            #     if self.path == "/api/users":
            #         # 1. Read the length of the data payload
            #         content_length = int(self.headers.get("Content-Length", 0))
            #
            #         # 2. Read and decode the raw body text
            #         raw_body = self.rfile.read(content_length).decode("utf-8")
            #
            #         try:
            #             # 3. Parse the string data into a Python dictionary
            #             new_user = json.loads(raw_body)
            #
            #             # Generate a quick ID and simulate saving to DB
            #             new_user["id"] = len(mock_db) + 1
            #             mock_db.append(new_user)
            #
            #             # 4. Respond with the successfully added item
            #             self._set_headers(201)
            #             self.wfile.write(json.dumps(new_user).encode("utf-8"))
            #
            #         except json.JSONDecodeError:
            #             # Handle bad formatting
            #             self._set_headers(400)
            #             self.wfile.write(
            #                 json.dumps({"error": "Invalid JSON payload"}).encode(
            #                     "utf-8"
            #                 )
            #             )
            #     else:
            #         self._set_headers(404)
            #         self.wfile.write(json.dumps({"error": "Not Found"}).encode("utf-8"))

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
    args = arg_parser.parse_args()

    server = PhotolabSyncServer(photolab_root=args.photolab_root, serving_host=args.host, serving_port=int(args.port))
    server.run()
