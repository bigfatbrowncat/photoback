import argparse
import os
from urllib.parse import quote

import requests
from photolab.hash_collector import HashCollector
from algorithm_builder import build_sync_algorithm, OperationType

class Client:
    def __init__(self, host: str, port: int, photolab_root: str):
        self.host = host
        self.port = port
        self.photolab_root = photolab_root
        self.headers = {
            "accept": "application/json"
        }

    def collect_hashes(self):
        endpoint = f"http://{self.host}:{self.port}/api/collect_hashes"
        response = requests.get(endpoint, headers=self.headers)

        if 200 <= response.status_code < 300:
            print("Hashes collected successfully")
            return response.json()
        else:
            self._raise_for_error(response, "collect hashes")

    def create_album(self, album_path: str):
        endpoint = f"http://{self.host}:{self.port}/api/create_album"
        response = requests.post(endpoint, json={"album_path": album_path}, headers=self.headers)

        if 200 <= response.status_code < 300:
            print(f"Album created: {album_path}")
        else:
            self._raise_for_error(response, f"create album '{album_path}'")

    def upload(self, file_path: str):
        endpoint = f"http://{self.host}:{self.port}/api/upload"
        with open(os.path.join(self.photolab_root, file_path), "rb") as f:
            # Header values are latin-1 only (RFC 7230), so percent-encode the UTF-8 path
            # to keep non-ASCII characters safe; the server decodes it back with unquote
            response = requests.post(endpoint, data=f.read(),
                                     headers={**self.headers, "x-file-path": quote(file_path, safe="/")})

        if 200 <= response.status_code < 300:
            print(f"Uploaded: {file_path}")
        else:
            self._raise_for_error(response, f"upload '{file_path}'")

    def _raise_for_error(self, response, action):
        print(f"Error: Server responded with status code {response.status_code}")
        print(response.text)
        raise Exception(f"Failed to {action}")


def execute_operation(client, operation):
    if operation.operation_type == OperationType.UPLOAD:
        client.upload(operation.remote_path)
    elif operation.operation_type == OperationType.CREATE_ALBUM:
        client.create_album(operation.remote_path)
    else:
        # TODO: no server endpoints for DELETE / MOVE / DELETE_ALBUM / RENAME_ALBUM yet
        print(f"Skipped (not supported yet): {operation}")


if __name__ == "__main__":
    # Make 3 arguments. Host, port and photolab-root
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("photolab_root", help="Path to photolab root directory")
    arg_parser.add_argument("--host", default="127.0.0.1", help="Server host address")
    arg_parser.add_argument("-p", "--port", default=8080, help="Server port")
    args = arg_parser.parse_args()

    client = Client(args.host, args.port, args.photolab_root)
    server_hashes = client.collect_hashes()
    print(f"Total server hashes: {len(server_hashes)}")

    local_hash_collector = HashCollector(args.photolab_root)
    local_hash_collector.collect()
    local_hashes = local_hash_collector.get_map()
    print(f"Total local hashes: {len(local_hashes)}")

    print("Building synchronizing algorithm")
    sync_operations = build_sync_algorithm(local_hashes, server_hashes)
    print()
    print(f"The synchronizing algorithm containing {len(sync_operations)} operations built successfully:")
    for op in sync_operations:
        print(f" - {op}")

    print()
    print("Executing the synchronizing algorithm")
    for operation in sync_operations:
        execute_operation(client, operation)
    print("Done")