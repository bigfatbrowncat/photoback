import argparse

import requests
import pprint
from photolab.hash_collector import HashCollector
from algorithm_builder import build_sync_algorithm

class PhotolabSyncClient:
    def __init__(self, host: str, port: str):
        self.host = host
        self.port = port
        self.headers = {
            "accept": "application/json"
        }

    def collect_hashes(self):
        # Setting random password for the user
        endpoint = f"http://{self.host}:{self.port}/api/collect_hashes"
        req_data = {
            # Nothing here yet
        }
        response = requests.get(endpoint, json=req_data, headers=self.headers)

        if 200 <= response.status_code < 300:
            print(f"Hashes collected successfully")
            return response.json()
        else:
            print(f"Error: Server responded with status code {response.status_code}")
            print(response.text)
            raise Exception("Failed to collect hashes")

if __name__ == "__main__":
    # Make 3 arguments. Host, port and photolab-root
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("photolab_root", help="Path to photolab root directory")
    arg_parser.add_argument("--host", default="127.0.0.1", help="Server host address")
    arg_parser.add_argument("-p", "--port", default=8080, help="Server port")
    args = arg_parser.parse_args()

    client = PhotolabSyncClient(args.host, args.port)
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