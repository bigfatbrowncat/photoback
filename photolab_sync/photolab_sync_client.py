import requests
import pprint
from photolab_hash_collector import PhotolabHashCollector

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
    host = "localhost"
    port = "8080"
    local_photolab_root = "/Users/Shared/photolab"

    client = PhotolabSyncClient(host, port)
    server_hashes = client.collect_hashes()
    pprint.pprint(server_hashes, width=300, indent=4, compact=False)

    local_hash_collector = PhotolabHashCollector(local_photolab_root)
    local_hash_collector.collect()
    local_hashes = local_hash_collector.get_map()
    pprint.pprint(local_hashes, width=300, indent=4, compact=False)

    print(server_hashes == local_hashes)