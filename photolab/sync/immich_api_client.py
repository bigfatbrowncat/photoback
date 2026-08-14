import requests

from photolab.sync.unexpected_server_state import UnexpectedServerStateError

# This is an Immich API client designed be
# a bridge between Photolab and Immich, implementing the sync operations.
class ImmichAPIClient:
    def __init__(self, host, port, api_key):
        self.host = host
        self.port = port
        self.api_key = api_key

        self.headers = {
            "x-api-key": f"{self.api_key}",
            "accept": "application/json"
        }

    def _find_album_id(self, album_name):
        list_endpoint = f"http://{self.host}:{self.port}/api/albums"
        response = requests.get(list_endpoint, headers=self.headers)

        if not (200 <= response.status_code < 300):
            print(f"Error: Server responded with status code {response.status_code}")
            print(response.text)
            raise Exception("Failed to list albums")

        for album in response.json():
            if album["albumName"] == album_name:
                return album["id"]

        return None

    def _find_asset_id(self, image_path):
        search_endpoint = f"http://{self.host}:{self.port}/api/search/metadata"
        payload = {
            "originalPath": image_path
        }
        response = requests.post(search_endpoint, json=payload, headers=self.headers)

        if not (200 <= response.status_code < 300):
            print(f"Error: Server responded with status code {response.status_code}")
            print(response.text)
            raise Exception("Failed to search for asset")

        items = response.json()["assets"]["items"]
        if not items:
            return None

        return items[0]["id"]

    def create_album(self, album_name):
        existing_id = self._find_album_id(album_name)
        if existing_id is not None:
            raise UnexpectedServerStateError(f"Album '{album_name}' already exists")

        endpoint = f"http://{self.host}:{self.port}/api/albums"

        album_data = {
            "albumName": album_name
        }
        response = requests.post(endpoint, json=album_data, headers=self.headers)

        if 200 <= response.status_code < 300:
            print(f"Album '{album_name}' created successfully.")
            return response.json()["id"]
        else:
            print(f"Error: Server responded with status code {response.status_code}")
            print(response.text)
            raise Exception("Failed to create album")

    def rename_album(self, old_name, new_name):
        album_id = self._find_album_id(old_name)
        if album_id is None:
            raise UnexpectedServerStateError(f"Album '{old_name}' not found")

        new_album_preexisting_id = self._find_album_id(new_name)
        if new_album_preexisting_id is not None:
            raise UnexpectedServerStateError(f"Album '{new_name}' already exists")

        rename_endpoint = f"http://{self.host}:{self.port}/api/albums/{album_id}"
        album_data = {
            "albumName": new_name
        }
        response = requests.patch(rename_endpoint, json=album_data, headers=self.headers)

        if 200 <= response.status_code < 300:
            print(f"Album '{old_name}' renamed to '{new_name}' successfully.")
            return album_id
        else:
            print(f"Error: Server responded with status code {response.status_code}")
            print(response.text)
            raise Exception("Failed to rename album")

    def delete_album(self, album_name):
        album_id = self._find_album_id(album_name)
        if album_id is None:
            raise UnexpectedServerStateError(f"Album '{album_name}' not found")

        delete_endpoint = f"http://{self.host}:{self.port}/api/albums/{album_id}"
        response = requests.delete(delete_endpoint, headers=self.headers)

        if 200 <= response.status_code < 300:
            print(f"Album '{album_name}' deleted successfully.")
        else:
            print(f"Error: Server responded with status code {response.status_code}")
            print(response.text)
            raise Exception("Failed to delete album")

    def add_image_to_album(self, album_name, image_path):
        album_id = self._find_album_id(album_name)
        if album_id is None:
            raise UnexpectedServerStateError(f"Album '{album_name}' not found")

        asset_id = self._find_asset_id(image_path)
        if asset_id is None:
            raise UnexpectedServerStateError(f"Asset '{image_path}' not found")

        add_endpoint = f"http://{self.host}:{self.port}/api/albums/{album_id}/assets"
        asset_data = {
            "ids": [asset_id]
        }
        response = requests.put(add_endpoint, json=asset_data, headers=self.headers)

        if 200 <= response.status_code < 300:
            print(f"Image '{image_path}' added to album '{album_name}' successfully.")
        else:
            print(f"Error: Server responded with status code {response.status_code}")
            print(response.text)
            raise Exception("Failed to add image to album")

    def remove_image_from_album(self, album_name, image_path):
        album_id = self._find_album_id(album_name)
        if album_id is None:
            raise UnexpectedServerStateError(f"Album '{album_name}' not found")
        asset_id = self._find_asset_id(image_path)
        if asset_id is None:
            raise UnexpectedServerStateError(f"Asset '{image_path}' not found")

        remove_endpoint = f"http://{self.host}:{self.port}/api/albums/{album_id}/assets"
        asset_data = {
            "ids": [asset_id]
        }
        response = requests.delete(remove_endpoint, json=asset_data, headers=self.headers)

        if 200 <= response.status_code < 300:
            print(f"Image '{image_path}' removed from album '{album_name}' successfully.")
        else:
            print(f"Error: Server responded with status code {response.status_code}")
            print(response.text)
            raise Exception("Failed to remove image from album")