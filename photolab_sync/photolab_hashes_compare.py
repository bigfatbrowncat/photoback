import os
from enum import Enum
import Levenshtein

class OperationType(Enum):
    UPLOAD = 1
    DELETE = 2
    MOVE = 3
    CREATE_ALBUM = 4
    DELETE_ALBUM = 5
    RENAME_ALBUM = 6

class Operation:
    def __init__(self, operation_type, remote_path, local_path=None):
        self.operation_type = operation_type
        self.local_path = local_path
        self.remote_path = remote_path

    def __str__(self):
        if self.operation_type == OperationType.UPLOAD:
            return f"UPLOAD: {self.remote_path}"
        elif self.operation_type == OperationType.DELETE:
            return f"DELETE: {self.remote_path}"
        elif self.operation_type == OperationType.MOVE:
            return f"MOVE: {self.remote_path} -> {self.local_path}"
        elif self.operation_type == OperationType.CREATE_ALBUM:
            return f"CREATE_ALBUM: {self.remote_path}"
        elif self.operation_type == OperationType.DELETE_ALBUM:
            return f"DELETE_ALBUM: {self.remote_path}"
        elif self.operation_type == OperationType.RENAME_ALBUM:
            return f"RENAME_ALBUM: {self.remote_path} -> {self.local_path}"
        else:
            return None


# This structure contains a tree of pictures in the following format:
# {
#     ("event1", "subevent1"): (
#         "1a8b53...": "filename1.jpg",
#         "c9d8e7...": "filename2.jpg"
#     ),
#     ("event2", "subevent2"): (
#         "qwe876...": "filename3.jpg",
#         "a4s5d6...": "filename4.jpg"
#     )
# }

class AlbumHashNames:
    # Constructs from a dictionary where the key is hash and value is filename
    def __init__(self, pairs):
        self.__data = dict()
        for hash in pairs.keys():
            year = pairs[hash].split("/")[0]
            month = pairs[hash].split("/")[1]
            event_name = pairs[hash].split("/")[2]
            subevent_name = pairs[hash].split("/")[3]
            key = os.path.join(year, month, event_name, subevent_name)
            if key not in self.__data:
                self.__data[key] = set()

            self.__data[key].add((hash, pairs[hash]))

    def get_event_names(self):
        return list(self.__data.keys())

    def get_pairs_for(self, event_name):
        return self.__data[event_name]

    def event_contains_hash(self, event_name, hash):
        for pair in self.__data[event_name]:
            if pair[0] == hash:
                return True
        return False


def extract_album_name(image_path: str):
    s = image_path.split("/")
    return s[0] + "/" + s[1] + "/" + s[2] + "/" + s[3]


# Assuming the structure is a dict() where the key is a hash,
# and the value is a tree to the file relative to the photolab root.
# Item example:
#   'fef07ef14141b06370c1f37dd8ad5152f62bafe8f8e435f1ef181c7c82ca1a0618462a7716ba3d82618d92049528606eaf5b36ce48888e2791cd60781c967f2a': '2026/April/Misc/Джаз Sandia Quartet в Бабе-Яге/General/Personal/3 stars/0L5A0599_1.jpg'
def build_sync_algorithm(local_pairs, remote_pairs):
    remaining_local_pairs = local_pairs.copy()
    remaining_remote_pairs = remote_pairs.copy()

    # Collecting the albums of all the local hashes
    local_albums_pics = AlbumHashNames(local_pairs)
    local_event_names = local_albums_pics.get_event_names()

    # Collecting the albums of all the remote hashes
    remote_albums_pics = AlbumHashNames(remote_pairs)
    remote_event_names = remote_albums_pics.get_event_names()

    # Step 1: Finding new files
    # Check what hashes exist in local_hashes, but not in remote_hashes
    new_local_hashes = set()
    for local_hash in remaining_local_pairs:
        found = False
        for remote_hash in remaining_remote_pairs:
            if local_hash == remote_hash:
                found = True
                break
        if not found:
            new_local_hashes.add(local_hash)

    print(f"New hashes count: {len(new_local_hashes)}")


    # Adding UPLOAD operation for each new file
    uploading_operations = []
    for new_local_hash in new_local_hashes:
        uploading_operations.append(Operation(OperationType.UPLOAD, remaining_local_pairs[new_local_hash]))

    # Erasing all new_local_hashes from local_hashes
    for new_hash in new_local_hashes:
        remaining_local_pairs.pop(new_hash)

    # Step 2: Finding deleted files
    # Check what hashes exist in remote_hashes, but don't exist in local_hashes
    deleted_remote_hashes = set()
    for remote_hash in remaining_remote_pairs:
        found = False
        for local_hash in remaining_local_pairs:
            if local_hash == remote_hash:
                found = True
                break
        if not found:
            deleted_remote_hashes.add(remote_hash)
    print(f"Deleted hashes count: {len(deleted_remote_hashes)}")

    # Adding DELETE operation for each deleted old file
    deletion_operations = []
    for deleted_remote_hash in deleted_remote_hashes:
        deletion_operations.append(Operation(OperationType.DELETE, remaining_remote_pairs[deleted_remote_hash]))

    # Erasing all the deleted_remote_hashes from remote_hashes
    for deleted_remote_hash in deleted_remote_hashes:
        remaining_remote_pairs.pop(deleted_remote_hash)

    # Step 3a: Finding moved files (the files that exist in both local_hashes and remote_hashes, but have different path)
    moved_hashes = []
    assert(len(remaining_local_pairs) == len(remaining_remote_pairs))
    for local_hash in remaining_local_pairs:
        if remaining_remote_pairs[local_hash] != remaining_local_pairs[local_hash]:
            moved_hashes.append(local_hash)

    # Collecting remote albums that are missing in local
    remotes_missing_in_local = set()
    for remote_event in remote_event_names:
        found = False
        for local_event in local_event_names:
            if local_event == remote_event:
                found = True
        if not found:
            remotes_missing_in_local.add(remote_event)

    # For each remote missing in local, collecting list of local albums
    # containing former pictures from this remote album
    album_rename_operations = []
    album_deletion_operations = []
    renamed_albums = []
    for missing_in_local in remotes_missing_in_local:
        local_events_containing_images_from_missing_remote = set()
        for pair_from_missing in remote_albums_pics.get_pairs_for(missing_in_local):
            for local_event in local_event_names:
                if local_albums_pics.event_contains_hash(local_event, pair_from_missing[0]):
                    local_events_containing_images_from_missing_remote.add(local_event)

        # Looking for the successive (renamed from the original) album name.
        # To choose the one, comparing the album names according to Levenshtein ratio
        max_lratio = 0.0
        max_name = None
        for successor_album in local_events_containing_images_from_missing_remote:
            lratio = Levenshtein.ratio(successor_album, missing_in_local)
            if lratio > max_lratio:
                max_lratio = lratio
                max_name = successor_album

        if max_name is None:
            album_deletion_operations.append(Operation(OperationType.DELETE_ALBUM, missing_in_local))
        else:
            album_rename_operations.append(Operation(OperationType.RENAME_ALBUM, missing_in_local, max_name))
            renamed_albums.append(max_name)

    # Adding single file movement operations for the files that are moved one by one, not by renaming the album
    movement_operations = []
    for hash_to_move in moved_hashes:
        if extract_album_name(remaining_local_pairs[hash_to_move]) not in renamed_albums:
            movement_operations.append(Operation(OperationType.MOVE, remaining_remote_pairs[hash_to_move], remaining_local_pairs[hash_to_move]))


    # Adding CREATE_ALBUM operation for each new file's album that is missing on remote and wasn't renamed
    album_creation_operations = []
    for local_hash in local_pairs.keys():
        local_image_path = local_pairs[local_hash]
        local_image_album = extract_album_name(local_image_path)
        if not local_image_album in renamed_albums:
            if local_image_album in local_albums_pics.get_event_names():
                found = False
                for remote_album in remote_albums_pics.get_event_names():
                    if local_image_album == remote_album:
                        found = True
                        break
                if not found:
                    album_creation_operations.append(Operation(OperationType.CREATE_ALBUM, local_image_album))


    return deletion_operations + \
           album_deletion_operations + \
           album_creation_operations + \
           movement_operations + \
           album_rename_operations + \
           uploading_operations
