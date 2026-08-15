import os

from photolab.sync.unexpected_server_state import UnexpectedServerStateError

class FileManipulator:
    def __init__(self, photolab_dir):
        self.photolab_dir = photolab_dir

    def create_file(self, file_path, contents: bytes):
        file_path = os.path.join(self.photolab_dir, file_path)

        # Making sure the file does not exist
        if os.path.exists(file_path):
            raise UnexpectedServerStateError(f"File '{file_path}' already exists")

        with open(file_path, "wb") as f:
            f.write(contents)

    def delete_file(self, file_path):
        file_path = os.path.join(self.photolab_dir, file_path)

        # Making sure the file exists
        if not os.path.exists(file_path):
            raise UnexpectedServerStateError(f"File '{file_path}' does not exist")

        # TODO Wrap this into try
        os.remove(file_path)

    def move_file(self, old_file_path, new_file_path):
        old_file_path = os.path.join(self.photolab_dir, old_file_path)
        new_file_path = os.path.join(self.photolab_dir, new_file_path)

        # Making sure that the "old" file exists, but the "new" does not
        if not os.path.exists(old_file_path):
            raise UnexpectedServerStateError(f"File '{old_file_path}' does not exist")
        if os.path.exists(new_file_path):
            raise UnexpectedServerStateError(f"File '{new_file_path}' already exists")

        # TODO Wrap this into try
        os.rename(old_file_path, new_file_path)

    def is_dir_existing(self, dir_path):
        dir_path = os.path.join(self.photolab_dir, dir_path)

        return os.path.exists(dir_path)

    def create_dir(self, dir_path):
        dir_path = os.path.join(self.photolab_dir, dir_path)

        # Making sure the directory does not exist
        if os.path.exists(dir_path):
            raise UnexpectedServerStateError(f"Directory '{dir_path}' already exists")

        # TODO Wrap this into try
        os.mkdir(dir_path)

    def delete_dir(self, dir_path):
        dir_path = os.path.join(self.photolab_dir, dir_path)

        # Making sure the directory exists and empty
        if os.path.exists(dir_path):
            if os.listdir(dir_path):
                raise UnexpectedServerStateError(f"Directory '{dir_path}' is not empty")
            else:
                os.rmdir(dir_path)
        else:
            raise UnexpectedServerStateError(f"Directory '{dir_path}' does not exist")

    def rename_dir(self, old_dir_path, new_dir_path):
        old_dir_path = os.path.join(self.photolab_dir, old_dir_path)
        new_dir_path = os.path.join(self.photolab_dir, new_dir_path)

        # Making sure the source directory exists, but the target does not
        if not os.path.exists(old_dir_path):
            raise UnexpectedServerStateError(f"Directory '{old_dir_path}' does not exist")
        if os.path.exists(new_dir_path):
            raise UnexpectedServerStateError(f"Directory '{new_dir_path}' already exists")

        os.rename(old_dir_path, new_dir_path)
