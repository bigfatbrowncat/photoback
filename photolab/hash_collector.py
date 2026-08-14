import datetime
import os

from iptcinfo3 import IPTCInfo
from pathlib import Path
import hashlib

class SingleFileMetadataHasher:
    @staticmethod
    def __parse_optional_offset(date_str):
        formats = ["%Y%m%d %H%M%S%z", "%Y%m%d %H%M%S"]
        for fmt in formats:
            try:
                return datetime.datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        raise ValueError(f"Time data '{date_str}' does not match any known format")

    def __init__(self, filepath):
        self.__info = IPTCInfo(filepath)
        self.__filename = Path(filepath).name
        self.__filepath = filepath

        #self.__subcategory = [x.decode('utf-8') for x in self.__info['supplemental category']]
        #self.__keywords = self.__info['keywords']

        if 'date created' in self.__info and 'time created' in self.__info:
            dtc = self.__info['date created'].decode('utf-8') + " " + self.__info['time created'].decode('utf-8')
            self.__date_time_created = SingleFileMetadataHasher.__parse_optional_offset(dtc)
        else:
            self.__date_time_created = None

        if 'digital creation date' in self.__info and 'digital creation time' in self.__info:
            ddtc = self.__info['digital creation date'].decode('utf-8') + " " + self.__info['digital creation time'].decode('utf-8')
            self.__digital_creation_date_time = SingleFileMetadataHasher.__parse_optional_offset(ddtc)
        else:
            self.__digital_creation_date_time = None

        # if 'by-line' in self.__info:
        #     self.__creator = self.__info['by-line'].decode('utf-8')
        # else:
        #     self.__creator = None

    def __str__(self):
        return ("{ filename: " + self.__filename + ", " +
#                "subcategory: " + str(self.__subcategory) + ", " +
#                "keywords: " + str(self.__keywords) + ", " +
                "date_time_created: " + str(self.__date_time_created) + ", " +
                "digital_creation_date_time: " + str(self.__digital_creation_date_time) + ", " +
#                "creator: " + str(self.__creator) +
                " }")

    # def metadata_hash(self):
    #     dig = hashlib.sha512(self.__filename.encode('utf-8')).hexdigest()
    #     # for sc in self.__subcategory:
    #     #   dig += hashlib.sha512(sc.encode('utf-8')).hexdigest()
    #     # for kw in self.__keywords:
    #     #    dig += hashlib.sha512(kw).hexdigest()
    #     if self.__date_time_created is not None:
    #         dig += hashlib.sha512(str(self.__date_time_created).encode('utf-8')).hexdigest()
    #     if self.__digital_creation_date_time is not None:
    #         dig += hashlib.sha512(str(self.__digital_creation_date_time).encode('utf-8')).hexdigest()
    #     # if self.__creator is not None:
    #     #     dig += hashlib.sha512(str(self.__creator).encode('utf-8')).hexdigest()
    #
    #     # Now hashing the hash
    #     hdres = hashlib.sha512(dig.encode('utf-8')).hexdigest()
    #     #hash_as_int = int(hdres, 16)
    #     #return hash_as_int
    #     return hdres

    def contents_hash(self):
        with open(self.__filepath, "rb") as file:
            # Automatically reads and hashes the file efficiently
            digest = hashlib.file_digest(file, "sha512")
        return digest.hexdigest()


class HashCollector:
    def __init__(self, root_dir):
        self.__map = None
        self.__root_dir = root_dir

    def collect(self):
        # Walking through the folders starting from the root dir and creating map
        self.__map = dict()

        original_dir = os.getcwd()
        os.chdir(self.__root_dir)
        try:
            # List root
            for year_folder in os.listdir("."):
                if year_folder != ".src":
                    for root, dirs, files in os.walk(os.path.join(".", year_folder)):
                        for file in files:
                            if file.endswith(".jpeg") or file.endswith(".jpg") or file.endswith(".png") or file.endswith(".tiff"):
                                filepath = os.path.join(root, file)
                                assert filepath.startswith("./")
                                filepath = filepath[2:]
                                hasher = SingleFileMetadataHasher(filepath)
                                self.__map[hasher.contents_hash()] = filepath
        finally:
            # Come back
            os.chdir(original_dir)

    def get_map(self):
        return self.__map

