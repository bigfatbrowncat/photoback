import argparse
import logging
import os
import sys
import json

import borg.repository
import borg.constants
import borg.helpers.parseformat
from borg.archiver import Archiver
import borg.archive

SUPPORTED_CONFIG_VERSIONS = [ "1" ]

a = Archiver()


class BaseArgs:
    def __init__(self, func, repo_path: str):
        self.func = func
        self.umask = 0o022
        self.log_level = 'INFO'
        self.progress = True
        self.show_version = False
        self.log_json = False
        self.lock_wait = None   # ????
        self.location = borg.helpers.parseformat.Location(repo_path)
        self.upload_ratelimit = None
        self.upload_buffer = 1
        self.debug_topics = []
        self.remote_path = None
        self.rsh = "ssh"
        self.encryption = "none"
        self.debug_profile = False

    def __contains__(self, item):
        # To behave like argparse.Namespace
        return hasattr(self, item)

class InitArgs(BaseArgs):
    def __init__(self, archiver: Archiver, repo_path: str):
        super().__init__(func=archiver.do_init, repo_path=repo_path)


class CheckArgs(BaseArgs):
    def __init__(self, archiver: Archiver, repo_path: str):
        super().__init__(func=archiver.do_check, repo_path=repo_path)
        self.repair = False
        self.repo_only = False
        self.archives_only = False
        self.max_duration = None
        self.save_space = False
        self.verify_data = True
        self.prefix = None
        self.first = None
        self.last = None
        self.sort_by = None
        self.glob_archives = None


class CreateArgs(BaseArgs):
    def __init__(self, archiver: Archiver, repo_path: str, local_path: str, archive_name: str):
        super().__init__(func=archiver.do_create, repo_path=repo_path)
        self.location = borg.helpers.parseformat.Location(repo_path + "::" + archive_name)
        self.dry_run = False
        self.comment = "Created automatically by photoback.py"
        self.sparse = True
        self.patterns = []
        self.output_filter = None
        self.output_list = True
        self.noflags = False
        self.numeric_ids = False
        self.nobsdflags = None
        self.noacls = False
        self.noxattrs = False
        self.atime = False
        self.noctime = False
        self.nobirthtime = False
        self.exclude_nodump = None
        self.content_from_command = False
        self.paths_from_command = False
        self.paths_from_stdin = False
        self.paths = [ local_path ]
        self.one_file_system = False
        self.exclude_caches = False
        self.exclude_if_present = None
        self.keep_exclude_tags = None
        self.read_special = False
        self.no_cache_sync = False
        self.files_cache_mode = borg.constants.FILES_CACHE_MODE_UI_DEFAULT
        self.chunker_params = borg.constants.CHUNKER_PARAMS
        self.iec = False # ?????
        self.checkpoint_interval = 1800
        self.timestamp = None
        self.stats = True

        self.log_json = False
        self.json = False


def backup_one_repo(repo_path: str, local_path: str, archive_name: str):

    checkArgs = CheckArgs(archiver=a, repo_path=repo_path)
    try:
        a.run(checkArgs)
    except borg.repository.Repository.DoesNotExist as e:
        print("Repository does not exist at " + repo_path)
        print("We are going to initialize a new one.")

        initArgs = InitArgs(archiver=a, repo_path=repo_path)
        try:
            a.run(initArgs)
        except borg.repository.Repository.AlreadyExists as e:
            print("Archive already exists at " + repo_path)

    print("The repository at " + repo_path + " is verified.")
    print("Backing up the local path " + local_path + " to " + repo_path)

    already_exists = True
    index = 0
    while already_exists:
        already_exists = False
        archive_name_index = archive_name
        try:
            if index > 0:
                archive_name_index += "-" + str(index)
            createArgs = CreateArgs(archiver=a, repo_path=repo_path, local_path=local_path, archive_name=archive_name_index)
            a.run(createArgs)
        except borg.archive.Archive.AlreadyExists as e:
            print("Archive " + archive_name_index + " already exists at " + repo_path)
            print("Trying another name")
            index += 1
            already_exists = True


def backup_all_repos_from_dir(repo_path_root: str, local_root_path: str, archive_name: str):
    old_dir = os.curdir
    try:
        os.chdir(local_root_path)
        for dir in os.listdir("."):
            if os.path.isdir(dir):
                repo_path = repo_path_root + "/" + dir
                print("Backing up " + os.path.join(local_root_path, dir) + " to " + repo_path)
                backup_one_repo(repo_path=repo_path, local_path=dir, archive_name=archive_name)
    finally:
        os.chdir(old_dir)


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('repo_config_filename', action='store', help="The repository set configuration file. "
                                                          "Has to be present in the same directory as the subrepos."
                                                          " Default name is photoback.json")
    parser.add_argument('--archive-name', '-a', action='store', help="The new archive name.", default=None)
    args = parser.parse_args(argv[1:])

    if args.repo_config_filename is None:
        raise RuntimeError(f"Missing repository configuration filename.")

    with open(args.repo_config_filename, "r") as conf:
        repo_conf = json.load(conf)

    if repo_conf["version"] not in SUPPORTED_CONFIG_VERSIONS:
        raise RuntimeError(f"The config version {repo_conf['version']} is not supported.")

    repo_root = None
    if "repo_root" in repo_conf:
        repo_root = repo_conf["repo_root"]

    archive_name = "unnamed-backup"
    if "standard_archive_name" in repo_conf:
        archive_name = repo_conf["standard_archive_name"]
    if args.archive_name is not None:
        archive_name = args.archive_name

    if repo_root is None:
        raise RuntimeError(f"The repository root is not specified in the configuration file {args.repo_config_filename}")

    local_root_path = os.path.dirname(args.repo_config_filename)

    print("* STARTING ARCHIVE BACKUP: " + archive_name)

    backup_all_repos_from_dir(repo_path_root=repo_root, local_root_path=local_root_path, archive_name=archive_name)

    print("* ARCHIVE BACKUP " + archive_name + " HAS SUCCESSFULLY FINISHED")


if __name__ == '__main__':
    exit(main(sys.argv))
