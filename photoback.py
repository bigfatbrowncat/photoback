#!/usr/bin/env python3
from io import StringIO

from mock import patch
import argparse
import logging
import os
import sys
import json
import datetime

json_output = ""

# This import should be first in borg
# import borg.helpers.parseformat

# @patch('borg.helpers.parseformat.json_print')
# def list_printer(obj):
#     global json_output
#     json_output = borg.helpers.parseformat.json_dump(obj)
#
#
# borg.helpers.parseformat.json_print = list_printer
# borg.helpers.parseformat.json_print("hello")

import borg.repository
import borg.cache
import borg.constants
from borg.archiver import Archiver
import borg.archive

import borg.helpers.parseformat

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


class ListArgsSingleLast(BaseArgs):
    def __init__(self, archiver: Archiver, repo_path: str):
        super().__init__(func=archiver.do_list, repo_path=repo_path)
        self.json_lines = False
        self.format = "{archive}\t{time}"
        self.json = True
        self.iec = False # ?????
        self.prefix = None
        self.consider_checkpoints = False
        self.glob_archives = None

        self.sort_by = "ts"     # Sort by timestamp
        self.first = None
        self.last = 1           # Single last archive


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


# The function checks the time when the latest archive was created in the repository
# Returns the time in datetime object format
# If no archives found, it returns None
def check_last_archive_time(repo_path: str):
    listArgs = ListArgsSingleLast(archiver=a, repo_path=repo_path)
    #try:
    old_stdout = sys.stdout
    try:
        sys.stdout = listRes = StringIO()

        a.run(listArgs)
    finally:
        sys.stdout = old_stdout

    #listRes.seek(0)
    listRes = listRes.getvalue() #.readlines()
    list_res_json = json.loads(listRes)

    #print(list_res_json)

    # Parsing the output
    archives = list_res_json['archives']
    if len(archives) > 0:
        archive = archives[0]
        time_str = archive['time']
        dt = datetime.datetime.fromisoformat(time_str)
        return dt
    return None

    #except borg.repository.Repository.DoesNotExist as e:
    #    print("Repository does not exist at " + repo_path)


# The function decides if it should backup a repo and does if it has to.
# Returns True if it did the backup
# Returns False if it decided not to backup
def backup_one_repo_if_needed(repo_path: str, local_path: str, archive_name: str, not_older_than: datetime.timedelta=None):
    if not_older_than:
        # Here we are checking if the latest backup is older than
        # the specified timedelta and refusing to do a new one if it is not
        last_time = check_last_archive_time(repo_path)
        if last_time:
            now = datetime.datetime.now()
            if now < last_time + not_older_than:
                print(f"Backing up {repo_path} to {archive_name} is not necessary because the latest archive is only {now - last_time} old. Proceeding.")
                # Do not do a new backup
                return False

    # Do the new backup
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
    return True


def backup_all_repos_from_dir_if_needed(repo_path_root: str, local_root_path: str, archive_name: str, not_older_than: datetime.timedelta=None):
    how_many_backed_up = 0
    total_count = 0
    old_dir = os.curdir
    try:
        os.chdir(local_root_path)
        for dir in os.listdir("."):
            if os.path.isdir(dir):
                repo_path = repo_path_root + "/" + dir
                print("Backing up " + os.path.join(local_root_path, dir) + " to " + repo_path)
                if backup_one_repo_if_needed(repo_path=repo_path, local_path=dir, archive_name=archive_name, not_older_than=not_older_than):
                    how_many_backed_up += 1
                total_count += 1
    finally:
        os.chdir(old_dir)

    return how_many_backed_up, total_count


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('repo_config_filename', action='store', help="The repository set configuration file. "
                                                                     "Has to be present in the same directory as the subrepos."
                                                                     " Default name is photoback.json")
    parser.add_argument('--archive-name', '-a', action='store', help="The new archive name.", default=None)
    args = parser.parse_args(argv[1:])

    if args.repo_config_filename is None:
        raise RuntimeError(f"Missing repository configuration filename.")

    try:
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

        borg_env = None
        if "borg_env" in repo_conf:
            borg_env = repo_conf["borg_env"]

        not_older_than = None
        if "not_older_than_hours" in repo_conf:
            not_older_than = datetime.timedelta(hours=int(repo_conf["not_older_than_hours"]))

        for borg_varname in ("BORG_DELETE_I_KNOW_WHAT_I_AM_DOING",
                             "BORG_CHECK_I_KNOW_WHAT_I_AM_DOING",
                             "BORG_DISPLAY_PASSPHRASE",
                             "BORG_RELOCATED_REPO_ACCESS_IS_OK"):
            if borg_env is not None and borg_varname in borg_env:
                print(f"Setting Borg variable {borg_varname} to {borg_env[borg_varname]}")
                if borg_env[borg_varname].lower() == "ask":
                    #  Set nothing, this is the default behaviour
                    pass
                elif borg_env[borg_varname].lower() == "yes":
                    os.environ[borg_varname] = borg_env[borg_varname]
                elif borg_env[borg_varname].lower() == "no":
                    os.environ[borg_varname] = "no"
                else:
                    raise RuntimeError(f"Invalid value for Borg variable {borg_varname}.")
            else:
                print(f"Setting Borg variable {borg_varname} to 'no' (default value)")
                os.environ[borg_varname] = "no"

        local_root_path = os.path.dirname(args.repo_config_filename)

        print(f"Starting total backup. Archive name: {archive_name}")

        how_many_backed_up, total_count = backup_all_repos_from_dir_if_needed(repo_path_root=repo_root, local_root_path=local_root_path, archive_name=archive_name, not_older_than=not_older_than)
        print(f"Backuped up {how_many_backed_up} out of {total_count} archives.")
        print(f"Total backup for a new archive prefixed {archive_name} has completed successfully.")
    except borg.cache.Cache.RepositoryAccessAborted:
        print(f"Repository access aborted because of a chosen policy. The repository was probably relocated.\n" +
              f"Check your repo config file ({args.repo_config_filename}) to set a proper flag.")


if __name__ == '__main__':
    exit(main(sys.argv))
