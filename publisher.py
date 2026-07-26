import os
import re
from contextlib import closing

import paramiko
import scp
from paramiko import SSHClient
from scp import SCPClient
import scpclient
import argparse
import unicodedata


def put_dir(sftp, source: str, dest: str):
    source = os.path.expandvars(source).rstrip('\\').rstrip('/')
    # Let's normalize destination characters so ё and й are encoded in a single character
    dest = unicodedata.normalize('NFC', dest)  # NFC - Composed

    dest = os.path.expandvars(dest).rstrip('\\').rstrip('/')

    for root, dirs, files in os.walk(source):
        for dir in dirs:
            try:
                destdir = unicodedata.normalize('NFC', dir)
                mkdir_or_false(sftp, os.path.join(dest, ''.join(root.rsplit(source))[1:], destdir))
            except Exception as e:
                print(e)
                pass
        for file in files:
            destfile = unicodedata.normalize('NFC', file)
            sftp.put(os.path.join(root, file), os.path.join(dest, ''.join(root.rsplit(source))[1:], destfile))



MONTH_NAMES = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December"
]

CLASS_NAMES = [
    "General",
    "Semi-Private",
    "Private"
]

GENERAL_CATEGORIES = [
    "Personal",
    "Impersonal"
]

SEMIPRIV_CATEGORIES = [
    "Recognizable",
    "Unrecognizable"
]

PRIV_CATEGORIES = [
    "Recognizable",
    "Unrecognizable"
]


MIDDAY = 'midday/'
TWILIGHT = 'twilight/'
MIDNIGHT = 'midnight/'


def mkdir_or_false(sftp, path):
    try:
        sftp.listdir(path)
        return False
    except IOError:
        sftp.mkdir(path)
        return True


def main():
    ap = argparse.ArgumentParser(description='Publisher script')
    ap.add_argument('-y', '--year', required=False, type=str, help='Filtered year')
    ap.add_argument('-m', '--month', required=False, type=str, help='Filtered month (name or number)')
    ap.add_argument('-e', '--event', required=False, type=str, help='Filtered event')
    args = ap.parse_args()

    ssh = SSHClient()
    ssh.load_system_host_keys()
    res = ssh.connect(hostname='lumiere', username="app-runner")

    scpc = scp.SCPClient(ssh.get_transport())

    year_format = re.compile('\d\d\d\d')
    folders = os.listdir('.')
    folders.sort()

    sftp = ssh.open_sftp()
    sftp.chdir('/var/server-apps/immich-app/photolab')
    mkdir_or_false(sftp, MIDDAY)
    mkdir_or_false(sftp, TWILIGHT)
    mkdir_or_false(sftp, MIDNIGHT)

    for f in folders:
        if year_format.match(f):
            year = f
            if args.year and year != args.year:
                print(f"Skipping year {year}, filtered out")
                continue
            path = year
            print(f" - {year}")
            mkdir_or_false(sftp, os.path.join(MIDDAY, path))
            mkdir_or_false(sftp, os.path.join(TWILIGHT, path))
            mkdir_or_false(sftp, os.path.join(MIDNIGHT, path))
            month_indices = []
            for ff in os.listdir(path):
                try:
                    month_indices.append(MONTH_NAMES.index(ff))
                except ValueError:
                    # Do nothing
                    pass

            month_indices.sort()
            for month_index in month_indices:
                month = MONTH_NAMES[month_index]
                if args.month and month.lower() != args.month.lower() and str(month_index + 1) != args.month:
                    print(f"Skipping month {month}, filtered out")
                    continue
                path = os.path.join(year, month)
                print(f"   - {month}")
                mkdir_or_false(sftp, os.path.join(MIDDAY, path))
                mkdir_or_false(sftp, os.path.join(TWILIGHT, path))
                mkdir_or_false(sftp, os.path.join(MIDNIGHT, path))
                #print("Month exists on the target. Skipping")

                for fff in os.listdir(path):
                    if not fff.startswith("."):
                        event = fff
                        if args.event and event != args.event:
                            print(f"Skipping event {event}, filtered out")
                            continue

                        path = os.path.join(year, month, event)
                        print(f"     - {event}")
                        mkdir_or_false(sftp, os.path.join(MIDDAY, path))
                        mkdir_or_false(sftp, os.path.join(TWILIGHT, path))
                        mkdir_or_false(sftp, os.path.join(MIDNIGHT, path))

                        for ffff in os.listdir(path):
                            if not ffff.startswith("."):
                                subevent = ffff
                                path = os.path.join(year, month, event, subevent)
                                print(f"       - {subevent}")
                                mkdir_or_false(sftp, os.path.join(MIDDAY, path))
                                mkdir_or_false(sftp, os.path.join(TWILIGHT, path))
                                mkdir_or_false(sftp, os.path.join(MIDNIGHT, path))

                                for fffff in os.listdir(path):
                                    path = os.path.join(year, month, event, subevent, fffff)

                                    if fffff == "General":
                                        if mkdir_or_false(sftp, os.path.join(MIDDAY, path)):
                                            put_dir(sftp, path, os.path.join(MIDDAY, path))
                                            print(f"         Published {path} to midday")
                                        else:
                                            print(f"         Skipping {path}. Already published to midday")

                                    if fffff == "Semi-Private":
                                        if mkdir_or_false(sftp, os.path.join(TWILIGHT, path)):
                                            put_dir(sftp, path, os.path.join(TWILIGHT, path))
                                            print(f"         Published {path} to twilight")
                                        else:
                                            print(f"         Skipping {path}. Already published to twilight")

                                    elif fffff == "Private":
                                        if mkdir_or_false(sftp, os.path.join(MIDNIGHT, path)):
                                            put_dir(sftp, path, os.path.join(MIDNIGHT, path))
                                            print(f"         Published {path} to midnight")
                                        else:
                                            print(f"         Skipping {path}. Already published to midnight")

    scpc.close()
    return 0


if __name__ == '__main__':
    exit(main())