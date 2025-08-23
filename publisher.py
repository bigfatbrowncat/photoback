import os
import re

from paramiko import SSHClient
from scp import SCPClient

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

ssh = SSHClient()
ssh.load_system_host_keys()
res = ssh.connect('lumiere.local')

scpc = SCPClient(ssh.get_transport())

year_format = re.compile('\d\d\d\d')
folders = os.listdir('.')
folders.sort()

for f in folders:
    if year_format.match(f):
        year = f
        path = year
        print(f" - {year}")
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
            path = os.path.join(year, month)
            print(f"   - {month}")

            for fff in os.listdir(path):
                if not fff.startswith("."):
                    event = fff
                    path = os.path.join(year, month, event)
                    print(f"     - {event}")

                    for ffff in os.listdir(path):
                        if not ffff.startswith("."):
                            subevent = ffff
                            path = os.path.join(year, month, event, subevent)
                            print(f"       - {subevent}")

                            for fffff in os.listdir(path):
                                path = os.path.join(year, month, event, subevent, fffff)
                                if fffff == "General" or fffff == "Semi-Private":
                                    scpc.put(path, recursive=True, remote_path='/var/server-apps/immich-app/photolab/midday/')
                                    print(f"         Published {path} to midday")
                                elif fffff == "Private":
                                    scpc.put(path, recursive=True, remote_path='/var/server-apps/immich-app/photolab/twilight/')
                                    print(f"         Published {path} to twilight")

scpc.close()
