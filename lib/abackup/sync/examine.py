import logging
import os
import tabulate

from tabulate import tabulate

from abackup import appcron, fs
from abackup.sync import Config, syncinfo


def perform_examine(config: Config, cron: appcron.AppCronTab, log: logging.Logger):
    # print owned_data
    rows = [[name, datadir.path, fs.to_human_readable(fs.get_total_size(datadir.path, log), prefix='Ki')] 
        for name, datadir in config.owned_data.items()]
    print(tabulate(rows, headers=['Owned Data', 'Directory', 'Size']))
    print()

    # print latest syncs for owned_data
    rows = []
    for name, datadir in config.owned_data.items():
        sync_infos = syncinfo.read_sync_infos(config, name)
        if sync_infos:
            for info in sync_infos:
                if not info.pull:
                    rows.append([name, "{:7}: {}".format(info.sync_type, info.timestamp),
                        "{}: {}".format(info.remote_host, info.destination) if info.remote_host else info.destination,
                        "{} (-{})".format(info.sync_count, info.sync_deleted), fs.to_human_readable(info.sync_bytes),
                        info.duration])
        else:
            rows.append([name, 'unknown', '-', '-', '-', '-'])
    print(tabulate(rows, headers=['Owned Data', 'Last Sync', 'Destination', 'Files', 'Sent', 'Duration']))
    print()

    # print stored_data
    rows = [[name, datadir.path, fs.to_human_readable(fs.get_total_size(datadir.path, log), prefix='Ki')]
        for name, datadir in config.stored_data.items()]
    print(tabulate(rows, headers=['Stored Data', 'Directory', 'Size']))
    print()

    # print latest syncs for stored_data
    rows = []
    for name, datadir in config.stored_data.items():
        sync_infos = syncinfo.read_sync_infos(config, name)
        if sync_infos:
            for info in sync_infos:
                if info.pull:
                    rows.append([name, "{:7}: {}".format(info.sync_type, info.timestamp),
                        "{}: {}".format(info.remote_host, info.origin) if info.remote_host else info.origin,
                        "{} (-{})".format(info.sync_count, info.sync_deleted), fs.to_human_readable(info.sync_bytes),
                        info.duration])
        else:
            rows.append([name, 'unknown', '-', '-', '-', '-'])
    print(tabulate(rows, headers=['Stored Data', 'Last Sync', 'Origin', 'Files', 'Sent', 'Duration']))
    print()

    # print cron
    print('Crontab:')
    for job in cron.jobs():
        print(job)
    print()
