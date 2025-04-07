import logging

import tabulate
from tabulate import tabulate

from abackup import appcron, fs
from abackup.sync import Config, syncinfo


def perform_examine(config: Config, cron: appcron.AppCronTab, log: logging.Logger):
    # print owned_data
    rows = [
        [name, data_dir.path, fs.to_human_readable(fs.get_total_size(data_dir.path, log), prefix="Ki")]
        for name, data_dir in config.owned_data.items()
    ]
    print(tabulate(rows, headers=["Owned Data", "Directory", "Size"]))
    print()

    # print latest syncs for owned_data
    rows = []
    for name, _ in config.owned_data.items():
        sync_infos = syncinfo.read_sync_infos(config, name)
        if sync_infos:
            for info in sync_infos:
                if "pull" in info.location_info:
                    if not info.location_info["pull"]:
                        rows.append(
                            [
                                name,
                                "{:7}: {}".format(info.sync_type, info.timestamp),
                                "{}: {}".format(info.location_info["remote_host"], info.location_info["destination"])
                                if info.location_info["remote_host"]
                                else info.location_info["destination"],
                                "{} (-{})".format(info.transfer_info["sync_count"], info.transfer_info["sync_deleted"]),
                                info.transfer_info["sync_bytes"],
                                info.duration,
                            ]
                        )
                else:
                    data_added = "-"
                    files = "-"
                    data_added = "-"
                    if "backup_info" in info.transfer_info and info.transfer_info["backup"] == "SUCCEEDED":
                        data_added = info.transfer_info["backup_info"]["data_added"]
                        data_added = fs.to_human_readable(data_added)
                        files_new = info.transfer_info["backup_info"]["files_new"]
                        files_changed = info.transfer_info["backup_info"]["files_changed"]
                        files = str(int(files_new) + int(files_changed))

                    rows.append(
                        [
                            name,
                            "{:7}: {}".format(info.sync_type, info.timestamp),
                            "{}: {}".format(info.location_info["repo_name"], info.location_info["path"]),
                            files,
                            data_added,
                            info.duration,
                        ]
                    )
        else:
            rows.append([name, "unknown", "-", "-", "-", "-"])
    print(tabulate(rows, headers=["Owned Data", "Last Sync", "Destination", "Files", "Sent/Received", "Duration"]))
    print()

    # print stored_data
    rows = [
        [name, data_dir.path, fs.to_human_readable(fs.get_total_size(data_dir.path, log), prefix="Ki")]
        for name, data_dir in config.stored_data.items()
    ]
    print(tabulate(rows, headers=["Stored Data", "Directory", "Size"]))
    print()

    # print latest syncs for stored_data
    rows = []
    for name, _ in config.stored_data.items():
        sync_infos = syncinfo.read_sync_infos(config, name)
        if sync_infos:
            for info in sync_infos:
                if "pull" in info.location_info:
                    if info.location_info["pull"]:
                        origin = info.location_info["origin"]
                        if not isinstance(origin, str):
                            origin = "<{} paths>".format(len(origin))
                        rows.append(
                            [
                                name,
                                "{:7}: {}".format(info.sync_type, info.timestamp),
                                "{}: {}".format(info.location_info["remote_host"], origin)
                                if info.location_info["remote_host"]
                                else origin,
                                "{} (-{})".format(info.transfer_info["sync_count"], info.transfer_info["sync_deleted"]),
                                info.transfer_info["sync_bytes"],
                                info.duration,
                            ]
                        )
        else:
            rows.append([name, "unknown", "-", "-", "-", "-"])
    print(tabulate(rows, headers=["Stored Data", "Last Sync", "Origin", "Files", "Sent/Received", "Duration"]))
    print()

    # print cron
    print("Crontab:")
    for job in cron.jobs():
        print(job)
    print()
