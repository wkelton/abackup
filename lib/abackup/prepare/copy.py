import logging
import shutil

from pathlib import Path
from typing import List


def copy_most_recent_backup_file(
    stored_path: bool, destinations: List[str], log: logging.Logger, overwrite: bool = True
) -> bool:
    dir = Path(stored_path)
    if not dir.is_dir():
        log.error("{} is not a directory!".format(stored_path))
        return False

    backup_files = [f for f in dir.iterdir() if f.is_file()]
    if len(backup_files) < 1:
        log.error("{} is empty!".format(stored_path))
        return False

    most_recent_backup = max(backup_files, key=lambda f: f.stat().st_ctime)

    status = True

    for dest in destinations:
        p = Path(dest)
        if not p.is_absolute():
            p = dir / p
        if p.exists():
            if p.is_file():
                if overwrite:
                    log.info("overwritting {}".format(str(p)))
                else:
                    log.info("skipping overwrite of {}".format(str(p)))
                    continue
            else:
                log.error("path exists and is not a file, not sure how to proceed: {}".format(str(p)))
                status = False
                continue

        log.info("copying most recent backup {} -> {}".format(most_recent_backup, str(p)))
        shutil.copyfile(most_recent_backup, str(p))
        # TODO: handle error cases

    return status
