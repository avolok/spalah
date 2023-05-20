import os
from collections import namedtuple


def check_dbfs_mounts(mounts: list, print_output: bool = False) -> list:
    """DBFS Helper: checks if dbfs mounts are accessible

    Args:
        mounts (list): The list of mounts from `dbutils.fs.mounts()`
        print_output (bool, optional): Flag to print debug information. Defaults to False.

    Returns:
        list: the list of mounts with corresponding statuses
    """

    dbfs_mount = namedtuple("dbfs_mount", ("result", "mount"))
    _result = list()

    for mount in mounts:
        _mount_path = f"/dbfs{mount.mountPoint}"

    if (
        "/databricks" not in _mount_path
        and os.path.isdir(_mount_path)
        and "mount.err" not in os.listdir(_mount_path)
    ):
        _result.append(dbfs_mount("ok", mount.mountPoint))
    else:
        _result.append(dbfs_mount("failed", mount.mountPoint))

    if print_output:
        for item in _result:
            print(f"{item.result:<8}  {item.mount}")

    return _result
