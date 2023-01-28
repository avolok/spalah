import os
from collections import namedtuple
from delta import DeltaTable
from pyspark.sql import SparkSession
from typing import Union


def check_dbfs_mounts(mounts: list, print_output: bool = False):

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


def get_delta_properties(
    table_path: str = "",
    table_name: str = "",
    spark_session: Union[SparkSession, None] = None,
) -> Union[dict, None]:
    """Get dataset's delta properties as a dictionary.

    Args:
        table_path (str, optional): Path to delta table. For instance: /mnt/db1/table1
        table_name (str, optional): Delta table name. For instance: db1.table1
        spark_session: (SparkSession, optional)  The current spark context.
            If not defined the getActiveSession() will be used to get the current context

    Raises:
        ValueError: when both 'table_path' and 'table_name' provided
        ValueError: when neither 'table_path' nor 'table_name' provided

    Examples:
        >>> from spalah.datalake import get_delta_properties
        >>> table_properties = get_delta_properties(table_path="/path/dataset")
        >>> print(table_properties)
        {'delta.deletedFileRetentionDuration': 'interval 15 days'}
    """

    if table_path and table_name:
        raise ValueError("Both 'table_path' and 'table_name' provided. Use one of them.")

    if not table_path and not table_name:
        raise ValueError("Neither 'table_path' nor 'table_name' provided. Use one of them.")

    if not spark_session:
        spark_session = SparkSession.getActiveSession()

    if table_path:
        table_name = f"delta.`{table_path}`"
        _identifier_to_check = table_path
    else:
        _identifier_to_check = table_name

    if not DeltaTable.isDeltaTable(sparkSession=spark_session, identifier=_identifier_to_check):
        print(f"{table_name} is not a Delta Table")
        return None

    existing_properties = (
        spark_session.sql(f"DESCRIBE DETAIL {table_name}")
        .select("properties")
        .collect()[0]
        .asDict()["properties"]
    )

    return existing_properties


def set_delta_properties(
    properties: dict,
    table_path: str = "",
    table_name: str = "",
    keep_existing: bool = True,
    spark_session: Union[SparkSession, None] = None,
) -> None:
    """Set and override delta properties. Function checks ff the property already
    set and it's value matches in such case it will skip setting the property again

    Args:
        properties (dict):              A dictionary with properties to set.
                                        Example: {"delta.logRetentionDuration": "interval 10 days"}
        table_path (str, optional):     Path to delta table Defaults to ''.
        table_name (str, optional):     Delta table name. For instance: db1.table1. Defaults to ''.
        allow_unset (bool, optional):   If enabled, not listed properties in an arg. 'properties'
                                        but set on table will be unset. Defaults to False.
        spark_session: (SparkSession, optional)  The current spark context.
            If not defined the getActiveSession() will be used to get the current context

    Raises:
        ValueError: if values for both 'table_path' and 'table_name' provided
                    provide values to one of them
        ValueError: if values for neither 'table_path' nor 'table_name' provided
                    provide values to one of them

    Examples:
        >>> from spalah.datalake import set_delta_properties
        >>> set_delta_properties(
        ...     table_path="/path/dataset",
        ...     properties={"delta.logRetentionDuration": "interval 10 days"}
        ... )
        Applying table properties on 'delta.`/path/dataset`':
         - Checking if 'delta.logRetentionDuration = interval 10 days' is set on \
delta.`/path/dataset`
        Result: The property has been set
    """

    if table_path and table_name:
        raise ValueError("Both 'table_path' and 'table_name' provided. Use one of them.")

    if not table_path and not table_name:
        raise ValueError("Neither 'table_path' nor 'table_name' provided. Use one of them.")

    original_table_name = table_name

    if not spark_session:
        spark_session = SparkSession.getActiveSession()

    if table_path:
        table_name = f"delta.`{table_path}`"

    _existing_properties = get_delta_properties(
        table_name=original_table_name, table_path=table_path
    )

    if _existing_properties is not None:
        print(f"Applying table properties on '{table_name}':")

        for k, v in properties.items():

            print(f" - Checking if '{k} = {v}' is set on {table_name}")

            if k in _existing_properties and _existing_properties[k] == str(v):
                print("   Result: The property already exists on the table")
            else:
                _sql = f"ALTER TABLE {table_name} SET TBLPROPERTIES ({k} = '{v}')"
                spark_session.sql(_sql)
                print("   Result: The property has been set")

        if not keep_existing:
            for k, v in _existing_properties.items():
                if k not in properties:
                    _sql = f"ALTER TABLE {table_name} UNSET TBLPROPERTIES ({k})"
                    spark_session.sql(_sql)
                    print(
                        f"   The property '{k} = {v}' has been unset because it is not defined in "
                        "the original dict"
                    )
