from pprint import pformat, pprint
from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def __process_schema_node(
    node: T.StructType,
    column_prefix: str = "",
    columns_to_include: list = [],  # type: ignore
    columns_to_exclude: list = [],  # type: ignore
    nullify_only: bool = False,
    debug: bool = False,
) -> Union[F.col, F.lit, None]:
    """Internal function to process a node of the schema

    Args:
        node (dict): json representation of a certain node in schema
        column_prefix (str, optional): starting path of the nested column name
        columns_to_include (list): Columns that must remain in the dataframe unchanged
        columns_to_exclude (list): Columns that must be removed (or nullified)
        debug (bool, optional): _description_. Defaults to False.

    Returns:
        Union[F.col, F.lit, None]: The pyspark column instance of a certain node of schema
        if column to be rendered or None if column to be removed from the final projection
    """

    # Default values
    col_expression = F.lit(-1)
    node_name = node.name
    is_struct_without_children = False
    is_struct = True

    if column_prefix == "":
        column_prefix = node_name
    else:
        column_prefix = column_prefix + "." + node_name
    column_prefix_dot = f"{column_prefix}.".lower()
    include_this_node = False

    # Process exclude/include rules
    if columns_to_exclude and any(
        [
            x
            for x in [f"{x}." for x in columns_to_exclude]
            if x == column_prefix_dot or column_prefix_dot.startswith(x)
        ]
    ):
        include_this_node = False
    # If no columns to include specified, then filter out other columns
    elif columns_to_include and any(
        [
            x
            for x in [f"{x}." for x in columns_to_include]
            if x.startswith(column_prefix_dot) or column_prefix_dot.startswith(x)
        ]
    ):
        include_this_node = True
    # If no columns to include specified, then all columns are accepted
    elif len(columns_to_include) == 0:
        include_this_node = True

    # Structs
    if isinstance(node.dataType, T.StructType):

        children = []

        for field in node.dataType.fields:

            _child = __process_schema_node(
                node=field,
                column_prefix=column_prefix,
                columns_to_include=columns_to_include,
                columns_to_exclude=columns_to_exclude,
                nullify_only=nullify_only,
                debug=debug,
            )
            if _child is not None:
                children.append(_child)

        if len(children) == 0:
            is_struct_without_children = True
        col_expression = F.struct(children).alias(node_name)

    # Regular columns
    else:

        # get node data type
        # In case of nullification of array it converted to a string null value
        if isinstance(node.dataType, T.ArrayType):
            node_type = T.StringType()
        else:
            node_type = node.dataType

        # regular column expression, valid for non-nulLified columns
        col_expression = F.col(column_prefix).alias(node_name)

        # column expression for nullified fields
        nullified_col_expression = F.lit(None).cast(node_type).alias(node_name)

        # flag for further processing that the field is not struct
        is_struct = False

    if include_this_node and debug:
        print(f" - {column_prefix}")

    if include_this_node and not is_struct_without_children:
        return col_expression
    if not include_this_node and nullify_only:

        if is_struct:
            return col_expression
        else:
            return nullified_col_expression
    else:
        return None


def slice_dataframe(
    input_dataframe: DataFrame,
    columns_to_include: list = [],
    columns_to_exclude: list = [],
    nullify_only: bool = False,
    debug: bool = False,
) -> DataFrame:
    """Process flat or nested schema of the dataframe by slicing the schema
    or nullifying columns

    Args:
        input_dataframe (DataFrame): Input dataframe
        columns_to_include (list): Columns that must remain in the dataframe unchanged
        columns_to_exclude (list): Columns that must be removed (or nullified)
        nullify_only (bool, optional): Nullify columns instead of removing them. Defaults to False
        debug (bool, optional): For extra debug output. Defaults to False.

    Raises:
        TypeError: If the 'column_to_include' or 'column_to_exclude' are not lists
        ValueError: If the included columns overlay excluded columns, so nothing to return

    Returns:
        DataFrame: The processed dataframe
    """

    projection = []
    _schema = input_dataframe.schema

    if not (type(columns_to_include) is list and type(columns_to_exclude) is list):
        raise TypeError("'columns_to_include' and 'columns_to_exclude' must be a list")

    if not all(isinstance(item, str) for item in columns_to_include + columns_to_exclude):
        raise TypeError("Member of 'columns_to_include' and 'columns_to_exclude' must be a string")

    if debug:
        print("The list of columns to include:")
        pprint(columns_to_include)

        print("The list of columns to exclude:")
        pprint(columns_to_exclude)

        if nullify_only:
            print("Columns to nullify in the final projection:")
        else:
            print("Columns to include into the final projection:")

    # lower-case items for making further filtering case-insensitive
    columns_to_include = [item.lower() for item in columns_to_include]
    columns_to_exclude = [item.lower() for item in columns_to_exclude]

    for field in _schema.fields:
        node_result = __process_schema_node(
            node=field,
            columns_to_include=columns_to_include,
            columns_to_exclude=columns_to_exclude,
            nullify_only=nullify_only,
            debug=debug,
        )

        if node_result is not None:
            projection.append(node_result)

    result = input_dataframe.select(*projection)

    if len(result.columns) == 0:
        raise ValueError(
            "At least one column should be listed in the "
            + "included/excluded attributes of the datalocker config "
            + "and included column should not directly overlap with excluded one"
        )

    return result


def schema_as_flat_list(
    schema: T.StructType, include_datatype: bool = False, column_prefix: str = None
) -> list:
    """Generates the flatten list of columns from the complex dataframe schema

    Args:
        schema (StructType): input dataframe's schema
        include_type (bool, optional): Include column types
        column_prefix (str, optional): The column name prefix. Defaults to None.

    Returns:
        The list of (flattened) column names
    """

    columns = []

    for column in schema.fields:
        if column_prefix:
            name = column_prefix + "." + column.name
        else:
            name = column.name

        column_data_type = column.dataType

        if isinstance(column_data_type, T.ArrayType):
            column_data_type = column_data_type.elementType

        if isinstance(column_data_type, T.StructType):
            columns += schema_as_flat_list(
                column_data_type, include_datatype=include_datatype, column_prefix=name
            )
        else:
            if include_datatype:
                result = name, str(column_data_type)
            else:
                result = name

            columns.append(result)

    return columns


def script_dataframe(input_dataframe: DataFrame, suppress_print_output: bool = False) -> str:

    """Generates a script of a dataframe"""

    MAX_ROWS_IN_SCRIPT = 20

    __dataframe = input_dataframe

    if __dataframe.count() > MAX_ROWS_IN_SCRIPT:
        raise ValueError(
            "This method is limited to script up " f"to {MAX_ROWS_IN_SCRIPT} row(s) per call"
        )

    __schema = input_dataframe.schema.jsonValue()

    __script_lines = [
        "from pyspark.sql import Row",
        "import datetime",
        "from decimal import Decimal",
        "from pyspark.sql.types import *",
        "",
        "# Scripted data and schema:",
        f"__data = {pformat(__dataframe.collect())}",
        "",
        f"__schema = {__schema}",
        "",
        "outcome_dataframe = spark.createDataFrame(__data, StructType.fromJson(__schema))",
    ]

    __final_script = "\n".join(__script_lines)

    if not suppress_print_output:
        print("#", "=" * 80)
        print(
            "# IMPORTANT!!! REMOVE PII DATA BEFORE RE-CREATING IT IN NON-PRODUCTION ENVIRONMENTS",
            " " * 3,
            "#",
        )
        print("#", "=" * 80)
        print("")
        print(__final_script)

    return __final_script
