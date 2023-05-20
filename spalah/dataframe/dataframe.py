"""This module contains various dataframe specific functions and classes"""

import copy
from collections import namedtuple
from pprint import pformat, pprint
from typing import List, Set, Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

MatchedColumn = namedtuple("MatchedColumn", ["name", "data_type"])
NotMatchedColumn = namedtuple("NotMatchedColumn", ["name", "data_type", "reason"])


def __remove_suffix(input_string: str, suffix: str) -> str:
    """Remove suffix from string if it exists

    Args:
        input_string (str):  Input string
        suffix (str):  Suffix to remove

    Returns:
        str: Input string without suffix
    """
    if suffix and input_string.endswith(suffix):
        return input_string[: -len(suffix)]
    return input_string


def __process_schema_node(
    node: T.StructField,
    column_prefix: str = "",
    columns_to_include: Union[List, None] = None,  # type: ignore
    columns_to_exclude: Union[List, None] = None,  # type: ignore
    nullify_only: bool = False,
    array_element: Union[None, F.col] = None,
    debug: bool = False,
) -> Union[F.col, F.lit, None]:
    """Internal function to process a node of the schema

    Args:
        node (dict): json representation of a certain node in schema
        column_prefix (str, optional): starting path of the nested column name
        columns_to_include (list): Columns that must remain in the dataframe unchanged
        columns_to_exclude (list): Columns that must be removed (or nullified)
        nullify_only (bool, optional): If True, only nullify columns, do not remove them
        array_element (Union[None, F.col], optional): reference to the array element if
            it must be transformed
        debug (bool, optional): If True, print debug information, defaults to False

    Returns:
        Union[Column, None]: The pyspark column instance of a certain node of schema
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
    elif not columns_to_include or len(columns_to_include) == 0:
        include_this_node = True

    # Structs
    if isinstance(node.dataType, T.StructType):
        children = []

        for field in node.dataType.fields:
            if isinstance(field.dataType, T.StructType) and array_element is not None:
                array_col_child = array_element[field.name]
            elif array_element is not None:
                array_col_child = array_element
            else:
                array_col_child = None

            _child = __process_schema_node(
                node=field,
                column_prefix=column_prefix,
                columns_to_include=columns_to_include,
                columns_to_exclude=columns_to_exclude,
                nullify_only=nullify_only,
                array_element=array_col_child,
                debug=debug,
            )
            if _child is not None:
                children.append(_child)

        if len(children) == 0:
            is_struct_without_children = True
        col_expression = F.struct(children).alias(node_name)

    # Struct within array (or: array(struct()))
    elif (
        isinstance(node.dataType, T.ArrayType)
        and isinstance(node.dataType.elementType, T.StructType)
        and
        # to include this node only when it must appear in the final projection
        (nullify_only or include_this_node)
    ):
        children = []

        def _transform_array(array_element: F.col) -> F.col:
            """Internal function to process elements of an array
            one by one and return the transformed array

            Args:
                row (F.col): array element to transform

            Returns:
                F.col: transformed array element
            """

            # converts schema node array(struct()) -> struct()
            struct_in_array_node = copy.deepcopy(node)
            struct_in_array_node.dataType = node.dataType.elementType

            # removes the name of the array node from the element path
            # because the existence of the array does not create a new level in the path
            column_prefix_new = __remove_suffix(
                __remove_suffix(column_prefix, node_name), "."
            )

            struct_extracted_from_array_element = __process_schema_node(
                node=struct_in_array_node,
                column_prefix=column_prefix_new,
                columns_to_include=columns_to_include,
                columns_to_exclude=columns_to_exclude,
                nullify_only=nullify_only,
                array_element=array_element,
                debug=debug,
            )
            return struct_extracted_from_array_element

        # if this is first iteration (or level of arrays) then
        # the array path is the element path
        # otherwise, the array path is child node of the parent array
        if array_element is None:
            array_path = column_prefix
        else:
            array_path = array_element[node_name]

        col_expression = F.transform(
            array_path, lambda x: _transform_array(array_element=x)
        ).alias(node_name)

    # Regular columns
    else:
        # get node data type
        # In case of nullification of array it converted to a string null value
        if isinstance(node.dataType, T.ArrayType):
            node_type = T.StringType()  # arrays will be nullified and casted as strings
        else:
            node_type = node.dataType

        # regular column expression, valid for non-nullified columns
        if array_element is None:
            col_expression = F.col(column_prefix).alias(node_name)
        else:
            col_expression = array_element[node_name].alias(node_name)

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
    columns_to_include: Union[List, None] = None,
    columns_to_exclude: Union[List, None] = None,
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
        TypeError: If the 'column_to_include' or 'column_to_exclude' are not type list
        ValueError: If the included columns overlay excluded columns, so nothing to return

    Returns:
        DataFrame: The processed dataframe

    Examples:
        >>> from spalah.dataframe import slice_dataframe
        >>> df = spark.sql(
        ...         'SELECT 1 as ID, "John" AS Name,
        ...         struct("line1" AS Line1, "line2" AS Line2) AS Address'
        ...     )
        >>> df_sliced = slice_dataframe(
        ...     input_dataframe=df,
        ...     columns_to_include=["Name", "Address"],
        ...     columns_to_exclude=["Address.Line2"]
        ... )

        As the result, the dataframe will contain only the columns `Name` and `Address.Line1` \
            because `Name` and `Address` are included and a nested element `Address.Line2` is \
            excluded
        >>> df_result.printSchema()
        root
        |-- Name: string (nullable = false)
        |-- Address: struct (nullable = false)
        |    |-- Line1: string (nullable = false)
    """

    projection = []
    _schema = input_dataframe.schema

    if not columns_to_include:
        columns_to_include = []

    if not columns_to_exclude:
        columns_to_exclude = []

    if not (type(columns_to_include) is list and type(columns_to_exclude) is list):
        raise TypeError("'columns_to_include' and 'columns_to_exclude' must be a list")

    if not all(
        isinstance(item, str) for item in columns_to_include + columns_to_exclude
    ):
        raise TypeError(
            "Member of 'columns_to_include' and 'columns_to_exclude' must be a string"
        )

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
            + "columns_to_include/columns_to_exclude attributes "
            + "and included column should not directly overlap with excluded one"
        )

    return result


def flatten_schema(
    schema: T.StructType,
    include_datatype: bool = False,
    column_prefix: Union[str, None] = None,
) -> list:
    """Parses spark dataframe schema and returns the list of columns
    If the schema is nested, the columns are flattened

    Args:
        schema (StructType): Input dataframe schema
        include_type (bool, optional): Flag to include column types
        column_prefix (str, optional): Column name prefix. Defaults to None.

    Returns:
        The list of (flattened) column names

    Examples:
        >>> from spalah.dataframe import flatten_schema
        >>> flatten_schema(schema=df_complex_schema.schema)

        returns the list of columns, nested are flattened:
        >>> ['ID', 'Name', 'Address.Line1', 'Address.Line2']
    """

    if not isinstance(schema, T.StructType):
        raise TypeError("Parameter schema must be a StructType")

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
            columns += flatten_schema(
                column_data_type,
                include_datatype=include_datatype,
                column_prefix=name,
            )
        else:
            if include_datatype:
                result = name, str(column_data_type)
            else:
                result = name

            columns.append(result)

    return columns


def script_dataframe(
    input_dataframe: DataFrame, suppress_print_output: bool = True
) -> str:
    """Generate a script to recreate the dataframe
    The script includes the schema and the data

    Args:
        input_dataframe (DataFrame): Input spark dataframe
        suppress_print_output (bool, optional): Disable prints to console. \
            Defaults to True.

    Raises:
        ValueError: when the dataframe is too large (by default > 20 rows)

    Returns:
        The script to recreate the dataframe

    Examples:
        >>> from spalah.dataframe import script_dataframe
        >>> script = script_dataframe(input_dataframe=df)
        >>> print(script)
    """

    MAX_ROWS_IN_SCRIPT = 20

    __dataframe = input_dataframe

    if __dataframe.count() > MAX_ROWS_IN_SCRIPT:
        raise ValueError(
            "This method is limited to script up "
            f"to {MAX_ROWS_IN_SCRIPT} row(s) per call"
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


class SchemaComparer:
    """
    The SchemaComparer is to compare two spark dataframe schemas and find matched
    and not matched columns.
    """

    def __init__(
        self, source_schema: T.StringType, target_schema: T.StringType
    ) -> None:
        """Constructs all the necessary input attributes for the SchemaComparer object.

        Args:
            source_schema (T.StringType): source schema to match
            target_schema (T.StringType): target schema to match

        Examples:
            >>> from spalah.dataframe import SchemaComparer
            >>> schema_comparer = SchemaComparer(
            ...     source_schema = df_source.schema,
            ...     target_schema = df_target.schema
            ... )
        """
        self._source = self.__import_schema(source_schema)
        self._target = self.__import_schema(target_schema)

        self.matched: List[tuple] = list()
        """List of matched columns"""
        self.not_matched: List[tuple] = list()
        """The list of not matched columns"""

    def __import_schema(self, input_schema: T.StructType) -> Set[tuple]:
        """Import StructType as the flatten set of tuples: (column_name, data_type)

        Args:
            input_schema (T.StructType): Schema to process

        Raises:
            TypeError: if input schema has a type: DataFrame
            TypeError: if input schema hasn't a type: StructType

        Returns:
            Set[tuple]: Set of tuples: (column_name, data_type)
        """

        if isinstance(input_schema, DataFrame):
            raise TypeError(
                "One of 'source_schema or 'target_schema' passed as a DataFrame. "
                "Use DataFrame.schema instead"
            )
        elif not isinstance(input_schema, T.StructType):
            raise TypeError(
                "Parameters 'source_schema and 'target_schema' must have a type: StructType"
            )

        return set(flatten_schema(input_schema, True))

    def __match_by_name_and_type(
        self, source: Set[tuple] = set(), target: Set[tuple] = set()
    ) -> Set[tuple]:
        """Matches columns in source and target schemas by name and data type

        Args:
            source (Set[tuple], optional): Flattened source schema. Defaults to set().
            target (Set[tuple], optional): Flattened target schema. Defaults to set().

        Returns:
            Set[tuple]: Fully matched columns as a set of tuples: (column_name, data_type)
        """

        # If source and target is not provided, use class attributes as the input
        _source = self._source if not source else source
        _target = self._target if not target else target

        result = _source & _target

        # Remove matched values of case 1 from further processing
        self.__remove_matched_by_name_and_type(result)

        if not (source and target):
            self.__populate_matched(result)

        return result

    def __remove_matched_by_name_and_type(self, subtract_value: Set[tuple]) -> None:
        """Removes fully matched columns from the further processing

        Args:
            subtract_value (Set[tuple]): Set of matched columns
        """

        self._source = self._source - subtract_value
        self._target = self._target - subtract_value

    def __remove_matched_by_name(self, subtract_value: Set[tuple]) -> None:
        """Removes matched by name columns from the further processing

        Args:
            subtract_value (Set[tuple]): Set of matched column
        """

        def _remove(input_value: Set[tuple], subtract_value: Set[tuple]) -> Set[tuple]:
            """Internal helper for removal of Tuples by the first member"""
            return {
                (x, y)
                for (x, y) in input_value
                if x.lower() not in [z[0].lower() for z in subtract_value]
            }

        self._source = _remove(self._source, subtract_value)  # type: ignore
        self._target = _remove(self._target, subtract_value)  # type: ignore

    def __lower_column_names(self, base_value: Set[tuple]) -> Set[tuple]:
        """Lower-case all column names of the input set

        Args:
            base_value (Set[tuple]): Input set of columns

        Returns:
            Set[tuple]: Output set of columns with lower-case column names
        """
        return {(x.lower(), y) for (x, y) in base_value}

    def __match_by_name_type_excluding_case(self) -> None:
        """Matches columns in source and target schemas by name and data type
        without taking into account column name case
        """

        _source_lowered = self.__lower_column_names(self._source)
        _target_lowered = self.__lower_column_names(self._target)

        result = self.__match_by_name_and_type(_source_lowered, _target_lowered)

        # Remove matched values of case 2 from further processing
        self.__remove_matched_by_name(result)

        self.__populate_not_matched(
            result,
            "The column exists in source and target schemas but it's name is case-mismatched",
        )

    def __match_by_name_but_not_type(self) -> None:
        """Matches columns in source and target schemas only by column name"""

        x = dict(self._source)  # type: ignore
        y = dict(self._target)  # type: ignore

        result = {(k, f"{x[k]} <=> {y[k]}") for k in x if k in y and x[k] != y[k]}

        # Remove matched values of case 3 from further processing
        self.__remove_matched_by_name(result)  # type: ignore

        self.__populate_not_matched(
            result,  # type: ignore
            "The column exists in source and target schemas but it is not matched by a data type",
        )

    def __process_remaining_non_matched_columns(self) -> None:
        """Process remaining not matched columns"""

        self.__populate_not_matched(
            self._source, "The column exists only in the source schema"
        )

        self.__populate_not_matched(
            self._target, "The column exists only in the target schema"
        )

        self.__remove_matched_by_name(self._source)
        self.__remove_matched_by_name(self._target)

    def __populate_matched(self, input_value: Set[tuple]) -> None:
        """Populate class property 'matched' with a list of fully matched columns

        Args:
            input_value (Set[tuple]): The set of tuples with a list of column names and data types
        """

        for match in input_value:
            self.matched.append(MatchedColumn(name=match[0], data_type=match[1]))

    def __populate_not_matched(self, input_value: Set[tuple], reason: str) -> None:
        """Populate class property 'not_matched' with a list of columns that didn't match for some
        reason with included an actual reason

        Args:
            input_value (Set[tuple]): The set of tuples with a list of column names and data types
            reason (str): Reason for not match

        """

        for match in input_value:
            self.not_matched.append(
                NotMatchedColumn(name=match[0], data_type=match[1], reason=reason)
            )

    def compare(self) -> None:
        """
        Compares the source and target schemas and populates properties `matched` and `not_matched`

        Examples:
            >>> # instantiate schema_comparer firstly, see example above
            >>> schema_comparer.compare()

            Get list of all columns that are matched by name and type:
            >>> schema_comparer.matched
            [MatchedColumn(name='Address.Line1',  data_type='StringType')]

            Get unmatched columns:
            >>> schema_comparer.not_matched
            [
                NotMatchedColumn(
                    name='name',
                    data_type='StringType',
                    reason="The column exists in source and target schemas but it's name is \
case-mismatched"
                ),
                NotMatchedColumn(
                    name='Address.Line2',
                    data_type='StringType',
                    reason='The column exists only in the source schema'
                )
            ]
        """

        # Case 1: find columns that are matched by name and type and remove them
        # from further processing
        self.__match_by_name_and_type()

        # Case 2: find columns that match mismatched by name due to case: ID <-> Id
        self.__match_by_name_type_excluding_case()

        # Case 3: Find columns matched by name, but not by data type
        self.__match_by_name_but_not_type()

        # Case 4: Find columns that exists only in the source or target
        self.__process_remaining_non_matched_columns()
