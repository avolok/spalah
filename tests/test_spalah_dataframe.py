import pytest
from pyspark.sql import Row

from spalah.dataframe import slice_dataframe, schema_as_flat_list


@pytest.mark.parametrize(
    "assert_message,input_dataset,columns_to_include,columns_to_exclude,nullify_only,expected",
    [
        # nested: include
        (
            "The dataframe must contain only columns listed in 'columns_to_include'",
            "nested_dataset",
            ["column_a", "column_c"],
            [],
            False,
            Row(
                column_a=1,
                column_c=Row(
                    column_c_1="c1", column_c_2=Row(c_2_1="c_2_1", c_2_2="c_2_2", c_2_3="c_2_3")
                ),
            ),
        ),
        # nested: include, mixed case
        (
            "The dataframe must contain only columns listed in 'columns_to_include'",
            "nested_dataset",
            ["Column_A", "column_c"],
            [],
            False,
            Row(
                column_a=1,
                column_c=Row(
                    column_c_1="c1", column_c_2=Row(c_2_1="c_2_1", c_2_2="c_2_2", c_2_3="c_2_3")
                ),
            ),
        ),
        # "nested: include and exclude"
        (
            "The dataframe must contain columns listed in 'columns_to_include' beside of 'columns_to_exclude'",
            "nested_dataset",
            ["column_b", "column_c"],
            ["column_c.column_c_2.c_2_1"],
            False,
            Row(
                column_b=2.0,
                column_c=Row(column_c_1="c1", column_c_2=Row(c_2_2="c_2_2", c_2_3="c_2_3")),
            ),
        ),
        # "nested: exclude"
        (
            "All columns in 'columns_to_exclude' must be excluded (nested)'",
            "nested_dataset",
            [],
            ["column_b", "column_c.column_c_2.c_2_1"],
            False,
            Row(
                column_a=1,
                column_c=Row(column_c_1="c1", column_c_2=Row(c_2_2="c_2_2", c_2_3="c_2_3")),
            ),
        ),
        # "nested: exclude, mixed case"
        (
            "All columns in 'columns_to_exclude' must be excluded (nested)'",
            "nested_dataset",
            [],
            ["Column_B", "Column_C.column_c_2.C_2_1"],
            False,
            Row(
                column_a=1,
                column_c=Row(column_c_1="c1", column_c_2=Row(c_2_2="c_2_2", c_2_3="c_2_3")),
            ),
        ),
        # "nested: nullify only for excluded columns"
        (
            "All columns in 'columns_to_exclude' must be nullified (nested)'",
            "nested_dataset",
            [],
            ["column_b", "column_c.column_c_2.c_2_1"],
            True,
            Row(
                column_a=1,
                column_b=None,
                column_c=Row(
                    column_c_1="c1", column_c_2=Row(c_2_1=None, c_2_2="c_2_2", c_2_3="c_2_3")
                ),
            ),
        ),
        # "flat: nullify only for excluded columns"
        (
            "All columns in 'columns_to_exclude' must be nullified (flat)'",
            "flat_dataset",
            [],
            ["d", "e"],
            True,
            Row(a=1, b=2.0, c="string1", d=None, e=None),
        ),
        # "flat: exclude"
        (
            "All columns in 'columns_to_exclude' must be excluded (flat)'",
            "flat_dataset",
            [],
            ["d", "e"],
            False,
            Row(a=1, b=2.0, c="string1"),
        ),
    ],
    ids=[
        "nested: include",
        "nested: include, mixed case",
        "nested: include and exclude",
        "nested: exclude",
        "nested: exclude, mixed case",
        "nested: nullify only for excluded columns",
        "flat: nullify only for excluded columns",
        "flat: exclude",
    ],
)
def test_slice_dataframe(
    assert_message,
    input_dataset,
    columns_to_include,
    columns_to_exclude,
    nullify_only,
    expected,
    request,
) -> None:

    dataset = request.getfixturevalue(input_dataset)

    actual = slice_dataframe(
        input_dataframe=dataset,
        columns_to_include=columns_to_include,
        columns_to_exclude=columns_to_exclude,
        nullify_only=nullify_only,
    ).first()

    assert actual == expected, assert_message


@pytest.mark.parametrize(
    "assert_message,input_dataset,expected",
    [
        (
            "Must return all columns, including nested attributes of the complex dataset as the list",
            "nested_dataset",
            [
                "column_a",
                "column_b",
                "column_c.column_c_1",
                "column_c.column_c_2.c_2_1",
                "column_c.column_c_2.c_2_2",
                "column_c.column_c_2.c_2_3",
            ],
        ),
        (
            "Must return all columns of the flat dataset as the list",
            "flat_dataset",
            ["a", "b", "c", "d", "e"],
        ),
    ],
    ids=[
        "nested: include",
        "flat: exclude",
    ],
)
def test_schema_as_flat_list(
    assert_message,
    input_dataset,
    expected,
    request,
) -> None:

    dataset = request.getfixturevalue(input_dataset)
    actual = schema_as_flat_list(schema=dataset.schema)

    assert actual == expected, assert_message
