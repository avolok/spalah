import pytest
from spalah.dataframe import flatten_schema


@pytest.mark.parametrize(
    "assert_message,input_dataset,include_datatype,expected",
    [
        (
            "Must return all columns, including nested attributes"
            "of the complex dataset as the list",
            "nested_dataset",
            False,
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
            False,
            ["a", "b", "c", "d", "e"],
        ),
        (
            "Must return all columns, including nested attributes of the"
            "complex dataset as the list and the column data type",
            "nested_dataset",
            True,
            [
                ("column_a", "IntegerType"),
                ("column_b", "DecimalType(2,1)"),
                ("column_c.column_c_1", "StringType"),
                ("column_c.column_c_2.c_2_1", "StringType"),
                ("column_c.column_c_2.c_2_2", "StringType"),
                ("column_c.column_c_2.c_2_3", "StringType"),
            ],
        ),
        (
            "Must return all columns of the flat dataset as the list and the column data type",
            "flat_dataset",
            True,
            [
                ("a", "LongType"),
                ("b", "DoubleType"),
                ("c", "StringType"),
                ("d", "DateType"),
                ("e", "TimestampType"),
            ],
        ),
    ],
    ids=[
        "nested: without data types",
        "flat: without data types",
        "nested: with data types",
        "flat: with data types",
    ],
)
def test_flatten_schema(
    assert_message,
    input_dataset,
    include_datatype,
    expected,
    request,
) -> None:

    dataset = request.getfixturevalue(input_dataset)
    actual = flatten_schema(schema=dataset.schema, include_datatype=include_datatype)

    assert actual == expected, assert_message
