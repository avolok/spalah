from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import pytest
from spalah.dataframe import SchemaComparer, MatchedColumn, NotMatchedColumn


def test_SchemaComparer_equal_schema(flat_dataset: DataFrame) -> None:
    """Two equal schemas must result into all matched columns"""

    df1 = flat_dataset

    schema_comparer = SchemaComparer(source_schema=df1.schema, target_schema=df1.schema)

    schema_comparer.compare()

    assert len(schema_comparer.matched) > 0
    assert len(schema_comparer.not_matched) == 0


def test_SchemaComparer_compare_different_schemas(flat_dataset: DataFrame) -> None:
    """Two equal schemas must result into all matched columns"""

    df1 = flat_dataset

    df2 = (
        flat_dataset.withColumn("z", F.lit("z"))  # add extra column z
        .drop("a", "b")  # drop columns a, b
        .withColumn("b", F.lit("b"))  # recreate column b with a string type
        .withColumnRenamed("c", "C")  # change the case of column: c=>C
    )

    schema_comparer = SchemaComparer(source_schema=df1.schema, target_schema=df2.schema)

    schema_comparer.compare()

    assert set(schema_comparer.matched) == {
        MatchedColumn(name="d", data_type="DateType"),
        MatchedColumn(name="e", data_type="TimestampType"),
    }

    assert set(schema_comparer.not_matched) == {
        NotMatchedColumn(
            name="c",
            data_type="StringType",
            reason="The column exists in source and target schemas but it's name "
            "is case-mismatched",
        ),
        NotMatchedColumn(
            name="b",
            data_type="DoubleType <=> StringType",
            reason="The column exists in source and target schemas but it is not "
            "matched by a data type",
        ),
        NotMatchedColumn(
            name="a", data_type="LongType", reason="The column exists only in the source schema"
        ),
        NotMatchedColumn(
            name="z",
            data_type="StringType",
            reason="The column exists only in the target schema",
        ),
    }


def test_SchemaComparer_compare_exception_passed_dataframe(flat_dataset: DataFrame) -> None:
    """Two equal schemas must result into all matched columns"""

    df1 = flat_dataset

    with pytest.raises(Exception) as e:
        schema_comparer = SchemaComparer(
            source_schema=df1.schema,
            target_schema=df1,  # Dataframe passed instead of StructType (schema)
        )
        schema_comparer.compare()
    assert str(e.value).startswith(
        "One of 'source_schema or 'target_schema' passed as a DataFrame."
    )


def test_SchemaComparer_compare_exception_passed_string(flat_dataset: DataFrame) -> None:
    """Two equal schemas must result into all matched columns"""

    df1 = flat_dataset

    with pytest.raises(Exception) as e:
        schema_comparer = SchemaComparer(
            source_schema=df1.schema,
            target_schema="abcde",  # String passed instead of StructType (schema)
        )
        schema_comparer.compare()
    assert str(e.value).startswith(
        "Parameters 'source_schema and 'target_schema' " "must have a type: StructType"
    )
