import pytest
from pathlib import Path
from spalah.dataset import DeltaTableConfig
from pyspark.sql import SparkSession


def test_validate_access_by_hive_name_get_properties(spark: SparkSession, tmp_path: Path) -> None:
    """
    Tested method must get correctly table properties as a dictionary
    using hive name as an identifier
    """

    delta_path = f"{tmp_path}/sample_delta_dataset"
    hive_name = "sample_delta_dataset"
    (
        spark.range(0, 1)
        .write.format("delta")
        .mode("overwrite")
        .option("path", delta_path)
        .saveAsTable(hive_name)
    )

    _sql = f"""
        ALTER TABLE `{hive_name}`
        SET TBLPROPERTIES (delta.deletedFileRetentionDuration = 'interval 15 days')
    """
    spark.sql(_sql)

    dp = DeltaTableConfig(table_name=hive_name, spark_session=spark)

    existing_properties = dp.properties

    assert existing_properties == {"delta.deletedFileRetentionDuration": "interval 15 days"}


def test_get_delta_properties(spark: SparkSession, tmp_path: Path) -> None:
    """Tested method must get correctly table properties as a dictionary"""

    delta_path = f"{tmp_path}/sample_delta_dataset"
    spark.range(0, 1).write.format("delta").mode("overwrite").save(delta_path)

    _sql = f"""
        ALTER TABLE delta.`{delta_path}`
        SET TBLPROPERTIES (delta.deletedFileRetentionDuration = 'interval 15 days')
    """
    spark.sql(_sql)

    dp = DeltaTableConfig(table_path=delta_path, spark_session=spark)

    existing_properties = dp.properties

    assert existing_properties == {"delta.deletedFileRetentionDuration": "interval 15 days"}


def test_set_table_properties_exceptions_both_provided_as_identifier() -> None:
    "When both table_path and table_name provided the exception must occur"

    with pytest.raises(ValueError) as e:
        DeltaTableConfig(table_path="/some/path", table_name="tbl_name").properties
    assert str(e.value).startswith("Both 'table_path' and 'table_name' provided")


def test_set_table_properties_exceptions_noting_provided_as_identifier() -> None:
    "When neither table_path nor table_name provided the exception must occur"

    with pytest.raises(ValueError) as e:
        DeltaTableConfig(table_path="", table_name="").properties
    assert str(e.value).startswith("Neither 'table_path' nor 'table_name' provided")


def test_set_table_properties_new_table_no_existing_properties(
    spark: SparkSession, tmp_path: Path
) -> None:
    """Tested method must set correctly table properties to the brand new dataset"""

    delta_path = f"{tmp_path}/sample_delta_dataset"

    properties_to_set = {
        "delta.logRetentionDuration": "interval 10 days",
        "delta.deletedFileRetentionDuration": "interval 16 days",
    }

    spark.range(0, 1).write.format("delta").mode("overwrite").save(delta_path)

    dp = DeltaTableConfig(table_path=delta_path)
    dp.properties = properties_to_set

    assert properties_to_set == dp.properties


def test_validate_hive_name_access_set_properties(spark: SparkSession, tmp_path: Path) -> None:
    """
    Tested method must set correctly table properties to the brand new dataset
    using hive name as an identifier
    """

    delta_path = f"{tmp_path}/sample_delta_dataset"
    hive_name = "sample_delta_dataset_set"

    properties_to_set = {
        "delta.logRetentionDuration": "interval 10 days",
        "delta.deletedFileRetentionDuration": "interval 16 days",
    }

    (
        spark.range(0, 1)
        .write.format("delta")
        .mode("overwrite")
        .option("path", delta_path)
        .saveAsTable(hive_name)
    )

    dp = DeltaTableConfig(table_name=hive_name)
    dp.properties = properties_to_set

    assert properties_to_set == dp.properties


def test_set_table_properties_new_table_with_existing_properties(
    spark: SparkSession, tmp_path: Path
) -> None:
    """Tested method must set correctly table properties to the brand new dataset.
    An existing property must be preserved
    """

    delta_path = f"{tmp_path}/sample_delta_dataset"

    properties_to_set = {
        "delta.logRetentionDuration": "interval 10 days",
        "delta.deletedFileRetentionDuration": "interval 16 days",
    }

    spark.range(0, 1).write.format("delta").mode("overwrite").save(delta_path)

    _sql = f"ALTER TABLE delta.`{delta_path}` SET TBLPROPERTIES (prop.property1 = 'abc')"
    spark.sql(_sql)

    dp = DeltaTableConfig(table_path=delta_path)
    dp.keep_existing_properties = True
    dp.properties = properties_to_set

    properties_to_set.update({"prop.property1": "abc"})

    assert properties_to_set == dp.properties


def test_set_table_properties_new_table_remove_existing(
    spark: SparkSession, tmp_path: Path
) -> None:
    """Tested method must set correctly table properties to the brand new dataset.
    An existing property must be removed because of option allow_unset=True,
    therefore only the table options defined in the input dictionary must be set"""

    delta_path = f"{tmp_path}/sample_delta_dataset"

    properties_to_set = {
        "delta.logRetentionDuration": "interval 10 days",
        "delta.deletedFileRetentionDuration": "interval 16 days",
    }

    spark.range(0, 1).write.format("delta").mode("overwrite").save(delta_path)

    _sql = f"ALTER TABLE delta.`{delta_path}` SET TBLPROPERTIES (prop.property1 = 'abc')"
    spark.sql(_sql)

    dp = DeltaTableConfig(table_path=delta_path)
    dp.keep_existing_properties = False
    dp.properties = properties_to_set

    assert properties_to_set == dp.properties


def test_get_delta_constraints(spark: SparkSession, tmp_path: Path) -> None:
    """Must get correctly table constraints as a dictionary"""

    delta_path = f"{tmp_path}/sample_delta_dataset"
    spark.range(0, 1).write.format("delta").mode("overwrite").save(delta_path)

    _sql = f"""
        ALTER TABLE delta.`{delta_path}`
        ADD CONSTRAINT id_is_not_null CHECK (id is not null)
    """
    spark.sql(_sql)

    dp = DeltaTableConfig(table_path=delta_path, spark_session=spark)
    existing_constraints = dp.check_constraints
    expected_constraints = {"id_is_not_null": "id is not null"}

    assert existing_constraints == expected_constraints


@pytest.mark.parametrize(
    "constraint_to_set,keep_existing",
    [
        ({"name_is_not_null": "name is not null"}, True),
        ({"name_is_not_null": "name is not null"}, False),
        (
            {
                "name_is_not_null": "name is not null",
                "value_is_not_null": "value is not null",
            },
            True,
        ),
    ],
    ids=[
        "Add new constraint with keep_existing=True",
        "Add new constraint with keep_existing=False",
        "Add multiple constraints with keep_existing=True",
    ],
)
def test_set_table_constraint_on_new_table_with_existing_constraint(
    spark: SparkSession, tmp_path: Path, constraint_to_set: dict, keep_existing: bool
) -> None:
    """Must set correctly table constraint to the brand new dataset.
    And then add another constraint, the existing one must be preserved
    """

    delta_path = f"{tmp_path}/sample_delta_dataset"

    (
        spark.createDataFrame([(1, "a", "abc")], ["id", "name", "value"])
        .write.format("delta")
        .mode("overwrite")
        .save(delta_path)
    )

    _sql = f"""
        ALTER TABLE delta.`{delta_path}`
        ADD CONSTRAINT id_is_not_null CHECK (id is not null)
    """
    spark.sql(_sql)

    dp = DeltaTableConfig(table_path=delta_path, spark_session=spark)
    dp.keep_existing_check_constraints = keep_existing

    dp.check_constraints = constraint_to_set

    current_constraints = dp.check_constraints

    if keep_existing:
        # if keep_existing=True then the existing constraint must be preserved
        constraint_to_set.update({"id_is_not_null": "id is not null"})

    assert current_constraints == constraint_to_set


def test_get_delta_table_columns(spark: SparkSession, tmp_path: Path) -> None:
    """
    Must get correctly table columns as a dictionary
    """

    delta_path = f"{tmp_path}/sample_delta_dataset"

    (
        spark.createDataFrame([(1, "a", "abc")], ["id", "name", "value"])
        .write.format("delta")
        .mode("overwrite")
        .partitionBy("id")
        .save(delta_path)
    )

    dtc = DeltaTableConfig(table_path=delta_path, spark_session=spark)

    actual_columns = dtc.columns

    expected_columns = {"id": "bigint", "name": "string", "value": "string"}

    assert actual_columns == expected_columns


def test_get_delta_partition_columns(spark: SparkSession, tmp_path: Path) -> None:
    """
    Must get correctly partition columns as a list
    """

    delta_path = f"{tmp_path}/sample_delta_dataset"

    (
        spark.createDataFrame([(1, "a", "abc")], ["id", "name", "value"])
        .write.format("delta")
        .mode("overwrite")
        .partitionBy("id")
        .save(delta_path)
    )

    dtc = DeltaTableConfig(table_path=delta_path, spark_session=spark)

    actual_partition_columns = dtc.partition_columns
    expected_partition_columns = ["id"]

    assert actual_partition_columns == expected_partition_columns


def test_get_delta_clustering_columns(spark: SparkSession, tmp_path: Path) -> None:
    """
    Must get correctly clustering columns as a list
    """

    delta_path = f"{tmp_path}/sample_delta_dataset"

    (
        spark.createDataFrame([(1, "a", "abc")], ["id", "name", "value"])
        .write.format("delta")
        .mode("overwrite")
        .save(delta_path)
    )

    dtc = DeltaTableConfig(table_path=delta_path, spark_session=spark)

    actual_clustering_columns = dtc.clustering_columns
    expected_clustering_columns = []

    assert actual_clustering_columns == expected_clustering_columns


def test_get_delta_table_details(spark: SparkSession, tmp_path: Path) -> None:
    """
    Must get correctly table columns and partition columns as dictionaries
    """

    delta_path = f"{tmp_path}/sample_delta_dataset"

    (
        spark.createDataFrame([(1, "a", "abc")], ["id", "name", "value"])
        .write.format("delta")
        .mode("overwrite")
        .partitionBy("id")
        .save(delta_path)
    )

    dtc = DeltaTableConfig(table_path=delta_path, spark_session=spark)

    # set extra properties to check they are included in details
    dtc.properties = {
        "delta.logRetentionDuration": "interval 10 days",
        "delta.deletedFileRetentionDuration": "interval 16 days",
    }

    actual_details = dtc.details
    expected_details = {
        "columns": {"id": "bigint", "name": "string", "value": "string"},
        "properties": {
            "delta.deletedFileRetentionDuration": "interval 16 days",
            "delta.logRetentionDuration": "interval 10 days",
        },
        "constraints": {},
        "clustering_columns": [],
        "partition_columns": ["id"],
    }

    assert actual_details == expected_details
