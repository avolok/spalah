import pytest
from spalah.datalake import set_table_properties, get_table_properties


def test_set_table_properties_new_table_no_existing_properties(spark, tmp_path) -> None:
    """Tested method must set correctly table properties to the brand new dataset"""

    delta_path = f"{tmp_path}/sample_delta_dataset"

    properties_to_set = {
        "delta.logRetentionDuration": "interval 10 days",
        "delta.deletedFileRetentionDuration": "interval 16 days",
    }

    spark.range(0, 1).write.format("delta").mode("overwrite").save(delta_path)

    set_table_properties(table_path=delta_path, properties=properties_to_set)

    get_properties = get_table_properties(table_path=delta_path)

    assert properties_to_set == get_properties


def test_set_table_properties_new_table_with_existing_properties(spark, tmp_path) -> None:
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

    set_table_properties(table_path=delta_path, properties=properties_to_set)

    get_properties = get_table_properties(table_path=delta_path)

    properties_to_set.update({"prop.property1": "abc"})

    assert get_properties == properties_to_set


def test_set_table_properties_new_table_remove_existing(spark, tmp_path) -> None:
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

    set_table_properties(table_path=delta_path, properties=properties_to_set, keep_existing=False)

    get_properties = get_table_properties(table_path=delta_path)

    assert get_properties == properties_to_set


def test_set_table_properties_exceptions_both_provided_as_identifier() -> None:
    "When both table_path and table_name provided the exception must occur"

    with pytest.raises(ValueError) as e:
        set_table_properties(
            table_path="/some/path",
            table_name="tbl_name",
            properties={"a": "b"},
        )
    assert str(e.value).startswith("Both 'table_path' and 'table_name' provided")


def test_set_table_properties_exceptions_noting_provided_as_identifier() -> None:
    "When neither table_path nor table_name provided the exception must occur"

    with pytest.raises(ValueError) as e:
        set_table_properties(
            table_path="",
            table_name="",
            properties={"a": "b"},
        )
    assert str(e.value).startswith("Neither 'table_path' nor 'table_name' provided")
