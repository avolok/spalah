import pytest
from spalah.datalake import get_table_properties


def test_get_table_properties(spark, tmp_path) -> None:
    """Tested method must get correctly table properties as a dictionary"""

    delta_path = f"{tmp_path}/sample_delta_dataset"
    spark.range(0, 1).write.format("delta").mode("overwrite").save(delta_path)

    _sql = f"""
        ALTER TABLE delta.`{delta_path}`
        SET TBLPROPERTIES (delta.deletedFileRetentionDuration = 'interval 15 days')
    """
    spark.sql(_sql)

    existing_properties = get_table_properties(table_path=delta_path)

    assert existing_properties == {"delta.deletedFileRetentionDuration": "interval 15 days"}


def test_set_table_properties_exceptions_both_provided_as_identifier() -> None:
    "When both table_path and table_name provided the exception must occur"

    with pytest.raises(ValueError) as e:
        get_table_properties(table_path="/some/path", table_name="tbl_name")
    assert str(e.value).startswith("Both 'table_path' and 'table_name' provided")


def test_set_table_properties_exceptions_noting_provided_as_identifier() -> None:
    "When neither table_path nor table_name provided the exception must occur"

    with pytest.raises(ValueError) as e:
        get_table_properties(table_path="", table_name="")
    assert str(e.value).startswith("Neither 'table_path' nor 'table_name' provided")
