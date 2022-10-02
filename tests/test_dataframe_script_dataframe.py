import pytest
from pyspark.sql import DataFrame, Row
from spalah.dataframe import script_dataframe


def test_script_dataframe(spark, flat_dataset: DataFrame) -> None:
    """The input dataset must be scripted and then recreated from the script:
    |  a|  b|
    +---+---+
    |  1|2.0|
    |  2|3.0|
    """
    df_input = flat_dataset.select("a", "b")

    generated_script = script_dataframe(df_input)

    # dynamically run the script which will generate "outcome_dataframe"
    loc = {"spark": spark}
    exec(generated_script, globals(), loc)

    df_outcome = loc["outcome_dataframe"]

    assert df_outcome.collect() == [Row(a=1, b=2.0), Row(a=2, b=3.0)]


def test_script_dataframe_exception_limit(spark) -> None:
    """The input dataframe has more than 20, this must raise an exception"""

    df_input = spark.range(1, 22)

    with pytest.raises(ValueError) as e:
        script_dataframe(df_input)
    assert str(e.value).startswith("This method is limited to script up")
