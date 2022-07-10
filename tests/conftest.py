from datetime import date, datetime

import pytest
from pyspark.sql import DataFrame, Row, SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()


@pytest.fixture(scope="session")
def nested_dataset(spark: SparkSession) -> DataFrame:
    return spark.sql(
        """
        select
            1 as column_a
        ,   2.0 as column_b
        ,   struct(
                "c1" as column_c_1
            ,   struct(
                    "c_2_1" as c_2_1,
                    "c_2_2" as c_2_2,
                    "c_2_3" as c_2_3
            ) as column_c_2
        ) as column_c
        """
    )


@pytest.fixture(scope="session")
def flat_dataset(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [
            Row(a=1, b=2.0, c="string1", d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
            Row(a=2, b=3.0, c="string2", d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
        ]
    )
