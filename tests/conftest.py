from datetime import date, datetime
import pytest
from pyspark.sql import DataFrame, Row, SparkSession
from delta import configure_spark_with_delta_pip


@pytest.fixture(scope="session")
def spark():
    builder = (
        SparkSession.builder.appName("MyApp")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark


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
def simple_delta_dataset(spark: SparkSession) -> DataFrame:

    return spark.createDataFrame(
        [
            Row(a=1, b=2.0, c="string1", d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
            Row(a=2, b=3.0, c="string2", d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
        ]
    )


@pytest.fixture(scope="session")
def flat_dataset(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [
            Row(
                a=1,
                b=2.0,
                c="string1",
                d=date(2000, 1, 1),
                e=datetime(2000, 1, 1, 12, 0),
            ),
            Row(
                a=2,
                b=3.0,
                c="string2",
                d=date(2000, 2, 1),
                e=datetime(2000, 1, 2, 12, 0),
            ),
        ]
    )


@pytest.fixture(scope="session")
def nested_flat_array(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        Row(
            value=Row(
                RetailTransaction=Row(
                    StoreID="1234",
                    LineItem=["Card", "Discount"],
                )
            )
        )
    )


@pytest.fixture(scope="session")
def nested_array_struct(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        Row(
            value=Row(
                RetailTransaction=Row(
                    StoreID="1234",
                    LineItem=[
                        Row(
                            SequenceNumber="2",
                            LoyaltyCard=Row(CardId="111", CardType="Premium"),
                        ),
                        Row(
                            SequenceNumber="3",
                            LoyaltyCard=Row(CardId="222", CardType="Gold"),
                        ),
                    ],
                )
            )
        )
    )


@pytest.fixture(scope="session")
def nested_array_array(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        Row(
            value=Row(
                RetailTransaction=Row(
                    StoreID="1234",
                    LineItem=[
                        Row(
                            Discount=[
                                Row(POSItemId="100", POSName="POS01"),
                                Row(POSItemId="101", POSName="POS02"),
                            ]
                        )
                    ],
                )
            )
        )
    )


@pytest.fixture(scope="session")
def nested_array_struct_array(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        Row(
            value=Row(
                RetailTransaction=Row(
                    StoreID="1234",
                    LineItem=[
                        Row(
                            Discount=Row(
                                DiscountId="1",
                                ItemList=[Row(ItemID="10", TestId="200")],
                            )
                        )
                    ],
                )
            )
        )
    )


@pytest.fixture(scope="session")
def nested_array_struct_array_multiple_rows(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        Row(
            value=Row(
                RetailTransaction=Row(
                    StoreID="1234",
                    LineItem=[
                        Row(
                            Discount=Row(
                                DiscountId="1",
                                ItemList=[Row(ItemID="10", ItemNum="11")],
                            )
                        ),
                        Row(
                            Discount=Row(
                                DiscountId="2",
                                ItemList=[Row(ItemID="12", ItemNum="13")],
                            )
                        ),
                    ],
                )
            )
        )
    )


@pytest.fixture(scope="session")
def nested_array_struct_struct(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        Row(
            value=Row(
                RetailTransaction=Row(
                    StoreID="1234",
                    LineItem=[
                        Row(
                            LoyaltyItem=Row(
                                SequenceNumber="2",
                                LoyaltyCard=Row(CardId="2222", CardType="Premium"),
                            )
                        )
                    ],
                )
            )
        )
    )


@pytest.fixture(scope="session")
def nested_tlog_example(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        Row(
            value=Row(
                RetailTransaction=Row(
                    StoreID="1234",
                    LineItem=[
                        Row(
                            LoyaltyItem=Row(
                                SequenceNumber="2",
                                LoyaltyCard=Row(CardId="2222", CardType="Premium"),
                            ),
                            Discount=[Row(POSItemId="100", POSName="POS01")],
                        )
                    ],
                )
            )
        )
    )


@pytest.fixture(scope="session")
def nested_root_array(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        Row(value=Row(RetailTransaction=[Row(StoreID="1234", StoreName="Test Store")]))
    )
