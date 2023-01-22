import pytest
from pyspark.sql import Row

from spalah.dataframe import slice_dataframe


@pytest.mark.parametrize(
    "assert_message,input_dataset,columns_to_include,columns_to_exclude,nullify_only,expected",
    [
        # nested structs: include
        (
            "The dataframe must contain only columns listed in "
            "'columns_to_include' (nested structs)",
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
        # nested structs: include, mixed case
        (
            "The dataframe must contain only columns listed in "
            "'columns_to_include' (nested structs)",
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
        # "nested structs: include and exclude"
        (
            "The dataframe must contain columns listed in "
            "'columns_to_include' beside of 'columns_to_exclude'  (nested structs)",
            "nested_dataset",
            ["column_b", "column_c"],
            ["column_c.column_c_2.c_2_1"],
            False,
            Row(
                column_b=2.0,
                column_c=Row(column_c_1="c1", column_c_2=Row(c_2_2="c_2_2", c_2_3="c_2_3")),
            ),
        ),
        # "nested structs: exclude"
        (
            "All columns in 'columns_to_exclude' must be excluded (nested structs)'",
            "nested_dataset",
            [],
            ["column_b", "column_c.column_c_2.c_2_1"],
            False,
            Row(
                column_a=1,
                column_c=Row(column_c_1="c1", column_c_2=Row(c_2_2="c_2_2", c_2_3="c_2_3")),
            ),
        ),
        # "nested structs: exclude, mixed case"
        (
            "All columns in 'columns_to_exclude' must be excluded (nested structs)'",
            "nested_dataset",
            [],
            ["Column_B", "Column_C.column_c_2.C_2_1"],
            False,
            Row(
                column_a=1,
                column_c=Row(column_c_1="c1", column_c_2=Row(c_2_2="c_2_2", c_2_3="c_2_3")),
            ),
        ),
        # "nested structs: nullify only for excluded columns"
        (
            "All columns in 'columns_to_exclude' must be nullified (nested structs)'",
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
        # "nested: nullify flat ArrayType"
        (
            "All columns in 'columns_to_exclude' must be nullified (nested)'",
            "nested_flat_array",
            [],
            ["RetailTransaction.LineItem"],
            True,
            Row(RetailTransaction=Row(StoreID="1234", LineItem=None)),
        ),
        # "nested: nullify fields within ArrayType-Struct"
        (
            "All columns in 'columns_to_exclude' must be nullified (nested)'",
            "nested_array_struct",
            [],
            ["RetailTransaction.LineItem.LoyaltyCard.CardId"],
            True,
            Row(
                RetailTransaction=Row(
                    StoreID="1234",
                    LineItem=[
                        Row(
                            SequenceNumber="2",
                            LoyaltyCard=Row(CardId=None, CardType="Premium"),
                        ),
                        Row(
                            SequenceNumber="3",
                            LoyaltyCard=Row(CardId=None, CardType="Gold"),
                        ),
                    ],
                )
            ),
        ),
        # "nested: nullify Root of ArrayType-Struct"
        (
            "All columns in 'columns_to_exclude' must be nullified (nested)'",
            "nested_array_struct",
            [],
            ["RetailTransaction.LineItem"],
            True,
            Row(
                RetailTransaction=Row(
                    StoreID="1234",
                    LineItem=[
                        Row(
                            SequenceNumber=None,
                            LoyaltyCard=Row(CardId=None, CardType=None),
                        ),
                        Row(
                            SequenceNumber=None,
                            LoyaltyCard=Row(CardId=None, CardType=None),
                        ),
                    ],
                )
            ),
        ),
        # "nested: nullify fields within ArrayType-Struct-ArrayType"
        (
            "All columns in 'columns_to_exclude' must be nullified (nested)'",
            "nested_array_struct_array",
            [],
            ["RetailTransaction.LineItem.Discount.ItemList.ItemID"],
            True,
            Row(
                RetailTransaction=Row(
                    StoreID="1234",
                    LineItem=[
                        Row(
                            Discount=Row(
                                DiscountId="1",
                                ItemList=[Row(ItemID=None, TestId="200")],
                            )
                        )
                    ],
                )
            ),
        ),
        # "nested: nullify fields within ArrayType-Struct-ArrayType multiple rows"
        (
            "All columns in 'columns_to_exclude' must be nullified (nested)'",
            "nested_array_struct_array_multiple_rows",
            [],
            ["RetailTransaction.LineItem.Discount.ItemList.ItemID"],
            True,
            Row(
                RetailTransaction=Row(
                    StoreID="1234",
                    LineItem=[
                        Row(
                            Discount=Row(
                                DiscountId="1",
                                ItemList=[Row(ItemID=None, ItemNum="11")],
                            )
                        ),
                        Row(
                            Discount=Row(
                                DiscountId="2",
                                ItemList=[Row(ItemID=None, ItemNum="13")],
                            )
                        ),
                    ],
                )
            ),
        ),
        # "nested: nullify fields within ArrayType-ArrayType",
        (
            "All columns in 'columns_to_exclude' must be nullified (nested)'",
            "nested_array_array",
            [],
            ["RetailTransaction.LineItem.Discount.POSItemId"],
            True,
            Row(
                RetailTransaction=Row(
                    StoreID="1234",
                    LineItem=[
                        Row(
                            Discount=[
                                Row(POSItemId=None, POSName="POS01"),
                                Row(POSItemId=None, POSName="POS02"),
                            ]
                        )
                    ],
                )
            ),
        ),
        # "nested: nullify fields within ArrayType-Struct-Struct"
        (
            "All columns in 'columns_to_exclude' must be nullified (nested)'",
            "nested_array_struct_struct",
            [],
            ["RetailTransaction.LineItem.LoyaltyItem.LoyaltyCard.CardId"],
            True,
            Row(
                RetailTransaction=Row(
                    StoreID="1234",
                    LineItem=[
                        Row(
                            LoyaltyItem=Row(
                                SequenceNumber="2",
                                LoyaltyCard=Row(CardId=None, CardType="Premium"),
                            )
                        )
                    ],
                )
            ),
        ),
        # "nested: exclude fields in nested arrays"
        (
            "All columns in 'columns_to_exclude' must be nullified (nested)'",
            "nested_tlog_example",
            [],
            [
                "RetailTransaction.LineItem.LoyaltyItem.LoyaltyCard.CardId",
                "RetailTransaction.LineItem.Discount.POSName",
            ],
            False,
            Row(
                RetailTransaction=Row(
                    StoreID="1234",
                    LineItem=[
                        Row(
                            LoyaltyItem=Row(
                                SequenceNumber="2",
                                LoyaltyCard=Row(CardType="Premium"),
                            ),
                            Discount=[Row(POSItemId="100")],
                        )
                    ],
                )
            ),
        ),
        # "nested: nullify fields within array as root"
        (
            "All columns in 'columns_to_exclude' must be nullified (nested)'",
            "nested_root_array",
            [],
            ["RetailTransaction.StoreID"],
            True,
            Row(RetailTransaction=[Row(StoreID=None, StoreName="Test Store")]),
        ),
    ],
    ids=[
        "nested structs: include",
        "nested structs: include, mixed case",
        "nested structs: include and exclude",
        "nested structs: exclude",
        "nested structs: exclude, mixed case",
        "nested structs: nullify only for excluded columns",
        "flat: nullify only for excluded columns",
        "flat: exclude",
        "nested: nullify flat ArrayType",
        "nested: nullify fields within ArrayType-Struct",
        "nested: nullify Root of ArrayType-Struct",
        "nested: nullify fields within ArrayType-Struct-ArrayType",
        "nested: nullify fields within ArrayType-Struct-ArrayType multiple rows",
        "nested: nullify fields within ArrayType-ArrayType",
        "nested: nullify fields within ArrayType-Struct-Struct",
        "nested: exclude fields in nested arrays",
        "nested: nullify fields within array as root",
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
        debug=True,
    )

    assert actual.first() == expected, assert_message


def test_slice_dataframe_invalid_parameters(request):
    dataset = request.getfixturevalue("flat_dataset")

    with pytest.raises(Exception) as e:
        slice_dataframe(
            input_dataframe=dataset, columns_to_include="a", columns_to_exclude=("b", "c")
        )
    assert str(e.value).startswith("'columns_to_include' and 'columns_to_exclude' must be a list")


def test_slice_dataframe_invalid_parameters2(request):
    dataset = request.getfixturevalue("flat_dataset")

    with pytest.raises(Exception) as e:
        slice_dataframe(input_dataframe=dataset, columns_to_include=[1, "a", None])
    assert str(e.value).startswith(
        "Member of 'columns_to_include' and" " 'columns_to_exclude' must be a string"
    )


def test_slice_dataframe_invalid_parameters3(request):
    dataset = request.getfixturevalue("flat_dataset")

    with pytest.raises(Exception) as e:
        slice_dataframe(input_dataframe=dataset, columns_to_include=["a"], columns_to_exclude=["a"])
    assert str(e.value).startswith("At least one column should be listed")
