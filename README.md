# spalah

Spalah is a set of python helpers to deal with PySpark dataframes

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install foobar.

```bash
pip install spalah
```

## Examples of use

### Module: dataframe
This module contains various dataframe specific functions and classes, like SchemaComparer, script_dataframe, slice_dataframe. 

#### SchemaComparer
Let's define a source and target dataframes:
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# A source dataframe
df_source = spark.sql(
    'SELECT 1 as ID, "John" AS Name, struct("line1" AS Line1, "line2" AS Line2) AS Address'
)
df_source.printSchema()

root
 |-- ID: integer (nullable = false)
 |-- Name: string (nullable = false)
 |-- Address: struct (nullable = false)
 |    |-- Line1: string (nullable = false)
 |    |-- Line2: string (nullable = false)


# A target dataframe
df_target = spark.sql(
    'SELECT "a" as ID, "John" AS name, struct("line1" AS Line1) AS Address'
)
df_target.printSchema()

root
 |-- ID: string (nullable = false)
 |-- name: string (nullable = false)
 |-- Address: struct (nullable = false)
 |    |-- Line1: string (nullable = false)

```

Then, let's define a SchemaComparer and perform comparison

```python
from spalah.dataframe import SchemaComparer

schema_comparer = SchemaComparer(
    source_schema = df_source.schema,
    target_schema = df_target.schema
)

schema_comparer.compare()
```

The comparison results are stored in the class instance attributes `matched` and `not_matched`

```python
schema_comparer.matched
```

```
[MatchedColumn(name='Address.Line1',  data_type='StringType')]
```

```python
schema_comparer.not_matched
```

```
[
    NotMatchedColumn(
        name='name', 
        data_type='StringType', 
        reason="The column exists in source and target schemas but it's name is case-mismatched"
    ),
    NotMatchedColumn(
        name='ID', 
        data_type='IntegerType <=> StringType', 
        reason='The column exists in source and target schemas but it is not matched by a data type'
    ),
    NotMatchedColumn(
        name='Address.Line2', 
        data_type='StringType', 
        reason='The column exists only in the source schema'
    )
]
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)
