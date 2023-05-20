# spalah

Spalah is a set of python helpers to deal with PySpark dataframes, transformations, schemas and Delta Tables.

The word "spalah" means "spark" in Ukrainian 🇺🇦 

# Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install spalah.

```bash
pip install spalah
```

# Examples of use
Spalah currently has two different groups of helpers: `dataframe` and `datalake`.

## spalah.dataframe

### slice_dataframe

```python
from spalah.dataframe import slice_dataframe

df = spark.sql(
    'SELECT 1 as ID, "John" AS Name, struct("line1" AS Line1, "line2" AS Line2) AS Address'
)
df.printSchema()

""" output:
root
 |-- ID: integer (nullable = false)
 |-- Name: string (nullable = false)
 |-- Address: struct (nullable = false)
 |    |-- Line1: string (nullable = false)
 |    |-- Line2: string (nullable = false)
"""

# Create a new dataframe by cutting of root and nested attributes
df_result = slice_dataframe(
    input_dataframe=df,
    columns_to_include=["Name", "Address"],
    columns_to_exclude=["Address.Line2"]
)
df_result.printSchema()

""" output:
root
 |-- Name: string (nullable = false)
 |-- Address: struct (nullable = false)
 |    |-- Line1: string (nullable = false)
"""
```

Beside of nested regular structs it also supported slicing of structs in arrays, including multiple levels of nesting


### flatten_schema

```python
from spalah.dataframe import flatten_schema

# Pass the sample dataframe to get the list of all attributes as single dimension list
flatten_schema(df_complex_schema.schema)

""" output:
['ID', 'Name', 'Address.Line1', 'Address.Line2']
"""


# Alternatively, the function can return data types of the attributes
flatten_schema(
    schema=df_complex_schema.schema,
    include_datatype=True
)

""" output:
[
    ('ID', 'IntegerType'),
    ('Name', 'StringType'),
    ('Address.Line1', 'StringType'),
    ('Address.Line2', 'StringType')
]
"""
```

### script_dataframe

```python
from spalah.dataframe import script_dataframe

script = script_dataframe(df)

print(script)

""" output:
from pyspark.sql import Row
import datetime
from decimal import Decimal
from pyspark.sql.types import *

# Scripted data and schema:
__data = [Row(ID=1, Name='John', Address=Row(Line1='line1', Line2='line2'))]

__schema = {'type': 'struct', 'fields': [{'name': 'ID', 'type': 'integer', 'nullable': False, 'metadata': {}}, {'name': 'Name', 'type': 'string', 'nullable': False, 'metadata': {}}, {'name': 'Address', 'type': {'type': 'struct', 'fields': [{'name': 'Line1', 'type': 'string', 'nullable': False, 'metadata': {}}, {'name': 'Line2', 'type': 'string', 'nullable': False, 'metadata': {}}]}, 'nullable': False, 'metadata': {}}]}

outcome_dataframe = spark.createDataFrame(__data, StructType.fromJson(__schema))
"""
```

### SchemaComparer

```python
from spalah.dataframe import SchemaComparer

schema_comparer = SchemaComparer(
    source_schema = df_source.schema,
    target_schema = df_target.schema
)

schema_comparer.compare()

# The comparison results are stored in the class instance properties `matched` and `not_matched`

# Contains a list of matched columns:
schema_comparer.matched

""" output:
[MatchedColumn(name='Address.Line1',  data_type='StringType')]
"""

# Contains a list of all not matched columns with a reason as description of non-match:
schema_comparer.not_matched

""" output:
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
"""
```

## spalah.dataset

### Get delta table properties

```python
from spalah.dataset import DeltaProperty

dp = DeltaProperty(table_path="/path/dataset")

print(dp.properties) 

# output: 
# {'delta.deletedFileRetentionDuration': 'interval 15 days'}
```

### Set delta table properties

```python
rom spalah.dataset import DeltaProperty

dp = DeltaProperty(table_path="/path/dataset")

dp.properties = {
    "delta.logRetentionDuration": "interval 10 days",
    "delta.deletedFileRetentionDuration": "interval 15 days"
}

```
and the standard output is:

```
2023-05-20 18:27:42,070 INFO      Applying check constraints on 'delta.`/tmp/nested_schema_dataset`':
2023-05-20 18:27:42,071 INFO      Checking if constraint 'id_is_not_null' was already set on delta.`/tmp/nested_schema_dataset`
2023-05-20 18:27:42,433 INFO      The constraint id_is_not_null has been successfully added to 'delta.`/tmp/nested_schema_dataset`
```

Please note that check constraints can be retrieved and set using property: `.check_constraints`

Check for more information in [examples: dataframe](docs/examples/dataframe.md), [examples: datalake](docs/examples/dataset.md) pages and related [notebook](docs/usage.ipynb)

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)
