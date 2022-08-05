# Examples of use

## Module: dataframe
This module contains various dataframe specific functions and classes, like `SchemaComparer`, `script_dataframe`, `slice_dataframe` etc. 


### SchemaComparer

Let's define a source and target dataframes that will be used further in the schema comparison. The target schema contains a few adjustments that the class to catch and display
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
 |-- ID: string (nullable = false)             # Changed data type
 |-- name: string (nullable = false)           # Changed case of the column name
 |-- Address: struct (nullable = false)        # Removed field Line2
 |    |-- Line1: string (nullable = false)

```

Then, let's initiate a SchemaComparer and run the comparison

```python
from spalah.dataframe import SchemaComparer

schema_comparer = SchemaComparer(
    source_schema = df_source.schema,
    target_schema = df_target.schema
)

schema_comparer.compare()
```

The comparison results are stored in the class instance properties `matched` and `not_matched`

```python
schema_comparer.matched
```
Contains a single matched column

```
[MatchedColumn(name='Address.Line1',  data_type='StringType')]
```

```python
schema_comparer.not_matched
```
Contains a list of all not matched columns with a reason as description of non-match:
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

### flatten_schema

The `flatten_schema` can be helpful when dealing with complex (nested) data types.
To see it in action, let's define a sample dataframe 
```python
df_complex_schema = spark.sql(
    'SELECT 1 as ID, "John" AS Name, struct("line1" AS Line1, "line2" AS Line2) AS Address'
)
df_source.printSchema()


root
 |-- ID: integer (nullable = false)
 |-- Name: string (nullable = false)
 |-- Address: struct (nullable = false)
 |    |-- Line1: string (nullable = false)
 |    |-- Line2: string (nullable = false)

```
Pass the sample dataframe to get the list of all attributes as  one dimensional list
```python
flatten_schema(df_complex_schema.schema)
```

```
['ID', 'Name', 'Address.Line1', 'Address.Line2']
```
Alternatively, the function can return data types of the attributes
```python
flatten_schema(
    schema=df_complex_schema.schema,
    include_datatype=True
)
```

```
[
    ('ID', 'IntegerType'),
    ('Name', 'StringType'),
    ('Address.Line1', 'StringType'),
    ('Address.Line2', 'StringType')
]
```

### script_dataframe

Pass the dataframe to get the `script_dataframe` to get the script of it that can be ported to another environment. The function can generate the script for dataframes with up to 20 rows. Use `.limit(20)` to reduce the number of rows when needed

```python
from spalah.dataframe import script_dataframe

script = script_dataframe(df)

print(script)
```
output:
```python
from pyspark.sql import Row
import datetime
from decimal import Decimal
from pyspark.sql.types import *

# Scripted data and schema:
__data = [Row(ID=1, Name='John', Address=Row(Line1='line1', Line2='line2'))]

__schema = {'type': 'struct', 'fields': [{'name': 'ID', 'type': 'integer', 'nullable': False, 'metadata': {}}, {'name': 'Name', 'type': 'string', 'nullable': False, 'metadata': {}}, {'name': 'Address', 'type': {'type': 'struct', 'fields': [{'name': 'Line1', 'type': 'string', 'nullable': False, 'metadata': {}}, {'name': 'Line2', 'type': 'string', 'nullable': False, 'metadata': {}}]}, 'nullable': False, 'metadata': {}}]}

outcome_dataframe = spark.createDataFrame(__data, StructType.fromJson(__schema))
```


### slice_dataframe
Slice the schema of the dataframe by selecting which attributes must be included and/or excluded.
The function supports also complex structures and can be helpful for cases when sensitive/PII attributes must be cut off during the data transformation:
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

Alternatively, excluded columns can be nullified instead of removed:
```python
df_result = slice_dataframe(
    input_dataframe=df,
    columns_to_include=["Name", "Address"],
    columns_to_exclude=["Address.Line2"],
    nullify_only=True
)

df_result.show()

""" output:
+----+----+-------------+
|  ID|Name|      Address|
+----+----+-------------+
|null|John|{line1, null}|
+----+----+-------------+

"""
```