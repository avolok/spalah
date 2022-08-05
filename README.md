# spalah

Spalah is a set of python helpers to deal with PySpark dataframes, transformations, schemas etc.

The word "spalah" means "spark" in Ukrainian language 🇺🇦 

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install foobar.

```bash
pip install spalah
```

## Examples of use

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

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)
