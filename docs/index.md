---
hide:
  - navigation
---

# spalah

Spalah is a set of python helpers to deal with PySpark dataframes, transformations, schemas etc.

It's main feature is to simplify dealing with advanced spark schemas. Think nested structures, arrays, arrays in arrays in nested structures in arrays. Sometimes such schemas happens. Especially if the lakehouses stores ingested json dataset as is.



And.. the word "spalah" means "spark" in Ukrainian 🇺🇦 :)

---------------------------------------
**Documentation:** [https://avolok.github.io/spalah](https://avolok.github.io/spalah){:target="_blank"} <br />
**Source Code for spalah:** [https://github.com/avolok/spalah](https://github.com/avolok/spalah){:target="_blank"}
---------------------------------------

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install spalah.

```bash
pip install spalah
```

## Examples

### Slicing complex schema by removing (or nullifying) nested elements

``` py
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
!!! note
    Beside of nested regular structs it also supported slicing of structs in arrays, including multiple levels of nesting


### Get list of flattened elements from the complex schema


=== "All elements flattened to a single dimension list"

    ``` py
    from spalah.dataframe import flatten_schema

    # Pass the sample dataframe to get the list of all attributes as single dimension list
    flatten_schema(df_complex_schema.schema)

    """ output:
    ['ID', 'Name', 'Address.Line1', 'Address.Line2']
    """
    ```

=== "Use tuples to return element name and data type"

    ``` py
    from spalah.dataframe import flatten_schema

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

See more examples in [examples: dataframes](examples_dataframe.md) and [examples: datalake](examples_datalake.md)

## License
This project is licensed under the terms of the MIT license.






