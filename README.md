# spalah

Spalah is a set of python helpers to deal with PySpark dataframes

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install foobar.

```bash
pip install spalah
```

## Usage

```python
from spalah.dataframe import slice_dataframe
from pyspark.sql import SparkSession

slice_dataframe(
    input_dataframe=df,
    columns_to_include=[],
    columns_to_exclude=["d", "e"],
    nullify_only=False
)
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)
