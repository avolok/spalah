# Examples of use: spalah.datalake


This module contains various storage and dataset specific functions, like: `get_table_properties`, `set_table_properties`, `check_dbfs_mounts` etc. 


### get_table_properties


```python
from spalah.datalake import get_table_properties

table_properties = get_table_properties(table_path="/path/dataset")

print(table_properties)

# results to:

{'delta.deletedFileRetentionDuration': 'interval 15 days'}
```



### set_table_properties

```python
from spalah.datalake import set_table_properties

set_table_properties(
    table_path='/path/dataset',
    properties={
        "delta.logRetentionDuration": "interval 10 days",
        "delta.deletedFileRetentionDuration": "interval 15 days"
    }
)
```
and the outcome is:
```
Applying table properties on 'delta.`/path/dataset`':
 - Checking if 'delta.logRetentionDuration = interval 10 days' is set on delta.`/path/dataset`
   Result: The property has been set
 - Checking if 'delta.deletedFileRetentionDuration = interval 15 days' is set on delta.`/path/dataset`
   Result: The property has been set
```

If the existing properties not to be preserved use parameter `keep_existing`:

```python
from spalah.datalake import set_table_properties

set_table_properties(
    table_path='/path/dataset',
    properties={
        "delta.logRetentionDuration": "interval 10 days",
        "delta.deletedFileRetentionDuration": "interval 15 days",        
    },
    keep_existing= False,
)
```