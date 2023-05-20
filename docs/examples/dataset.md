# Examples of use: spalah.dataset


This module contains various storage and dataset specific functions, like: `DeltaProperty`, `check_dbfs_mounts` etc. 


## DeltaProperty


### Retrieve delta table properties


```python
from spalah.dataset import DeltaProperty

dp = DeltaProperty(table_path="/tmp/nested_schema_dataset")

print(dp.properties)

# results to:
{'delta.deletedFileRetentionDuration': 'interval 15 days'}
```

### Set delta table properties


```python
from spalah.dataset import DeltaProperty

dp = DeltaProperty(table_path="/tmp/nested_schema_dataset")

dp.properties = {
    "delta.logRetentionDuration": "interval 10 days",
    "delta.deletedFileRetentionDuration": "interval 15 days"
}
```

and the outcome is:

```
2023-05-20 18:21:35,155 INFO      Applying table properties on 'delta.`/tmp/nested_schema_dataset`':
2023-05-20 18:21:35,156 INFO      Checking if 'delta.logRetentionDuration = interval 10 days' is set on delta.`/tmp/nested_schema_dataset`
2023-05-20 18:21:35,534 INFO      The property has been set
2023-05-20 18:21:35,535 INFO      Checking if 'delta.deletedFileRetentionDuration = interval 15 days' is set on delta.`/tmp/nested_schema_dataset`
2023-05-20 18:21:35,837 INFO      The property has been set
```

If the existing properties to be preserved use parameter `keep_existing_properties=True`:


```python
from spalah.dataset import DeltaProperty

dp = DeltaProperty(table_path="/tmp/nested_schema_dataset")

dp.keep_existing_properties = True

dp.properties = {
    "delta.logRetentionDuration": "interval 10 days",
    "delta.deletedFileRetentionDuration": "interval 15 days"
}
```


### Retrieve delta table check constraints


```python
from spalah.dataset import DeltaProperty

dp = DeltaProperty(table_path="/tmp/nested_schema_dataset")

print(dp.check_constraints)

{} # results to empty dictionary, so no check constraints are set yet
```

### Set delta table check constraints


```python
from spalah.dataset import DeltaProperty

dp = DeltaProperty(table_path="/tmp/nested_schema_dataset")

dp.check_constraints = {'id_is_not_null': 'id is not null'} 
```

and the outcome is:

```
2023-05-20 18:27:42,070 INFO      Applying check constraints on 'delta.`/tmp/nested_schema_dataset`':
2023-05-20 18:27:42,071 INFO      Checking if constraint 'id_is_not_null' was already set on delta.`/tmp/nested_schema_dataset`
2023-05-20 18:27:42,433 INFO      The constraint id_is_not_null has been successfully added to 'delta.`/tmp/nested_schema_dataset`
```

If the existing constraints to be preserved use parameter `keep_existing_check_constraints=True`:


```python
from spalah.dataset import DeltaProperty

dp = DeltaProperty(table_path="/tmp/nested_schema_dataset")
dp.keep_existing_check_constraints = True

dp.check_constraints = {'Name_is_not_null': 'Name is not null'} 
```

so, the second check of the check constraints shows both constraints defined: existing and the new one:

```python
print(dp.check_constraints)

{'id_is_not_null': 'id is not null', 'name_is_not_null': 'Name is not null'}
```

The following check shows that the constraint `id_is_not_null` is already set and protects the column ID from being null:

```python
spark.sql("insert into delta.`/tmp/nested_schema_dataset` (ID, Name, Address) VALUES (NULL, 'Alex', NULL) ")

ERROR Utils: Aborting task
org.apache.spark.sql.delta.schema.DeltaInvariantViolationException: 
CHECK constraint id_is_not_null (id IS NOT NULL) violated by row with values:
 - id : null
...
```
