# Sample Parquet data

`simple/example1.parquet` and `simple/example2.parquet` schema:

| column |        type |
|--------|-------------|
|    one |       INT64 |
|    two | LIST<INT64> |
|  three |      STRING |
|   four |   TIMESTAMP |
|   five |      DATE32 |
|    six |        BOOL |
|  seven |      DOUBLE |

`complex/example3.parquet`  schema:

| column |               type |
|--------|--------------------|
|    one | MAP<INT32, STRING> |
|    two | MAP<DATE32, INT16> |
|  three |             STRING |

## Generator

Generator script requires `pyarrow` and `pandas` python modules installed. To
generate Parquet file run:

```
python generate.py
```
