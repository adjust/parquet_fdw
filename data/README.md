# Sample Parquet data

`example.parquet` schema:

| column |        type |
|--------|-------------|
|    one |       INT64 |
|    two | LIST<INT64> |
|  three |      STRING |
|   four |   TIMESTAMP |
|   five |      DATE32 |
|    six |        BOOL |

## Generator

Generator script requires `pyarrow` and `pandas` python modules installed. To
generate Parquet file run:

```
python generate.py
```
