import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

df = pd.DataFrame({'one': [1, 2, 3],
                   'two': [[1, 2, 3], [None, 5, 6], [7, 8, 9]],
                   'three': ['foo', 'bar', 'baz']})

table = pa.Table.from_pandas(df)
pq.write_table(table, 'example.parquet')
