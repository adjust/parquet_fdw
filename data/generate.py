import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, date

# row group 1
df1 = pd.DataFrame({'one': [1, 2, 3],
                    'two': [[1, 2, 3], [None, 5, 6], [7, 8, 9]],
                    'three': ['foo', 'bar', 'baz'],
                    'four': [datetime(2018, 1, 1),
                             datetime(2018, 1, 2),
                             datetime(2018, 1, 3)],
                    'five': [date(2018, 1, 1),
                             date(2018, 1, 2),
                             date(2018, 1, 3)]})
table1 = pa.Table.from_pandas(df1)

# row group 2
df2 = pd.DataFrame({'one': [4, 5, 6],
                    'two': [[10, 11, 12], [13, 14, 15], [16, 17, 18]],
                    'three': ['uno', 'dos', 'tres'],
                    'four': [datetime(2018, 1, 4),
                             datetime(2018, 1, 5),
                             datetime(2018, 1, 6)],
                    'five': [date(2018, 1, 4),
                             date(2018, 1, 5),
                             date(2018, 1, 6)]})
table2 = pa.Table.from_pandas(df2)

with pq.ParquetWriter('example.parquet', table1.schema) as writer:
    writer.write_table(table1)
    writer.write_table(table2)

#pq.write_table(table, 'example.parquet')
