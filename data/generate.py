#!/usr/bin/env python3

import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, date

# example1.parquet file
df1 = pd.DataFrame({'one': [1, 2, 3],
                    'two': [[1, 2, 3], [None, 5, 6], [7, 8, 9]],
                    'three': ['foo', 'bar', 'baz'],
                    'four': [datetime(2018, 1, 1),
                             datetime(2018, 1, 2),
                             datetime(2018, 1, 3)],
                    'five': [date(2018, 1, 1),
                             date(2018, 1, 2),
                             date(2018, 1, 3)],
                    'six': [True, False, True],
                    'seven': [0.5, None, 1.0]})
table1 = pa.Table.from_pandas(df1)

df2 = pd.DataFrame({'one': [4, 5, 6],
                    'two': [[10, 11, 12], [13, 14, 15], [16, 17, 18]],
                    'three': ['uno', 'dos', 'tres'],
                    'four': [datetime(2018, 1, 4),
                             datetime(2018, 1, 5),
                             datetime(2018, 1, 6)],
                    'five': [date(2018, 1, 4),
                             date(2018, 1, 5),
                             date(2018, 1, 6)],
                    'six': [False, False, False],
                    'seven': [1.5, None, 2.0]})
table2 = pa.Table.from_pandas(df2)

with pq.ParquetWriter('simple/example1.parquet', table1.schema) as writer:
    writer.write_table(table1)
    writer.write_table(table2)

# example2.parquet file
df3 = pd.DataFrame({'one': [1, 3, 5, 7, 9],
                    'two': [[19, 20], [21, 22], [23, 24], [25, 26], [27, 28]],
                    'three': ['eins', 'zwei', 'drei', 'vier', 'fünf'],
                    'four': [datetime(2018, 1, 1),
                             datetime(2018, 1, 3),
                             datetime(2018, 1, 5),
                             datetime(2018, 1, 7),
                             datetime(2018, 1, 9)],
                    'five': [date(2018, 1, 1),
                             date(2018, 1, 3),
                             date(2018, 1, 5),
                             date(2018, 1, 7),
                             date(2018, 1, 9)],
                    'six': [True, False, True, False, True]})
table3 = pa.Table.from_pandas(df3)

with pq.ParquetWriter('simple/example2.parquet', table3.schema) as writer:
    writer.write_table(table3)

# example3.parquet file
mdt1 = pa.map_(pa.int32(), pa.string())
mdt2 = pa.map_(pa.date32(), pa.int16())
df = pd.DataFrame({
        'one': pd.Series([
            [(1, 'foo'), (2, 'bar'), (3, 'baz')],
            [(4, 'test1'), (5,'test2')],
        ]),
        'two': pd.Series([
            [(date(2018, 1, 1), 10), (date(2018, 1, 2), 15)],
            [(date(2018, 1, 3), 20), (date(2018, 1, 4), 25)],
        ]),
        'three': pd.Series([1, 2]),
    }
)

schema = pa.schema([
    pa.field('one', mdt1),
    pa.field('two', mdt2),
    pa.field('three', pa.int32())])
table = pa.Table.from_pandas(df, schema)

with pq.ParquetWriter('complex/example3.parquet', table.schema) as writer:
    writer.write_table(table)
