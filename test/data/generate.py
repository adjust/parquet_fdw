#!/usr/bin/env python3

import pyarrow.parquet as pq
import pandas as pd
import pyarrow as pa
from datetime import datetime, date, timedelta

# Define explicit schema to avoid large_string (which parquet_fdw doesn't support)
example1_schema = pa.schema([
    pa.field('one', pa.int64()),
    pa.field('two', pa.list_(pa.int64())),
    pa.field('three', pa.string()),  # Use string, not large_string
    pa.field('four', pa.timestamp('us')),
    pa.field('five', pa.date32()),
    pa.field('six', pa.bool_()),
    pa.field('seven', pa.float64()),
])

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
table1 = pa.Table.from_pandas(df1, schema=example1_schema, preserve_index=False)

df2 = pd.DataFrame({'one': [4, 5, 6],
                    'two': [[10, 11, 12], [13, 14, 15], [16, 17, 18]],
                    'three': ['uno', 'dos', 'tres'],
                    'four': [datetime(2018, 1, 4)+timedelta(seconds=10),
                             datetime(2018, 1, 5)+timedelta(milliseconds=10),
                             datetime(2018, 1, 6)+timedelta(microseconds=10)],
                    'five': [date(2018, 1, 4),
                             date(2018, 1, 5),
                             date(2018, 1, 6)],
                    'six': [False, False, False],
                    'seven': [1.5, None, 2.0]})
table2 = pa.Table.from_pandas(df2, schema=example1_schema, preserve_index=False)

with pq.ParquetWriter('simple/example1.parquet', example1_schema) as writer:
    writer.write_table(table1)
    writer.write_table(table2)

# example2.parquet file - schema without 'seven' column
example2_schema = pa.schema([
    pa.field('one', pa.int64()),
    pa.field('two', pa.list_(pa.int64())),
    pa.field('three', pa.string()),  # Use string, not large_string
    pa.field('four', pa.timestamp('us')),
    pa.field('five', pa.date32()),
    pa.field('six', pa.bool_()),
])

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
table3 = pa.Table.from_pandas(df3, schema=example2_schema, preserve_index=False)

# an empty data frame to test corner case
df4 = df3.drop([0, 1, 2, 3, 4])
table4 = pa.Table.from_pandas(df4, schema=example2_schema, preserve_index=False)

with pq.ParquetWriter('simple/example2.parquet', example2_schema) as writer:
    writer.write_table(table3)
    writer.write_table(table4)

# example3.parquet file
mdt1 = pa.map_(pa.int32(), pa.string())
mdt2 = pa.map_(pa.date32(), pa.int16())
df = pd.DataFrame({
        'one': pd.Series([
            [(1, 'foo'), (2, 'bar'), (3, 'baz')],
            [(4, 'test1'), (5, 'test2')],
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

# Parquet files for partitions
partition_schema = pa.schema([
    pa.field('id', pa.int64()),
    pa.field('token', pa.int64()),
    pa.field('date', pa.timestamp('us')),
    pa.field('num', pa.int64()),
])

df_part1 = pd.DataFrame({'id': [1, 1, 2, 3],
                         'token': [1, 1, 2, 2],
                         'date': [datetime(2018, 1, 1),
                                  datetime(2018, 1, 2),
                                  datetime(2018, 1, 3),
                                  datetime(2018, 1, 4)],
                         'num': [10, 23, 9, 38]})
table_part1 = pa.Table.from_pandas(df_part1, schema=partition_schema, preserve_index=False)

with pq.ParquetWriter(
        'partition/example_part1.parquet', partition_schema) as writer:
    writer.write_table(table_part1)

df_part2 = pd.DataFrame({'id': [1, 2, 2, 3],
                         'token': [1, 2, 2, 2],
                         'date': [datetime(2018, 2, 1),
                                  datetime(2018, 2, 2),
                                  datetime(2018, 2, 3),
                                  datetime(2018, 2, 4)],
                         'num': [59, 1, 32, 96]})
table_part2 = pa.Table.from_pandas(df_part2, schema=partition_schema, preserve_index=False)

with pq.ParquetWriter(
        'partition/example_part2.parquet', partition_schema) as writer:
    writer.write_table(table_part2)

# UUID test file (using fixed-size binary of 16 bytes)
import uuid

# Generate some fixed UUIDs for predictable test results
uuid1 = uuid.UUID('550e8400-e29b-41d4-a716-446655440000')
uuid2 = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')
uuid3 = uuid.UUID('6ba7b811-9dad-11d1-80b4-00c04fd430c8')
uuid4 = uuid.UUID('00000000-0000-0000-0000-000000000000')  # nil UUID
uuid5 = uuid.UUID('ffffffff-ffff-ffff-ffff-ffffffffffff')  # max UUID

# Create fixed-size binary array for UUIDs
uuid_data = [
    uuid1.bytes,
    uuid2.bytes,
    uuid3.bytes,
    uuid4.bytes,
    uuid5.bytes,
]

uuid_array = pa.FixedSizeBinaryArray.from_buffers(
    pa.binary(16),
    len(uuid_data),
    [None, pa.py_buffer(b''.join(uuid_data))]
)

# Create table with UUID and other columns
uuid_table = pa.table({
    'id': pa.array([1, 2, 3, 4, 5], type=pa.int32()),
    'uuid_col': uuid_array,
    'name': pa.array(['first', 'second', 'third', 'nil', 'max'], type=pa.string()),
})

with pq.ParquetWriter('simple/example_uuid.parquet', uuid_table.schema) as writer:
    writer.write_table(uuid_table)
