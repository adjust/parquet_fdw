# parquet_fdw

Parquet foreign data wrapper for PostgreSQL.

## Installation

`parquet_fdw` requires `libarrow` and `libparquet` installed in your system. Please refer to [libarrow installation page](https://arrow.apache.org/install/) or [building guide](https://github.com/apache/arrow/blob/master/cpp/README.md).
To build `parquet_fdw` run:
```sh
make install
```
or in case when PostgreSQL is installed in a custom location:
```sh
make install PG_CONFIG=/path/to/pg_config
```
After extension was successfully installed run in `psql`:
```sql
create extension parquet_fdw;
```

## Using
To start using `parquet_fdw` you should first create server and user mapping. For example:
```sql
create server parquet_srv foreign data wrapper parquet_fdw;
create user user mapping postgres server parquet_srv options (user 'postgres');
```
Now you should be able to create foreign table from Parquet files. Currently `parquet_fdw` supports the following column [types](https://github.com/apache/arrow/blob/master/cpp/src/arrow/type.h) (to be extended shortly):

| Parquet type | SQL type |
|--------------|----------|
|        INT32 |     INT4 |
|        INT64 |     INT8 |
|       STRING |     TEXT |

Following options are supported:
* **filename** - path to Parquet file to read;
* **sorted** - space separated list of columns that Parquet file is already sorted by; that would help postgres to avoid redundant sorting when running query with `ORDER BY` clause.

Example:
```sql
create foreign table userdata (
    id           int,
    first_name   text,
    last_name    text
)
server parquet_srv
options (
    filename '/mnt/userdata1.parquet',
    sorted 'id'
);
```
