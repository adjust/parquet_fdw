[![Build Status](https://travis-ci.org/adjust/parquet_fdw.svg?branch=master)](https://travis-ci.org/adjust/parquet_fdw) ![experimental](https://img.shields.io/badge/status-experimental-orange)

# parquet_fdw

Parquet foreign data wrapper for PostgreSQL.

## Installation

`parquet_fdw` requires `libarrow` and `libparquet` installed in your system (requires version 0.15, for previous versions use branch [arrow-0.14](https://github.com/adjust/parquet_fdw/tree/arrow-0.14)). Please refer to [libarrow installation page](https://arrow.apache.org/install/) or [building guide](https://github.com/apache/arrow/blob/master/cpp/README.md).
To build `parquet_fdw` run:
```sh
make install
```
or in case when PostgreSQL is installed in a custom location:
```sh
make install PG_CONFIG=/path/to/pg_config
```
It is possible to pass additional compilation flags through either custom
`CCFLAGS` or standard `PG_CFLAGS`, `PG_CXXFLAGS`, `PG_CPPFLAGS` variables.

After extension was successfully installed run in `psql`:
```sql
create extension parquet_fdw;
```

## Using
To start using `parquet_fdw` you should first create server and user mapping. For example:
```sql
create server parquet_srv foreign data wrapper parquet_fdw;
create user mapping for postgres server parquet_srv options (user 'postgres');
```
Now you should be able to create foreign table from Parquet files. Currently `parquet_fdw` supports the following column [types](https://github.com/apache/arrow/blob/master/cpp/src/arrow/type.h) (to be extended shortly):

| Parquet type |  SQL type |
|--------------|-----------|
|        INT32 |      INT4 |
|        INT64 |      INT8 |
|        FLOAT |    FLOAT4 |
|       DOUBLE |    FLOAT8 |
|    TIMESTAMP | TIMESTAMP |
|       TIME64 |      TIME |
|       DATE32 |      DATE |
|       STRING |      TEXT |
|       BINARY |     BYTEA |
|         LIST |     ARRAY |

Currently `parquet_fdw` doesn't support structs and nested lists.

Following options are supported:
* **filename** - space separated list of paths to Parquet files to read;
* **sorted** - space separated list of columns that Parquet files are presorted by; that would help postgres to avoid redundant sorting when running query with `ORDER BY` clause or in other cases when having a presorted set is beneficial (Group Aggregate, Merge Join);
* **use_mmap** - whether memory map operations will be used instead of file read operations (default `false`);
* **use_threads** - enables `arrow`'s parallel columns decoding/decompression (default `false`).

GUC variables:
* **parquet_fdw.use_threads** - global switch that allow user to enable or disable threads (default `true`).

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

### Parallel queries
`parquet_fdw` also supports [parallel query execution](https://www.postgresql.org/docs/current/parallel-query.html) (not to confuse with multi-threaded decoding feature of `arrow`). It is disabled by default; to enable it run `ANALYZE` command on the table. The reason behind this is that without statistics postgres may end up choosing a terrible parallel plan for certain queries which would be much worse than a serial one (e.g. grouping by a column with large number of distinct values).

### Import

`parquet_fdw` also supports [`IMPORT FOREIGN SCHEMA`](https://www.postgresql.org/docs/current/sql-importforeignschema.html) command to discover parquet files in the specified directory on filesystem and create foreign tables according to those files. It can be used as follows:

```sql
import foreign schema "/path/to/directory"
from server parquet_srv
into public;
```

It is important that `remote_schema` here is a path to a local filesystem directory and is double quoted.

Another way to import parquet files into foreign tables is to use `import_parquet` or `import_parquet_explicit`:

```sql
create function import_parquet(
    tablename   text,
    schemaname  text,
    servername  text,
    userfunc    regproc,
    args        jsonb,
    options     jsonb)

create function import_parquet_explicit(
    tablename   text,
    schemaname  text,
    servername  text,
    attnames    text[],
    atttypes    regtype[],
    userfunc    regproc,
    args        jsonb,
    options     jsonb)
```

The only difference between `import_parquet` and `import_parquet_explicit` is that the latter allows to specify a set of attributes (columns) to import. `attnames` and `atttypes` here are the attributes names and attributes types arrays respectively (see the example below).

`userfunc` is a user-defined function. It must take a `jsonb` argument and return a text array of filesystem paths to parquet files to be imported. `args` is user-specified jsonb object that is passed to `userfunc` as its argument. A simple implementation of such function and its usage may look like this:

```sql
create function list_parquet_files(args jsonb)
returns text[] as
$$
begin
    return array_agg(args->>'dir' || '/' || filename)
           from pg_ls_dir(args->>'dir') as files(filename)
           where filename ~~ '%.parquet';
end
$$
language plpgsql;

select import_parquet_explicit(
    'abc',
    'public',
    'parquet_srv',
    array['one', 'three', 'six'],
    array['int8', 'text', 'bool']::regtype[],
    'list_parquet_files',
    '{"dir": "/path/to/directory"}',
    '{"sorted": "one"}'
);
```

