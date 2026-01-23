[![build](https://github.com/adjust/parquet_fdw/actions/workflows/ci.yml/badge.svg)](https://github.com/adjust/parquet_fdw/actions/workflows/ci.yml) ![experimental](https://img.shields.io/badge/status-experimental-orange)

# parquet_fdw

Read-only Apache Parquet foreign data wrapper for PostgreSQL.

## Requirements

- PostgreSQL 12-18 (versions 10-11 are end-of-life and no longer tested)
- C++17 compiler
- Apache Arrow 0.15+ (`libarrow` and `libparquet`)

## Installation

`parquet_fdw` requires `libarrow` and `libparquet` installed in your system (requires version 0.15+, for previous versions use branch [arrow-0.14](https://github.com/adjust/parquet_fdw/tree/arrow-0.14)). Please refer to [libarrow installation page](https://arrow.apache.org/install/) or [building guide](https://github.com/apache/arrow/blob/master/docs/source/developers/cpp/building.rst).
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

## Basic usage

To start using `parquet_fdw` one should first create a server and user mapping. For example:
```sql
create server parquet_srv foreign data wrapper parquet_fdw;
create user mapping for postgres server parquet_srv options (user 'postgres');
```

Now you should be able to create foreign table for Parquet files.
```sql
create foreign table userdata (
    id           int,
    first_name   text,
    last_name    text
)
server parquet_srv
options (
    filename '/mnt/userdata1.parquet'
);
```

You can use [globbing](https://en.wikipedia.org/wiki/Glob_(programming)) to list all the Parquet files, like `options (filename '/mnt/userdata*.parquet')` and it will import all matching files. This can be usefull when you have a Hive directory structure, for instance organized by `year/month/day` and you can consider all Parquet files with `/mnt/userdata/*/*/*.parquet`. You can also use named enumerations using braces like `/mnt/userdata/data_{1,3}.parquet` that will consider only files `data_1.parquet` and `data_3.parquet`.

## Advanced

Currently `parquet_fdw` supports the following column [types](https://github.com/apache/arrow/blob/master/cpp/src/arrow/type.h):

|         Arrow type |                         SQL type |
|-------------------:|---------------------------------:|
|               INT8 |                             INT2 |
|              INT16 |                             INT2 |
|              INT32 |                             INT4 |
|              INT64 |                             INT8 |
|              FLOAT |                           FLOAT4 |
|             DOUBLE |                           FLOAT8 |
|          TIMESTAMP |                        TIMESTAMP |
|             DATE32 |                             DATE |
|             STRING |                             TEXT |
|             BINARY |                            BYTEA |
|               LIST |                            ARRAY |
|                MAP |                            JSONB |
|               UUID |                             UUID |
| FIXED_SIZED_BINARY | UUID when length 16, else BINARY |
|             BINARY |                           BINARY |

Currently `parquet_fdw` doesn't support structs and nested lists.

Foreign table may be created for a single Parquet file and for a set of files. It is also possible to specify a user defined function, which would return a list of file paths. Depending on the number of files and table options `parquet_fdw` may use one of the following execution strategies:

| Strategy                | Description              |
|-------------------------|--------------------------|
| **Single File**         | Basic single file reader
| **Multifile**           | Reader which process Parquet files one by one in sequential manner |
| **Multifile Merge**     | Reader which merges presorted Parquet files so that the produced result is also ordered; used when `sorted` option is specified and the query plan implies ordering (e.g. contains `ORDER BY` clause) |
| **Caching Multifile Merge** | Same as `Multifile Merge`, but keeps the number of simultaneously open files limited; used when the number of specified Parquet files exceeds `max_open_files` |

Following table options are supported:
* **filename** - space separated list of paths to Parquet files to read;
* **sorted** - space separated list of columns that Parquet files are presorted by; that would help postgres to avoid redundant sorting when running query with `ORDER BY` clause or in other cases when having a presorted set is beneficial (Group Aggregate, Merge Join);
* **files_in_order** - specifies that files specified by `filename` or returned by `files_func` are ordered according to `sorted` option and have no intersection rangewise; this allows to use `Gather Merge` node on top of parallel Multifile scan (default `false`);
* **use_mmap** - whether memory map operations will be used instead of file read operations (default `false`);
* **use_threads** - enables Apache Arrow's parallel columns decoding/decompression (default `false`);
* **files_func** - user defined function that is used by parquet_fdw to retrieve the list of parquet files on each query; function must take one `JSONB` argument and return text array of full paths to parquet files;
* **files_func_arg** - argument for the function, specified by **files_func**;
* **max_open_files** - the limit for the number of Parquet files open simultaneously.

GUC variables:
* **parquet_fdw.use_threads** - global switch that allow user to enable or disable threads (default `true`);
* **parquet_fdw.enable_multifile** - enable Multifile reader (default `true`).
* **parquet_fdw.enable_multifile_merge** - enable Multifile Merge reader (default `true`).
* **parquet_fdw.allowed_directories** - comma-separated list of directories from which Parquet files can be read. See [Security](#security) section below.

### Security

By default, `parquet_fdw` allows **only superusers** to create foreign tables that read Parquet files. This is because the extension can read any file accessible to the PostgreSQL server process.

To allow non-superuser access, a superuser must configure the `parquet_fdw.allowed_directories` GUC variable with a comma-separated list of directories:

```sql
-- Allow reading from specific directories (superuser only)
ALTER SYSTEM SET parquet_fdw.allowed_directories = '/data/parquet, /home/analytics/data';
SELECT pg_reload_conf();
```

**Security model:**

| User Type | `allowed_directories` | Access |
|-----------|----------------------|--------|
| Superuser | Empty (default) | All files accessible to PostgreSQL |
| Superuser | Set | All files accessible to PostgreSQL |
| Non-superuser | Empty (default) | **Denied** |
| Non-superuser | Set | Only files within listed directories |

**Important notes:**

1. Paths are validated using `realpath()` to prevent symlink-based bypasses
2. The `allowed_directories` setting uses `PGC_SUSET` context, meaning only superusers can modify it
3. Path traversal attempts (e.g., `../`) are blocked by canonicalizing paths before validation
4. The `files_func` option is also subject to these restrictions - paths returned by user-defined functions are validated

**Example setup for multi-tenant environments:**

```sql
-- Create a dedicated directory for each department
-- (run as superuser)
ALTER SYSTEM SET parquet_fdw.allowed_directories = '/data/sales, /data/marketing';
SELECT pg_reload_conf();

-- Now non-superuser roles can create foreign tables pointing to these directories
```

### Parallel queries

`parquet_fdw` also supports [parallel query execution](https://www.postgresql.org/docs/current/parallel-query.html) (not to confuse with multi-threaded decoding feature of Apache Arrow).

### Import

`parquet_fdw` also supports [`IMPORT FOREIGN SCHEMA`](https://www.postgresql.org/docs/current/sql-importforeignschema.html) command to discover parquet files in the specified directory on filesystem and create foreign tables according to those files. It can be used as follows:

```sql
import foreign schema "/path/to/directory"
from server parquet_srv
into public;
```

It is important that `remote_schema` here is a path to a local filesystem directory and is double quoted.

#### Finding Parquet files from `tables_map` option

Foreign schema import also support specifying a mapping between tables and filenames with the `tables_map` option. Its value is a space-separated list of `key=value` values with table names as keys and Parquet files paths as values. Like for `filename`, file globbing can be used, and like with Linux PATH variable, the colon `:` is used to separate multiple paths within a value.

```sql
import foreign schema "/path/to/directory"
from server parquet_srv
into public options (tables_map 'table1=/path/to/directory/2022/*/*.parquet:/path/to/directory/2023/*/*.parquet table2=/path/to/directory{2024,2025}/*/*.parquet')
;
```

As a security, the extension checks that the paths in the `tables_map` are within the external schema path, preventing accessing files elsewhere on the server. Also, if foreign schema `limit to` or `except` clauses are used specifiying table names, only these names will be considered in the `tables_map`.

Tables created through the `tables_map` options are identical as if they were created with a `create foreign table` command with a `filename` option. All other options from the `import foreign schema` are transmitted to the `create foreign table`.

Each table must have at least one Parquet file when created, to query Parquet field and map them to colomun names. Of course, files globbing is evaluated at query-time, so one can add files to a directory and have them considered in the query results.

#### Finding Parquet files from `import_parquet` function

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

## Packaging

### Debian / Ubuntu

After `sudo make install PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config`

```sh
cd Debian
# Copy the new extension binary
sudo cp ../parquet_fdw.so postgresql-17-parquet-fdw/usr/lib/postgresql/17/lib/
# Copy LLVM bytecode
sudo cp ../src/*.bc postgresql-17-parquet-fdw/usr/lib/postgresql/17/lib/bitcode/parquet_fdw/src/
# Copy extension bytecode index from installation
sudo cp /usr/lib/postgresql/17/lib/bitcode/parquet_fdw.index.bc postgresql-17-parquet-fdw/usr/lib/postgresql/17/lib/bitcode/parquet_fdw.index.bc
# Create package
dpkg-deb --build --root-owner-group postgresql-17-parquet-fdw
```

After installation, dont't forget to restart the PostgreSQL server.

## Miscelaneous

* [Synthetic documentation for developers, from AI](https://deepwiki.com/adjust/parquet_fdw)

