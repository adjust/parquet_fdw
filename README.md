Apache Parquet Foreign Data Wrapper for PostgreSQL
==================================================

This is a foreign data wrapper (FDW) to connect [PostgreSQL](https://www.postgresql.org/)
to [Apache Parquet](https://parquet.apache.org/) in **readonly** mode.

[![build](https://github.com/adjust/parquet_fdw/actions/workflows/ci.yml/badge.svg)](https://github.com/adjust/parquet_fdw/actions/workflows/ci.yml) ![experimental](https://img.shields.io/badge/status-experimental-orange)

<img src="https://upload.wikimedia.org/wikipedia/commons/2/29/Postgresql_elephant.svg" align="center" height="100" alt="PostgreSQL"/>	+	<img src="https://img.stackshare.io/service/4264/OGYr_m6J_400x400.jpeg" align="center" height="100" alt="Parquet"/>

Contents
--------

1. [Features](#features)
2. [Supported platforms](#supported-platforms)
3. [Installation](#installation)
4. [Usage](#usage)
5. [Functions](#functions)
6. [Identifier case handling](#identifier-case-handling)
7. [Generated columns](#generated-columns)
8. [Character set handling](#character-set-handling)
9. [Examples](#examples)
10. [Limitations](#limitations)
11. [Contributing](#contributing)
12. [Useful links](#useful-links)
13. [License](#license)

Features
--------
## Common features

Currently `parquet_fdw` supports the following column [types](https://github.com/apache/arrow/blob/master/cpp/src/arrow/type.h):

|   Arrow type |  SQL type |
|-------------:|----------:|
|         INT8 |      INT2 |
|        INT16 |      INT2 |
|        INT32 |      INT4 |
|        INT64 |      INT8 |
|        FLOAT |    FLOAT4 |
|       DOUBLE |    FLOAT8 |
|    TIMESTAMP | TIMESTAMP |
|       DATE32 |      DATE |
|       STRING |      TEXT |
|       BINARY |     BYTEA |
|         LIST |     ARRAY |
|          MAP |     JSONB |

- Parallel queries. `parquet_fdw` also supports [parallel query execution](https://www.postgresql.org/docs/current/parallel-query.html) (not to confuse with multi-threaded decoding feature of Apache Arrow).

- GUC variables:
  - **parquet_fdw.use_threads** - global switch that allow user to enable or disable threads (default `true`);
  - **parquet_fdw.enable_multifile** - enable Multifile reader (default `true`).
  - **parquet_fdw.enable_multifile_merge** - enable Multifile Merge reader (default `true`).

- Currently `parquet_fdw` doesn't support structs and nested lists.

## Pushdowning

**yet not described**

Supported platforms
-------------------

`parquet_fdw` was developed on Linux, and should run on any
reasonably POSIX-compliant system.

`parquet_fdw` is designed to be compatible with PostgreSQL 10 ~ 15.

Installation
------------
### Prerequisites

`parquet_fdw` requires `libarrow` and `libparquet` installed in your system (requires version 0.15+, for previous versions use branch [arrow-0.14](https://github.com/adjust/parquet_fdw/tree/arrow-0.14)). Please refer to [libarrow installation page](https://arrow.apache.org/install/) or [building guide](https://github.com/apache/arrow/blob/master/docs/source/developers/cpp/building.rst).

### Source installation

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

Usage
-----

## CREATE SERVER options

`parquet_fdw` accepts no options via the `CREATE SERVER` command.

## CREATE USER MAPPING options

`parquet_fdw` accepts the following options via the `CREATE USER MAPPING`
command:

- **user** as *string*, no default

  Username to use when connecting to Apache Parquet.

## CREATE FOREIGN TABLE options

Foreign table may be created for a single Parquet file and for a set of files. It is also possible to specify a user defined function, which would return a list of file paths. Depending on the number of files and table options `parquet_fdw` may use one of the following execution strategies:

| Strategy                | Description              |
|-------------------------|--------------------------|
| **Single File**         | Basic single file reader
| **Multifile**           | Reader which process Parquet files one by one in sequential manner |
| **Multifile Merge**     | Reader which merges presorted Parquet files so that the produced result is also ordered; used when `sorted` option is specified and the query plan implies ordering (e.g. contains `ORDER BY` clause) |
| **Caching Multifile Merge** | Same as `Multifile Merge`, but keeps the number of simultaneously open files limited; used when the number of specified Parquet files exceeds `max_open_files` |

`parquet_fdw` accepts the following table-level options via the
`CREATE FOREIGN TABLE` command.

- **filename** as *string*, optional, no default

  Space separated list of paths to Parquet files to read.
  
- **sorted** as *string*, optional, no default

  Space separated list of columns that Parquet files are presorted by; that would help postgres to avoid redundant sorting when running query with `ORDER BY` clause or in other cases when having a presorted set is beneficial (Group Aggregate, Merge Join).
  
- **files_in_order** as *boolean*, optional, default `false`

  Specifies that files specified by `filename` or returned by `files_func` are ordered according to `sorted` option and have no intersection rangewise; this allows to use `Gather Merge` node on top of parallel Multifile scan.
  
- **use_mmap** as *boolean*, optional, default `false`

  Whether memory map operations will be used instead of file read operations.
  
- **use_threads** as *boolean*, optional, default `false`

  Enables Apache Arrow's parallel columns decoding/decompression.
  
- **files_func** as *string*, optional, no default

  User defined function that is used by `parquet_fdw` to retrieve the list of parquet files on each query; function must take one `JSONB` argument and return text array of full paths to parquet files.
  
- **files_func_arg** as *string*, optional, no default

  Argument for the function, specified by **files_func**.
  
- **max_open_files** as *integer*, optional

  The limit for the number of Parquet files open simultaneously.
  
## IMPORT FOREIGN SCHEMA options

`parquet_fdw` supports [IMPORT FOREIGN SCHEMA](https://www.postgresql.org/docs/current/sql-importforeignschema.html) and 
 accepts no custom options. `IMPORT FOREIGN SCHEMA` command discover parquet files in the specified directory on filesystem and create foreign tables according to those files.
 
Example:

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

## TRUNCATE support

`parquet_fdw` as **readonly** don't implements the foreign data wrapper `TRUNCATE` API, available
from PostgreSQL 14. 

Functions
---------

As well as the standard `parquet_fdw_handler()` and `parquet_fdw_validator()`
functions, `parquet_fdw` provides the following user-callable utility functions:

Functions from this FDW in PostgreSQL catalog are **yet not described**.

Identifier case handling
------------------------

PostgreSQL folds identifiers to lower case by default.
All rules and problems with Apache Parquet identifiers **yet not tested and described**.

Generated columns
-----------------

Behavoiur within generated columns **yet not tested**. Generated columns isn't conceptual
aplicable to **readonly** data sources.

For more details on generated columns see:

- [Generated Columns](https://www.postgresql.org/docs/current/ddl-generated-columns.html)
- [CREATE FOREIGN TABLE](https://www.postgresql.org/docs/current/sql-createforeigntable.html)

Character set handling
----------------------

Encodings mapping between PostgeeSQL and Apache Parquet **yet not described**.

Examples
--------

After extension was successfully installed run in `psql` as superuser for a database:
```sql
create extension parquet_fdw;
```

To start using `parquet_fdw` one should first create a server and user mapping as superuser. For example:
```sql
create server parquet_srv foreign data wrapper parquet_fdw;
create user mapping for postgres server parquet_srv options (user 'postgres');

-- make a {non_privileged_user} (insert a username you need) able to work with Apache Parquet
grant usage on foreign server to {non_privileged_user};
create user mapping for {non_privileged_user} server parquet_srv options (user '.....');
```

Now you should be able to create foreign table for Parquet files as {non_privileged_user}.
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

Limitations
-----------

* Currently `parquet_fdw` doesn't support structs and nested lists.
   
Contributing
------------

Pull requests is welcome.

Useful links
------------

### Source code

 - https://github.com/pgspider/parquet_fdw
 
 Reference FDW realisation, `postgres_fdw`
 - https://git.postgresql.org/gitweb/?p=postgresql.git;a=tree;f=contrib/postgres_fdw;hb=HEAD

### General FDW Documentation

 - https://www.postgresql.org/docs/current/ddl-foreign-data.html
 - https://www.postgresql.org/docs/current/sql-createforeigndatawrapper.html
 - https://www.postgresql.org/docs/current/sql-createforeigntable.html
 - https://www.postgresql.org/docs/current/sql-importforeignschema.html
 - https://www.postgresql.org/docs/current/fdwhandler.html
 - https://www.postgresql.org/docs/current/postgres-fdw.html

### Other FDWs

 - https://wiki.postgresql.org/wiki/Fdw
 - https://pgxn.org/tag/fdw/
 
License
-------

See the [`LICENSE`](LICENSE) file for full details.
