-- directory paths are passed to us in environment variables
\getenv abs_srcdir PG_ABS_SRCDIR

SET datestyle = 'ISO';
SET client_min_messages = WARNING;
SET log_statement TO 'none';
CREATE EXTENSION parquet_fdw;
DROP ROLE IF EXISTS regress_parquet_fdw;
CREATE ROLE regress_parquet_fdw LOGIN SUPERUSER;
CREATE SERVER parquet_srv FOREIGN DATA WRAPPER parquet_fdw;
CREATE USER MAPPING FOR regress_parquet_fdw SERVER parquet_srv;
SET ROLE regress_parquet_fdw;

\set simplepath :abs_srcdir '/data/simple/'

-- import foreign schema
IMPORT FOREIGN SCHEMA :"simplepath"
FROM SERVER parquet_srv
INTO public
OPTIONS (sorted 'one');
\d
SELECT * FROM example2;

-- import_parquet
CREATE FUNCTION list_parquet_files(args jsonb)
RETURNS text[] as
$$
    SELECT array_agg(args->>'dir' || filename)
    FROM (VALUES
        ('/example1.parquet', 'simple'),
        ('/example2.parquet', 'simple'),
        ('/example3.parquet', 'complex')
    ) AS files(filename, filetype)
    WHERE filetype = args->>'type';
$$
LANGUAGE SQL;

\set func_arg '{"dir": "' :simplepath '", "type": "simple"}'

SELECT import_parquet(
    'example_import',
    'public',
    'parquet_srv',
    'list_parquet_files',
    :'func_arg',
    '{"sorted": "one"}');
SELECT * FROM example_import ORDER BY one, three;

SELECT import_parquet_explicit(
    'example_import2',
    'public',
    'parquet_srv',
    array['one', 'three', 'six'],
    array['int8', 'text', 'bool']::regtype[],
    'list_parquet_files',
    :'func_arg',
    '{"sorted": "one"}');
SELECT * FROM example_import2;

\set complexpath :abs_srcdir '/data/complex/'
\set complex_func_arg '{"dir": "' :complexpath '", "type": "complex"}'

SELECT import_parquet(
    'example_import3',
    'public',
    'parquet_srv',
    'list_parquet_files',
    :'complex_func_arg');
SELECT * FROM example_import3;

DROP OWNED by regress_parquet_fdw;
DROP EXTENSION parquet_fdw CASCADE;
