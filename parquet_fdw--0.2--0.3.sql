CREATE TYPE internal_map;

CREATE FUNCTION export_to_parquet(query text, filename text)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION internal_map_out(internal_map)
RETURNS cstring
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION internal_map_in(cstring)
RETURNS internal_map
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE internal_map (
    INPUT   = internal_map_in,
    OUTPUT  = internal_map_out,
    STORAGE = EXTENDED
);

CREATE FUNCTION internal_map_to_jsonb(internal_map)
RETURNS jsonb
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (internal_map as jsonb)
WITH FUNCTION internal_map_to_jsonb;
