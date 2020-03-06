CREATE FUNCTION import_parquet(text, text, text, regproc, jsonb)
RETURNS VOID
AS 'MODULE_PATHNAME'
LANGUAGE C;
