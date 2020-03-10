CREATE FUNCTION import_parquet(text, text, text, regproc, jsonb, jsonb default NULL)
RETURNS VOID
AS 'MODULE_PATHNAME'
LANGUAGE C;
