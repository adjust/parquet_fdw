#ifndef PARQUET_COMMON_H
#define PARQUET_COMMON_H

typedef enum
{
    CT_INT32,
    CT_INT64,
    CT_BOOLEAN
} ColumnType;

typedef void* ParquetHandler;

/* Parquet routines */
ParquetHandler create_parquet_state(const char *filename);
void release_parquet_state(ParquetHandler handler);
TupleTableSlot *parquetIterateForeignScan(ForeignScanState *node);

#endif /* PARQUET_COMMON_H */
