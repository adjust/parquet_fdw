#ifndef PARQUET_FDW_INTERNAL_MAP_H
#define PARQUET_FDW_INTERNAL_MAP_H

#include "postgres.h"

typedef struct
{
    Oid     key_type;
    Oid     val_type;
    uint32  nkeys;      /* number of key-value pairs */
    char    data[];     /* keys array (Datum), followed by values array (Datum)
                           followed by value nulls array (bool) */
} internal_map;

#define INTERNAL_MAP_KEYS(map) \
    ((Datum *) (map)->data)
#define INTERNAL_MAP_VALUES(map) \
    ((Datum *) ((map)->data + sizeof(Datum) * (map)->nkeys))
#define INTERNAL_MAP_NULLS(map) \
    ((bool *) ((map)->data + sizeof(Datum) * (map)->nkeys * 2))

Datum make_internal_map(Oid key_type, Oid val_type, uint32 nkeys);
Datum *internal_map_keys(void);
Datum *internal_map_values(void);
Datum *internal_map_nulls(void);

#endif
