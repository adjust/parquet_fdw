#include "internal_map.h"

#include "fmgr.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"


void datum_to_jsonb(Datum value, Oid typoid, bool isnull, FmgrInfo *outfunc,
                    JsonbParseState *result, bool iskey);

Datum
make_internal_map(Oid key_type, Oid val_type, uint32 nkeys)
{
    void           *res;
    internal_map   *map;
    Size            map_size;

    map_size = VARHDRSZ + sizeof(internal_map) + sizeof(Datum) * nkeys * 2 \
               + sizeof(bool) * nkeys;

    res = palloc0(map_size);
    SET_VARSIZE(res, map_size);
    map = (internal_map *) VARDATA(res);
    map->nkeys = nkeys;
    map->key_type = key_type;
    map->val_type = val_type;

    PG_RETURN_POINTER(res);
}

PG_FUNCTION_INFO_V1(internal_map_in);
Datum
internal_map_in(PG_FUNCTION_ARGS)
{
    elog(ERROR, "construction of internal_map is not supported");
}

PG_FUNCTION_INFO_V1(internal_map_out);
Datum
internal_map_out(PG_FUNCTION_ARGS)
{
    void         *in = PG_GETARG_POINTER(0);
    internal_map *map;
    char         *str;

    map = (internal_map *) VARDATA(in);
    str = psprintf("internal_map (%i)", map->nkeys);

    PG_RETURN_CSTRING(str);
}

/* TODO: use the same version of this function in ParquetReader */
static FmgrInfo *find_outfunc(Oid typoid)
{
    MemoryContext oldctx;
    Oid         funcoid;
    bool        isvarlena;
    FmgrInfo   *outfunc;

    getTypeOutputInfo(typoid, &funcoid, &isvarlena);

    if (!OidIsValid(funcoid))
        elog(ERROR, "output function for '%s' not found", format_type_be(typoid));

    oldctx = MemoryContextSwitchTo(CurTransactionContext);
    outfunc = (FmgrInfo *) palloc0(sizeof(FmgrInfo));
    fmgr_info(funcoid, outfunc);
    MemoryContextSwitchTo(oldctx);

    return outfunc;
}

PG_FUNCTION_INFO_V1(internal_map_to_jsonb);
Datum
internal_map_to_jsonb(PG_FUNCTION_ARGS)
{
    void            *in = PG_GETARG_POINTER(0);
    internal_map    *map;
	JsonbParseState *parseState = NULL;
    JsonbValue *jb;
    Datum   *keys;
    Datum   *values;
    bool    *nulls;

    map    = (internal_map *) VARDATA(in);
    keys   = INTERNAL_MAP_KEYS(map);
    values = INTERNAL_MAP_VALUES(map);
    nulls  = INTERNAL_MAP_NULLS(map);

    FmgrInfo *keyout = find_outfunc(map->key_type); 
    FmgrInfo *valout = find_outfunc(map->val_type); 

    jb = pushJsonbValue(&parseState, WJB_BEGIN_OBJECT, NULL);

    for (uint i = 0; i < map->nkeys; ++i)
    {
        datum_to_jsonb(keys[i], map->key_type, false, keyout,
                       parseState, true);
        datum_to_jsonb(values[i], map->val_type, nulls[i], valout,
                       parseState, false);
    }
    jb = pushJsonbValue(&parseState, WJB_END_OBJECT, NULL);
    //JsonbPGetDatum(JsonbValueToJsonb(jb));

    PG_RETURN_JSONB_P(JsonbValueToJsonb(jb));
}

void datum_to_jsonb(Datum value, Oid typoid, bool isnull, FmgrInfo *outfunc,
                    JsonbParseState *parseState, bool iskey)
{
    JsonbValue  jb;

	if (isnull)
	{
		Assert(!iskey);
		jb.type = jbvNull;
        pushJsonbValue(&parseState, WJB_VALUE, &jb);
        return;
	}
    switch (typoid)
    {
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case FLOAT4OID:
        case FLOAT8OID:
        {
            /* If key is integer, we must convert it to text, not numeric */
            if (iskey) {
                char    *strval;

                strval = DatumGetCString(FunctionCall1(outfunc, value));

                jb.type = jbvString;
                jb.val.string.len = strlen(strval);
                jb.val.string.val = strval;
            }
            else {
                Datum numeric;

                switch (typoid)
                {
                    case INT2OID:
                    case INT4OID:
                        numeric = DirectFunctionCall1(int4_numeric, value);
                        break;
                    case INT8OID:
                        numeric = DirectFunctionCall1(int8_numeric, value);
                        break;
                    case FLOAT4OID:
                        numeric = DirectFunctionCall1(float4_numeric, value);
                        break;
                    case FLOAT8OID:
                        numeric = DirectFunctionCall1(float8_numeric, value);
                        break;
                    default:
                        Assert(false && "should never happen");
                }

                jb.type = jbvNumeric;
                jb.val.numeric = DatumGetNumeric(numeric);
            }
            break;
        }
        case TEXTOID:
        {
            char *str = TextDatumGetCString(value);

            jb.type = jbvString;
            jb.val.string.len = strlen(str);
            jb.val.string.val = str;
            break;
        }
        default:
        {
            char    *strval;

            strval = DatumGetCString(FunctionCall1(outfunc, value));

            jb.type = jbvString;
            jb.val.string.len = strlen(strval);
            jb.val.string.val = strval;
        }
    }

    pushJsonbValue(&parseState, iskey ? WJB_KEY : WJB_VALUE, &jb);
}
