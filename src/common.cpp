#include "common.hpp"

extern "C"
{
#include "postgres.h"
#include "fmgr.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/memutils.h"
#include "utils/memdebug.h"
#include "utils/timestamp.h"
}

#if PG_VERSION_NUM < 130000
#define MAXINT8LEN 25
#endif

/*
 * exc_palloc
 *      C++ specific memory allocator that utilizes postgres allocation sets.
 */
void *
exc_palloc(std::size_t size)
{
	/* duplicates MemoryContextAllocZero to avoid increased overhead */
	void	   *ret;
	MemoryContext context = CurrentMemoryContext;

	AssertArg(MemoryContextIsValid(context));

	if (!AllocSizeIsValid(size))
		throw std::bad_alloc();

	context->isReset = false;

	ret = context->methods->alloc(context, size);
	if (unlikely(ret == NULL))
		throw std::bad_alloc();

	VALGRIND_MEMPOOL_ALLOC(context, ret, size);

	return ret;
}

Oid
to_postgres_type(int arrow_type)
{
    switch (arrow_type)
    {
        case arrow::Type::BOOL:
            return BOOLOID;
        case arrow::Type::INT32:
            return INT4OID;
        case arrow::Type::INT64:
            return INT8OID;
        case arrow::Type::FLOAT:
            return FLOAT4OID;
        case arrow::Type::DOUBLE:
            return FLOAT8OID;
        case arrow::Type::STRING:
            return TEXTOID;
        case arrow::Type::BINARY:
            return BYTEAOID;
        case arrow::Type::TIMESTAMP:
            return TIMESTAMPOID;
        case arrow::Type::DATE32:
            return DATEOID;
        default:
            return InvalidOid;
    }
}

/*
 * bytes_to_postgres_type
 *      Convert min/max values from column statistics stored in parquet file as
 *      plain bytes to postgres Datum.
 */
Datum
bytes_to_postgres_type(const char *bytes, arrow::DataType *arrow_type)
{
    switch(arrow_type->id())
    {
        case arrow::Type::BOOL:
            return BoolGetDatum(*(bool *) bytes);
        case arrow::Type::INT32:
            return Int32GetDatum(*(int32 *) bytes);
        case arrow::Type::INT64:
            return Int64GetDatum(*(int64 *) bytes);
        case arrow::Type::FLOAT:
            return Float4GetDatum(*(float *) bytes);
        case arrow::Type::DOUBLE:
            return Float8GetDatum(*(double *) bytes);
        case arrow::Type::STRING:
        case arrow::Type::BINARY:
            return CStringGetTextDatum(bytes);
        case arrow::Type::TIMESTAMP:
            {
                TimestampTz ts;
                auto tstype = (arrow::TimestampType *) arrow_type;

                to_postgres_timestamp(tstype, *(int64 *) bytes, ts);
                return TimestampGetDatum(ts);
            }
            break;
        case arrow::Type::DATE32:
            return DateADTGetDatum(*(int32 *) bytes +
                                   (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE));
        default:
            return PointerGetDatum(NULL);
    }
}

arrow::Type::type
get_arrow_list_elem_type(arrow::DataType *type)
{
    auto children = type->children();

    Assert(children.size() == 1);
    return children[0]->type()->id();
}

void datum_to_jsonb(Datum value, Oid typoid, bool isnull,
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
        {
            /* If key is integer, we must convert it to text, not numeric */
            if (iskey) {
                char    buf[MAXINT8LEN + 1];
                char   *strval;

                switch (typoid)
                {
                    case INT2OID:
                        pg_lltoa((int64) DatumGetInt16(value), buf);
                        break;
                    case INT4OID:
                        pg_lltoa((int64) DatumGetInt32(value), buf);
                        break;
                    case INT8OID:
                        pg_lltoa(DatumGetInt64(value), buf);
                        break;
                    default:
                        Assert(false && "should never happen");
                }
                strval = pstrdup(buf);

                jb.type = jbvString;
                jb.val.string.len = strlen(strval);
                jb.val.string.val = strval;
            }
            else {
                Datum num_datum;

                switch (typoid)
                {
                    case INT2OID:
                    case INT4OID:
                        num_datum = DirectFunctionCall1(int4_numeric, value);
                        break;
                    case INT8OID:
                        num_datum = DirectFunctionCall1(int8_numeric, value);
                        break;
                    default:
                        Assert(false && "should never happen");
                }

                jb.type = jbvNumeric;
                jb.val.numeric = DatumGetNumeric(num_datum);
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
            throw std::runtime_error("not implemented");
    }

    pushJsonbValue(&parseState, iskey ? WJB_KEY : WJB_VALUE, &jb);
}

