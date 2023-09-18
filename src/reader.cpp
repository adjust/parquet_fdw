#include <list>

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/array.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/exception.h"
#include "parquet/file_reader.h"
#include "parquet/statistics.h"

#include "common.hpp"
#include "reader.hpp"

extern "C"
{
#include "postgres.h"
#include "access/sysattr.h"
#include "parser/parse_coerce.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#if PG_VERSION_NUM < 110000
#include "catalog/pg_type.h"
#else
#include "catalog/pg_type_d.h"
#endif
}

#define SEGMENT_SIZE (1024 * 1024)


bool parquet_fdw_use_threads = true;


class FastAllocator
{
private:
    /*
     * Special memory segment to speed up bytea/Text allocations.
     */
    MemoryContext       segments_cxt;
    char               *segment_start_ptr;
    char               *segment_cur_ptr;
    char               *segment_last_ptr;
    std::list<char *>   garbage_segments;

public:
    FastAllocator(MemoryContext cxt)
        : segments_cxt(cxt), segment_start_ptr(nullptr), segment_cur_ptr(nullptr),
          segment_last_ptr(nullptr), garbage_segments()
    {}

    ~FastAllocator()
    {
        this->recycle();
    }

    /*
     * fast_alloc
     *      Preallocate a big memory segment and distribute blocks from it. When
     *      segment is exhausted it is added to garbage_segments list and freed
     *      on the next executor's iteration. If requested size is bigger that
     *      SEGMENT_SIZE then just palloc is used.
     */
    inline void *fast_alloc(long size)
    {
        void   *ret;

        Assert(size >= 0);

        /* If allocation is bigger than segment then just palloc */
        if (size > SEGMENT_SIZE)
        {
            MemoryContext oldcxt = MemoryContextSwitchTo(this->segments_cxt);
            void *block = exc_palloc(size);
            this->garbage_segments.push_back((char *) block);
            MemoryContextSwitchTo(oldcxt);

            return block;
        }

        size = MAXALIGN(size);

        /* If there is not enough space in current segment create a new one */
        if (this->segment_last_ptr - this->segment_cur_ptr < size)
        {
            MemoryContext oldcxt;

            /*
             * Recycle the last segment at the next iteration (if there
             * was one)
             */
            if (this->segment_start_ptr)
                this->garbage_segments.
                    push_back(this->segment_start_ptr);

            oldcxt = MemoryContextSwitchTo(this->segments_cxt);
            this->segment_start_ptr = (char *) exc_palloc(SEGMENT_SIZE);
            this->segment_cur_ptr = this->segment_start_ptr;
            this->segment_last_ptr =
                this->segment_start_ptr + SEGMENT_SIZE - 1;
            MemoryContextSwitchTo(oldcxt);
        }

        ret = (void *) this->segment_cur_ptr;
        this->segment_cur_ptr += size;

        return ret;
    }

    void recycle(void)
    {
        /* recycle old segments if any */
        if (!this->garbage_segments.empty())
        {
            bool    error = false;

            PG_TRY();
            {
                for (auto it : this->garbage_segments)
                    pfree(it);
            }
            PG_CATCH();
            {
                error = true;
            }
            PG_END_TRY();
            if (error)
                throw std::runtime_error("garbage segments recycle failed");

            this->garbage_segments.clear();
            elog(DEBUG1, "parquet_fdw: garbage segments recycled");
        }
    }

    MemoryContext context()
    {
        return segments_cxt;
    }
};


ParquetReader::ParquetReader(MemoryContext cxt)
    : allocator(new FastAllocator(cxt))
{}

int32_t ParquetReader::id()
{
    return reader_id;
}

/*
 * create_column_mapping
 *      Create mapping between tuple descriptor and parquet columns.
 */
void ParquetReader::create_column_mapping(TupleDesc tupleDesc, const std::set<int> &attrs_used)
{
    parquet::ArrowReaderProperties  props;
    std::shared_ptr<arrow::Schema>  a_schema;
    parquet::arrow::SchemaManifest  manifest;
    auto    p_schema = this->reader->parquet_reader()->metadata()->schema();

    if (!parquet::arrow::SchemaManifest::Make(p_schema, nullptr, props, &manifest).ok())
        throw Error("error creating arrow schema ('%s')", this->filename.c_str());

    this->map.resize(tupleDesc->natts);
    for (int i = 0; i < tupleDesc->natts; i++)
    {
        AttrNumber  attnum = i + 1 - FirstLowInvalidHeapAttributeNumber;
        char        pg_colname[255];
        const char *attname = NameStr(TupleDescAttr(tupleDesc, i)->attname);

        this->map[i] = -1;

        /* Skip columns we don't intend to use in query */
        if (attrs_used.find(attnum) == attrs_used.end())
            continue;

        tolowercase(NameStr(TupleDescAttr(tupleDesc, i)->attname), pg_colname);

        for (auto &schema_field : manifest.schema_fields)
        {
            auto field_name = schema_field.field->name();
            auto arrow_type = schema_field.field->type();
            char arrow_colname[255];

            if (field_name.length() > NAMEDATALEN)
                throw Error("parquet column name '%s' is too long (max: %d, file: '%s')",
                            field_name.c_str(), NAMEDATALEN - 1, this->filename.c_str());
            tolowercase(schema_field.field->name().c_str(), arrow_colname);

            /*
             * Compare postgres attribute name to the column name in arrow
             * schema.
             */
            if (strcmp(pg_colname, arrow_colname) == 0)
            {
                TypeInfo        typinfo(arrow_type);
                bool            error(false);

                /* Found mapping! */

                this->column_names.push_back(std::move(arrow_colname));

                /* index of last element */
                this->map[i] = this->column_names.size() - 1;

                typinfo.pg.oid = TupleDescAttr(tupleDesc, i)->atttypid;
                switch (arrow_type->id())
                {
                    case arrow::Type::LIST:
                    {
                        Assert(schema_field.children.size() == 1);

                        Oid     elem_type;
                        int16   elem_len;
                        bool    elem_byval;
                        char    elem_align;

                        PG_TRY();
                        {
                            elem_type = get_element_type(typinfo.pg.oid);
                            if (OidIsValid(elem_type)) {
                                get_typlenbyvalalign(elem_type, &elem_len,
                                                     &elem_byval, &elem_align);
                            }
                        }
                        PG_CATCH();
                        {
                            error = true;
                        }
                        PG_END_TRY();
                        if (error)
                            throw Error("failed to get type length (column '%s')",
                                        pg_colname);

                        if (!OidIsValid(elem_type))
                            throw Error("cannot convert parquet column of type "
                                        "LIST to scalar type of postgres column '%s'",
                                        pg_colname);

                        auto     &child = schema_field.children[0];
                        typinfo.children.emplace_back(child.field->type(),
                                                      elem_type);
                        TypeInfo &elem = typinfo.children[0];
                        elem.pg.len = elem_len;
                        elem.pg.byval = elem_byval;
                        elem.pg.align = elem_align;
                        initialize_cast(elem, attname);

                        this->indices.push_back(child.column_index);
                        break;
                    }
                    case arrow::Type::MAP:
                    {
                        /* 
                         * Map has the following structure:
                         *
                         * Type::MAP
                         * └─ Type::STRUCT
                         *    ├─  key type
                         *    └─  item type
                         */

                        Assert(schema_field.children.size() == 1);
                        auto &strct = schema_field.children[0];

                        Assert(strct.children.size() == 2);
                        auto &key = strct.children[0];
                        auto &item = strct.children[1];
                        Oid pg_key_type = to_postgres_type(key.field->type()->id());
                        Oid pg_item_type = to_postgres_type(item.field->type()->id());

                        typinfo.children.emplace_back(key.field->type(),
                                                      pg_key_type);
                        typinfo.children.emplace_back(item.field->type(),
                                                      pg_item_type);

                        PG_TRY();
                        {
                            typinfo.children[0].outfunc = find_outfunc(pg_key_type);
                            typinfo.children[1].outfunc = find_outfunc(pg_item_type);
                        }
                        PG_CATCH();
                        {
                            error = true;
                        }
                        PG_END_TRY();
                        if (error)
                            throw Error("failed to initialize output function for "
                                        "Map column '%s'", attname);

                        this->indices.push_back(key.column_index);
                        this->indices.push_back(item.column_index);

                        /* JSONB might need cast (e.g. to TEXT) */
                        initialize_cast(typinfo, attname);
                        break;
                    }
                    default:
                        initialize_cast(typinfo, attname);
                        typinfo.index = schema_field.column_index;
                        this->indices.push_back(schema_field.column_index);
                }
                this->types.push_back(std::move(typinfo));

                break;
            }
        }
    }
}

Datum ParquetReader::do_cast(Datum val, const TypeInfo &typinfo)
{
    MemoryContext   ccxt = CurrentMemoryContext;
    bool            error = false;
    char            errstr[ERROR_STR_LEN];

    /* du, du cast, du cast mich... */
    PG_TRY();
    {
        if (typinfo.castfunc != NULL)
        {
            val = FunctionCall1(typinfo.castfunc, val);
        }
        else if (typinfo.outfunc && typinfo.infunc)
        {
            char *str;

            str = OutputFunctionCall(typinfo.outfunc, val);

            /* TODO: specify typioparam and typmod */
            val = InputFunctionCall(typinfo.infunc, str, 0, 0);
        }
    }
    PG_CATCH();
    {
        ErrorData *errdata;

        MemoryContextSwitchTo(ccxt);
        error = true;
        errdata = CopyErrorData();
        FlushErrorState();

        strncpy(errstr, errdata->message, ERROR_STR_LEN - 1);
        FreeErrorData(errdata);
    }
    PG_END_TRY();
    if (error)
        throw std::runtime_error(errstr);

    return val;
}

/*
 * read_primitive_type
 *      Returns primitive type value from arrow array
 */
Datum ParquetReader::read_primitive_type(arrow::Array *array,
                                         const TypeInfo &typinfo,
                                         int64_t i)
{
    Datum   res;

    /* Get datum depending on the column type */
    switch (typinfo.arrow.type_id)
    {
        case arrow::Type::BOOL:
        {
            arrow::BooleanArray *boolarray = (arrow::BooleanArray *) array;

            res = BoolGetDatum(boolarray->Value(i));
            break;
        }
        case arrow::Type::INT8:
        {
            arrow::Int8Array *intarray = (arrow::Int8Array *) array;
            int value = intarray->Value(i);

            res = Int8GetDatum(value);
            break;
        }
        case arrow::Type::INT16:
        {
            arrow::Int16Array *intarray = (arrow::Int16Array *) array;
            int value = intarray->Value(i);

            res = Int16GetDatum(value);
            break;
        }
        case arrow::Type::INT32:
        {
            arrow::Int32Array *intarray = (arrow::Int32Array *) array;
            int value = intarray->Value(i);

            res = Int32GetDatum(value);
            break;
        }
        case arrow::Type::INT64:
        {
            arrow::Int64Array *intarray = (arrow::Int64Array *) array;
            int64 value = intarray->Value(i);

            res = Int64GetDatum(value);
            break;
        }
        case arrow::Type::FLOAT:
        {
            arrow::FloatArray *farray = (arrow::FloatArray *) array;
            float value = farray->Value(i);

            res = Float4GetDatum(value);
            break;
        }
        case arrow::Type::DOUBLE:
        {
            arrow::DoubleArray *darray = (arrow::DoubleArray *) array;
            double value = darray->Value(i);

            res = Float8GetDatum(value);
            break;
        }
        case arrow::Type::STRING:
        case arrow::Type::BINARY:
        {
            arrow::BinaryArray *binarray = (arrow::BinaryArray *) array;

            int32_t vallen = 0;
            const char *value = reinterpret_cast<const char*>(binarray->GetValue(i, &vallen));

            /* Build bytea */
            int64 bytea_len = vallen + VARHDRSZ;
            bytea *b = (bytea *) this->allocator->fast_alloc(bytea_len);
            SET_VARSIZE(b, bytea_len);
            memcpy(VARDATA(b), value, vallen);

            res = PointerGetDatum(b);
            break;
        }
        case arrow::Type::TIMESTAMP:
        {
            /* TODO: deal with timezones */
            TimestampTz ts;
            arrow::TimestampArray *tsarray = (arrow::TimestampArray *) array;
            auto tstype = (arrow::TimestampType *) array->type().get();

            to_postgres_timestamp(tstype, tsarray->Value(i), ts);
            res = TimestampGetDatum(ts);
            break;
        }
        case arrow::Type::DATE32:
        {
            arrow::Date32Array *tsarray = (arrow::Date32Array *) array;
            int32 d = tsarray->Value(i);

            /*
             * Postgres date starts with 2000-01-01 while unix date (which
             * Parquet is using) starts with 1970-01-01. So we need to do
             * simple calculations here.
             */
            res = DateADTGetDatum(d + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE));
            break;
        }
        /* TODO: add other types */
        default:
            throw Error("parquet_fdw: unsupported column type: %s",
                        typinfo.arrow.type_name.c_str());
    }

    /* Call cast function if needed */
    if (typinfo.need_cast)
        res = do_cast(res, typinfo);

    return res;
}

/*
 * nested_list_to_datum
 *      Returns postgres array build from elements of array. Only one
 *      dimensional arrays are supported.
 */
Datum ParquetReader::nested_list_to_datum(arrow::ListArray *larray, int pos,
                                           const TypeInfo &typinfo)
{
    MemoryContext oldcxt;
    ArrayType  *res;
    Datum      *values;
    bool       *nulls = NULL;
    int         dims[1];
    int         lbs[1];
    bool        error = false;

    std::shared_ptr<arrow::Array> array =
        larray->values()->Slice(larray->value_offset(pos),
                                larray->value_length(pos));

    const TypeInfo &elemtypinfo = typinfo.children[0];

    values = (Datum *) this->allocator->fast_alloc(sizeof(Datum) * array->length());

#if SIZEOF_DATUM == 8
    /* Fill values and nulls arrays */
    if (array->null_count() == 0 && typinfo.arrow.type_id == arrow::Type::INT64)
    {
        /*
         * Ok, there are no nulls, so probably we could just memcpy the
         * entire array.
         *
         * Warning: the code below is based on the assumption that Datum is
         * 8 bytes long, which is true for most contemporary systems but this
         * will not work on some exotic or really old systems.
         */
        copy_to_c_array<int64_t>((int64_t *) values, array.get(), elemtypinfo.pg.len);
        goto construct_array;
    }
#endif
    for (int64_t i = 0; i < array->length(); ++i)
    {
        if (!array->IsNull(i))
            values[i] = this->read_primitive_type(array.get(), elemtypinfo, i);
        else
        {
            if (!nulls)
            {
                Size size = sizeof(bool) * array->length();

                nulls = (bool *) this->allocator->fast_alloc(size);
                memset(nulls, 0, size);
            }
            nulls[i] = true;
        }
    }

construct_array:
    /*
     * Construct one dimensional array. We have to use PG_TRY / PG_CATCH
     * to prevent any kind leaks of resources allocated by c++ in case of
     * errors.
     */
    dims[0] = array->length();
    lbs[0] = 1;
    PG_TRY();
    {
        oldcxt = MemoryContextSwitchTo(allocator->context());
        res = construct_md_array(values, nulls, 1, dims, lbs,
                                 elemtypinfo.pg.oid, elemtypinfo.pg.len,
                                 elemtypinfo.pg.byval, elemtypinfo.pg.align);
        MemoryContextSwitchTo(oldcxt);
    }
    PG_CATCH();
    {
        error = true;
    }
    PG_END_TRY();
    if (error)
        throw std::runtime_error("failed to constuct an array");

    return PointerGetDatum(res);
}

Datum
ParquetReader::map_to_datum(arrow::MapArray *maparray, int pos,
                            const TypeInfo &typinfo)
{
	JsonbParseState *parseState = NULL;
    JsonbValue *jb;
    Datum       res;

    auto keys = maparray->keys()->Slice(maparray->value_offset(pos),
                                        maparray->value_length(pos));
    auto values = maparray->items()->Slice(maparray->value_offset(pos),
                                           maparray->value_length(pos));

    Assert(keys->length() == values->length());
    Assert(typinfo.children.size() == 2);

    jb = pushJsonbValue(&parseState, WJB_BEGIN_OBJECT, NULL);

    for (int i = 0; i < keys->length(); ++i)
    {
        Datum   key = (Datum) 0,
                value = (Datum) 0;
        bool    isnull = false;
        const TypeInfo &key_typinfo = typinfo.children[0];
        const TypeInfo &val_typinfo = typinfo.children[1];

        if (keys->IsNull(i))
            throw std::runtime_error("key is null");
        
        if (!values->IsNull(i))
        {
            key = this->read_primitive_type(keys.get(), key_typinfo, i);
            value = this->read_primitive_type(values.get(), val_typinfo, i);
        } else
            isnull = true;

        /* TODO: adding cstring would be cheaper than adding text */
        datum_to_jsonb(key, key_typinfo.pg.oid, false, key_typinfo.outfunc,
                       parseState, true);
        datum_to_jsonb(value, val_typinfo.pg.oid, isnull, val_typinfo.outfunc,
                       parseState, false);
    }

    jb = pushJsonbValue(&parseState, WJB_END_OBJECT, NULL);
    res = JsonbPGetDatum(JsonbValueToJsonb(jb));

    if (typinfo.need_cast)
        res = do_cast(res, typinfo);

    return res;
}


/*
 * find_castfunc
 *      Check wether implicit cast will be required and prepare cast function
 *      call.
 */
void ParquetReader::initialize_cast(TypeInfo &typinfo, const char *attname)
{
    MemoryContext ccxt = CurrentMemoryContext;
    Oid         src_oid = to_postgres_type(typinfo.arrow.type_id);
    Oid         dst_oid = typinfo.pg.oid;
    bool        error = false;
    char        errstr[ERROR_STR_LEN];

    if (!OidIsValid(src_oid))
    {
        if (typinfo.arrow.type_id == arrow::Type::MAP)
            src_oid = JSONBOID;
        else
            elog(ERROR, "failed to initialize cast function for column '%s'",
                 attname);
    }

    PG_TRY();
    {

        if (IsBinaryCoercible(src_oid, dst_oid))
        {
            typinfo.castfunc = nullptr;
        }
        else
        {
            CoercionPathType ct;
            Oid     funcid;

            ct = find_coercion_pathway(dst_oid, src_oid,
                                       COERCION_EXPLICIT,
                                       &funcid);
            switch (ct)
            {
                case COERCION_PATH_FUNC:
                    {
                        MemoryContext   oldctx;

                        oldctx = MemoryContextSwitchTo(CurTransactionContext);
                        typinfo.castfunc = (FmgrInfo *) palloc0(sizeof(FmgrInfo));
                        fmgr_info(funcid, typinfo.castfunc);
                        typinfo.need_cast = true;
                        MemoryContextSwitchTo(oldctx);

                        break;
                    }

                case COERCION_PATH_RELABELTYPE:
                    /* Cast is not needed */
                    typinfo.castfunc = nullptr;
                    break;

                case COERCION_PATH_COERCEVIAIO:
                    /* Cast via IO */
                    typinfo.outfunc = find_outfunc(src_oid);
                    typinfo.infunc = find_infunc(dst_oid);
                    typinfo.need_cast = true;
                    break;

                default:
                    elog(ERROR, "coercion pathway from '%s' to '%s' not found",
                         format_type_be(src_oid), format_type_be(dst_oid));
            }
        }
    }
    PG_CATCH();
    {
        ErrorData *errdata;

        ccxt = MemoryContextSwitchTo(ccxt);
        error = true;
        errdata = CopyErrorData();
        FlushErrorState();

        strncpy(errstr, errdata->message, ERROR_STR_LEN - 1);
        FreeErrorData(errdata);
        MemoryContextSwitchTo(ccxt);
    }
    PG_END_TRY();
    if (error)
        throw Error("failed to initialize cast function for column '%s' (%s)",
                    attname, errstr);
}

FmgrInfo *ParquetReader::find_outfunc(Oid typoid)
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

FmgrInfo *ParquetReader::find_infunc(Oid typoid)
{
    MemoryContext oldctx;
    Oid         funcoid;
    Oid         typIOParam;
    FmgrInfo   *infunc;

    getTypeInputInfo(typoid, &funcoid, &typIOParam);

    if (!OidIsValid(funcoid))
        elog(ERROR, "input function for '%s' not found", format_type_be(typoid));

    oldctx = MemoryContextSwitchTo(CurTransactionContext);
    infunc = (FmgrInfo *) palloc0(sizeof(FmgrInfo));
    fmgr_info(funcoid, infunc);
    MemoryContextSwitchTo(oldctx);

    return infunc;
}

/*
 * copy_to_c_array
 *      memcpy plain values from Arrow array to a C array.
 */
template<typename T> inline void
ParquetReader::copy_to_c_array(T *values, const arrow::Array *array, int elem_size)
{
    const T *in = GetPrimitiveValues<T>(*array);

    memcpy(values, in, elem_size * array->length());
}

/*
 * GetPrimitiveValues
 *      Get plain C value array. Copy-pasted from Arrow.
 */
template <typename T> inline const T*
ParquetReader::GetPrimitiveValues(const arrow::Array& arr) {
    if (arr.length() == 0) {
        return nullptr;
    }
    const auto& prim_arr = arrow::internal::checked_cast<const arrow::PrimitiveArray&>(arr);
    const T* raw_values = reinterpret_cast<const T*>(prim_arr.values()->data());
    return raw_values + arr.offset();
}

void ParquetReader::set_rowgroups_list(const std::vector<int> &rowgroups)
{
    this->rowgroups = rowgroups;
}

void ParquetReader::set_options(bool use_threads, bool use_mmap)
{
    this->use_threads = use_threads;
    this->use_mmap = use_mmap;
}

void ParquetReader::set_coordinator(ParallelCoordinator *coord)
{
    this->coordinator = coord;
}

class DefaultParquetReader : public ParquetReader
{
private:
    struct ChunkInfo
    {
        int     chunk;      /* current chunk number */
        int64   pos;        /* current pos within chunk */
        int64   len;        /* current chunk length */

        ChunkInfo (int64 len) : chunk(0), pos(0), len(len) {}
    };

    /* Current row group */
    std::shared_ptr<arrow::Table>   table;

    /*
     * Plain pointers to inner the structures of row group. It's needed to
     * prevent excessive shared_ptr management.
     */
    std::vector<arrow::Array *>     chunks;

    int             row_group;          /* current row group index */
    uint32_t        row;                /* current row within row group */
    uint32_t        num_rows;           /* total rows in row group */
    std::vector<ChunkInfo> chunk_info;  /* current chunk and position per-column */

public:
    /* 
     * Constructor.
     * The reader_id parameter is only used for parallel execution of
     * MultifileExecutionState.
     */
    DefaultParquetReader(const char* filename, MemoryContext cxt, int reader_id = -1)
        : ParquetReader(cxt), row_group(-1), row(0), num_rows(0)
    {
        this->filename = filename;
        this->reader_id = reader_id;
        this->coordinator = NULL;
        this->initialized = false;
    }

    ~DefaultParquetReader()
    {}

    void open()
    {
        arrow::Status   status;
        std::unique_ptr<parquet::arrow::FileReader> reader;

        status = parquet::arrow::FileReader::Make(
                        arrow::default_memory_pool(),
                        parquet::ParquetFileReader::OpenFile(filename, use_mmap),
                        &reader);
        if (!status.ok())
            throw Error("failed to open Parquet file %s ('%s')",
                        status.message().c_str(), filename.c_str());
        this->reader = std::move(reader);

        /* Enable parallel columns decoding/decompression if needed */
        this->reader->set_use_threads(this->use_threads && parquet_fdw_use_threads);
    }

    void close()
    {
        throw std::runtime_error("DefaultParquetReader::close() not implemented");
    }

    bool read_next_rowgroup()
    {
        arrow::Status               status;

        /*
         * In case of parallel query get the row group index from the
         * coordinator. Otherwise just increment it.
         */
        if (coordinator)
        {
            coordinator->lock();
            if ((this->row_group = coordinator->next_rowgroup(reader_id)) == -1)
            {
                coordinator->unlock();
                return false;
            }
            coordinator->unlock();
        }
        else
            this->row_group++;

        /*
         * row_group cannot be less than zero at this point so it is safe to cast
         * it to unsigned int
         */
        if ((uint) this->row_group >= this->rowgroups.size())
            return false;

        int  rowgroup = this->rowgroups[this->row_group];
        auto rowgroup_meta = this->reader
                                ->parquet_reader()
                                ->metadata()
                                ->RowGroup(rowgroup);

        status = this->reader
            ->RowGroup(rowgroup)
            ->ReadTable(this->indices, &this->table);

        if (!status.ok())
            throw Error("failed to read rowgroup #%i: %s ('%s')",
                        rowgroup, status.message().c_str(), this->filename.c_str());

        if (!this->table)
            throw std::runtime_error("got empty table");

        /* TODO: don't clear each time */
        this->chunk_info.clear();
        this->chunks.clear();

        for (uint64_t i = 0; i < types.size(); ++i)
        {
            const auto &column = this->table->column(i);

            int64 len = column->chunk(0)->length();
            this->chunk_info.emplace_back(len);
            this->chunks.push_back(column->chunk(0).get());
        }

        this->row = 0;
        this->num_rows = this->table->num_rows();

        return true;
    }

    ReadStatus next(TupleTableSlot *slot, bool fake=false)
    {
        allocator->recycle();

        if (this->row >= this->num_rows)
        {
            /*
             * Read next row group. We do it in a loop to skip possibly empty
             * row groups.
             */
            do
            {
                if (!this->read_next_rowgroup())
                    return RS_EOF;
            }
            while (!this->num_rows);
        }

        this->populate_slot(slot, fake);
        this->row++;

        return RS_SUCCESS;
    }

    /*
     * populate_slot
     *      Fill slot with the values from parquet row.
     *
     * If `fake` set to true the actual reading and populating the slot is skipped.
     * The purpose of this feature is to correctly skip rows to collect sparse
     * samples.
     */
    void populate_slot(TupleTableSlot *slot, bool fake=false)
    {
        /* Fill slot values */
        for (int attr = 0; attr < slot->tts_tupleDescriptor->natts; attr++)
        {
            int arrow_col = this->map[attr];
            /*
             * We only fill slot attributes if column was referred in targetlist
             * or clauses. In other cases mark attribute as NULL.
             * */
            if (arrow_col >= 0)
            {
                ChunkInfo   &chunkInfo = this->chunk_info[arrow_col];
                arrow::Array *array = this->chunks[arrow_col];
                TypeInfo    &typinfo = this->types[arrow_col];

                if (chunkInfo.pos >= chunkInfo.len)
                {
                    const auto &column = this->table->column(arrow_col);

                    /* There are no more chunks */
                    if (++chunkInfo.chunk >= column->num_chunks())
                        break;

                    array = column->chunk(chunkInfo.chunk).get();
                    this->chunks[arrow_col] = array;
                    chunkInfo.pos = 0;
                    chunkInfo.len = array->length();
                }

                /* Don't do actual reading data into slot in fake mode */
                if (fake)
                    continue;

                if (array->IsNull(chunkInfo.pos))
                {
                    slot->tts_isnull[attr] = true;
                    chunkInfo.pos++;
                    continue;
                }
                slot->tts_isnull[attr] = false;

                /* Currently only primitive types and lists are supported */
                switch (typinfo.arrow.type_id)
                {
                    case arrow::Type::LIST:
                    {
                        arrow::ListArray   *larray = (arrow::ListArray *) array;

                        slot->tts_values[attr] =
                            this->nested_list_to_datum(larray, chunkInfo.pos,
                                                       typinfo);
                        break;
                    }
                    case arrow::Type::MAP:
                    {
                        arrow::MapArray* maparray = (arrow::MapArray*) array;

                        slot->tts_values[attr] =
                            this->map_to_datum(maparray, chunkInfo.pos, typinfo);
                        break;
                    }
                    default:
                        slot->tts_values[attr] = 
                            this->read_primitive_type(array, typinfo, chunkInfo.pos);
                }

                chunkInfo.pos++;
            }
            else
            {
                slot->tts_isnull[attr] = true;
            }
        }
    }

    void rescan(void)
    {
        this->row_group = 0;
        this->row = 0;
        this->num_rows = 0;
    }
};

class CachingParquetReader : public ParquetReader
{
private:
    std::vector<void *>             column_data;
    std::vector<std::vector<bool> > column_nulls;

    bool            is_active;          /* weather reader is active */

    int             row_group;          /* current row group index */
    uint32_t        row;                /* current row within row group */
    uint32_t        num_rows;           /* total rows in row group */

public:
    CachingParquetReader(const char* filename, MemoryContext cxt, int reader_id = -1)
        : ParquetReader(cxt), is_active(false), row_group(-1), row(0), num_rows(0)
    {
        this->filename = filename;
        this->reader_id = reader_id;
        this->coordinator = NULL;
        this->initialized = false;
    }

    ~CachingParquetReader()
    {}

    void open()
    {
        arrow::Status   status;
        std::unique_ptr<parquet::arrow::FileReader> reader;

        status = parquet::arrow::FileReader::Make(
                        arrow::default_memory_pool(),
                        parquet::ParquetFileReader::OpenFile(filename, use_mmap),
                        &reader);
        if (!status.ok())
            throw Error("failed to open Parquet file %s ('%s')",
                        status.message().c_str(), filename.c_str());
        this->reader = std::move(reader);

        /* Enable parallel columns decoding/decompression if needed */
        this->reader->set_use_threads(this->use_threads && parquet_fdw_use_threads);

        is_active = true;
    }

    void close()
    {
        this->reader = nullptr;  /* destroy the reader */
        is_active = false;
    }

    bool read_next_rowgroup()
    {
        arrow::Status                   status;
        std::shared_ptr<arrow::Table>   table;

        /* TODO: release previously stored data */
        this->column_data.resize(this->types.size(), nullptr);
        this->column_nulls.resize(this->types.size());

        /*
         * In case of parallel query get the row group index from the
         * coordinator. Otherwise just increment it.
         */
        if (this->coordinator)
        {
            coordinator->lock();
            if ((this->row_group = coordinator->next_rowgroup(reader_id)) == -1)
            {
                coordinator->unlock();
                return false;
            }
            coordinator->unlock();
        }
        else
            this->row_group++;

        /*
         * row_group cannot be less than zero at this point so it is safe to cast
         * it to unsigned int
         */
        if ((uint) this->row_group >= this->rowgroups.size())
            return false;

        int  rowgroup = this->rowgroups[this->row_group];
        auto rowgroup_meta = this->reader
                                ->parquet_reader()
                                ->metadata()
                                ->RowGroup(rowgroup);

        status = this->reader
            ->RowGroup(rowgroup)
            ->ReadTable(this->indices, &table);
        if (!status.ok())
            throw Error("failed to read rowgroup #%i: %s ('%s')",
                        rowgroup, status.message().c_str(), this->filename.c_str());

        /* Release resources acquired in the previous iteration */
        allocator->recycle();

        /* Read columns data and store it into column_data vector */
        for (std::vector<TypeInfo>::size_type col = 0; col < types.size(); ++col)
        {
            bool            has_nulls;
            std::shared_ptr<parquet::Statistics>  stats;

            if (types[col].index >= 0)
                stats = rowgroup_meta->ColumnChunk(types[col].index)->statistics();
            has_nulls = stats ? stats->null_count() > 0 : true;

            this->num_rows = table->num_rows();
            this->column_nulls[col].resize(this->num_rows);

            this->read_column(table, col, has_nulls);
        }

        this->row = 0;
        return true;
    }

    void read_column(std::shared_ptr<arrow::Table> table,
                     int col,
                     bool has_nulls)
    {
        std::shared_ptr<arrow::ChunkedArray> column = table->column(col);
        TypeInfo &typinfo = this->types[col];
        void   *data;
        size_t  sz;
        int     row = 0;

        switch(typinfo.arrow.type_id) {
            case arrow::Type::BOOL:
                sz = sizeof(bool);
                break;
            case arrow::Type::INT8:
                sz = sizeof(int8);
                break;
            case arrow::Type::INT16:
                sz = sizeof(int16);
                break;
            case arrow::Type::INT32:
                sz = sizeof(int32);
                break;
            case arrow::Type::FLOAT:
                sz = sizeof(float);
                break;
            case arrow::Type::DATE32:
                sz = sizeof(int);
                break;
            default:
                sz = sizeof(Datum);
        }

        data = allocator->fast_alloc(sz * num_rows);

        for (int i = 0; i < column->num_chunks(); ++i) {
            arrow::Array *array = column->chunk(i).get();

            /*
             * XXX We could probably optimize here by copying the entire array
             * by using copy_to_c_array when has_nulls = false.
             */

            for (int j = 0; j < array->length(); ++j) {
                if (has_nulls && array->IsNull(j)) {
                    this->column_nulls[col][row++] = true;
                    continue;
                }
                switch (typinfo.arrow.type_id)
                {
                    /*
                     * For types smaller than Datum (assuming 8 bytes) we
                     * copy raw values to save memory and only convert them
                     * into Datum on the slot population stage.
                     */
                    case arrow::Type::BOOL:
                        {
                            arrow::BooleanArray *boolarray = (arrow::BooleanArray *) array;
                            ((bool *) data)[row] = boolarray->Value(row);
                            break;
                        }
                    case arrow::Type::INT8:
                        {
                            arrow::Int8Array *intarray = (arrow::Int8Array *) array;
                            ((int8 *) data)[row] = intarray->Value(row);
                            break;
                        }
                    case arrow::Type::INT16:
                        {
                            arrow::Int16Array *intarray = (arrow::Int16Array *) array;
                            ((int16 *) data)[row] = intarray->Value(row);
                            break;
                        }
                    case arrow::Type::INT32:
                        {
                            arrow::Int32Array *intarray = (arrow::Int32Array *) array;
                            ((int32 *) data)[row] = intarray->Value(row);
                            break;
                        }
                    case arrow::Type::FLOAT:
                        {
                            arrow::FloatArray *farray = (arrow::FloatArray *) array;
                            ((float *) data)[row] = farray->Value(row);
                            break;
                        }
                    case arrow::Type::DATE32:
                        {
                            arrow::Date32Array *tsarray = (arrow::Date32Array *) array;
                            ((int *) data)[row] = tsarray->Value(row);
                            break;
                        }

                    case arrow::Type::LIST:
                        {
                            auto larray = (arrow::ListArray *) array;

                            ((Datum *) data)[row] =
                                this->nested_list_to_datum(larray, j, typinfo);
                            break;
                        }
                    case arrow::Type::MAP:
                        {
                            arrow::MapArray* maparray = (arrow::MapArray*) array;

                            Datum jsonb =
                                this->map_to_datum(maparray, j, typinfo);

                            /*
                             * Copy jsonb into memory block allocated by
                             * FastAllocator to prevent its destruction though
                             * to be able to recycle it once it fulfilled its
                             * purpose.
                             */
                            void *res = allocator->fast_alloc(VARSIZE_ANY(jsonb));
                            memcpy(res, (Jsonb *) jsonb, VARSIZE_ANY(jsonb));
                            ((Datum *) data)[row] = (Datum) res;
                            pfree((Jsonb *) jsonb);
                            break;
                        }
                    default:
                        /*
                         * For larger types we copy already converted into
                         * Datum values.
                         */
                        ((Datum *) data)[row] =
                            this->read_primitive_type(array, typinfo, j);
                }
                this->column_nulls[col][row] = false;

                row++;
            }
        }

        this->column_data[col] = data;
    }

    ReadStatus next(TupleTableSlot *slot, bool fake=false)
    {
        if (this->row >= this->num_rows)
        {
            if (!is_active)
                return RS_INACTIVE;

            /*
             * Read next row group. We do it in a loop to skip possibly empty
             * row groups.
             */
            do
            {
                if (!this->read_next_rowgroup())
                    return RS_EOF;
            }
            while (!this->num_rows);
        }

        if (!fake)
            this->populate_slot(slot, false);

        return RS_SUCCESS;
    }

    void populate_slot(TupleTableSlot *slot, bool fake=false)
    {
        /* Fill slot values */
        for (int attr = 0; attr < slot->tts_tupleDescriptor->natts; attr++)
        {
            int arrow_col = this->map[attr];
            /*
             * We only fill slot attributes if column was referred in targetlist
             * or clauses. In other cases mark attribute as NULL.
             * */
            if (arrow_col >= 0)
            {
                TypeInfo &typinfo = this->types[arrow_col];
                void     *data = this->column_data[arrow_col];
                bool      need_cast = typinfo.need_cast;

                switch(typinfo.arrow.type_id)
                {
                    case arrow::Type::BOOL:
                        slot->tts_values[attr] = BoolGetDatum(((bool *) data)[this->row]);
                        break;
                    case arrow::Type::INT8:
                        slot->tts_values[attr] = Int8GetDatum(((int8 *) data)[this->row]);
                        break;
                    case arrow::Type::INT16:
                        slot->tts_values[attr] = Int16GetDatum(((int16 *) data)[this->row]);
                        break;
                    case arrow::Type::INT32:
                        slot->tts_values[attr] = Int32GetDatum(((int32 *) data)[this->row]);
                        break;
                    case arrow::Type::FLOAT:
                        slot->tts_values[attr] = Float4GetDatum(((float *) data)[this->row]);
                        break;
                    case arrow::Type::DATE32:
                        {
                            /*
                             * Postgres date starts with 2000-01-01 while unix date (which
                             * Parquet is using) starts with 1970-01-01. So we need to do
                             * simple calculations here.
                             */
                            int dt = ((int *) data)[this->row]
                                + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE);
                            slot->tts_values[attr] = DateADTGetDatum(dt);
                        }
                        break;
                    default:
                        slot->tts_values[attr] = ((Datum *) data)[this->row];
                        need_cast = false;
                }

                if (need_cast)
                    slot->tts_values[attr] = do_cast(slot->tts_values[attr], typinfo);
                slot->tts_isnull[attr] = this->column_nulls[arrow_col][this->row];
            }
            else
            {
                slot->tts_isnull[attr] = true;
            }
        }

        this->row++;
    }

    void rescan(void)
    {
        this->row_group = 0;
        this->row = 0;
        this->num_rows = 0;
    }
};

ParquetReader *create_parquet_reader(const char *filename,
                                     MemoryContext cxt,
                                     int reader_id,
                                     bool caching)
{
#ifdef CACHING_TEST
    /* For testing purposes only */
    caching = true;
#endif

    if (!caching)
        return new DefaultParquetReader(filename, cxt, reader_id);
    else
        return new CachingParquetReader(filename, cxt, reader_id);
}


/* Default destructor is required */
ParquetReader::~ParquetReader()
{}
