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
}

#define SEGMENT_SIZE (1024 * 1024)


bool parquet_fdw_use_threads = true;


/*
 * XXX Currently only supports ascii strings
 */
static char *
tolowercase(const char *input, char *output)
{
    int i = 0;

    Assert(strlen(input) < NAMEDATALEN - 1);

    do
    {
        output[i] = tolower(input[i]);
    }
    while (input[i++]);

    return output;
}


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
        : garbage_segments()
    {
        this->segments_cxt = cxt;
        this->segment_start_ptr = NULL;
        this->segment_cur_ptr = NULL;
        this->segment_last_ptr = NULL;
    }
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
            return palloc(size);

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
        throw Error("error creating arrow schema");

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
            auto arrow_type = schema_field.field->type();
            auto arrow_colname = schema_field.field->name();

            /*
             * Compare postgres attribute name to the column name in arrow
             * schema.
             */
            if (strcmp(pg_colname, arrow_colname.c_str()) == 0)
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
                            throw Error("parquet_fdw: cannot convert parquet "
                                        "column of type LIST to scalar type of "
                                        " postgres column '%s'", pg_colname);

                        auto     &child = schema_field.children[0];
                        FmgrInfo *castfunc = find_castfunc(child.field->type()->id(),
                                                           elem_type, attname);
                        typinfo.children.emplace_back(child.field->type(),
                                                      elem_type, castfunc);
                        TypeInfo &elem = typinfo.children[0];
                        elem.pg.len = elem_len;
                        elem.pg.byval = elem_byval;
                        elem.pg.align = elem_align;

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
                                                      pg_key_type, nullptr);
                        typinfo.children.emplace_back(item.field->type(),
                                                      pg_item_type, nullptr);

                        this->indices.push_back(key.column_index);
                        this->indices.push_back(item.column_index);
                        break;
                    }
                    default:
                        typinfo.castfunc = find_castfunc(typinfo.arrow.type_id,
                                                         typinfo.pg.oid,
                                                         attname);
                        this->indices.push_back(schema_field.column_index);
                }
                this->types.push_back(std::move(typinfo));

                break;
            }
        }
    }

    this->has_nulls = (bool *) exc_palloc(sizeof(bool) * this->map.size());
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
    if (typinfo.castfunc != NULL)
    {
        MemoryContext   ccxt = CurrentMemoryContext;
        bool            error = false;
        char            errstr[ERROR_STR_LEN];

        PG_TRY();
        {
            res = FunctionCall1(typinfo.castfunc, res);
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
    }

    return res;
}

/*
 * nested_list_get_datum
 *      Returns postgres array build from elements of array. Only one
 *      dimensional arrays are supported.
 */
Datum ParquetReader::nested_list_get_datum(arrow::Array *array,
                                           const TypeInfo &typinfo)
{
    MemoryContext oldcxt;
    ArrayType  *res;
    Datum      *values;
    bool       *nulls = NULL;
    int         dims[1];
    int         lbs[1];
    bool        error = false;

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
        copy_to_c_array<int64_t>((int64_t *) values, array, typinfo.pg.len);
        goto construct_array;
    }
#endif
    for (int64_t i = 0; i < array->length(); ++i)
    {
        if (!array->IsNull(i))
            values[i] = this->read_primitive_type(array, typinfo, i);
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
                                 typinfo.pg.oid, typinfo.pg.len,
                                 typinfo.pg.byval, typinfo.pg.align);
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
ParquetReader::map_to_jsonb(arrow::Array *keys, arrow::Array *values,
                            const TypeInfo &typinfo)
{
	JsonbParseState *parseState = NULL;
    JsonbValue *res;

    Assert(keys->length() == values->length());
    Assert(typinfo.children.size() == 2);

	res = pushJsonbValue(&parseState, WJB_BEGIN_OBJECT, NULL);

    for (int i = 0; i < keys->length(); ++i)
    {
        Datum   key, value;
        bool    isnull;
        const TypeInfo &key_typinfo = typinfo.children[0];
        const TypeInfo &val_typinfo = typinfo.children[1];

        if (keys->IsNull(i))
            throw std::runtime_error("key is null");
        
        if (!values->IsNull(i))
        {
            key = this->read_primitive_type(keys, key_typinfo, i);
            value = this->read_primitive_type(values, val_typinfo, i);
        } else
            isnull = false;

        /* TODO: adding cstring would be cheaper than adding text */
        datum_to_jsonb(key, key_typinfo.pg.oid, false, parseState, true);
        datum_to_jsonb(value, val_typinfo.pg.oid, isnull, parseState, false);
    }

	res = pushJsonbValue(&parseState, WJB_END_OBJECT, NULL);

    return JsonbPGetDatum(JsonbValueToJsonb(res));
}


/*
 * find_castfunc
 *      Check wether implicit cast will be required and prepare cast function
 *      call.
 */
FmgrInfo *ParquetReader::find_castfunc(arrow::Type::type src_type,
                                       Oid dst_type, const char *attname)
{
    MemoryContext ccxt = CurrentMemoryContext;
    FmgrInfo   *castfunc;
    Oid         src_oid = to_postgres_type(src_type);
    Oid         dst_oid = dst_type;
    bool        error = false;
    char        errstr[ERROR_STR_LEN];

    PG_TRY();
    {

        if (IsBinaryCoercible(src_oid, dst_oid))
        {
            castfunc = nullptr;
        }
        else
        {
            Oid funcid;
            CoercionPathType ct;

            ct = find_coercion_pathway(dst_oid, src_oid,
                                       COERCION_EXPLICIT,
                                       &funcid);
            switch (ct)
            {
                case COERCION_PATH_FUNC:
                    {
                        MemoryContext   oldctx;

                        oldctx = MemoryContextSwitchTo(CurTransactionContext);
                        castfunc = (FmgrInfo *) palloc0(sizeof(FmgrInfo));
                        fmgr_info(funcid, castfunc);
                        MemoryContextSwitchTo(oldctx);
                        break;
                    }
                case COERCION_PATH_RELABELTYPE:
                case COERCION_PATH_COERCEVIAIO:  /* TODO: double check that we
                                                  * shouldn't do anything here*/
                    /* Cast is not needed */
                    castfunc = nullptr;
                    break;
                default:
                    elog(ERROR, "cast function to %s ('%s' column) is not found",
                         format_type_be(dst_type), attname);
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
        throw std::runtime_error(errstr);

    return castfunc;
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
        : row_group(-1), row(0), num_rows(0)
    {
        this->filename = filename;
        this->reader_id = reader_id;
        this->coordinator = NULL;
        this->initialized = false;
        this->allocator = new FastAllocator(cxt);
    }

    ~DefaultParquetReader()
    {
        if (allocator)
            delete allocator;
    }

    void open()
    {
        arrow::Status   status;
        std::unique_ptr<parquet::arrow::FileReader> reader;

        status = parquet::arrow::FileReader::Make(
                        arrow::default_memory_pool(),
                        parquet::ParquetFileReader::OpenFile(filename, use_mmap),
                        &reader);
        if (!status.ok())
            throw Error("failed to open Parquet file %s",
                                 status.message().c_str());
        this->reader = std::move(reader);

        /* Enable parallel columns decoding/decompression if needed */
        this->reader->set_use_threads(this->use_threads && parquet_fdw_use_threads);
    }

    void close()
    {
        throw std::runtime_error("DefaultParquetReader::close() not implemented");
    }

    bool read_next_rowgroup(TupleDesc tupleDesc)
    {
        arrow::Status               status;

        /*
         * In case of parallel query get the row group index from the
         * coordinator. Otherwise just increment it.
         */
        if (this->coordinator)
        {
            SpinLockAcquire(&this->coordinator->lock);

            /* Did we finish reading from this reader? */
            if (this->reader_id != (this->coordinator->next_reader - 1)) {
                SpinLockRelease(&this->coordinator->lock);
                return false;
            }
            this->row_group = this->coordinator->next_rowgroup++;

            SpinLockRelease(&this->coordinator->lock);
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

        /* Determine which columns have null values */
        for (uint i = 0; i < this->map.size(); i++)
        {
            std::shared_ptr<parquet::Statistics>  stats;
            int arrow_col = this->map[i];

            if (arrow_col < 0)
                continue;

            stats = rowgroup_meta
                ->ColumnChunk(this->indices[arrow_col])
                ->statistics();

            if (stats)
                this->has_nulls[arrow_col] = (stats->null_count() > 0);
            else
                this->has_nulls[arrow_col] = true;
        }

        status = this->reader
            ->RowGroup(rowgroup)
            ->ReadTable(this->indices, &this->table);

        if (!status.ok())
            throw Error("failed to read rowgroup #%i: %s",
                        rowgroup, status.message().c_str());

        if (!this->table)
            throw std::runtime_error("got empty table");

        /* TODO: don't clear each time */
        this->chunk_info.clear();
        this->chunks.clear();

        /* TODO: don't need tupleDesc in this function at all */
        for (int i = 0; i < tupleDesc->natts; i++)
        {
            if (this->map[i] >= 0)
            {
                ChunkInfo chunkInfo = { .chunk = 0, .pos = 0, .len = 0 };
                auto column = this->table->column(this->map[i]);

                this->chunk_info.push_back(chunkInfo);
                this->chunks.push_back(column->chunk(0).get());
            }
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
            /* Read next row group */
            if (!this->read_next_rowgroup(slot->tts_tupleDescriptor))
                return RS_EOF;
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

                chunkInfo.len = array->length();

                if (chunkInfo.pos >= chunkInfo.len)
                {
                    auto column = this->table->column(arrow_col);

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

                if (this->has_nulls[arrow_col] && array->IsNull(chunkInfo.pos))
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
                        int64 pos = chunkInfo.pos;
                        arrow::ListArray   *larray = (arrow::ListArray *) array;

                        std::shared_ptr<arrow::Array> slice =
                            larray->values()->Slice(larray->value_offset(pos),
                                                    larray->value_length(pos));

                        slot->tts_values[attr] =
                            this->nested_list_get_datum(slice.get(),
                                                        typinfo.children[0]);
                        break;
                    }
                    case arrow::Type::MAP:
                    {
                        int64 pos = chunkInfo.pos;
                        arrow::MapArray* maparray = (arrow::MapArray*) array;
                        auto ks = maparray->keys()->Slice(maparray->value_offset(pos),
                                                          maparray->value_length(pos));
                        auto vs = maparray->items()->Slice(maparray->value_offset(pos),
                                                            maparray->value_length(pos));

                        slot->tts_values[attr] = this->map_to_jsonb(ks.get(), vs.get(), typinfo);
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
        : is_active(false), row_group(-1), row(0), num_rows(0)
    {
        this->filename = filename;
        this->reader_id = reader_id;
        this->coordinator = NULL;
        this->initialized = false;
        this->allocator = new FastAllocator(cxt);
    }

    ~CachingParquetReader()
    {
        if (allocator)
            delete allocator;
    }

    void open()
    {
        arrow::Status   status;
        std::unique_ptr<parquet::arrow::FileReader> reader;

        status = parquet::arrow::FileReader::Make(
                        arrow::default_memory_pool(),
                        parquet::ParquetFileReader::OpenFile(filename, use_mmap),
                        &reader);
        if (!status.ok())
            throw Error("failed to open Parquet file %s",
                                 status.message().c_str());
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

    bool read_next_rowgroup(TupleDesc)
    {
        arrow::Status                   status;
        std::shared_ptr<arrow::Table>   table;

        /* TODO: release previously stored data */
        this->column_data.resize(this->indices.size(), nullptr);
        this->column_nulls.resize(this->indices.size());

        /*
         * In case of parallel query get the row group index from the
         * coordinator. Otherwise just increment it.
         */
        if (this->coordinator)
        {
            SpinLockAcquire(&this->coordinator->lock);

            /* Did we finish reading from this reader? */
            if (this->reader_id != (this->coordinator->next_reader - 1)) {
                SpinLockRelease(&this->coordinator->lock);
                return false;
            }
            this->row_group = this->coordinator->next_rowgroup++;

            SpinLockRelease(&this->coordinator->lock);
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
            throw Error("failed to read rowgroup #%i: %s",
                        rowgroup, status.message().c_str());

        /* Release resources acquired in the previous iteration */
        allocator->recycle();

        /* Read columns data and store it into column_data vector */
        for (std::vector<int>::size_type col = 0; col < indices.size(); ++col)
        {
            int             arrow_col = indices[col];
            bool            has_nulls;
            std::shared_ptr<parquet::Statistics>  stats;

            stats = rowgroup_meta->ColumnChunk(arrow_col)->statistics();

            has_nulls = stats ? stats->null_count() > 0 : true;

            if (this->column_data[col])
                pfree(this->column_data[col]);
            this->column_data[col] = (Datum *)
                MemoryContextAlloc(allocator->context(),
                                   sizeof(Datum) * table->num_rows());

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

        data = MemoryContextAlloc(allocator->context(), sz * num_rows);

        for (int i = 0; i < column->num_chunks(); ++i) {
            arrow::Array *array = column->chunk(i).get();

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
                    case arrow::Type::INT32:
                        {
                            arrow::Int32Array *intarray = (arrow::Int32Array *) array;
                            ((int *) data)[row] = intarray->Value(row);
                            break;
                        }
                    case arrow::Type::FLOAT:
                        {
                            arrow::FloatArray *farray = (arrow::FloatArray *) array;
                            ((int *) data)[row] = farray->Value(row);
                            break;
                        }
                    case arrow::Type::DATE32:
                        {
                            arrow::Date32Array *tsarray = (arrow::Date32Array *) array;
                            ((int *) data)[row] = tsarray->Value(row);
                            break;
                        }

                    case arrow::Type::LIST:
                        /* Special case for lists */
                        {
                            auto larray = (arrow::ListArray *) array;
                            auto slice =
                                larray->values()->Slice(larray->value_offset(j),
                                                        larray->value_length(j));
                            ((Datum *) data)[row] =
                                this->nested_list_get_datum(slice.get(),
                                                            typinfo.children[0]);
                            break;
                        }
                    case arrow::Type::MAP:
                        /* TODO */
                        throw std::runtime_error("not implemented");
                        break;

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

            /* Read next row group */
            if (!this->read_next_rowgroup(slot->tts_tupleDescriptor))
                return RS_EOF;
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
                void *data = this->column_data[arrow_col];

                switch(typinfo.arrow.type_id)
                {
                    case arrow::Type::BOOL:
                        slot->tts_values[attr] = BoolGetDatum(((bool *) data)[this->row]);
                        break;
                    case arrow::Type::INT32:
                        slot->tts_values[attr] = Int32GetDatum(((int *) data)[this->row]);
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
                }
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
    if (!caching)
        return new DefaultParquetReader(filename, cxt, reader_id);
    else
        return new CachingParquetReader(filename, cxt, reader_id);
}


/* Default destructor is required */
ParquetReader::~ParquetReader()
{}
