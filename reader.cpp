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


/*
 * create_column_mapping
 *      Create mapping between tuple descriptor and parquet columns.
 */
void ParquetReader::create_column_mapping(TupleDesc tupleDesc, std::set<int> &attrs_used)
{
    parquet::ArrowReaderProperties props;
    auto    schema = this->reader->parquet_reader()->metadata()->schema();

    if (!parquet::arrow::FromParquetSchema(schema, props, &this->schema).ok())
        throw Error("error reading parquet schema");

    this->map.resize(tupleDesc->natts);
    for (int i = 0; i < tupleDesc->natts; i++)
    {
        AttrNumber  attnum = i + 1 - FirstLowInvalidHeapAttributeNumber;
        char        pg_colname[255];

        this->map[i] = -1;

        /* Skip columns we don't intend to use in query */
        if (attrs_used.find(attnum) == attrs_used.end())
            continue;

        tolowercase(NameStr(TupleDescAttr(tupleDesc, i)->attname), pg_colname);

        for (int k = 0; k < schema->num_columns(); k++)
        {
            parquet::schema::NodePtr node = schema->Column(k)->schema_node();
            std::vector<std::string> path = node->path()->ToDotVector();
            char    parquet_colname[NAMEDATALEN];

            /* 
             * Postgres names are NAMEDATALEN bytes long at most (including
             * terminal zero)
             */
            if (path[0].length() > NAMEDATALEN - 1)
                continue;

            tolowercase(path[0].c_str(), parquet_colname);

            /*
             * Compare postgres attribute name to the top level column name in
             * parquet.
             *
             * XXX If we will ever want to support structs then this should be
             * changed.
             */
            if (strcmp(pg_colname, parquet_colname) == 0)
            {
                PgTypeInfo      pg_typinfo;
                ArrowTypeInfo   arrow_typinfo;
                bool            error = false;
                arrow::DataType *arrow_type = this->schema->field(k)->type().get();

                /* Found mapping! */
                this->indices.push_back(k);

                this->column_names.push_back(path[0]);

                /* index of last element */
                this->map[i] = this->indices.size() - 1;

                arrow_typinfo.type_id = arrow_type->id();
                if (arrow_typinfo.type_id == arrow::Type::LIST) {
                    auto children = arrow_type->children();

                    Assert(children.size() == 1);
                    auto child_type = children[0]->type();

                    arrow_typinfo.elem_type_id = child_type->id();
                    arrow_typinfo.type_name = child_type->name();
                } else {
                    arrow_typinfo.elem_type_id = arrow::Type::NA;
                    arrow_typinfo.type_name = arrow_type->name();
                }
                this->arrow_types.push_back(arrow_typinfo);

                /* Find the element type in case the column type is array */
                PG_TRY();
                {
                    pg_typinfo.oid = TupleDescAttr(tupleDesc, i)->atttypid;
                    pg_typinfo.elem_type = get_element_type(pg_typinfo.oid);

                    if (OidIsValid(pg_typinfo.elem_type))
                    {
                        get_typlenbyvalalign(pg_typinfo.elem_type,
                                             &pg_typinfo.elem_len,
                                             &pg_typinfo.elem_byval,
                                             &pg_typinfo.elem_align);
                    }
                }
                PG_CATCH();
                {
                    error = true;
                }
                PG_END_TRY();

                if (error)
                    throw Error("failed to get the element type of '%s' column", pg_colname);
                this->pg_types.push_back(pg_typinfo);
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
Datum ParquetReader::read_primitive_type(arrow::Array *array, int type_id,
                                         int64_t i, FmgrInfo *castfunc)
{
    Datum   res;

    /* Get datum depending on the column type */
    switch (type_id)
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
            throw Error("parquet_fdw: unsupported column type: %d", type_id);
    }

    /* Call cast function if needed */
    if (castfunc != NULL)
    {
        MemoryContext   ccxt = CurrentMemoryContext;
        bool            error = false;
        char            errstr[ERROR_STR_LEN];

        PG_TRY();
        {
            res = FunctionCall1(castfunc, res);
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
Datum ParquetReader::nested_list_get_datum(arrow::Array *array, int arrow_type,
                                           PgTypeInfo *pg_type, FmgrInfo *castfunc)
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
    if (array->null_count() == 0 && arrow_type == arrow::Type::INT64)
    {
        /*
         * Ok, there are no nulls, so probably we could just memcpy the
         * entire array.
         *
         * Warning: the code below is based on the assumption that Datum is
         * 8 bytes long, which is true for most contemporary systems but this
         * will not work on some exotic or really old systems.
         */
        copy_to_c_array<int64_t>((int64_t *) values, array, pg_type->elem_len);
        goto construct_array;
    }
#endif
    for (int64_t i = 0; i < array->length(); ++i)
    {
        if (!array->IsNull(i))
            values[i] = this->read_primitive_type(array, arrow_type, i, castfunc);
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
                                 pg_type->elem_type, pg_type->elem_len,
                                 pg_type->elem_byval, pg_type->elem_align);
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

/*
 * initialize_castfuncs
 *      Check wether implicit cast will be required and prepare cast function
 *      call. For arrays find cast functions for its elements.
 */
void ParquetReader::initialize_castfuncs(TupleDesc tupleDesc)
{
    this->castfuncs.resize(this->map.size());

    for (uint i = 0; i < this->map.size(); ++i)
    {
        MemoryContext ccxt = CurrentMemoryContext;
        int     arrow_col = this->map[i];
        bool    error = false;
        char    errstr[ERROR_STR_LEN];

        if (this->map[i] < 0)
        {
            /* Null column */
            this->castfuncs[i] = NULL;
            continue;
        }

        ArrowTypeInfo &arrow_typinfo = this->arrow_types[arrow_col];
        int     src_type,
                dst_type;
        bool    src_is_list,
                dst_is_array;
        Oid     funcid;
        CoercionPathType ct;

        /* Find underlying type of list */
        src_is_list = (arrow_typinfo.type_id == arrow::Type::LIST);
        src_type = src_is_list ?
            to_postgres_type(arrow_typinfo.elem_type_id) :
            to_postgres_type(arrow_typinfo.type_id);

        dst_type = TupleDescAttr(tupleDesc, i)->atttypid;

        if (!OidIsValid(src_type))
            throw Error("unsupported column type: %s", arrow_typinfo.type_name.c_str());

        /* Find underlying type of array */
        dst_is_array = type_is_array(dst_type);
        if (dst_is_array)
            dst_type = get_element_type(dst_type);

        /* Make sure both types are compatible */
        if (src_is_list != dst_is_array)
        {
            throw Error("incompatible types in column \"%s\"; %s",
                        this->column_names[arrow_col].c_str(),
                        src_is_list ?
                            "parquet column is of type list while postgres type is scalar" :
                            "parquet column is of scalar type while postgres type is array");
        }

        PG_TRY();
        {
            if (IsBinaryCoercible(src_type, dst_type))
            {
                this->castfuncs[i] = NULL;
            }
            else
            {
                ct = find_coercion_pathway(dst_type,
                                           src_type,
                                           COERCION_EXPLICIT,
                                           &funcid);
                switch (ct)
                {
                    case COERCION_PATH_FUNC:
                        {
                            MemoryContext   oldctx;

                            oldctx = MemoryContextSwitchTo(CurTransactionContext);
                            this->castfuncs[i] = (FmgrInfo *) palloc0(sizeof(FmgrInfo));
                            fmgr_info(funcid, this->castfuncs[i]);
                            MemoryContextSwitchTo(oldctx);
                            break;
                        }
                    case COERCION_PATH_RELABELTYPE:
                    case COERCION_PATH_COERCEVIAIO:  /* TODO: double check that we
                                                      * shouldn't do anything here*/
                        /* Cast is not needed */
                        this->castfuncs[i] = NULL;
                        break;
                    default:
                        elog(ERROR, "cast function to %s ('%s' column) is not found",
                             format_type_be(dst_type),
                             NameStr(TupleDescAttr(tupleDesc, i)->attname));
                }
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
    }
    this->initialized = true;
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

private:

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
            throw std::runtime_error(status.message().c_str());

        if (!this->table)
            throw std::runtime_error("got empty table");

        /* TODO: don't clear each time */
        this->chunk_info.clear();
        this->chunks.clear();
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

    bool next(TupleTableSlot *slot, bool fake=false)
    {
        allocator->recycle();

        if (this->row >= this->num_rows)
        {
            /* Read next row group */
            if (!this->read_next_rowgroup(slot->tts_tupleDescriptor))
                return false;

            /* Lookup cast funcs */
            if (!this->initialized)
                this->initialize_castfuncs(slot->tts_tupleDescriptor);
        }

        this->populate_slot(slot, fake);
        this->row++;

        return true;
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
                ChunkInfo &chunkInfo = this->chunk_info[arrow_col];
                arrow::Array       *array = this->chunks[arrow_col];
                ArrowTypeInfo      &arrow_type = this->arrow_types[arrow_col];
                PgTypeInfo         &pg_type = this->pg_types[arrow_col];

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

                /* Currently only primitive types and lists are supported */
                if (arrow_type.type_id != arrow::Type::LIST)
                {
                    if (this->has_nulls[arrow_col] && array->IsNull(chunkInfo.pos))
                    {
                        slot->tts_isnull[attr] = true;
                    }
                    else
                    {
                        slot->tts_values[attr] = 
                            this->read_primitive_type(array,
                                                      arrow_type.type_id,
                                                      chunkInfo.pos,
                                                      this->castfuncs[attr]);
                        slot->tts_isnull[attr] = false;
                    }
                }
                else
                {
                    if (!OidIsValid(pg_type.elem_type))
                    {
                        throw std::runtime_error("parquet_fdw: cannot convert parquet column of type "
                                                 "LIST to postgres column of scalar type");
                    }
                    int64 pos = chunkInfo.pos;
                    arrow::ListArray   *larray = (arrow::ListArray *) array;

                    if (this->has_nulls[arrow_col] && array->IsNull(pos))
                    {
                        slot->tts_isnull[attr] = true;
                    }
                    else
                    {
                        std::shared_ptr<arrow::Array> slice =
                            larray->values()->Slice(larray->value_offset(pos),
                                                    larray->value_length(pos));

                        slot->tts_values[attr] =
                            this->nested_list_get_datum(slice.get(),
                                                        arrow_type.elem_type_id,
                                                        &pg_type,
                                                        this->castfuncs[attr]);
                        slot->tts_isnull[attr] = false;
                    }
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

ParquetReader *parquet_reader_create(const char *filename,
                                     MemoryContext cxt,
                                     int reader_id)
{
    return new DefaultParquetReader(filename, cxt, reader_id);
}


/* Default destructor is required */
ParquetReader::~ParquetReader()
{}
