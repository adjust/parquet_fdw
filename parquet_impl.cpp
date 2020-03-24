/*
 * Parquet processing implementation
 */
#include <sys/stat.h>

#include <list>
#include <set>
#include <mutex>

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/array.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/exception.h"
#include "parquet/file_reader.h"
#include "parquet/statistics.h"

extern "C"
{
#include "postgres.h"

#include "access/htup_details.h"
#include "access/parallel.h"
#include "access/sysattr.h"
#include "access/nbtree.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "executor/spi.h"
#include "executor/tuptable.h"
#include "foreign/foreign.h"
#include "foreign/fdwapi.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "parser/parse_coerce.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/memdebug.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"

#if PG_VERSION_NUM < 120000
#include "nodes/relation.h"
#include "optimizer/var.h"
#else
#include "access/table.h"
#include "access/relation.h"
#include "optimizer/optimizer.h"
#endif
}


#define SEGMENT_SIZE (1024 * 1024)

#define ERROR_STR_LEN 512

/* from costsize.c */
#define LOG2(x)  (log(x) / 0.693147180559945)

#define to_postgres_timestamp(tstype, i, ts)                    \
    switch ((tstype)->unit()) {                                 \
        case arrow::TimeUnit::SECOND:                           \
            ts = time_t_to_timestamptz((i)); break;             \
        case arrow::TimeUnit::MILLI:                            \
            ts = time_t_to_timestamptz((i) / 1000); break;      \
        case arrow::TimeUnit::MICRO:                            \
            ts = time_t_to_timestamptz((i) / 1000000); break;   \
        case arrow::TimeUnit::NANO:                             \
            ts = time_t_to_timestamptz((i) / 1000000000); break;\
        default:                                                \
            elog(ERROR, "Timestamp of unknown precision: %d",   \
                 (tstype)->unit());                             \
    }

#if PG_VERSION_NUM < 110000
#define PG_GETARG_JSONB_P PG_GETARG_JSONB
#endif


static void find_cmp_func(FmgrInfo *finfo, Oid type1, Oid type2);
static Datum bytes_to_postgres_type(const char *bytes, arrow::DataType *arrow_type);
static Oid to_postgres_type(int arrow_type);
static void destroy_parquet_state(void *arg);

bool parquet_fdw_use_threads = true;

/*
 * exc_palloc
 *      C++ specific memory allocator that utilizes postgres allocation sets.
 */
void *
exc_palloc(Size size)
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

struct Error : std::exception
{
    char text[1000];

    Error(char const* fmt, ...) __attribute__((format(printf,2,3))) {
        va_list ap;
        va_start(ap, fmt);
        vsnprintf(text, sizeof text, fmt, ap);
        va_end(ap);
    }

    char const* what() const throw() { return text; }
};

/*
 * Restriction
 */
struct RowGroupFilter
{
    AttrNumber  attnum;
    Const      *value;
    int         strategy;
};

enum ReaderType
{
    RT_SINGLE = 0,
    RT_MULTI,
    RT_MULTI_MERGE
};

/*
 * Plain C struct for fdw_state
 */
struct ParquetFdwPlanState
{
    List       *filenames;
    List       *attrs_sorted;
    Bitmapset  *attrs_used;     /* attributes actually used in query */
    bool        use_mmap;
    bool        use_threads;
    List       *rowgroups;      /* List of Lists (per filename) */
    uint64      ntuples;
    ReaderType  type;
};

struct ChunkInfo
{
    int     chunk;      /* current chunk number */
    int64   pos;        /* current pos within chunk */
    int64   len;        /* current chunk length */
};

struct ParallelCoordinator
{
    std::mutex  lock;
    int32       next_reader;
    int32       next_rowgroup;
};


static int
get_arrow_list_elem_type(arrow::DataType *type)
{
    auto children = type->children();

    Assert(children.size() == 1);
    return children[0]->type()->id();
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

static char *
tolowercase(const char *input, char *output)
{
    int i = 0;

    Assert(strlen(input) < 254);

    do
    {
        output[i] = tolower(input[i]);
    }
    while (input[i++]);

    return output;
}

class ParquetFdwReader
{
private:
    struct PgTypeInfo
    {
        Oid     oid;

        /* For array types. elem_type == InvalidOid means type is not an array */
        Oid     elem_type;
        int16   elem_len;
        bool    elem_byval;
        char    elem_align;
    };
public:
    /* id needed for parallel execution */
    int32                           reader_id;

    std::unique_ptr<parquet::arrow::FileReader> reader;

    std::shared_ptr<arrow::Schema>  schema;

    /* Arrow column indices that are used in query */
    std::vector<int>                indices;

    /*
     * Mapping between slot attributes and arrow result set columns.
     * Corresponds to 'indices' vector.
     */
    std::vector<int>                map;

    /*
     * Cast functions from dafult postgres type defined in `to_postgres_type`
     * to actual table column type.
     */
    std::vector<FmgrInfo *>         castfuncs;

    /* Current row group */
    std::shared_ptr<arrow::Table>   table;

    /*
     * Plain pointers to inner the structures of row group. It's needed to
     * prevent excessive shared_ptr management.
     */
    std::vector<arrow::Array *>     chunks;
    std::vector<arrow::DataType *>  types;

    std::vector<PgTypeInfo>         pg_types;

    bool           *has_nulls;          /* per-column info on nulls */

    int             row_group;          /* current row group index */
    uint32_t        row;                /* current row within row group */
    uint32_t        num_rows;           /* total rows in row group */
    std::vector<ChunkInfo> chunk_info;  /* current chunk and position per-column */

    /*
     * Filters built from query restrictions that help to filter out row
     * groups.
     */
    std::list<RowGroupFilter>       filters;

    /*
     * List of row group indexes to scan
     */
    std::vector<int>                rowgroups;

    FastAllocator                  *allocator;

    /* Coordinator for parallel query execution */
    ParallelCoordinator            *coordinator;

    /* Wether object is properly initialized */
    bool     initialized;

    ParquetFdwReader(int reader_id)
        : reader_id(reader_id), row_group(-1), row(0), num_rows(0),
          coordinator(NULL), initialized(false)
    { }

    ~ParquetFdwReader()
    {
        if (allocator)
            delete allocator;
    }

    void open(const char *filename,
              MemoryContext cxt,
              TupleDesc tupleDesc,
              std::set<int> &attrs_used,
              bool use_threads,
              bool use_mmap)
    {
        parquet::ArrowReaderProperties props;
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

        auto    schema = this->reader->parquet_reader()->metadata()->schema();
        if (!parquet::arrow::FromParquetSchema(schema, props, &this->schema).ok())
            throw Error("error reading parquet schema");

        /* Enable parallel columns decoding/decompression if needed */
        this->reader->set_use_threads(use_threads && parquet_fdw_use_threads);

        /* Create mapping between tuple descriptor and parquet columns. */
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
                char    parquet_colname[255];

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
                    PgTypeInfo  typinfo;
                    bool        error = false;

                    /* Found mapping! */
                    this->indices.push_back(k);

                    /* index of last element */
                    this->map[i] = this->indices.size() - 1;

                    this->types.push_back(this->schema->field(k)->type().get());

                    /* Find the element type in case the column type is array */
                    PG_TRY();
                    {
                        typinfo.oid = TupleDescAttr(tupleDesc, i)->atttypid;
                        typinfo.elem_type = get_element_type(typinfo.oid);

                        if (OidIsValid(typinfo.elem_type))
                        {
                            get_typlenbyvalalign(typinfo.elem_type,
                                                 &typinfo.elem_len,
                                                 &typinfo.elem_byval,
                                                 &typinfo.elem_align);
                        }
                    }
                    PG_CATCH();
                    {
                        error = true;
                    }
                    PG_END_TRY();

                    if (error)
                        throw Error("failed to get the element type of '%s' column", pg_colname);
                    this->pg_types.push_back(typinfo);

                    break;
                }
            }
        }

        /* TODO: use c++ compatible allocator */
        this->has_nulls = (bool *) exc_palloc(sizeof(bool) * this->map.size());

        this->allocator = new FastAllocator(cxt);
    }

    bool read_next_rowgroup(TupleDesc tupleDesc)
    {
        ParallelCoordinator        *coord;
        arrow::Status               status;

        coord = this->coordinator;

        /*
         * Use atomic increment for parallel query or just regular one for single
         * threaded execution.
         */
        if (coord)
        {
            std::lock_guard<std::mutex> guard(coord->lock);
            if (this->reader_id != (coord->next_reader - 1))
                return false;
            this->row_group = coord->next_rowgroup++;
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

        /* Determine which columns has null values */
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
                arrow::DataType    *arrow_type = this->types[arrow_col];
                int                 arrow_type_id = arrow_type->id();
                PgTypeInfo         *pg_type = &this->pg_types[arrow_col];

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
                if (arrow_type_id != arrow::Type::LIST)
                {
                    if (this->has_nulls[arrow_col] && array->IsNull(chunkInfo.pos))
                    {
                        slot->tts_isnull[attr] = true;
                    }
                    else
                    {
                        slot->tts_values[attr] = 
                            this->read_primitive_type(array,
                                                      arrow_type_id,
                                                      chunkInfo.pos,
                                                      this->castfuncs[attr]);
                        slot->tts_isnull[attr] = false;
                    }
                }
                else
                {
                    /* TODO: do this during initialization stage */
                    if (!OidIsValid(pg_type->elem_type))
                    {
                        throw std::runtime_error("parquet_fdw: cannot convert parquet column of type "
                                                 "LIST to postgres column of scalar type");
                    }
                    arrow_type_id = get_arrow_list_elem_type(arrow_type);

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
                                                        arrow_type_id,
                                                        pg_type,
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

    /*
     * read_primitive_type
     *      Returns primitive type value from arrow array
     */
    Datum
    read_primitive_type(arrow::Array *array,
                        int type_id, int64_t i,
                        FmgrInfo *castfunc)
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
            Datum           res;
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

                strncpy(errstr, errdata->message, ERROR_STR_LEN);
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
    Datum
    nested_list_get_datum(arrow::Array *array, int arrow_type,
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
    void initialize_castfuncs(TupleDesc tupleDesc)
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

            arrow::DataType *type = this->types[arrow_col];
            int     type_id = type->id();
            int     src_type,
                    dst_type;
            bool    src_is_list,
                    dst_is_array;
            Oid     funcid;
            CoercionPathType ct;

            /* Find underlying type of list */
            src_is_list = (type_id == arrow::Type::LIST);
            if (src_is_list)
                type_id = get_arrow_list_elem_type(type);

            src_type = to_postgres_type(type_id);
            dst_type = TupleDescAttr(tupleDesc, i)->atttypid;

            if (!OidIsValid(src_type))
                throw Error("unsupported column type: %s", type->name().c_str());

            /* Find underlying type of array */
            dst_is_array = type_is_array(dst_type);
            if (dst_is_array)
                dst_type = get_element_type(dst_type);

            /* Make sure both types are compatible */
            if (src_is_list != dst_is_array)
            {
                throw Error("incompatible types in column \"%s\"; %s",
                            this->table->field(arrow_col)->name().c_str(),
                            src_is_list ?
                                "parquet column is of type list while postgres type is scalar" :
                                "parquet column is of scalar type while postgres type is array");
            }

            PG_TRY();
            {
                if (IsBinaryCoercible(src_type, dst_type))
                {
                    this->castfuncs[i] = NULL;
                    continue;
                }

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
                        elog(ERROR, "cast function is not found");
                }
            }
            PG_CATCH();
            {
                ErrorData *errdata;

                MemoryContextSwitchTo(ccxt);
                error = true;
                errdata = CopyErrorData();
                FlushErrorState();

                strncpy(errstr, errdata->message, ERROR_STR_LEN);
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
    template<typename T> static inline void
    copy_to_c_array(T *values, const arrow::Array *array, int elem_size)
    {
        const T *in = GetPrimitiveValues<T>(*array);

        memcpy(values, in, elem_size * array->length());
    }

    /*
     * GetPrimitiveValues
     *      Get plain C value array. Copy-pasted from Arrow.
     */
    template <typename T>
    static inline const T* GetPrimitiveValues(const arrow::Array& arr) {
        if (arr.length() == 0) {
            return nullptr;
        }
        const auto& prim_arr = arrow::internal::checked_cast<const arrow::PrimitiveArray&>(arr);
        const T* raw_values = reinterpret_cast<const T*>(prim_arr.values()->data());
        return raw_values + arr.offset();
    }

    void set_rowgroups_list(const std::vector<int> &rowgroups)
    {
        this->rowgroups = rowgroups;
    }
};

class ParquetFdwExecutionState
{
public:
    virtual ~ParquetFdwExecutionState() {};
    virtual bool next(TupleTableSlot *slot, bool fake=false) = 0;
    virtual void rescan(void) = 0;
    virtual void add_file(const char *filename, List *rowgroups) = 0;
    virtual void set_coordinator(ParallelCoordinator *coord) = 0;
};

class SingleFileExecutionState : public ParquetFdwExecutionState
{
private:
    ParquetFdwReader   *reader;
    MemoryContext       cxt;
    TupleDesc           tupleDesc;
    std::set<int>       attrs_used;
    bool                use_mmap;
    bool                use_threads;

public:
    MemoryContext       estate_cxt;

    SingleFileExecutionState(MemoryContext cxt,
                             TupleDesc tupleDesc,
                             std::set<int> attrs_used,
                             bool use_threads,
                             bool use_mmap)
        : cxt(cxt), tupleDesc(tupleDesc), attrs_used(attrs_used),
          use_mmap(use_mmap), use_threads(use_threads)
    { }

    ~SingleFileExecutionState()
    {
        if (reader)
            delete reader;
    }

    bool next(TupleTableSlot *slot, bool fake)
    {
        bool res;

        if ((res = reader->next(slot, fake)) == true)
            ExecStoreVirtualTuple(slot);

        return res;
    }

    void rescan(void)
    {
        reader->rescan();
    }

    void add_file(const char *filename, List *rowgroups)
    {
        ListCell           *lc;
        std::vector<int>    rg;
        int                 reader_id = 0;

        foreach (lc, rowgroups)
            rg.push_back(lfirst_int(lc));

        reader = new ParquetFdwReader(reader_id);
        reader->open(filename, cxt, tupleDesc, attrs_used, use_threads, use_mmap);
        reader->set_rowgroups_list(rg);
    }

    void set_coordinator(ParallelCoordinator *coord)
    {
        if (reader)
            reader->coordinator = coord;
    }
};

class MultifileExecutionState : public ParquetFdwExecutionState
{
private:
    struct FileRowgroups
    {
        std::string         filename;
        std::vector<int>    rowgroups;
    };
private:
    ParquetFdwReader       *reader;

    std::vector<FileRowgroups> files;
    uint64_t                cur_reader;

    MemoryContext           cxt;
    TupleDesc               tupleDesc;
    std::set<int>           attrs_used;
    bool                    use_threads;
    bool                    use_mmap;

    ParallelCoordinator    *coord;

private:
    ParquetFdwReader *get_next_reader()
    {
        ParquetFdwReader *r;

        if (coord)
        {
            std::lock_guard<std::mutex> guard(coord->lock);
            cur_reader = coord->next_reader++;
        }

        if (cur_reader >= files.size())
            return NULL;

        r = new ParquetFdwReader(cur_reader);
        r->open(files[cur_reader].filename.c_str(), cxt, tupleDesc, attrs_used, use_threads, use_mmap);
        r->set_rowgroups_list(files[cur_reader].rowgroups);

        cur_reader++;

        return r;
    }

public:
    MultifileExecutionState(MemoryContext cxt,
                            TupleDesc tupleDesc,
                            std::set<int> attrs_used,
                            bool use_threads,
                            bool use_mmap)
        : reader(NULL), cur_reader(0), cxt(cxt), tupleDesc(tupleDesc),
          attrs_used(attrs_used), use_threads(use_threads), use_mmap(use_mmap),
          coord(NULL)
    { }

    ~MultifileExecutionState()
    {
        if (reader)
            delete reader;
    }

    bool next(TupleTableSlot *slot, bool fake=false)
    {
        bool    res;

        if (unlikely(reader == NULL))
        {
            if ((reader = this->get_next_reader()) == NULL)
                return false;
        }

        res = reader->next(slot, fake);

        /* Finished reading current reader? Proceed to the next one */
        if (unlikely(!res))
        {
            while (true)
            {
                if (reader)
                    delete reader;

                reader = this->get_next_reader();
                if (!reader)
                    return false;
                res = reader->next(slot, fake);
                if (res)
                    break;
            }
        }

        if (res)
        {
            /*
             * ExecStoreVirtualTuple doesn't throw postgres exceptions thus no
             * need to wrap it into PG_TRY / PG_CATCH
             */
            ExecStoreVirtualTuple(slot);
        }

        return res;
    }

    void rescan(void)
    {
        reader->rescan();
    }

    void add_file(const char *filename, List *rowgroups)
    {
        FileRowgroups   fr;
        ListCell       *lc;

        fr.filename = filename;
        foreach (lc, rowgroups)
            fr.rowgroups.push_back(lfirst_int(lc));
        files.push_back(fr);
    }

    void set_coordinator(ParallelCoordinator *coord)
    {
        this->coord = coord;
    }
};

class MultifileMergeExecutionState : public ParquetFdwExecutionState
{
    struct FileSlot
    {
        int             reader_id;
        TupleTableSlot *slot;
    };
    typedef std::vector<FileSlot> BinHeap;
private:
    std::vector<ParquetFdwReader *>   readers;

    MemoryContext       cxt;
    TupleDesc           tupleDesc;
    std::set<int>       attrs_used;
    std::list<SortSupportData> sort_keys;
    bool                use_threads;
    bool                use_mmap;

    /*
     * Heap is used to store tuples in prioritized manner along with file
     * number. Priority is given to the tuples with minimal key. Once next
     * tuple is requested it is being taken from the top of the heap and a new
     * tuple from the same file is read and inserted back into the heap. Then
     * heap is rebuilt to sustain its properties. The idea is taken from
     * nodeGatherMerge.c in PostgreSQL but reimplemented using STL.
     */
    BinHeap             slots;
    bool                slots_initialized;

private:
    bool FileTupleCmp(FileSlot &a, FileSlot &b)
    {
        /* TODO */
        return true;
    }

    /* 
     * Compares two slots according to sort keys. Returns true if a > b,
     * false otherwise. The function is stolen from nodeGatherMerge.c
     * (postgres) and adapted.
     */
    bool compare_slots(FileSlot &a, FileSlot &b)
    {
        bool    error = false;

        PG_TRY();
        {
            TupleTableSlot *s1 = a.slot;
            TupleTableSlot *s2 = b.slot;

            Assert(!TupIsNull(s1));
            Assert(!TupIsNull(s2));

            for (auto sort_key: sort_keys)
            {
                AttrNumber  attno = sort_key.ssup_attno;
                Datum       datum1,
                            datum2;
                bool        isNull1,
                            isNull2;
                int         compare;

                datum1 = slot_getattr(s1, attno, &isNull1);
                datum2 = slot_getattr(s2, attno, &isNull2);

                compare = ApplySortComparator(datum1, isNull1,
                                              datum2, isNull2,
                                              &sort_key);
                if (compare != 0)
                    return (compare > 0);
            }
        }
        PG_CATCH();
        {
            error = true;
        }
        PG_END_TRY();

        if (error)
            throw std::runtime_error("slots comparison failed");

        return false;
    }

public:
    MultifileMergeExecutionState(MemoryContext cxt,
                                 TupleDesc tupleDesc,
                                 std::set<int> attrs_used,
                                 std::list<SortSupportData> sort_keys,
                                 bool use_threads,
                                 bool use_mmap)
        : cxt(cxt), tupleDesc(tupleDesc), attrs_used(attrs_used),
          sort_keys(sort_keys), use_threads(use_threads), use_mmap(use_mmap),
          slots_initialized(false)
    { }

    ~MultifileMergeExecutionState()
    {
#if PG_VERSION_NUM < 110000
        /* Destroy tuple slots if any */
        for (auto it: slots)
            ExecDropSingleTupleTableSlot(it.slot);
#endif

        for (auto it: readers)
            delete it;
    }

    bool next(TupleTableSlot *slot, bool fake=false)
    {
        bool error = false;
        auto cmp = [this] (FileSlot &a, FileSlot &b) { return compare_slots(a, b); };

        if (unlikely(!slots_initialized))
        {
            /* Initialize binary heap on the first run */
            int i = 0;

            for (auto reader: readers)
            {
                FileSlot    fs;
                bool        error = false;

                PG_TRY();
                {
                    MemoryContext oldcxt;

                    oldcxt = MemoryContextSwitchTo(cxt);
#if PG_VERSION_NUM < 110000
                    fs.slot = MakeSingleTupleTableSlot(tupleDesc);
#elif PG_VERSION_NUM < 120000
                    fs.slot = MakeTupleTableSlot(tupleDesc);
#else
                    fs.slot = MakeTupleTableSlot(tupleDesc, &TTSOpsVirtual);
#endif
                    MemoryContextSwitchTo(oldcxt);
                }
                PG_CATCH();
                {
                    error = true;
                }
                PG_END_TRY();

                if (error)
                    throw std::runtime_error("failed to create a TupleTableSlot");

                if (reader->next(fs.slot))
                {
                    ExecStoreVirtualTuple(fs.slot);
                    fs.reader_id = i;
                    slots.push_back(fs);
                }
                else
                {
                    /* TODO: remove from readers */
                }
                ++i;
            }
            std::make_heap(slots.begin(), slots.end(), cmp);
            slots_initialized = true;
        }

        if (unlikely(slots.empty()))
            return false;

        const FileSlot &fs = slots.front();

        PG_TRY();
        {
            ExecCopySlot(slot, fs.slot);
            ExecClearTuple(fs.slot);
        }
        PG_CATCH();
        {
            error = true;
        }
        PG_END_TRY();
        if (error)
            throw std::runtime_error("failed to copy a virtual tuple slot");

        if (readers[fs.reader_id]->next(fs.slot))
        {
            ExecStoreVirtualTuple(fs.slot);
        }
        else
        {
#if PG_VERSION_NUM < 110000
            PG_TRY();
            {
                ExecDropSingleTupleTableSlot(fs.slot);
            }
            PG_CATCH();
            {
                error = true;
            }
            PG_END_TRY();
            if (error)
                throw std::runtime_error("failed to drop a tuple slot");
#endif
            /* TODO: remove from readers */
            std::pop_heap(slots.begin(), slots.end(), cmp);
            slots.pop_back();
        }
        std::make_heap(slots.begin(), slots.end(), cmp);
        return true;
    }

    void rescan(void)
    {
        /* TODO: clean binheap */
        for (auto reader: readers)
            reader->rescan();
        slots.clear();
        slots_initialized = false;
    }

    void add_file(const char *filename, List *rowgroups)
    {
        ParquetFdwReader *r;
        ListCell         *lc;
        std::vector<int>    rg;

        foreach (lc, rowgroups)
            rg.push_back(lfirst_int(lc));

        r = new ParquetFdwReader(0);
        r->open(filename, cxt, tupleDesc, attrs_used, use_threads, use_mmap);
        r->set_rowgroups_list(rg);
        readers.push_back(r);
    }

    void set_coordinator(ParallelCoordinator *coord)
    {
        Assert(true);   /* not supported, should never happen */
    }
};

/*
 * extract_rowgroup_filters
 *      Build a list of expressions we can use to filter out row groups.
 */
static void
extract_rowgroup_filters(List *scan_clauses,
                         std::list<RowGroupFilter> &filters)
{
    ListCell *lc;

    foreach (lc, scan_clauses)
    {
        TypeCacheEntry *tce;
        Expr       *clause = (Expr *) lfirst(lc);
        OpExpr     *expr;
        Expr       *left, *right;
        int         strategy;
        Const      *c;
        Var        *v;
        Oid         opno;

        if (IsA(clause, RestrictInfo))
            clause = ((RestrictInfo *) clause)->clause;

        if (IsA(clause, OpExpr))
        {
            expr = (OpExpr *) clause;

            /* Only interested in binary opexprs */
            if (list_length(expr->args) != 2)
                continue;

            left = (Expr *) linitial(expr->args);
            right = (Expr *) lsecond(expr->args);

            /*
             * Looking for expressions like "EXPR OP CONST" or "CONST OP EXPR"
             *
             * XXX Currently only Var as expression is supported. Will be
             * extended in future.
             */
            if (IsA(right, Const))
            {
                if (!IsA(left, Var))
                    continue;
                v = (Var *) left;
                c = (Const *) right;
                opno = expr->opno;
            }
            else if (IsA(left, Const))
            {
                /* reverse order (CONST OP VAR) */
                if (!IsA(right, Var))
                    continue;
                v = (Var *) right;
                c = (Const *) left;
                opno = get_commutator(expr->opno);
            }
            else
                continue;

            /* TODO */
            tce = lookup_type_cache(exprType((Node *) left),
                                    TYPECACHE_BTREE_OPFAMILY);
            strategy = get_op_opfamily_strategy(opno, tce->btree_opf);

            /* Not a btree family operator? */
            if (strategy == 0)
                continue;
        }
        else if (IsA(clause, Var))
        {
            /*
             * Trivial expression containing only a single boolean Var. This
             * also covers cases "BOOL_VAR = true"
             * */
            v = (Var *) clause;
            strategy = BTEqualStrategyNumber;
            c = (Const *) makeBoolConst(true, false);
        }
        else if (IsA(clause, BoolExpr))
        {
            /*
             * Similar to previous case but for expressions like "!BOOL_VAR" or
             * "BOOL_VAR = false"
             */
            BoolExpr *boolExpr = (BoolExpr *) clause;

            if (boolExpr->args && list_length(boolExpr->args) != 1)
                continue;

            if (!IsA(linitial(boolExpr->args), Var))
                continue;

            v = (Var *) linitial(boolExpr->args);
            strategy = BTEqualStrategyNumber;
            c = (Const *) makeBoolConst(false, false);
        }
        else
            continue;

        RowGroupFilter f
        {
            .attnum = v->varattno,
            .value = c,
            .strategy = strategy,
        };

        /* TODO: potentially this may throw exceptions */
        filters.push_back(f);
    }
}

/*
 * row_group_matches_filter
 *      Check if min/max values of the column of the row group match filter.
 */
static bool
row_group_matches_filter(parquet::Statistics *stats,
                         arrow::DataType *arrow_type,
                         RowGroupFilter *filter)
{
    FmgrInfo finfo;
    Datum    val = filter->value->constvalue;
    int      collid = filter->value->constcollid;
    int      strategy = filter->strategy;

    find_cmp_func(&finfo,
                  filter->value->consttype,
                  to_postgres_type(arrow_type->id()));

    switch (filter->strategy)
    {
        case BTLessStrategyNumber:
        case BTLessEqualStrategyNumber:
            {
                Datum   lower;
                int     cmpres;
                bool    satisfies;

                lower = bytes_to_postgres_type(stats->EncodeMin().c_str(),
                                               arrow_type);
                cmpres = FunctionCall2Coll(&finfo, collid, val, lower);

                satisfies =
                    (strategy == BTLessStrategyNumber      && cmpres > 0) ||
                    (strategy == BTLessEqualStrategyNumber && cmpres >= 0);

                if (!satisfies)
                    return false;
                break;
            }

        case BTGreaterStrategyNumber:
        case BTGreaterEqualStrategyNumber:
            {
                Datum   upper;
                int     cmpres;
                bool    satisfies;

                upper = bytes_to_postgres_type(stats->EncodeMax().c_str(),
                                               arrow_type);
                cmpres = FunctionCall2Coll(&finfo, collid, val, upper);

                satisfies =
                    (strategy == BTGreaterStrategyNumber      && cmpres < 0) ||
                    (strategy == BTGreaterEqualStrategyNumber && cmpres <= 0);

                if (!satisfies)
                    return false;
                break;
            }

        case BTEqualStrategyNumber:
            {
                Datum   lower,
                        upper;

                lower = bytes_to_postgres_type(stats->EncodeMin().c_str(),
                                               arrow_type);
                upper = bytes_to_postgres_type(stats->EncodeMax().c_str(),
                                               arrow_type);

                int l = FunctionCall2Coll(&finfo, collid, val, lower);
                int u = FunctionCall2Coll(&finfo, collid, val, upper);

                if (l < 0 || u > 0)
                    return false;
            }

        default:
            /* should not happen */
            Assert(true);
    }

    return true;
}

typedef enum
{
    PS_START = 0,
    PS_IDENT,
    PS_QUOTE
} ParserState;

/*
 * parse_filenames_list
 *      Parse space separated list of filenames. This function modifies 
 *      the original string.
 */
static List *
parse_filenames_list(const char *str)
{
    char       *cur = pstrdup(str);
    char       *f = cur;
    ParserState state = PS_START;
    List       *filenames = NIL;

    while (*cur)
    {
        switch (state)
        {
            case PS_START:
                switch (*cur)
                {
                    case ' ':
                        /* just skip */
                        break;
                    case '"':
                        f = cur + 1;
                        state = PS_QUOTE;
                        break;
                    default:
                        /* XXX we should check that *cur is a valid path symbol
                         * but let's skip it for now */
                        state = PS_IDENT;
                        f = cur;
                        break;
                }
                break;
            case PS_IDENT:
                switch (*cur)
                {
                    case ' ':
                        *cur = '\0';
                        filenames = lappend(filenames, makeString(f));
                        state = PS_START;
                        break;
                    default:
                        break;
                }
                break;
            case PS_QUOTE:
                switch (*cur)
                {
                    case '"':
                        *cur = '\0';
                        filenames = lappend(filenames, makeString(f));
                        state = PS_START;
                        break;
                    default:
                        break;
                }
            default:
                elog(ERROR, "parquet_fdw: unknown parse state");
        }
        cur++;
    }
    filenames = lappend(filenames, makeString(f));

    return filenames;
}

/*
 * extract_rowgroups_list
 *      Analyze query predicates and using min/max statistics determine which
 *      row groups satisfy clauses. Store resulting row group list to
 *      fdw_private.
 */
List *
extract_rowgroups_list(const char *filename,
                       TupleDesc tupleDesc,
                       std::list<RowGroupFilter> &filters,
                       uint64 *ntuples) noexcept
{
    std::unique_ptr<parquet::arrow::FileReader> reader;
    arrow::Status   status;
    List           *rowgroups = NIL;

    /* Open parquet file to read meta information */
    try
    {
        status = parquet::arrow::FileReader::Make(
                arrow::default_memory_pool(),
                parquet::ParquetFileReader::OpenFile(filename, false),
                &reader);

        if (!status.ok())
            throw Error("failed to open Parquet file %s",
                             status.message().c_str());


        auto meta = reader->parquet_reader()->metadata();
        parquet::ArrowReaderProperties  props;
        std::shared_ptr<arrow::Schema>  schema;

        status = parquet::arrow::FromParquetSchema(meta->schema(), props, &schema);
        if (!status.ok())
            throw Error("failed to convert from parquet schema: %s", status.message().c_str());

        /* Check each row group whether it matches the filters */
        for (int r = 0; r < reader->num_row_groups(); r++)
        {
            bool match = true;
            auto rowgroup = meta->RowGroup(r);

            for (auto it = filters.begin(); it != filters.end(); it++)
            {
                RowGroupFilter &filter = *it;
                AttrNumber      attnum;
                const char     *attname;

                attnum = filter.attnum - 1;
                attname = NameStr(TupleDescAttr(tupleDesc, attnum)->attname);

                /*
                 * Search for the column with the same name as filtered attribute
                 */
                for (int k = 0; k < rowgroup->num_columns(); k++)
                {
                    auto    column = rowgroup->ColumnChunk(k);
                    std::vector<std::string> path = column->path_in_schema()->ToDotVector();

                    if (strcmp(attname, path[0].c_str()) == 0)
                    {
                        /* Found it! */
                        std::shared_ptr<parquet::Statistics>  stats;
                        std::shared_ptr<arrow::Field>       field;
                        std::shared_ptr<arrow::DataType>    type;
                        MemoryContext   ccxt = CurrentMemoryContext;
                        bool            error = false;
                        char            errstr[ERROR_STR_LEN];

                        stats = column->statistics();

                        /* Convert to arrow field to get appropriate type */
                        field = schema->field(k);
                        type = field->type();

                        PG_TRY();
                        {
                            /*
                             * If at least one filter doesn't match rowgroup exclude
                             * the current row group and proceed with the next one.
                             */
                            if (stats &&
                                !row_group_matches_filter(stats.get(), type.get(), &filter))
                            {
                                match = false;
                                elog(DEBUG1, "parquet_fdw: skip rowgroup %d", r + 1);
                            }
                        }
                        PG_CATCH();
                        {
                            ErrorData *errdata;

                            MemoryContextSwitchTo(ccxt);
                            error = true;
                            errdata = CopyErrorData();
                            FlushErrorState();

                            strncpy(errstr, errdata->message, ERROR_STR_LEN);
                            FreeErrorData(errdata);
                        }
                        PG_END_TRY();
                        if (error)
                            throw Error("row group filter match failed: %s", errstr);
                        break;
                    }
                }  /* loop over columns */

                if (!match)
                    break;

            }  /* loop over filters */

            /* All the filters match this rowgroup */
            if (match)
            {
                /* TODO: PG_TRY */
                rowgroups = lappend_int(rowgroups, r);
                *ntuples += rowgroup->num_rows();
            }
        }  /* loop over rowgroups */
    }
    catch(const std::exception& e)
    {
        elog(ERROR, "parquet_fdw: failed to exctract row groups from Parquet file: %s", e.what());
    }
    return rowgroups;
}

struct FieldInfo
{
    char    name[NAMEDATALEN];
    Oid     oid;
};

/*
 * extract_parquet_fields
 *      Read parquet file and return a list of its fields
 */
static List *
extract_parquet_fields(const char *path) noexcept
{
    std::unique_ptr<parquet::arrow::FileReader> reader;
    std::shared_ptr<arrow::Schema>              schema;
    arrow::Status   status;
    List           *res = NIL;

    try
    {
        status = parquet::arrow::FileReader::Make(
                    arrow::default_memory_pool(),
                    parquet::ParquetFileReader::OpenFile(path, false),
                    &reader);
        if (!status.ok())
            throw Error("failed to open Parquet file %s",
                                 status.message().c_str());

        auto        meta = reader->parquet_reader()->metadata();
        parquet::ArrowReaderProperties props;
        FieldInfo  *fields;

        if (!parquet::arrow::FromParquetSchema(meta->schema(), props, &schema).ok())
            throw std::runtime_error("error reading parquet schema");

        fields = (FieldInfo *) exc_palloc(sizeof(FieldInfo) * schema->num_fields());

        for (int k = 0; k < schema->num_fields(); ++k)
        {
            std::shared_ptr<arrow::Field>       field;
            std::shared_ptr<arrow::DataType>    type;
            Oid     pg_type;

            /* Convert to arrow field to get appropriate type */
            field = schema->field(k);
            type = field->type();

            if (type->id() == arrow::Type::LIST)
            {
                int     subtype_id;
                Oid     pg_subtype;
                bool    error = false;

                if (type->children().size() != 1)
                    throw std::runtime_error("lists of structs are not supported");

                subtype_id = get_arrow_list_elem_type(type.get());
                pg_subtype = to_postgres_type(subtype_id);

                /* That sucks I know... */
                PG_TRY();
                {
                    pg_type = get_array_type(pg_subtype);
                }
                PG_CATCH();
                {
                    error = true;
                }
                PG_END_TRY();

                if (error)
                    throw std::runtime_error("failed to get the type of array elements");
            }
            else
            {
                pg_type = to_postgres_type(type->id());
            }

            if (pg_type != InvalidOid)
            {
                if (field->name().length() > 63)
                    throw Error("field name '%s' in '%s' is too long",
                                field->name().c_str(), path);

                memcpy(fields->name, field->name().c_str(), field->name().length() + 1);
                fields->oid = pg_type;
                res = lappend(res, fields++);
            }
            else
            {
                throw Error("cannot convert field '%s' of type '%s' in %s",
                            field->name().c_str(), type->name().c_str(), path);
            }
        }
    }
    catch (std::exception &e)
    {
        /* Destroy the reader on error */
        reader.reset();
        elog(ERROR, "parquet_fdw: %s", e.what());
    }

    return res;
}

/*
 * create_foreign_table_query
 *      Produce a query text for creating a new foreign table.
 */
char *
create_foreign_table_query(const char *tablename,
                           const char *schemaname,
                           const char *servername,
                           char **paths, int npaths,
                           List *fields, List *options)
{
    StringInfoData  str;
    ListCell       *lc;

    initStringInfo(&str);
    appendStringInfo(&str, "CREATE FOREIGN TABLE ");

    /* append table name */
    if (schemaname)
        appendStringInfo(&str, "%s.%s (",
                         schemaname, quote_identifier(tablename));
    else
        appendStringInfo(&str, "%s (", quote_identifier(tablename));

    /* append columns */
    bool is_first = true;
    foreach (lc, fields)
    {
        FieldInfo  *field = (FieldInfo *) lfirst(lc);
        char       *name = field->name;
        Oid         pg_type = field->oid;
        const char *type_name = format_type_be(pg_type);

        if (!is_first)
            appendStringInfo(&str, ", %s %s", name, type_name);
        else
        {
            appendStringInfo(&str, "%s %s", name, type_name);
            is_first = false;
        }
    }
    appendStringInfo(&str, ") SERVER %s ", servername);
    appendStringInfo(&str, "OPTIONS (filename '");

    /* list paths */
    is_first = true;
    for (int i = 0; i < npaths; ++i)
    {
        if (!is_first)
            appendStringInfoChar(&str, ' ');
        else
            is_first = false;

        appendStringInfoString(&str, paths[i]);
    }
    appendStringInfoChar(&str, '\'');

    /* list options */
    foreach(lc, options)
    {
        DefElem *def = (DefElem *) lfirst(lc);

        appendStringInfo(&str, ", %s '%s'", def->defname, defGetString(def));
    }

    appendStringInfo(&str, ")");

    return str.data;
}

static void
destroy_parquet_state(void *arg)
{
    ParquetFdwExecutionState *festate = (ParquetFdwExecutionState *) arg;

    if (festate)
        delete festate;
}

/*
 * C interface functions
 */

static List *
parse_attributes_list(char *start, Oid relid)
{
    List      *attrs = NIL;
    char      *token;
    const char *delim = std::string(" ").c_str(); /* to satisfy g++ compiler */
    AttrNumber attnum;

    while ((token = strtok(start, delim)) != NULL)
    {
        if ((attnum = get_attnum(relid, token)) == InvalidAttrNumber)
            elog(ERROR, "paruqet_fdw: invalid attribute name '%s'", token);
        attrs = lappend_int(attrs, attnum);
        start = NULL;
    }

    return attrs;
}

static List *
get_filenames_from_userfunc(const char *funcname, const char *funcarg)
{
    Jsonb      *j;
    Oid         funcid;
    List       *f = stringToQualifiedNameList(funcname);
    Datum       filenames;
    Oid         jsonboid = JSONBOID;
    Datum      *values;
    bool       *nulls;
    int         num;
    List       *res = NIL;
    ArrayType  *arr;

    j = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(funcarg)));

    funcid = LookupFuncName(f, 1, &jsonboid, false);
    filenames = OidFunctionCall1(funcid, (Datum) j);

    arr = DatumGetArrayTypeP(filenames);
    if (ARR_ELEMTYPE(arr) != TEXTOID)
        elog(ERROR, "function returned an array with non-TEXT element type");

    deconstruct_array(arr, TEXTOID, -1, false, 'i', &values, &nulls, &num);

    if (num == 0)
    {
        elog(WARNING,
             "'%s' function returned an empty array; foreign table wasn't created",
             get_func_name(funcid));
        return NIL;
    }

    for (int i = 0; i < num; ++i)
    {
        if (nulls[i])
            elog(ERROR, "user function returned an array containing NULL value(s)");
        res = lappend(res, makeString(TextDatumGetCString(values[i])));
    }

    return res;
}

static void
get_table_options(Oid relid, ParquetFdwPlanState *fdw_private)
{
	ForeignTable *table;
    ListCell     *lc;
    char         *funcname = NULL;
    char         *funcarg = NULL;

    fdw_private->use_mmap = false;
    fdw_private->use_threads = false;
    table = GetForeignTable(relid);

    foreach(lc, table->options)
    {
		DefElem    *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "filename") == 0)
        {
            fdw_private->filenames = parse_filenames_list(defGetString(def));
        }
        else if (strcmp(def->defname, "func") == 0)
        {
            funcname = defGetString(def);
        }
        else if (strcmp(def->defname, "funcarg") == 0)
        {
            funcarg = defGetString(def);
        }
        else if (strcmp(def->defname, "sorted") == 0)
        {
            fdw_private->attrs_sorted =
                parse_attributes_list(defGetString(def), relid);
        }
        else if (strcmp(def->defname, "use_mmap") == 0)
        {
            if (!parse_bool(defGetString(def), &fdw_private->use_mmap))
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("invalid value for boolean option \"%s\": %s",
                                def->defname, defGetString(def))));
        }
        else if (strcmp(def->defname, "use_threads") == 0)
        {
            if (!parse_bool(defGetString(def), &fdw_private->use_threads))
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("invalid value for boolean option \"%s\": %s",
                                def->defname, defGetString(def))));
        }
        else
            elog(ERROR, "unknown option '%s'", def->defname);
    }

    if (funcname)
    {
        if (!funcarg)
            elog(ERROR, "funcarg must be specified");

        fdw_private->filenames = get_filenames_from_userfunc(funcname, funcarg);
    }
}

extern "C" void
parquetGetForeignRelSize(PlannerInfo *root,
                         RelOptInfo *baserel,
                         Oid foreigntableid)
{
    ParquetFdwPlanState *fdw_private;

    fdw_private = (ParquetFdwPlanState *) palloc0(sizeof(ParquetFdwPlanState));
    get_table_options(foreigntableid, fdw_private);
    
    baserel->fdw_private = fdw_private;
}

static Oid
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

static void
estimate_costs(PlannerInfo *root, RelOptInfo *baserel, Cost *startup_cost,
               Cost *run_cost, Cost *total_cost)
{
    auto    fdw_private = (ParquetFdwPlanState *) baserel->fdw_private;
    double  ntuples;

    /* Use statistics if we have it */
    if (baserel->tuples)
    {
        ntuples = baserel->tuples *
            clauselist_selectivity(root,
                                   baserel->baserestrictinfo,
                                   0,
                                   JOIN_INNER,
                                   NULL);

    }
    else
    {
        /*
         * If there is no statistics then use estimate based on rows number
         * in the selected row groups.
         */
        ntuples = fdw_private->ntuples;
    }

    /*
     * Here we assume that parquet tuple cost is the same as regular tuple cost
     * even though this is probably not true in many cases. Maybe we'll come up
     * with a smarter idea later.
     */
    *run_cost = ntuples * cpu_tuple_cost;
	*startup_cost = baserel->baserestrictcost.startup;
	*total_cost = *startup_cost + *run_cost;

    baserel->rows = ntuples;
}

static void
extract_used_attributes(RelOptInfo *baserel)
{
    ParquetFdwPlanState *fdw_private = (ParquetFdwPlanState *) baserel->fdw_private;
    ListCell *lc;

    pull_varattnos((Node *) baserel->reltarget->exprs,
                   baserel->relid,
                   &fdw_private->attrs_used);

    foreach(lc, baserel->baserestrictinfo)
    {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

        pull_varattnos((Node *) rinfo->clause,
                       baserel->relid,
                       &fdw_private->attrs_used);
    }

    if (bms_is_empty(fdw_private->attrs_used))
    {
        bms_free(fdw_private->attrs_used);
        fdw_private->attrs_used = bms_make_singleton(1 - FirstLowInvalidHeapAttributeNumber);
    }
}

/*
 * cost_merge
 *      Calculate the cost of merging nfiles files. The entire logic is stolen
 *      from cost_gather_merge().
 */
static void
cost_merge(Path *path, uint32 nfiles, Cost input_startup_cost,
           Cost input_total_cost, double rows)
{
	Cost		startup_cost = 0;
	Cost		run_cost = 0;
	Cost		comparison_cost;
	double		N;
	double		logN;

    N = nfiles;
    logN = LOG2(N);

	/* Assumed cost per tuple comparison */
	comparison_cost = 2.0 * cpu_operator_cost;

	/* Heap creation cost */
	startup_cost += comparison_cost * N * logN;

	/* Per-tuple heap maintenance cost */
	run_cost += rows * comparison_cost * logN;

	/* small cost for heap management, like cost_merge_append */
	run_cost += cpu_operator_cost * rows;

	path->startup_cost = startup_cost + input_startup_cost;
	path->total_cost = (startup_cost + run_cost + input_total_cost);
}

extern "C" void
parquetGetForeignPaths(PlannerInfo *root,
                       RelOptInfo *baserel,
                       Oid foreigntableid)
{
	ParquetFdwPlanState *fdw_private;
    Path       *foreign_path;
	Cost		startup_cost;
	Cost		total_cost;
    Cost        run_cost;
    bool        is_sorted, is_multi;
    List       *pathkeys = NIL;
    RangeTblEntry  *rte;
    Relation        rel;
    TupleDesc       tupleDesc;
    std::list<RowGroupFilter> filters;
    ListCell       *lc;

    fdw_private = (ParquetFdwPlanState *) baserel->fdw_private;
    is_sorted = fdw_private->attrs_sorted != NIL;
    is_multi = list_length(fdw_private->filenames) > 1;

    /* Analyze query clauses and extract ones that can be of interest to us*/
    extract_rowgroup_filters(baserel->baserestrictinfo, filters);

    rte = root->simple_rte_array[baserel->relid];
    rel = heap_open(rte->relid, AccessShareLock);
    tupleDesc = RelationGetDescr(rel);

    /*
     * Extract list of row groups that match query clauses. Also calculate
     * approximate number of rows in result set based on total number of tuples
     * in those row groups. It isn't very precise but it is best we got.
     */
    foreach (lc, fdw_private->filenames)
    {
        char *filename = strVal((Value *) lfirst(lc));
        List *rowgroups = extract_rowgroups_list(filename, tupleDesc,
                                                 filters, &fdw_private->ntuples);

        fdw_private->rowgroups = lappend(fdw_private->rowgroups, rowgroups);
    }
    heap_close(rel, AccessShareLock);

    estimate_costs(root, baserel, &startup_cost, &run_cost, &total_cost);

    /* Collect used attributes to reduce number of read columns during scan */
    extract_used_attributes(baserel);

    fdw_private->type = is_multi ? RT_MULTI : RT_SINGLE;

    /* Build pathkeys based on attrs_sorted */
    foreach (lc, fdw_private->attrs_sorted)
    {
        Oid         relid = root->simple_rte_array[baserel->relid]->relid;
        int         attnum = lfirst_int(lc);
        Oid         typid,
                    collid;
        int32       typmod;
        Oid         sort_op;
        Var        *var;
        List       *attr_pathkey;

        /* Build an expression (simple var) */
        get_atttypetypmodcoll(relid, attnum, &typid, &typmod, &collid);
        var = makeVar(baserel->relid, attnum, typid, typmod, collid, 0);

        /* Lookup sorting operator for the attribute type */
        get_sort_group_operators(typid,
                                 true, false, false,
                                 &sort_op, NULL, NULL,
                                 NULL);

        attr_pathkey = build_expression_pathkey(root, (Expr *) var, NULL,
                                                sort_op, baserel->relids,
                                                true);
        pathkeys = list_concat(pathkeys, attr_pathkey);
    }

    /* Create a separate path with pathkeys for sorted parquet files. */
    if (is_sorted)
    {
        Path                   *path;
        ParquetFdwPlanState    *private_sort;

        private_sort = (ParquetFdwPlanState *) palloc(sizeof(ParquetFdwPlanState));
        memcpy(private_sort, fdw_private, sizeof(ParquetFdwPlanState));

        path = (Path *) create_foreignscan_path(root, baserel,
                                                NULL,	/* default pathtarget */
                                                baserel->rows,
                                                startup_cost,
                                                total_cost,
                                                pathkeys,
                                                NULL,	/* no outer rel either */
                                                NULL,	/* no extra plan */
                                                (List *) private_sort);

        /* For multifile case calculate the cost of merging files */
        if (is_multi)
        {
            private_sort->type = RT_MULTI_MERGE;

            cost_merge((Path *) path, list_length(private_sort->filenames),
                       startup_cost, total_cost, baserel->rows);
        }
        add_path(baserel, path);
    }

	foreign_path = (Path *) create_foreignscan_path(root, baserel,
                                                    NULL,	/* default pathtarget */
                                                    baserel->rows,
                                                    startup_cost,
                                                    total_cost,
                                                    NULL,   /* no pathkeys */
                                                    NULL,	/* no outer rel either */
                                                    NULL,	/* no extra plan */
                                                    (List *) fdw_private);
	add_path(baserel, (Path *) foreign_path);

    if (baserel->consider_parallel > 0)
    {
        ParquetFdwPlanState *private_parallel;

        private_parallel = (ParquetFdwPlanState *) palloc(sizeof(ParquetFdwPlanState));
        memcpy(private_parallel, fdw_private, sizeof(ParquetFdwPlanState));
        private_parallel->type = is_multi ? RT_MULTI : RT_SINGLE;

        Path *parallel_path = (Path *)
                 create_foreignscan_path(root, baserel,
                                         NULL,	/* default pathtarget */
                                         baserel->rows,
                                         startup_cost,
                                         total_cost,
                                         pathkeys,
                                         NULL,	/* no outer rel either */
                                         NULL,	/* no extra plan */
                                         (List *) private_parallel);

        int num_workers = max_parallel_workers_per_gather;

        parallel_path->rows = fdw_private->ntuples / (num_workers + 1);
        parallel_path->total_cost       = startup_cost + run_cost / num_workers;
        parallel_path->parallel_workers = num_workers;
        parallel_path->parallel_aware   = true;
        parallel_path->parallel_safe    = true;

        /* Create GatherMerge path for sorted parquet files */
        if (is_sorted)
        {
            GatherMergePath *gather_merge =
                create_gather_merge_path(root, baserel, parallel_path, NULL,
                                         pathkeys, NULL, NULL);
            add_path(baserel, (Path *) gather_merge);
        }
        add_partial_path(baserel, parallel_path);
    }
}

extern "C" ForeignScan *
parquetGetForeignPlan(PlannerInfo *root,
                      RelOptInfo *baserel,
                      Oid foreigntableid,
                      ForeignPath *best_path,
                      List *tlist,
                      List *scan_clauses,
                      Plan *outer_plan)
{
    ParquetFdwPlanState *fdw_private = (ParquetFdwPlanState *) best_path->fdw_private;
	Index		scan_relid = baserel->relid;
    List       *attrs_used = NIL;
    List       *attrs_sorted = NIL;
    AttrNumber  attr;
    List       *params = NIL;
    ListCell   *lc;

	/*
	 * We have no native ability to evaluate restriction clauses, so we just
	 * put all the scan_clauses into the plan node's qual list for the
	 * executor to check.  So all we have to do here is strip RestrictInfo
	 * nodes from the clauses and ignore pseudoconstants (which will be
	 * handled elsewhere).
	 */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

    /*
     * We can't just pass arbitrary structure into make_foreignscan() because
     * in some cases (i.e. plan caching) postgres may want to make a copy of
     * the plan and it can only make copy of something it knows of, namely
     * Nodes. So we need to convert everything in nodes and store it in a List.
     */
    attr = -1;
    while ((attr = bms_next_member(fdw_private->attrs_used, attr)) >= 0)
        attrs_used = lappend_int(attrs_used, attr);

    /* TODO: make sure that attrs_sorted is subset of attrs_used */
    foreach (lc, fdw_private->attrs_sorted)
        attrs_sorted = lappend_int(attrs_sorted, lfirst_int(lc));

    /* Packing all the data needed by executor into the list */
    params = lappend(params, fdw_private->filenames);
    params = lappend(params, attrs_used);
    params = lappend(params, attrs_sorted);
    params = lappend(params, makeInteger(fdw_private->use_mmap));
    params = lappend(params, makeInteger(fdw_private->use_threads));
    params = lappend(params, makeInteger(fdw_private->type));
    params = lappend(params, fdw_private->rowgroups);

	/* Create the ForeignScan node */
	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							NIL,	/* no expressions to evaluate */
							params,
							NIL,	/* no custom tlist */
							NIL,	/* no remote quals */
							outer_plan);
}

extern "C" void
parquetBeginForeignScan(ForeignScanState *node, int eflags)
{
    ParquetFdwExecutionState   *festate;
    MemoryContextCallback      *callback;
    MemoryContext   reader_cxt;
	ForeignScan    *plan = (ForeignScan *) node->ss.ps.plan;
	EState         *estate = node->ss.ps.state;
    List           *fdw_private = plan->fdw_private;
    List           *attrs_list;
    List           *rowgroups_list;
    ListCell       *lc, *lc2;
    List           *filenames;
    std::set<int>   attrs_used;
    List           *attrs_sorted;
    bool            use_mmap;
    bool            use_threads;
    int             i = 0;
    ReaderType      reader_type;

    /* Unwrap fdw_private */
    foreach (lc, fdw_private)
    {
        switch(i)
        {
            case 0:
                filenames = (List *) lfirst(lc);
                break;
            case 1:
                attrs_list = (List *) lfirst(lc);
                foreach (lc2, attrs_list)
                    attrs_used.insert(lfirst_int(lc2));
                break;
            case 2:
                attrs_sorted = (List *) lfirst(lc);
                break;
            case 3:
                use_mmap = (bool) intVal((Value *) lfirst(lc));
                break;
            case 4:
                use_threads = (bool) intVal((Value *) lfirst(lc));
                break;
            case 5:
                reader_type = (ReaderType) intVal((Value *) lfirst(lc));
                break;
            case 6:
                rowgroups_list = (List *) lfirst(lc);
                break;
        }
        ++i;
    }

    MemoryContext cxt = estate->es_query_cxt;
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    TupleDesc tupleDesc = slot->tts_tupleDescriptor;

    reader_cxt = AllocSetContextCreate(cxt,
                                       "parquet_fdw tuple data",
                                       ALLOCSET_DEFAULT_SIZES);

    std::list<SortSupportData> sort_keys;
    foreach (lc, attrs_sorted)
    {
        SortSupportData sort_key;
        int     attr = lfirst_int(lc);
        Oid     typid;
        int     typmod;
        Oid     collid;
        Oid     relid = RelationGetRelid(node->ss.ss_currentRelation);
        Oid     sort_op;

        memset(&sort_key, 0, sizeof(SortSupportData));

        get_atttypetypmodcoll(relid, attr, &typid, &typmod, &collid);

        sort_key.ssup_cxt = reader_cxt;
        sort_key.ssup_collation = collid;
        sort_key.ssup_nulls_first = true;
        sort_key.ssup_attno = attr;
        sort_key.abbreviate = false;

        get_sort_group_operators(typid,
                                 true, false, false,
                                 &sort_op, NULL, NULL,
                                 NULL);

        PrepareSortSupportFromOrderingOp(sort_op, &sort_key);

        try {
            sort_keys.push_back(sort_key);
        } catch (std::exception &e) {
            elog(ERROR, "parquet_fdw: scan initialization failed: %s", e.what());
        }
    }

    try
    {
        switch (reader_type)
        {
            case RT_SINGLE:
                festate = new SingleFileExecutionState(reader_cxt, tupleDesc,
                                                       attrs_used, use_threads,
                                                       use_mmap);
                break;
            case RT_MULTI:
                festate = new MultifileExecutionState(reader_cxt, tupleDesc,
                                                      attrs_used, use_threads,
                                                      use_mmap);
                break;
            case RT_MULTI_MERGE:
                festate = new MultifileMergeExecutionState(reader_cxt, tupleDesc,
                                                           attrs_used, sort_keys, 
                                                           use_threads, use_mmap);
                break;
            default:
                throw std::runtime_error("unknown reader type");
        }

        if (!filenames)
            elog(ERROR, "parquet_fdw: got an empty filenames list");

        forboth (lc, filenames, lc2, rowgroups_list)
        {
            char *filename = strVal((Value *) lfirst(lc));
            List *rowgroups = (List *) lfirst(lc2);

            festate->add_file(filename, rowgroups);
        }
    }
    catch(std::exception &e)
    {
        elog(ERROR, "parquet_fdw: %s", e.what());
    }

    /*
     * Enable automatic execution state destruction by using memory context
     * callback
     */
    callback = (MemoryContextCallback *) palloc(sizeof(MemoryContextCallback));
    callback->func = destroy_parquet_state;
    callback->arg = (void *) festate;
    MemoryContextRegisterResetCallback(reader_cxt, callback);

    node->fdw_state = festate;
}

/*
 * bytes_to_postgres_type
 *      Convert min/max values from column statistics stored in parquet file as
 *      plain bytes to postgres Datum.
 */
static Datum
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
            return Int32GetDatum(*(float *) bytes);
        case arrow::Type::DOUBLE:
            return Int64GetDatum(*(double *) bytes);
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
        case arrow::Type::DATE32:
            return DateADTGetDatum(*(int32 *) bytes +
                                   (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE));
        default:
            return PointerGetDatum(NULL);
    }
}

/*
 * find_cmp_func
 *      Find comparison function for two given types.
 */
static void
find_cmp_func(FmgrInfo *finfo, Oid type1, Oid type2)
{
    Oid cmp_proc_oid;
    TypeCacheEntry *tce_1, *tce_2;

    tce_1 = lookup_type_cache(type1, TYPECACHE_BTREE_OPFAMILY);
    tce_2 = lookup_type_cache(type2, TYPECACHE_BTREE_OPFAMILY);

    cmp_proc_oid = get_opfamily_proc(tce_1->btree_opf,
                                     tce_1->btree_opintype,
                                     tce_2->btree_opintype,
                                     BTORDER_PROC);
    fmgr_info(cmp_proc_oid, finfo);
}

extern "C" TupleTableSlot *
parquetIterateForeignScan(ForeignScanState *node)
{
    ParquetFdwExecutionState   *festate = (ParquetFdwExecutionState *) node->fdw_state;
	TupleTableSlot             *slot = node->ss.ss_ScanTupleSlot;

	ExecClearTuple(slot);
    try
    {
        festate->next(slot);
    }
    catch (std::exception &e)
    {
        elog(ERROR, "parquet_fdw: %s", e.what());
    }

    return slot;
}

extern "C" void
parquetEndForeignScan(ForeignScanState *node)
{
    /*
     * Destruction of execution state is done by memory context callback. See
     * destroy_parquet_state()
     */
}

extern "C" void
parquetReScanForeignScan(ForeignScanState *node)
{
    ParquetFdwExecutionState   *festate = (ParquetFdwExecutionState *) node->fdw_state;

    festate->rescan();
}

static int
parquetAcquireSampleRowsFunc(Relation relation, int elevel,
                             HeapTuple *rows, int targrows,
                             double *totalrows,
                             double *totaldeadrows)
{
    ParquetFdwExecutionState   *festate;
    ParquetFdwPlanState         fdw_private;
    MemoryContext               reader_cxt;
    TupleDesc       tupleDesc = RelationGetDescr(relation);
    TupleTableSlot *slot;
    std::set<int>   attrs_used;
    int             cnt = 0;
    uint64          num_rows = 0;
    ListCell       *lc;

    get_table_options(RelationGetRelid(relation), &fdw_private);

    for (int i = 0; i < tupleDesc->natts; ++i)
        attrs_used.insert(i + 1 - FirstLowInvalidHeapAttributeNumber);

    reader_cxt = AllocSetContextCreate(CurrentMemoryContext,
                                       "parquet_fdw tuple data",
                                       ALLOCSET_DEFAULT_SIZES);
    festate = new MultifileExecutionState(reader_cxt,
                                          tupleDesc,
                                          attrs_used,
                                          fdw_private.use_threads,
                                          false);

    foreach (lc, fdw_private.filenames)
    {
        char *filename = strVal((Value *) lfirst(lc));

        try
        {
            std::unique_ptr<parquet::arrow::FileReader> reader;
            arrow::Status   status;
            List           *rowgroups = NIL;

            status = parquet::arrow::FileReader::Make(
                        arrow::default_memory_pool(),
                        parquet::ParquetFileReader::OpenFile(filename, false),
                        &reader);
            if (!status.ok())
                throw Error("failed to open Parquet file: %s",
                                     status.message().c_str());
            auto meta = reader->parquet_reader()->metadata();
            num_rows += meta->num_rows();

            /* We need to scan all rowgroups */
            for (int i = 0; i < meta->num_row_groups(); ++i)
                rowgroups = lappend_int(rowgroups, i);
            festate->add_file(filename, rowgroups);
        }
        catch(const std::exception &e)
        {
            elog(ERROR, "parquet_fdw: %s", e.what());
        }
    }

    PG_TRY();
    {
        uint64  row = 0;
        int     ratio = num_rows / targrows;

        /* Set ratio to at least 1 to avoid devision by zero issue */
        ratio = ratio < 1 ? 1 : ratio;


#if PG_VERSION_NUM < 120000
        slot = MakeSingleTupleTableSlot(tupleDesc);
#else
        slot = MakeSingleTupleTableSlot(tupleDesc, &TTSOpsHeapTuple);
#endif

        while (true)
        {
            CHECK_FOR_INTERRUPTS();

            if (cnt >= targrows)
                break;

            bool fake = (row % ratio) != 0;
            ExecClearTuple(slot);
            if (!festate->next(slot, fake))
                break;

            if (!fake)
            {
                rows[cnt++] = heap_form_tuple(tupleDesc,
                                              slot->tts_values,
                                              slot->tts_isnull);
            }

            row++;
        }

        *totalrows = num_rows;
        *totaldeadrows = 0;

        ExecDropSingleTupleTableSlot(slot);
    }
    PG_CATCH();
    {
        elog(LOG, "Cancelled");
        delete festate;
        PG_RE_THROW();
    }
    PG_END_TRY();

    delete festate;

    return cnt - 1;
}

extern "C" bool
parquetAnalyzeForeignTable(Relation relation,
                           AcquireSampleRowsFunc *func,
                           BlockNumber *totalpages)
{
    *func = parquetAcquireSampleRowsFunc;
    return true;
}

/*
 * parquetExplainForeignScan
 *      Additional explain information, namely row groups list.
 */
extern "C" void
parquetExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
    List	   *fdw_private;
    ListCell   *lc, *lc2, *lc3;
    StringInfoData str;
    List       *filenames;
    List       *rowgroups_list;
    ReaderType  reader_type;

    initStringInfo(&str);

	fdw_private = ((ForeignScan *) node->ss.ps.plan)->fdw_private;
    filenames = (List *) linitial(fdw_private);
    reader_type = (ReaderType) intVal((Value *) list_nth(fdw_private, 5));
    rowgroups_list = (List *) llast(fdw_private);

    switch (reader_type)
    {
        case RT_SINGLE:
            ExplainPropertyText("Reader", "Single File", es);
            break;
        case RT_MULTI:
            ExplainPropertyText("Reader", "Multifile", es);
            break;
        case RT_MULTI_MERGE:
            ExplainPropertyText("Reader", "Multifile Merge", es);
            break;
    }

    forboth(lc, filenames, lc2, rowgroups_list)
    {
        char   *filename = strVal((Value *) lfirst(lc));
        List   *rowgroups = (List *) lfirst(lc2);
        bool    is_first = true;

        /* Only print filename if there're more than one file */
        if (list_length(filenames) > 1)
        {
            appendStringInfoChar(&str, '\n');
            appendStringInfoSpaces(&str, (es->indent + 1) * 2);

#ifdef _GNU_SOURCE
        appendStringInfo(&str, "%s: ", basename(filename));
#else
        appendStringInfo(&str, "%s: ", basename(pstrdup(filename)));
#endif
        }

        foreach(lc3, rowgroups)
        {
            /*
             * As parquet-tools use 1 based indexing for row groups it's probably
             * a good idea to output row groups numbers in the same way.
             */
            int rowgroup = lfirst_int(lc3) + 1;

            if (is_first)
            {
                appendStringInfo(&str, "%i", rowgroup);
                is_first = false;
            }
            else
                appendStringInfo(&str, ", %i", rowgroup);
        }
    }

    ExplainPropertyText("Row groups", str.data, es);
}

/* Parallel query execution */

extern "C" bool
parquetIsForeignScanParallelSafe(PlannerInfo *root, RelOptInfo *rel,
                                 RangeTblEntry *rte)
{
    /* Use parallel execution only when statistics are collected */
    return (rel->tuples > 0);
}

extern "C" Size
parquetEstimateDSMForeignScan(ForeignScanState *node, ParallelContext *pcxt)
{
    return sizeof(ParallelCoordinator);
}

extern "C" void
parquetInitializeDSMForeignScan(ForeignScanState *node, ParallelContext *pcxt,
                                void *coordinate)
{
    ParallelCoordinator        *coord = (ParallelCoordinator *) coordinate;
    ParquetFdwExecutionState   *festate;

    coord->next_rowgroup = 0;
    coord->next_reader = 0;
    festate = (ParquetFdwExecutionState *) node->fdw_state;
    festate->set_coordinator(coord);
}

extern "C" void
parquetReInitializeDSMForeignScan(ForeignScanState *node,
                                  ParallelContext *pcxt, void *coordinate)
{
    ParallelCoordinator    *coord = (ParallelCoordinator *) coordinate;

    coord->next_rowgroup = 0;
}

extern "C" void
parquetInitializeWorkerForeignScan(ForeignScanState *node,
                                   shm_toc *toc,
                                   void *coordinate)
{
    ParallelCoordinator        *coord   = (ParallelCoordinator *) coordinate;
    ParquetFdwExecutionState   *festate;

    coord = new(coordinate) ParallelCoordinator;
    festate = (ParquetFdwExecutionState *) node->fdw_state;
    festate->set_coordinator(coord);
}

extern "C" void
parquetShutdownForeignScan(ForeignScanState *node)
{
}

extern "C" List *
parquetImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid)
{
    struct dirent  *f;
    DIR            *d;
    List           *cmds = NIL;

    d = AllocateDir(stmt->remote_schema);
    if (!d)
    {
        int e = errno;

        elog(ERROR, "parquet_fdw: failed to open directory '%s': %s",
             stmt->remote_schema,
             strerror(e));
    }

    while ((f = readdir(d)) != NULL)
    {

        /* TODO: use lstat if d_type == DT_UNKNOWN */
        if (f->d_type == DT_REG)
        {
            ListCell   *lc;
            bool        skip = false;
            List       *fields;
            char       *filename = pstrdup(f->d_name);
            char       *path;
            char       *query;

            path = psprintf("%s/%s", stmt->remote_schema, filename);

            /* check that file extension is "parquet" */
            char *ext = strrchr(filename, '.');

            if (ext && strcmp(ext + 1, "parquet") != 0)
                continue;

            /*
             * Set terminal symbol to be able to run strcmp on filename
             * without file extension
             */
            *ext = '\0';

            foreach (lc, stmt->table_list)
            {
                RangeVar *rv = (RangeVar *) lfirst(lc);

                switch (stmt->list_type)
                {
                    case FDW_IMPORT_SCHEMA_LIMIT_TO:
                        if (strcmp(filename, rv->relname) != 0)
                        {
                            skip = true;
                            break;
                        }
                        break;
                    case FDW_IMPORT_SCHEMA_EXCEPT:
                        if (strcmp(filename, rv->relname) == 0)
                        {
                            skip = true;
                            break;
                        }
                        break;
                    default:
                        ;
                }
            }
            if (skip)
                continue;

            fields = extract_parquet_fields(path);
            query = create_foreign_table_query(filename, stmt->local_schema,
                                               stmt->server_name, &path, 1,
                                               fields, stmt->options);
            cmds = lappend(cmds, query);
        }

    }
    FreeDir(d);

    return cmds;
}

extern "C" Datum
parquet_fdw_validator_impl(PG_FUNCTION_ARGS)
{
    List       *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
    Oid         catalog = PG_GETARG_OID(1);
    ListCell   *lc;
    bool        filename_provided = false;
    bool        func_provided = false;
    bool        funcarg_provided = false;

    /* Only check table options */
    if (catalog != ForeignTableRelationId)
        PG_RETURN_VOID();

    foreach(lc, options_list)
    {
        DefElem    *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "filename") == 0)
        {
            char   *filename = pstrdup(defGetString(def));
            List   *filenames;
            ListCell *lc;

            filenames = parse_filenames_list(filename);

            foreach(lc, filenames)
            {
                struct stat stat_buf;
                char       *fn = strVal((Value *) lfirst(lc));

                if (stat(fn, &stat_buf) != 0)
                {
                    int e = errno;

                    ereport(ERROR,
                            (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                             errmsg("parquet_fdw: %s", strerror(e))));
                }
            }
            pfree(filenames);
            pfree(filename);
            filename_provided = true;
        }
        else if (strcmp(def->defname, "func") == 0)
        {
            /* TODO: check that this is a proper function */
            func_provided = true;
        }
        else if (strcmp(def->defname, "funcarg") == 0)
        {
            /* TODO: check that funcarg is a proper json */
            funcarg_provided = true;
        }
        else if (strcmp(def->defname, "sorted") == 0)
            ;  /* do nothing */
        else if (strcmp(def->defname, "batch_size") == 0)
            /* check that int value is valid */
            strtol(defGetString(def), NULL, 10);
        else if (strcmp(def->defname, "use_mmap") == 0)
        {
            /* Check that bool value is valid */
            bool    use_mmap;

            if (!parse_bool(defGetString(def), &use_mmap))
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("invalid value for boolean option \"%s\": %s",
                                def->defname, defGetString(def))));
        }
        else if (strcmp(def->defname, "use_threads") == 0)
        {
            /* Check that bool value is valid */
            bool    use_threads;

            if (!parse_bool(defGetString(def), &use_threads))
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("invalid value for boolean option \"%s\": %s",
                                def->defname, defGetString(def))));
        }
        else
        {
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                     errmsg("parquet_fdw: invalid option \"%s\"",
                            def->defname)));
        }
    }

    if (func_provided && !funcarg_provided)
        elog(ERROR, "parquet_fdw: funcarg is required");

    if (!filename_provided && !func_provided)
        elog(ERROR, "parquet_fdw: filename or func are required");

    PG_RETURN_VOID();
}

static List *
jsonb_to_options_list(Jsonb *options)
{
    List           *res = NIL;
	JsonbIterator  *it;
    JsonbValue      v;
    JsonbIteratorToken  type = WJB_DONE;

    if (!options)
        return NIL;

    if (!JsonContainerIsObject(&options->root))
        elog(ERROR, "options must be represented by a jsonb object");

    it = JsonbIteratorInit(&options->root);
    while ((type = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
    {
        switch (type)
        {
            case WJB_BEGIN_OBJECT:
            case WJB_END_OBJECT:
                break;
            case WJB_KEY:
                {
                    DefElem    *elem;
                    char       *key;
                    char       *val;

                    if (v.type != jbvString)
                        elog(ERROR, "expected a string key");
                    key = pnstrdup(v.val.string.val, v.val.string.len);

                    /* read value directly after key */
                    type = JsonbIteratorNext(&it, &v, false);
                    if (type != WJB_VALUE || v.type != jbvString)
                        elog(ERROR, "expected a string value");
                    val = pnstrdup(v.val.string.val, v.val.string.len);

                    elem = makeDefElem(key, (Node *) makeString(val), 0);
                    res = lappend(res, elem);

                    break;
                }
            default:
                elog(ERROR, "wrong options format");
        }
    }

    return res;
}

static List *
array_to_fields_list(ArrayType *arr)
{
    List   *res = NIL;
    int     ndims = ARR_NDIM(arr);
    int    *dims = ARR_DIMS(arr);
    Oid     elem_type = ARR_ELEMTYPE(arr);
    int16   elem_len;
    bool    elem_byval;
    char    elem_align;
    Datum  *values;
    bool   *nulls;
    int     num;

    if (ndims != 2)
        elog(ERROR, "expected 2-dimensional attributes array");

    if (dims[1] != 2)
        elog(ERROR, "each subarray expected to have 2 elements");

    if (ARR_HASNULL(arr))
        elog(ERROR, "attributes array must not contain NULLs");

    get_typlenbyvalalign(elem_type, &elem_len, &elem_byval, &elem_align);

    deconstruct_array(arr, elem_type, elem_len, elem_byval, elem_align,
                      &values, &nulls, &num);

    for (int i = 0; i < dims[0]; ++i)
    {
        FieldInfo  *field = (FieldInfo *) palloc(sizeof(FieldInfo));
        char       *attname;
        char       *typname;
        int         typmod;

        attname = text_to_cstring(DatumGetTextP(values[i * 2]));
        typname = text_to_cstring(DatumGetTextP(values[i * 2 + 1]));

        if (strlen(attname) >= NAMEDATALEN)
            elog(ERROR, "attribute name cannot be longer than %i", NAMEDATALEN - 1);

        strcpy(field->name, attname);
        parseTypeString(typname, &field->oid, &typmod, false);

        res = lappend(res, field);
    }

    return res;
}

static void
validate_import_args(const char *tablename, const char *servername, Oid funcoid)
{
    if (!tablename)
        elog(ERROR, "foreign table name is mandatory");

    if (!servername)
        elog(ERROR, "foreign server name is mandatory");

    if (!OidIsValid(funcoid))
        elog(ERROR, "function must be specified");
}

static void
import_parquet_internal(const char *tablename, const char *schemaname,
                        const char *servername, ArrayType *attrs, Oid funcid,
                        Jsonb *arg, Jsonb *options) noexcept
{
    Datum       res;
    FmgrInfo    finfo;
    ArrayType  *arr;
    Oid         ret_type;
    Oid         elem_type;
    List       *optlist;
    char       *query;

    validate_import_args(tablename, servername, funcid);

    fmgr_info(funcid, &finfo);

    ret_type = get_func_rettype(funcid);
    if (!type_is_array(ret_type))
        elog(ERROR,
             "return type of '%s' function must be array",
             get_func_name(funcid));

    if ((elem_type = get_element_type(ret_type)) == InvalidOid)
        elog(ERROR,
             "return type of '%s' function must be array of TEXT elements",
             get_func_name(funcid));

    optlist = jsonb_to_options_list(options);

    /* TODO: other validations: input arg is JSONB */

    /* Call the user provided function */
    res = FunctionCall1(&finfo, (Datum) arg);

    /*
     * In case function returns NULL the ERROR is thrown. So it's safe to
     * assume function returned something. Just for the sake of readability
     * I leave this condition
     */
    if (res != (Datum) 0)
    {
        int16   elem_len;
        bool    elem_byval;
        char    elem_align;
        Datum  *values;
        bool   *nulls;
        int     num;
        int     ret;
        List   *fields;

        arr = DatumGetArrayTypeP(res);
        get_typlenbyvalalign(elem_type, &elem_len, &elem_byval, &elem_align);

        deconstruct_array(arr, elem_type, elem_len, elem_byval, elem_align,
                          &values, &nulls, &num);

        /* TODO: check that there are no nulls */

        if (num == 0)
        {
            elog(WARNING,
                 "'%s' function returned an empty array; foreign table wasn't created",
                 get_func_name(funcid));
            return;
        }

        /* Convert values to cstring array */
        char **paths = (char **) palloc(num * sizeof(char *));
        for (int i = 0; i < num; ++i)
            paths[i] = text_to_cstring(DatumGetTextP(values[i]));

        /*
         * If attributes dict is provided then parse it. Otherwise get the list
         * from the first file provided by the user function. We trust the user
         * to provide a list of files with the same structure.
         */
        fields = attrs ? array_to_fields_list(attrs) : extract_parquet_fields(paths[0]);

        query = create_foreign_table_query(tablename, schemaname, servername,
                                           paths, num, fields, optlist);

        /* Execute query */
        if (SPI_connect() < 0)
            elog(ERROR, "parquet_fdw: SPI_connect failed");

        if ((ret = SPI_exec(query, 0)) != SPI_OK_UTILITY)
            elog(ERROR, "parquet_fdw: failed to create table '%s': %s",
                 tablename, SPI_result_code_string(ret));

        SPI_finish();
    }
}

extern "C"
{

PG_FUNCTION_INFO_V1(import_parquet);
Datum
import_parquet(PG_FUNCTION_ARGS)
{
    char       *tablename;
    char       *schemaname;
    char       *servername;
    Oid         funcid;
    Jsonb      *arg;
    Jsonb      *options;

    tablename = PG_ARGISNULL(0) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(0));
    schemaname = PG_ARGISNULL(1) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(1));
    servername = PG_ARGISNULL(2) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(2));
    funcid = PG_ARGISNULL(3) ? InvalidOid : PG_GETARG_OID(3);
    arg = PG_ARGISNULL(4) ? NULL : PG_GETARG_JSONB_P(4);
    options = PG_ARGISNULL(5) ? NULL : PG_GETARG_JSONB_P(5);

    import_parquet_internal(tablename, schemaname, servername, NULL, funcid, arg, options);

    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(import_parquet_with_attrs);
Datum
import_parquet_with_attrs(PG_FUNCTION_ARGS)
{
    char       *tablename;
    char       *schemaname;
    char       *servername;
    ArrayType  *attrs;
    Oid         funcid;
    Jsonb      *arg;
    Jsonb      *options;

    tablename = PG_ARGISNULL(0) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(0));
    schemaname = PG_ARGISNULL(1) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(1));
    servername = PG_ARGISNULL(2) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(2));
    attrs = PG_ARGISNULL(3) ? NULL : PG_GETARG_ARRAYTYPE_P(3);
    funcid = PG_ARGISNULL(4) ? InvalidOid : PG_GETARG_OID(4);
    arg = PG_ARGISNULL(5) ? NULL : PG_GETARG_JSONB_P(5);
    options = PG_ARGISNULL(6) ? NULL : PG_GETARG_JSONB_P(6);

    import_parquet_internal(tablename, schemaname, servername, attrs, funcid, arg, options);

    PG_RETURN_VOID();
}

}
