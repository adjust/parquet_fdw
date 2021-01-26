#ifndef PARQUET_FDW_READER_HPP
#define PARQUET_FDW_READER_HPP

#include <memory>
#include <mutex>
#include <set>
#include <vector>

#include "arrow/api.h"
#include "parquet/arrow/reader.h"

extern "C"
{
#include "postgres.h"
#include "fmgr.h"
#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "nodes/pg_list.h"
#include "storage/spin.h"
}


struct ParallelCoordinator
{
    slock_t     lock;
    int32       next_reader;
    int32       next_rowgroup;
};

class FastAllocator;

enum ReadStatus
{
    RS_SUCCESS = 0,
    RS_INACTIVE = 1,
    RS_EOF = 2
};

class ParquetReader
{
protected:
    struct PgTypeInfo
    {
        Oid     oid;

        /* For array types. elem_type == InvalidOid means type is not an array */
        Oid     elem_type;
        int16   elem_len;
        bool    elem_byval;
        char    elem_align;
    };

    struct ArrowTypeInfo
    {
        arrow::Type::type   type_id;

        /* For lists: elem_type_id == arrow::Type::NA means type is not a list */
        arrow::Type::type   elem_type_id;

        /*
         * Textual representation of the type corresponding to type_id (if it's
         * not a list) or elem_type_id (otherwise).
         */
        std::string         type_name;
    };

protected:
    std::string                     filename;

    /* The reader identifier needed for parallel execution */
    int32_t                         reader_id;

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

    /* TODO: probably unite those things into single object */
    std::vector<std::string>        column_names;
    std::vector<PgTypeInfo>         pg_types;
    std::vector<ArrowTypeInfo>      arrow_types;

    bool           *has_nulls;          /* per-column info on nulls */

    /* Coordinator for parallel query execution */
    ParallelCoordinator            *coordinator;

    /*
     * List of row group indexes to scan
     */
    std::vector<int>                rowgroups;

    FastAllocator                  *allocator;

    /*
     * libparquet options
     */
     bool    use_threads;
     bool    use_mmap;

    /* Wether object is properly initialized */
    bool     initialized;

protected:
    Datum read_primitive_type(arrow::Array *array, int type_id,
                              int64_t i, FmgrInfo *castfunc);
    Datum nested_list_get_datum(arrow::Array *array, int arrow_type,
                                PgTypeInfo *pg_type, FmgrInfo *castfunc);
    void initialize_castfuncs(TupleDesc tupleDesc);
    template<typename T> inline void copy_to_c_array(T *values,
                                                     const arrow::Array *array,
                                                     int elem_size);
    template <typename T> inline const T* GetPrimitiveValues(const arrow::Array& arr);

public:
    virtual ~ParquetReader() = 0;
    virtual ReadStatus next(TupleTableSlot *slot, bool fake=false) = 0;
    virtual void rescan() = 0;
    virtual void open() = 0;
    virtual void close() = 0;

    int32_t id();
    void create_column_mapping(TupleDesc tupleDesc, std::set<int> &attrs_used);
    void set_rowgroups_list(const std::vector<int> &rowgroups);
    void set_options(bool use_threads, bool use_mmap);
    void set_coordinator(ParallelCoordinator *coord);
};

ParquetReader *create_parquet_reader(const char *filename,
                                     MemoryContext cxt,
                                     int reader_id = -1,
                                     bool caching = false);

#endif
