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


class ParallelCoordinator
{
private:
    enum Type {
        PC_SINGLE = 0,
        PC_MULTI
    };

    Type        type;
    slock_t     latch;
    union
    {
        struct
        {
            int32   reader;     /* current reader */
            int32   rowgroup;   /* current rowgroup */
            int32   nfiles;     /* number of parquet files to read */
            int32   nrowgroups[FLEXIBLE_ARRAY_MEMBER]; /* per-file rowgroups numbers */
        } single;               /* single file and simple multifile case */
        struct
        {
            int32   next_rowgroup[FLEXIBLE_ARRAY_MEMBER]; /* per-reader counters */
        } multi;   /* multimerge case */
    } data;

public:
    void lock() { SpinLockAcquire(&latch); }
    void unlock() { SpinLockRelease(&latch); }

    void init_single(int32 *nrowgroups, int32 nfiles)
    {
        type = PC_SINGLE;
        data.single.reader = -1;
        data.single.rowgroup =-1;
        data.single.nfiles = nfiles;

        SpinLockInit(&latch);
        if (nfiles)
            memcpy(data.single.nrowgroups, nrowgroups, sizeof(int32) * nfiles);
    }

    void init_multi(int nfiles)
    {
        type = PC_MULTI;
        for (int i = 0; i < nfiles; ++i)
            data.multi.next_rowgroup[i] = 0;
    }

    /* Get the next reader id. Caller must hold the lock. */
    int32 next_reader()
    {
        if (type == PC_SINGLE)
        {
            /* Return current reader if it has more rowgroups to read */
            if (data.single.reader >= 0 && data.single.reader < data.single.nfiles
                && data.single.nrowgroups[data.single.reader] > data.single.rowgroup + 1)
                return data.single.reader;

            data.single.reader++;
            data.single.rowgroup = -1;

            return data.single.reader;
        }

        Assert(false && "unsupported");
        return -1;
    }

    /* Get the next reader id. Caller must hold the lock. */
    int32 next_rowgroup(int32 reader_id)
    {
        if (type == PC_SINGLE)
        {
            if (reader_id != data.single.reader)
                return -1;
            return ++data.single.rowgroup;
        }
        else
        {
            return data.multi.next_rowgroup[reader_id]++;
        }

        Assert(false && "unsupported");
        return -1;
    }
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

    struct TypeInfo
    {
        struct
        {
            arrow::Type::type   type_id;
            std::string         type_name;
        } arrow;

        struct
        {
            Oid         oid;
            int16       len;    /*                         */
            bool        byval;  /* Only for array elements */
            char        align;  /*                         */
        } pg;

        /*
         * Cast functions from dafult postgres type defined in `to_postgres_type`
         * to actual table column type.
         */
        bool            need_cast;
        FmgrInfo       *castfunc;
        FmgrInfo       *outfunc; /* For cast via IO and for maps */
        FmgrInfo       *infunc;  /* For cast via IO              */

        /* Underlying types for complex types like list and map */
        std::vector<TypeInfo> children;

        /*
         * Column index in parquet schema. For complex types and children
         * index is equal -1. Currently only used for checking column
         * statistics.
         */
        int             index;

        TypeInfo()
            : arrow{}, pg{}, need_cast(false),
              castfunc(nullptr), outfunc(nullptr), infunc(nullptr), index(-1)
        {}

        TypeInfo(TypeInfo &&ti)
            : arrow(ti.arrow), pg(ti.pg), need_cast(ti.need_cast),
              castfunc(ti.castfunc), outfunc(ti.outfunc), infunc(ti.infunc),
              children(std::move(ti.children)), index(-1)
        {}

        TypeInfo(std::shared_ptr<arrow::DataType> arrow_type, Oid typid=InvalidOid)
            : TypeInfo()
        {
            arrow.type_id = arrow_type->id();
            arrow.type_name = arrow_type->name();
            pg.oid = typid;
            pg.len = 0;
            pg.byval = false;
            pg.align = 0;
        }
    };

protected:
    std::string                     filename;

    /* The reader identifier needed for parallel execution */
    int32_t                         reader_id;

    std::unique_ptr<parquet::arrow::FileReader> reader;

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

    std::vector<std::string>        column_names;
    std::vector<TypeInfo>           types;

    /* Coordinator for parallel query execution */
    ParallelCoordinator            *coordinator;

    /*
     * List of row group indexes to scan
     */
    std::vector<int>                rowgroups;

    std::unique_ptr<FastAllocator>  allocator;

    /*
     * libparquet options
     */
    bool    use_threads;
    bool    use_mmap;

    /* Wether object is properly initialized */
    bool    initialized;

protected:
    Datum do_cast(Datum val, const TypeInfo &typinfo);
    Datum read_primitive_type(arrow::Array *array, const TypeInfo &typinfo,
                              int64_t i);
    Datum nested_list_to_datum(arrow::ListArray *larray, int pos, const TypeInfo &typinfo);
    Datum map_to_datum(arrow::MapArray *maparray, int pos, const TypeInfo &typinfo);
    FmgrInfo *find_castfunc(arrow::Type::type src_type, Oid dst_type,
                            const char *attname);
    FmgrInfo *find_outfunc(Oid typoid);
    FmgrInfo *find_infunc(Oid typoid);
    void initialize_cast(TypeInfo &typinfo, const char *attname);
    template<typename T> inline void copy_to_c_array(T *values,
                                                     const arrow::Array *array,
                                                     int elem_size);
    template <typename T> inline const T* GetPrimitiveValues(const arrow::Array& arr);

public:
    ParquetReader(MemoryContext cxt);
    virtual ~ParquetReader() = 0;
    virtual ReadStatus next(TupleTableSlot *slot, bool fake=false) = 0;
    virtual void rescan() = 0;
    virtual void open() = 0;
    virtual void close() = 0;

    int32_t id();
    void create_column_mapping(TupleDesc tupleDesc, const std::set<int> &attrs_used);
    void set_rowgroups_list(const std::vector<int> &rowgroups);
    void set_options(bool use_threads, bool use_mmap);
    void set_coordinator(ParallelCoordinator *coord);
};

ParquetReader *create_parquet_reader(const char *filename,
                                     MemoryContext cxt,
                                     int reader_id = -1,
                                     bool caching = false);

#endif
