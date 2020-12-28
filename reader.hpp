#ifndef PARQUET_READER_HPP
#define PARQUET_READER_HPP

#include <memory>
#include <mutex>
#include <set>
#include <vector>

extern "C"
{
#include "postgres.h"

#include "nodes/pg_list.h"
#include "access/tupdesc.h"
#include "executor/tuptable.h"
}


struct ParallelCoordinator
{
    std::mutex  lock;
    int32       next_reader;
    int32       next_rowgroup;
};


class ParquetReader
{
public:
    /* Coordinator for parallel query execution */
    ParallelCoordinator            *coordinator;

public:
    virtual bool next(TupleTableSlot *slot, bool fake=false) = 0;
    virtual void rescan() = 0;
    virtual void set_rowgroups_list(const std::vector<int> &rowgroups) = 0;
    virtual void open(const char *filename,
              MemoryContext cxt,
              TupleDesc tupleDesc,
              std::set<int> &attrs_used,
              bool use_threads,
              bool use_mmap) = 0;
    virtual ~ParquetReader() = 0;
};

ParquetReader *parquet_reader_create(int reader_id = -1);

#endif
