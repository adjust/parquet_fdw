#include "exec_state.hpp"
#include "heap.hpp"

#include <functional>
#include <list>


#if PG_VERSION_NUM < 110000
#define MakeTupleTableSlotCompat(tupleDesc) MakeSingleTupleTableSlot(tupleDesc)
#elif PG_VERSION_NUM < 120000
#define MakeTupleTableSlotCompat(tupleDesc) MakeTupleTableSlot(tupleDesc)
#else
#define MakeTupleTableSlotCompat(tupleDesc) MakeTupleTableSlot(tupleDesc, &TTSOpsVirtual)
#endif

/*
 * More compact form of common PG_TRY/PG_CATCH block which throws a c++
 * exception in case of errors.
 */
#define PG_TRY_INLINE(code_block, err) \
    do { \
        bool error = false; \
        PG_TRY(); \
        code_block \
        PG_CATCH(); { error = true; } \
        PG_END_TRY(); \
        if (error) { throw std::runtime_error(err); } \
    } while(0)


class SingleFileExecutionState : public ParquetFdwExecutionState
{
private:
    ParquetReader      *reader;
    MemoryContext       cxt;
    TupleDesc           tuple_desc;
    std::set<int>       attrs_used;
    bool                use_mmap;
    bool                use_threads;

public:
    MemoryContext       estate_cxt;

    SingleFileExecutionState(MemoryContext cxt,
                             TupleDesc tuple_desc,
                             std::set<int> attrs_used,
                             bool use_threads,
                             bool use_mmap)
        : cxt(cxt), tuple_desc(tuple_desc), attrs_used(attrs_used),
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

        foreach (lc, rowgroups)
            rg.push_back(lfirst_int(lc));

        reader = parquet_reader_create(filename, cxt);
        reader->set_options(use_threads, use_mmap);
        reader->set_rowgroups_list(rg);
        reader->open();
        reader->create_column_mapping(tuple_desc, attrs_used);
    }

    void set_coordinator(ParallelCoordinator *coord)
    {
        if (reader)
            reader->set_coordinator(coord);
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
    ParquetReader          *reader;

    std::vector<FileRowgroups> files;
    uint64_t                cur_reader;

    MemoryContext           cxt;
    TupleDesc               tuple_desc;
    std::set<int>           attrs_used;
    bool                    use_threads;
    bool                    use_mmap;

    ParallelCoordinator    *coord;

private:
    ParquetReader *get_next_reader()
    {
        ParquetReader *r;

        if (coord)
        {
            int32 reader_id;

            SpinLockAcquire(&coord->lock);

            /*
             * First let's check if the file other workers are reading has more
             * rowgroups to read
             */
            reader_id = coord->next_reader - 1;
            if (reader_id >= 0 && reader_id < (int) files.size()
                && (int) files[reader_id].rowgroups.size() > coord->next_rowgroup) {
                /* yep */;
            } else {
                /* If that's not the case then open the next file */
                reader_id = coord->next_reader++;
                coord->next_rowgroup = 0;
            }
            this->cur_reader = reader_id;
            SpinLockRelease(&coord->lock);
        }

        if (cur_reader >= files.size())
            return NULL;

        r = parquet_reader_create(files[cur_reader].filename.c_str(), cxt, cur_reader);
        r->set_rowgroups_list(files[cur_reader].rowgroups);
        r->set_options(use_threads, use_mmap);
        r->set_coordinator(coord);
        r->open();
        r->create_column_mapping(tuple_desc, attrs_used);

        cur_reader++;

        return r;
    }

public:
    MultifileExecutionState(MemoryContext cxt,
                            TupleDesc tuple_desc,
                            std::set<int> attrs_used,
                            bool use_threads,
                            bool use_mmap)
        : reader(NULL), cur_reader(0), cxt(cxt), tuple_desc(tuple_desc),
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

class MultifileMergeExecutionStateBase : public ParquetFdwExecutionState
{
protected:
    struct ReaderSlot
    {
        int             reader_id;
        TupleTableSlot *slot;
    };

protected:
    std::vector<ParquetReader *> readers;

    MemoryContext       cxt;
    TupleDesc           tuple_desc;
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
    Heap<ReaderSlot>    slots;
    bool                slots_initialized;

protected:
    /*
     * compare_slots
     *      Compares two slots according to sort keys. Returns true if a > b,
     *      false otherwise. The function is stolen from nodeGatherMerge.c
     *      (postgres) and adapted.
     */
    bool compare_slots(const ReaderSlot &a, const ReaderSlot &b)
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

        return false;
    }
};

class MultifileMergeExecutionState : public MultifileMergeExecutionStateBase
{
private:
    /*
     * initialize_slots
     *      Initialize slots binary heap on the first run.
     */
    void initialize_slots()
    {
        std::function<bool(const ReaderSlot &, const ReaderSlot &)> cmp =
            [this] (const ReaderSlot &a, const ReaderSlot &b) { return compare_slots(a, b); };
        int i = 0;

        slots.init(readers.size(), cmp);
        for (auto reader: readers)
        {
            ReaderSlot    rs;

            PG_TRY_INLINE(
                {
                    MemoryContext oldcxt;

                    oldcxt = MemoryContextSwitchTo(cxt);
                    rs.slot = MakeTupleTableSlotCompat(tuple_desc);
                    MemoryContextSwitchTo(oldcxt);
                }, "failed to create a TupleTableSlot"
            );

            if (reader->next(rs.slot))
            {
                ExecStoreVirtualTuple(rs.slot);
                rs.reader_id = i;
                slots.append(rs);
            }
            ++i;
        }
        PG_TRY_INLINE({ slots.heapify(); }, "heapify failed");
        slots_initialized = true;
    }

public:
    MultifileMergeExecutionState(MemoryContext cxt,
                                 TupleDesc tuple_desc,
                                 std::set<int> attrs_used,
                                 std::list<SortSupportData> sort_keys,
                                 bool use_threads,
                                 bool use_mmap)
    {
        this->cxt = cxt;
        this->tuple_desc = tuple_desc;
        this->attrs_used = attrs_used;
        this->sort_keys = sort_keys;
        this->use_threads = use_threads;
        this->use_mmap = use_mmap;
        this->slots_initialized = false;
    }

    ~MultifileMergeExecutionState()
    {
#if PG_VERSION_NUM < 110000
        /* Destroy tuple slots if any */
        for (int i = 0; i < slots.size(); i++)
            ExecDropSingleTupleTableSlot(slots[i].slot);
#endif

        for (auto it: readers)
            delete it;
    }

    bool next(TupleTableSlot *slot, bool /* fake=false */)
    {
        if (unlikely(!slots_initialized))
            initialize_slots();

        if (unlikely(slots.empty()))
            return false;

        /* Copy slot with the smallest key into the resulting slot */
        const ReaderSlot &head = slots.head();
        PG_TRY_INLINE(
            {
                ExecCopySlot(slot, head.slot);
                ExecClearTuple(head.slot);
            }, "failed to copy a virtual tuple slot"
        );

        /*
         * Try to read another record from the same reader as in the head slot.
         * In case of success the new record makes it into the heap and the
         * heap gets reheapified. Else if there are no more records in the
         * reader then current head is removed from the heap and heap gets
         * reheapified.
         */
        if (readers[head.reader_id]->next(head.slot))
        {
            ExecStoreVirtualTuple(head.slot);
            PG_TRY_INLINE({ slots.heapify_head(); }, "heapify failed");
        }
        else
        {
#if PG_VERSION_NUM < 110000
            /* Release slot resources */
            PG_TRY_INLINE(
                {
                    ExecDropSingleTupleTableSlot(head.slot);
                }, "failed to drop a tuple slot"
            );
#endif
            slots.pop();
        }
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
        ParquetReader      *r;
        ListCell           *lc;
        std::vector<int>    rg;

        foreach (lc, rowgroups)
            rg.push_back(lfirst_int(lc));

        r = parquet_reader_create(filename, cxt);
        r->set_rowgroups_list(rg);
        r->set_options(use_threads, use_mmap);
        r->open();
        r->create_column_mapping(tuple_desc, attrs_used);
        readers.push_back(r);
    }

    void set_coordinator(ParallelCoordinator * /* coord */)
    {
        Assert(true);   /* not supported, should never happen */
    }
};


ParquetFdwExecutionState *create_parquet_execution_state(ReaderType reader_type,
                                                         MemoryContext reader_cxt,
                                                         TupleDesc tuple_desc,
                                                         std::set<int> &attrs_used,
                                                         std::list<SortSupportData> sort_keys,
                                                         bool use_threads,
                                                         bool use_mmap)
{
    switch (reader_type)
    {
        case RT_SINGLE:
            return new SingleFileExecutionState(reader_cxt, tuple_desc,
                                                attrs_used, use_threads,
                                                use_mmap);
        case RT_MULTI:
            return new MultifileExecutionState(reader_cxt, tuple_desc,
                                               attrs_used, use_threads,
                                               use_mmap);
        case RT_MULTI_MERGE:
            return new MultifileMergeExecutionState(reader_cxt, tuple_desc,
                                                    attrs_used, sort_keys, 
                                                    use_threads, use_mmap);
        default:
            throw std::runtime_error("unknown reader type");
    }
}
