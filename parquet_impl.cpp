/*
 * Parquet processing implementation
 */

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/array.h"
#include "parquet/arrow/reader.h"
#include "parquet/exception.h"
#include "parquet/file_reader.h"

extern "C"
{
#include "postgres.h"

#include "access/sysattr.h"
#include "commands/defrem.h"
#include "executor/tuptable.h"
#include "foreign/foreign.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/relation.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "parser/parse_oper.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
}

/*
 * Just a plain C struct since we going to keep objects created by postgres
 */
struct ParquetFdwPlanState
{
    char       *filename;
    Bitmapset  *attrs_sorted;
    Bitmapset  *attrs_used; /* attributes actually used in query */
};

class ParquetFdwExecutionState
{
public:
    std::unique_ptr<parquet::arrow::FileReader> reader;

    /* Column indices */
    std::vector<int> indices;

    /* Mapping between slot attributes and parquet columns */
    std::vector<int> map;

    /* Current row group */
    std::shared_ptr<arrow::Table> table;

    uint32_t row_group;  /* current row group index */
    uint32_t row;        /* current row within row group */
    uint32_t row_num;    /* total rows in row group */

    ParquetFdwExecutionState(const char *filename)
        : row_group(0), row(0), row_num(0)
    {
        /* TODO: add exception handling */
        reader.reset(new parquet::arrow::FileReader(arrow::default_memory_pool(),
                                                    parquet::ParquetFileReader::OpenFile(filename)));
    }
};

static void *
create_parquet_state(ForeignScanState *scanstate,
                     ParquetFdwPlanState *fdw_private)
{
    /* TODO: try/catch */
    ParquetFdwExecutionState *festate;

    try
    {
        festate = new ParquetFdwExecutionState(fdw_private->filename);
    }
    catch(const std::exception& e)
    {
        elog(ERROR, "Parquet initialization failed: %s", e.what());
    }

    scanstate->fdw_state = festate;
    auto schema = festate->reader->parquet_reader()->metadata()->schema();
 
    TupleTableSlot *slot = scanstate->ss.ss_ScanTupleSlot;
    TupleDesc tupleDesc = slot->tts_tupleDescriptor;

    /* Create mapping between tuple descriptor and parquet columns */
    festate->map.resize(tupleDesc->natts);
    for (int i = 0; i < tupleDesc->natts; i++)
    {
        AttrNumber attnum = i + 1 - FirstLowInvalidHeapAttributeNumber;
        festate->map[i] = -1;

        /* Skip columns we don't intend to use in query */
        if (!bms_is_member(attnum, fdw_private->attrs_used))
            continue;

        for (int k = 0; k < schema->num_columns(); k++)
        {
            if (strcmp(NameStr(TupleDescAttr(tupleDesc, i)->attname),
                       schema->Column(k)->name().c_str()) == 0)
            {
                /* Found mapping! */
                festate->indices.push_back(k);
                festate->map[i] = festate->indices.size() - 1; /* index of last element */
                break;
            }
        }
    }

    return festate;
}

static void
release_parquet_state(void *handler)
{
    if (handler)
        delete (ParquetFdwExecutionState *) handler;
}

/*
 * C interface functions
 */

static Bitmapset *
parse_attributes_list(char *start, Oid relid)
{
    Bitmapset *attrs = NULL;
    char      *token;
    const char *delim = std::string(" ").c_str(); /* to satisfy g++ compiler */
    AttrNumber attnum;

    while ((token = strtok(start, delim)) != NULL)
    {
        attnum = get_attnum(relid, token);
        attrs = bms_add_member(attrs, attnum);
        start = NULL;
    }

    return attrs;
}

static void
get_table_options(Oid relid, ParquetFdwPlanState *fdw_private)
{
	ForeignTable *table;
    ListCell     *lc;

    table = GetForeignTable(relid);
    
    foreach(lc, table->options)
    {
		DefElem    *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "filename") == 0)
            fdw_private->filename = defGetString(def);
        else if (strcmp(def->defname, "sorted") == 0)
        {
            fdw_private->attrs_sorted =
                parse_attributes_list(defGetString(def), relid);
        }
        else
            elog(ERROR, "unknown option '%s'", def->defname);
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

static void
estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
			   Cost *startup_cost, Cost *total_cost)
{
	Cost		run_cost = 100;  /* TODO */

	*startup_cost = baserel->baserestrictcost.startup;
	*total_cost = *startup_cost + run_cost;
}

static void
extract_used_attributes(RelOptInfo *baserel, Bitmapset **attrs_used)
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
}

extern "C" void
parquetGetForeignPaths(PlannerInfo *root,
					RelOptInfo *baserel,
					Oid foreigntableid)
{
	ParquetFdwPlanState *fdw_private;
	Cost		startup_cost;
	Cost		total_cost;
    List       *pathkeys = NIL;
    ListCell   *lc;

	/* Estimate costs */
	estimate_costs(root, baserel, &startup_cost, &total_cost);

    /*
     * Collect used attributes to reduce number of read columns during scan
     */
    fdw_private = (ParquetFdwPlanState *) baserel->fdw_private;
    extract_used_attributes(baserel, &fdw_private->attrs_used);

    /* Build pathkeys based on attrs_sorted */
    int attnum = -1;
    while ((attnum = bms_next_member(fdw_private->attrs_sorted, attnum)) >= 0)
    {
        Oid         relid = root->simple_rte_array[baserel->relid]->relid;
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

        attr_pathkey = build_expression_pathkey(root, (Expr *) var, NULL, sort_op,
                                           baserel->relids, true);
        pathkeys = list_concat(pathkeys, attr_pathkey);
    }

	/*
	 * Create a ForeignPath node and add it as only possible path.  We use the
	 * fdw_private list of the path to carry the convert_selectively option;
	 * it will be propagated into the fdw_private list of the Plan node.
	 */
	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
									 NULL,	/* default pathtarget */
									 baserel->rows,
									 startup_cost,
									 total_cost,
									 pathkeys,
									 NULL,	/* no outer rel either */
									 NULL,	/* no extra plan */
									 (List *) fdw_private));
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
	Index		scan_relid = baserel->relid;

	/*
	 * We have no native ability to evaluate restriction clauses, so we just
	 * put all the scan_clauses into the plan node's qual list for the
	 * executor to check.  So all we have to do here is strip RestrictInfo
	 * nodes from the clauses and ignore pseudoconstants (which will be
	 * handled elsewhere).
	 */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Create the ForeignScan node */
	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							NIL,	/* no expressions to evaluate */
							best_path->fdw_private,
							NIL,	/* no custom tlist */
							NIL,	/* no remote quals */
							outer_plan);
}

extern "C" void
parquetBeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan *plan = (ForeignScan *) node->ss.ps.plan;

    node->fdw_state = create_parquet_state(node,
                                           (ParquetFdwPlanState *) plan->fdw_private);
}


/*
 * populate_slot
 *      Fill slot with the values from parquet row.
 */
static void
populate_slot(ParquetFdwExecutionState *festate, TupleTableSlot *slot)
{
    /* Fill slot values */
    for (int i = 0; i < slot->tts_tupleDescriptor->natts; i++)
    {
        /*
         * We only fill slot attributes if column was referred in targetlist
         * or clauses. In other cases mark attribute as NULL.
         * */
        if (festate->map[i] >= 0)
        {
            auto column = festate->table->column(festate->map[i]);
            /*
             * Each row group contains only one chunk so no reason to iterate 
             * over chunks
             */
            auto array = column->data()->chunk(0);
            int type_id = column->type()->id();

            if (array->IsNull(festate->row))
            {
                slot->tts_isnull[i] = true;
                continue;
            }

            /* Get datum depending on the column type */
            switch (type_id)
            {
                case arrow::Type::INT32:
                {
                    arrow::Int32Array* intarray = (arrow::Int32Array *) array.get();

                    int value = intarray->Value(festate->row);
                    slot->tts_values[i] = Int32GetDatum(value);
                    break;
                }
                case arrow::Type::INT64:
                {
                    arrow::Int64Array* intarray = (arrow::Int64Array *) array.get();

                    int64 value = intarray->Value(festate->row);
                    slot->tts_values[i] = Int64GetDatum(value);
                    break;
                }
                case arrow::Type::STRING:
                {
                    arrow::StringArray* stringarray = (arrow::StringArray *) array.get();

                    std::string value = stringarray->GetString(festate->row);
                    /* TODO: convert to postgres column type (TEXT, CSTRING, etc.) */
                    slot->tts_values[i] = CStringGetTextDatum(value.c_str());
                    break;
                }
                case arrow::Type::BINARY:
                {
                    arrow::BinaryArray* binarray = (arrow::BinaryArray *) array.get();
                    bytea *result;

                    std::string value = binarray->GetString(festate->row);

                    /* Build bytea */
                    result = cstring_to_text_with_len(value.data(), value.size());

                    slot->tts_values[i] = PointerGetDatum(result);
                    break;
                }
                case arrow::Type::TIMESTAMP:
                {
                    TimestampTz ts;
                    arrow::TimestampArray* tsarray = (arrow::TimestampArray *) array.get();

                    ts = time_t_to_timestamptz(
                            tsarray->Value(festate->row) / 1000000000);
                    slot->tts_values[i] = TimestampTzGetDatum(ts);
                    break;
                }
                case arrow::Type::DATE32:
                {
                    arrow::Date32Array* tsarray = (arrow::Date32Array *) array.get();

                    int32 d = tsarray->Value(festate->row);
                    slot->tts_values[i] = DateADTGetDatum(d);
                    break;
                }
                /* TODO: add other types */
                default:
                    elog(ERROR,
                         "parquet_fdw: unsupported column type: %d",
                         column->type()->id());
            }
            slot->tts_isnull[i] = false;
        }
        else
        {
            slot->tts_isnull[i] = true;
        }
    }
}

extern "C" TupleTableSlot *
parquetIterateForeignScan(ForeignScanState *node)
{
    ParquetFdwExecutionState   *festate = (ParquetFdwExecutionState *) node->fdw_state;
	TupleTableSlot     *slot = node->ss.ss_ScanTupleSlot;

	ExecClearTuple(slot);

    if (festate->row >= festate->row_num)
    {
        /* Read next row group */
        if (festate->row_group >= festate->reader->num_row_groups())
            return slot;

        festate->reader->RowGroup(festate->row_group)->ReadTable(festate->indices, &festate->table);
        festate->row = 0;
        festate->row_num = festate->table->num_rows();
        festate->row_group++;
    }

    populate_slot(festate, slot);
    festate->row++;
    ExecStoreVirtualTuple(slot);

    return slot;
}

extern "C" void
parquetEndForeignScan(ForeignScanState *node)
{
    release_parquet_state(node->fdw_state);
}

extern "C" void
parquetReScanForeignScan(ForeignScanState *node)
{
    ParquetFdwExecutionState   *festate = (ParquetFdwExecutionState *) node->fdw_state;

    festate->row_group = 0;
    festate->row = 0;
    festate->row_num = 0;
}

