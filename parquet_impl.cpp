/*
 * Parquet processing implementation
 */

#include <list>
#include <set>

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
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "executor/tuptable.h"
#include "foreign/foreign.h"
#include "foreign/fdwapi.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "nodes/relation.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "parser/parse_coerce.h"
#include "parser/parse_oper.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"
}

#define SEGMENT_SIZE (1024 * 1024)

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

static void find_cmp_func(FmgrInfo *finfo, Oid type1, Oid type2);
static Datum bytes_to_postgres_type(const char *bytes, arrow::DataType *arrow_type);

bool parquet_fdw_use_threads = true;

/*
 * Restriction
 */
struct RowGroupFilter
{
    AttrNumber  attnum;
    Const      *value;
    int         strategy;
};

/*
 * Plain C struct for fdw_state
 */
struct ParquetFdwPlanState
{
    char       *filename;
    Bitmapset  *attrs_sorted;
    Bitmapset  *attrs_used;    /* attributes actually used in query */
    bool        use_mmap;
    bool        use_threads;
    List       *rowgroups;
    uint64      ntuples;
};

struct ChunkInfo
{
    int     chunk;      /* current chunk number */
    int64   pos;        /* current pos within chunk */
    int64   len;        /* current chunk length */
};

struct ParallelCoordinator
{
    pg_atomic_uint32 next_rowgroup; 
};

class ParquetFdwExecutionState
{
public:
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

    /*
     * Special memory segment to speed up bytea/Text allocations.
     */
    MemoryContext                   segments_cxt;
    char                           *segment_start_ptr;
    char                           *segment_cur_ptr;
    char                           *segment_last_ptr;
    std::list<char *>               garbage_segments;

    /* Coordinator for parallel query execution */
    ParallelCoordinator            *coordinator;

    /* Wether object is properly initialized */
    bool     initialized;

    ParquetFdwExecutionState(const char *filename, bool use_mmap)
        : row_group(-1), row(0), num_rows(0), coordinator(NULL),
          initialized(false)
    {
        reader.reset(
            new parquet::arrow::FileReader(
                    arrow::default_memory_pool(),
                    parquet::ParquetFileReader::OpenFile(filename, use_mmap)));
    }
};

static ParquetFdwExecutionState *
create_parquet_state(const char *filename,
                     TupleDesc tupleDesc,
                     MemoryContext parent_cxt,
                     std::set<int> &attrs_used,
                     bool use_mmap,
                     bool use_threads)
{
    ParquetFdwExecutionState *festate;

    festate = new ParquetFdwExecutionState(filename, use_mmap);
    auto schema = festate->reader->parquet_reader()->metadata()->schema();

    if (!parquet::arrow::FromParquetSchema(schema, &festate->schema).ok())
        elog(ERROR, "parquet_fdw: error reading parquet schema");

    /* Enable parallel columns decoding/decompression if needed */
    festate->reader->set_use_threads(use_threads && parquet_fdw_use_threads);
 
    /* Create mapping between tuple descriptor and parquet columns. */
    festate->map.resize(tupleDesc->natts);
    for (int i = 0; i < tupleDesc->natts; i++)
    {
        AttrNumber attnum = i + 1 - FirstLowInvalidHeapAttributeNumber;

        festate->map[i] = -1;

        /* Skip columns we don't intend to use in query */
        if (attrs_used.find(attnum) == attrs_used.end())
            continue;

        for (int k = 0; k < schema->num_columns(); k++)
        {
            parquet::schema::NodePtr node = schema->Column(k)->schema_node();
            std::vector<std::string> path = node->path()->ToDotVector();

            /*
             * Compare postgres attribute name to the top level column name in
             * parquet.
             *
             * XXX If we will ever want to support structs then this should be
             * changed.
             */
            if (strcmp(NameStr(TupleDescAttr(tupleDesc, i)->attname),
                       path[0].c_str()) == 0)
            {
                /* Found mapping! */
                festate->indices.push_back(k);

                /* index of last element */
                festate->map[i] = festate->indices.size() - 1; 

                festate->types.push_back(festate->schema->field(k)->type().get());
                break;
            }
        }
    }

    festate->has_nulls = (bool *) palloc(sizeof(bool) * festate->map.size());

    festate->segments_cxt = AllocSetContextCreate(parent_cxt,
                                                  "parquet_fdw tuple data",
                                                  ALLOCSET_DEFAULT_SIZES);
    festate->segment_start_ptr = NULL;
    festate->segment_cur_ptr = NULL;
    festate->segment_last_ptr = NULL;

    return festate;
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
        if ((attnum = get_attnum(relid, token)) == InvalidAttrNumber)
            elog(ERROR, "paruqet_fdw: invalid attribute name '%s'", token);
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

    fdw_private->use_mmap = false;
    fdw_private->use_threads = false;
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

        filters.push_back(f);
    }
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
 * row_group_matches_filter
 *      Check if min/max values of the column of the row group match filter.
 */
static bool
row_group_matches_filter(parquet::RowGroupStatistics *stats,
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

/*
 * extract_rowgroups_list
 *      Analyze query predicates and using min/max statistics determine which
 *      row groups satisfy clauses. Store resulting row group list to
 *      fdw_private.
 */
static void
extract_rowgroups_list(PlannerInfo *root, RelOptInfo *baserel)
{
    std::unique_ptr<parquet::arrow::FileReader> reader;
    std::list<RowGroupFilter>       filters;
    RangeTblEntry  *rte;
    Relation        rel;
    TupleDesc       tupleDesc;
    auto            fdw_private = (ParquetFdwPlanState *) baserel->fdw_private;

    /*
     * Open relation to be able to access tuple descriptor
     */
    rte = root->simple_rte_array[baserel->relid];
    rel = heap_open(rte->relid, AccessShareLock);
    tupleDesc = RelationGetDescr(rel);

    /* Analyze query clauses and extract ones that can be of interest to us*/
    extract_rowgroup_filters(baserel->baserestrictinfo, filters);

    /* Open parquet file to read meta information */
    try
    {
        reader.reset(
            new parquet::arrow::FileReader(
                    arrow::default_memory_pool(),
                    parquet::ParquetFileReader::OpenFile(fdw_private->filename,
                                                         false)));
    }
    catch(const std::exception& e)
    {
        elog(ERROR, "parquet_fdw: parquet initialization failed: %s", e.what());
    }
    auto meta = reader->parquet_reader()->metadata();

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
                    std::shared_ptr<parquet::RowGroupStatistics>  stats;
                    std::shared_ptr<arrow::Field>       field;
                    std::shared_ptr<arrow::DataType>    type;
                    const auto  &schema_node = *meta->schema()->group_node();

                    stats = column->statistics();

                    /* Convert to arrow field to get appropriate type */
                    parquet::arrow::NodeToField(*schema_node.field(k), &field);
                    type = field->type();

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
                    break;
                }
            }  /* loop over columns */

            if (!match)
                break;

        }  /* loop over filters */
        
        /* All the filters match this rowgroup */
        if (match)
        {
            fdw_private->rowgroups = lappend_int(fdw_private->rowgroups, r);
            fdw_private->ntuples += rowgroup->num_rows();
        } 
    }  /* loop over rowgroups */

    heap_close(rel, AccessShareLock);
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

extern "C" void
parquetGetForeignPaths(PlannerInfo *root,
					RelOptInfo *baserel,
					Oid foreigntableid)
{
	ParquetFdwPlanState *fdw_private;
	Cost		startup_cost;
	Cost		total_cost;
    Cost        run_cost;
    List       *pathkeys = NIL;
    ListCell   *lc;

    fdw_private = (ParquetFdwPlanState *) baserel->fdw_private;

    /*
     * Extract list of row groups that match query clauses. Also calculate
     * approximate number of rows in result set based on total number of tuples
     * in those row groups. It isn't very precise but it is best we got.
     */
    extract_rowgroups_list(root, baserel);
    estimate_costs(root, baserel, &startup_cost, &run_cost, &total_cost);

    /* Collect used attributes to reduce number of read columns during scan */
    extract_used_attributes(baserel);

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

        attr_pathkey = build_expression_pathkey(root, (Expr *) var, NULL,
                                                sort_op, baserel->relids,
                                                true);
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

    if (max_parallel_workers_per_gather > 0)
    {
        Path *parallel_path = (Path *)
                 create_foreignscan_path(root, baserel,
                                         NULL,	/* default pathtarget */
                                         baserel->rows,
                                         startup_cost,
                                         total_cost,
									     NULL,
                                         NULL,	/* no outer rel either */
                                         NULL,	/* no extra plan */
                                         (List *) fdw_private);

        int num_workers = max_parallel_workers_per_gather;

        parallel_path->rows = fdw_private->ntuples / (num_workers + 1);
        parallel_path->total_cost       = startup_cost + run_cost / num_workers;
        parallel_path->parallel_aware   = true;
        parallel_path->parallel_workers = num_workers;
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
    AttrNumber  attr;
    List       *params;

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

    params = list_make5(makeString(fdw_private->filename),
                        attrs_used,
                        makeInteger(fdw_private->use_mmap),
                        makeInteger(fdw_private->use_threads),
                        fdw_private->rowgroups);

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
    ParquetFdwExecutionState *festate; 
	ForeignScan    *plan = (ForeignScan *) node->ss.ps.plan;
	EState         *estate = node->ss.ps.state;
    List           *fdw_private = plan->fdw_private;
    List           *attrs_used_list;
    List           *rowgroups_list;
    ListCell       *lc;
    char           *filename;
    std::set<int>   attrs_used;
    bool            use_mmap; 
    bool            use_threads;

    /* Unwrap fdw_private */
    filename = strVal((Value *) linitial(fdw_private));

    attrs_used_list = (List *) lsecond(fdw_private);
    foreach (lc, attrs_used_list)
        attrs_used.insert(lfirst_int(lc));

    use_mmap = (bool) intVal((Value *) lthird(fdw_private));
    use_threads = (bool) intVal((Value *) lfourth(fdw_private));

    MemoryContext cxt = estate->es_query_cxt;
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    TupleDesc tupleDesc = slot->tts_tupleDescriptor;

    try
    {
        festate = create_parquet_state(filename,
                                       tupleDesc,
                                       cxt,
                                       attrs_used,
                                       use_mmap,
                                       use_threads);
    }
    catch(const std::exception& e)
    {
        elog(ERROR, "parquet_fdw: parquet initialization failed: %s", e.what());
    }

    rowgroups_list = (List *) llast(fdw_private);
    foreach (lc, rowgroups_list)
        festate->rowgroups.push_back(lfirst_int(lc));

    node->fdw_state = festate;
}

static int
get_arrow_list_elem_type(arrow::DataType *type)
{
    auto children = type->children();

    Assert(children.size() == 1);
    return children[0]->type()->id();
}

/*
 * initialize_castfuncs
 *      Check wether implicit cast will be required and prepare cast function
 *      call. For arrays find cast functions for its elements.
 */
static void
initialize_castfuncs(ParquetFdwExecutionState *festate, TupleDesc tupleDesc)
{
    festate->castfuncs.resize(festate->map.size());

    for (int i = 0; i < festate->map.size(); ++i)
    {
        int arrow_col = festate->map[i];

        if (festate->map[i] < 0)
        {
            /* Null column */
            festate->castfuncs[i] = NULL;
            continue;
        }

        arrow::DataType *type = festate->types[arrow_col];
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
            elog(ERROR, "parquet_fdw: unsupported column type: %s",
                 type->name().c_str());

        /* Find underlying type of array */
        dst_is_array = type_is_array(dst_type);
        if (dst_is_array)
            dst_type = get_element_type(dst_type);

        /* Make sure both types are compatible */
        if (src_is_list != dst_is_array)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
                     errmsg("parquet_fdw: incompatible types in column \"%s\"",
                            festate->table->column(arrow_col)->name().c_str()),
                     errhint(src_is_list ?
                         "parquet column is of type list while postgres type is scalar" :
                         "parquet column is of scalar type while postgres type is array")));
        }

        if (IsBinaryCoercible(src_type, dst_type))
        {
            festate->castfuncs[i] = NULL;
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
                    festate->castfuncs[i] = (FmgrInfo *) palloc0(sizeof(FmgrInfo));
                    fmgr_info(funcid, festate->castfuncs[i]);
                    MemoryContextSwitchTo(oldctx);
                    break;
                }
            case COERCION_PATH_RELABELTYPE:
            case COERCION_PATH_COERCEVIAIO:  /* TODO: double check that we
                                              * shouldn't do anything here*/
                /* Cast is not needed */
                festate->castfuncs[i] = NULL;
                break;
            default:
                elog(ERROR, "parquet_fdw: cast function is not found");
        }
    }
    festate->initialized = true;
}

/*
 * fast_alloc
 *      Preallocate a big memory segment and distribute blocks from it. When
 *      segment is exhausted it is added to garbage_segments list and freed
 *      on the next executor's iteration. If requested size is bigger that
 *      SEGMENT_SIZE then just palloc is used.
 */
static inline void *
fast_alloc(ParquetFdwExecutionState *festate, Size size)
{
    void   *ret;

    /* If allocation is bigger than segment then just palloc */
    if (size > SEGMENT_SIZE)
        return palloc(size);

    size = MAXALIGN(size);

    /* If there is not enough space in current segment create a new one */
    if (festate->segment_last_ptr - festate->segment_cur_ptr < size)
    {
        MemoryContext oldcxt;
        
        /*
         * Recycle the last segment at the next iteration (if there
         * was one)
         */
        if (festate->segment_start_ptr)
            festate->garbage_segments.
                push_back(festate->segment_start_ptr);

        oldcxt = MemoryContextSwitchTo(festate->segments_cxt);
        festate->segment_start_ptr = (char *) palloc(SEGMENT_SIZE);
        festate->segment_cur_ptr = festate->segment_start_ptr;
        festate->segment_last_ptr =
            festate->segment_start_ptr + SEGMENT_SIZE - 1;
        MemoryContextSwitchTo(oldcxt);
    }

    ret = (void *) festate->segment_cur_ptr;
    festate->segment_cur_ptr += size;

    return ret;
}

/*
 * read_primitive_type
 *      Returns primitive type value from arrow array
 */
static Datum
read_primitive_type(ParquetFdwExecutionState *festate,
                    arrow::Array *array,
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
        case arrow::Type::STRING:
        case arrow::Type::BINARY:
        {
            arrow::BinaryArray *binarray = (arrow::BinaryArray *) array;

            int32_t vallen = 0;
            const char *value = reinterpret_cast<const char*>(binarray->GetValue(i, &vallen));

            /* Build bytea */
            int64 bytea_len = vallen + VARHDRSZ;
            bytea *b = (bytea *) fast_alloc(festate, bytea_len);
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
            elog(ERROR,
                 "parquet_fdw: unsupported column type: %d",
                 type_id);
    }

    /* Call cast function if needed */
    if (castfunc != NULL)
        return FunctionCall1(castfunc, res);

    return res;
}

/*
 * GetPrimitiveValues
 *      Get plain C value array. Copy-pasted from Arrow.
 */
template <typename T>
inline const T* GetPrimitiveValues(const arrow::Array& arr) {
  if (arr.length() == 0) {
    return nullptr;
  }
  const auto& prim_arr = arrow::internal::checked_cast<const arrow::PrimitiveArray&>(arr);
  const T* raw_values = reinterpret_cast<const T*>(prim_arr.values()->data());
  return raw_values + arr.offset();
}

/* 
 * copy_to_c_array
 *      memcpy plain values from Arrow array to a C array.
 */
template<typename T> inline void
copy_to_c_array(T *values, const arrow::Array *array, int elem_size)
{
    const T *in = GetPrimitiveValues<T>(*array);

    memcpy(values, in, elem_size * array->length());
}

/*
 * nested_list_get_datum
 *      Returns postgres array build from elements of array. Only one
 *      dimensional arrays are supported.
 */
static Datum
nested_list_get_datum(ParquetFdwExecutionState *festate,
                      arrow::Array *array, int type_id,
                      Oid elem_type, FmgrInfo *castfunc)
{
    ArrayType  *res;
    Datum      *values;
    bool       *nulls = NULL;
    int16       elem_len;
    bool        elem_byval;
    char        elem_align;
    int         dims[1];
    int         lbs[1];

    values = (Datum *) fast_alloc(festate, sizeof(Datum) * array->length());
    get_typlenbyvalalign(elem_type, &elem_len, &elem_byval, &elem_align);

    /* Fill values and nulls arrays */
    if (array->null_count() == 0 && type_id == arrow::Type::INT64)
    {
        /*
         * Ok, there are no nulls, so probably we could just memcpy the
         * entire array.
         *
         * Warning: the code below is based on the assumption that Datum is
         * 8 bytes long, which is true for most contemporary systems but this
         * will not work on some exotic or really old systems. In this case
         * the entire "if" branch should just be removed.
         */
        copy_to_c_array<int64_t>((int64_t *) values, array, elem_len);
        goto construct_array;
    }
    for (int64_t i = 0; i < array->length(); ++i)
    {
        if (!array->IsNull(i))
            values[i] = read_primitive_type(festate, array, type_id, i, castfunc);
        else
        {
            if (!nulls)
            {
                Size size = sizeof(bool) * array->length();

                nulls = (bool *) fast_alloc(festate, size);
                memset(nulls, 0, size);
            }
            nulls[i] = true;
        }
    }

construct_array:
    /* Construct one dimensional array */
    dims[0] = array->length();
    lbs[0] = 1;
    res = construct_md_array(values, nulls, 1, dims, lbs,
                             elem_type, elem_len, elem_byval, elem_align);

    return PointerGetDatum(res);
}

/*
 * populate_slot
 *      Fill slot with the values from parquet row.
 *
 * If `fake` set to true the actual reading and populating the slot is skipped.
 * The purpose of this feature is to correctly skip rows to collect sparse
 * samples.
 */
static void
populate_slot(ParquetFdwExecutionState *festate,
              TupleTableSlot *slot,
              bool fake=false)
{
    /* Fill slot values */
    for (int attr = 0; attr < slot->tts_tupleDescriptor->natts; attr++)
    {
        int arrow_col = festate->map[attr];
        /*
         * We only fill slot attributes if column was referred in targetlist
         * or clauses. In other cases mark attribute as NULL.
         * */
        if (arrow_col >= 0)
        {
            ChunkInfo &chunkInfo = festate->chunk_info[arrow_col];
            arrow::Array       *array = festate->chunks[arrow_col];
            arrow::DataType    *arrow_type = festate->types[arrow_col];
            int                 arrow_type_id = arrow_type->id();
            MemoryContext       oldcxt;

            chunkInfo.len = array->length();

            if (chunkInfo.pos >= chunkInfo.len)
            {
                auto column = festate->table->column(arrow_col);

                /* There are no more chunks */
                if (++chunkInfo.chunk >= column->data()->num_chunks())
                    break;

                array = column->data()->chunk(chunkInfo.chunk).get();
                festate->chunks[arrow_col] = array;
                chunkInfo.pos = 0;
                chunkInfo.len = array->length();
            }

            /* Don't do actual reading data into slot in fake mode */
            if (fake)
                continue;

            /* Currently only primitive types and lists are supported */
            if (arrow_type_id != arrow::Type::LIST)
            {
                int64   offset;

                if (festate->has_nulls[arrow_col] && array->IsNull(chunkInfo.pos))
                {
                    slot->tts_isnull[attr] = true;
                }
                else
                {
                    slot->tts_values[attr] = 
                        read_primitive_type(festate,
                                            array,
                                            arrow_type_id,
                                            chunkInfo.pos,
                                            festate->castfuncs[attr]);
                    slot->tts_isnull[attr] = false;
                }
            }
            else
            {
                Oid     pg_type_id;
                int64   row;

                pg_type_id = TupleDescAttr(slot->tts_tupleDescriptor, attr)->atttypid;
                if (!type_is_array(pg_type_id))
                    elog(ERROR,
                         "parquet_fdw: cannot convert parquet column of type "
                         "LIST to postgres column of scalar type");

                /* Figure out the base element types */
                pg_type_id = get_element_type(pg_type_id);
                arrow_type_id = get_arrow_list_elem_type(arrow_type);

                int64 pos = chunkInfo.pos;
                arrow::ListArray   *larray = (arrow::ListArray *) array;

                if (festate->has_nulls[arrow_col] && array->IsNull(pos))
                {
                    slot->tts_isnull[attr] = true;
                }
                else
                {
                    std::shared_ptr<arrow::Array> slice =
                        larray->values()->Slice(larray->value_offset(pos),
                                                larray->value_length(pos));

                    slot->tts_values[attr] =
                        nested_list_get_datum(festate,
                                              slice.get(),
                                              arrow_type_id,
                                              pg_type_id,
                                              festate->castfuncs[attr]);
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

static bool
read_next_rowgroup(ParquetFdwExecutionState *festate, TupleDesc tupleDesc)
{
    ParallelCoordinator        *coord;
    arrow::Status               status;

    coord = festate->coordinator;

    /*
     * Use atomic increment for parallel query or just regular one for single
     * threaded execution.
     */
    if (coord)
        festate->row_group = pg_atomic_fetch_add_u32(&coord->next_rowgroup, 1);
    else
        festate->row_group++;

    if (festate->row_group >= festate->rowgroups.size())
        return false;

    int  rowgroup = festate->rowgroups[festate->row_group];
    auto rowgroup_meta = festate->reader
                            ->parquet_reader()
                            ->metadata()
                            ->RowGroup(rowgroup);

    /* Determine which columns has null values */
    for (int i = 0; i < festate->map.size(); i++)
    {
        std::shared_ptr<parquet::RowGroupStatistics>  stats;
        int arrow_col = festate->map[i];

        if (arrow_col < 0)
            continue;

        stats = rowgroup_meta
            ->ColumnChunk(festate->indices[arrow_col])
            ->statistics();

        if (stats)
            festate->has_nulls[arrow_col] = (stats->null_count() > 0);
        else
            festate->has_nulls[arrow_col] = true;
    }

    status = festate->reader
        ->RowGroup(rowgroup)
        ->ReadTable(festate->indices, &festate->table);

    if (!status.ok())
        throw std::runtime_error(status.message().c_str());

    if (!festate->table)
        throw std::runtime_error("got empty table");

    /* Fill festate->columns and festate->types */
    /* TODO: don't clear each time */
    festate->chunk_info.clear();
    festate->chunks.clear();
    for (int i = 0; i < tupleDesc->natts; i++)
    {
        if (festate->map[i] >= 0)
        {
            ChunkInfo chunkInfo = { .chunk = 0, .pos = 0, .len = 0 };
            auto column = festate->table->column(festate->map[i]);

            festate->chunk_info.push_back(chunkInfo);
            festate->chunks.push_back(column->data()->chunk(0).get());
        }
    }

    festate->row = 0;
    festate->num_rows = festate->table->num_rows();

    return true;
}

extern "C" TupleTableSlot *
parquetIterateForeignScan(ForeignScanState *node)
{
    ParquetFdwExecutionState   *festate = (ParquetFdwExecutionState *) node->fdw_state;
	TupleTableSlot             *slot = node->ss.ss_ScanTupleSlot;
    ParallelCoordinator        *coord = festate->coordinator;

	ExecClearTuple(slot);

    /* recycle old segments if any */
    if (!festate->garbage_segments.empty())
    {
        for (auto it : festate->garbage_segments)
            pfree(it);
        festate->garbage_segments.clear();
        elog(DEBUG1, "parquet_fdw: garbage segments recycled");
    }

    if (festate->row >= festate->num_rows)
    {
        /* Read next row group */
        try
        {
            if (!read_next_rowgroup(festate, slot->tts_tupleDescriptor))
                return slot;
        }
        catch(const std::exception& e)
        {
            elog(ERROR,
                 "parquet_fdw: failed to read row group %d: %s",
                 festate->row_group, e.what());
        }

        /* Lookup cast funcs */
        if (!festate->initialized)
            initialize_castfuncs(festate, slot->tts_tupleDescriptor);
    }

    populate_slot(festate, slot);
    festate->row++;
    ExecStoreVirtualTuple(slot);

    return slot;
}

extern "C" void
parquetEndForeignScan(ForeignScanState *node)
{
    ParquetFdwExecutionState *festate = (ParquetFdwExecutionState *) node->fdw_state;

    delete festate;
}

extern "C" void
parquetReScanForeignScan(ForeignScanState *node)
{
    ParquetFdwExecutionState   *festate = (ParquetFdwExecutionState *) node->fdw_state;

    festate->row_group = 0;
    festate->row = 0;
    festate->num_rows = 0;
}

static int
parquetAcquireSampleRowsFunc(Relation relation, int elevel,
                             HeapTuple *rows, int targrows,
                             double *totalrows,
                             double *totaldeadrows)
{
    ParquetFdwExecutionState   *festate;
    ParquetFdwPlanState         fdw_private;
    TupleDesc       tupleDesc = RelationGetDescr(relation);
    TupleTableSlot *slot;
    std::set<int>   attrs_used;
    int cnt = 0;

    get_table_options(RelationGetRelid(relation), &fdw_private);

    for (int i = 0; i < tupleDesc->natts; ++i)
        attrs_used.insert(i + 1 - FirstLowInvalidHeapAttributeNumber);

    /* Open parquet file and build execution state */
    try
    {
        festate = create_parquet_state(fdw_private.filename,
                                       tupleDesc,
                                       CurrentMemoryContext,
                                       attrs_used,
                                       false,
                                       fdw_private.use_threads);
    }
    catch(const std::exception& e)
    {
        elog(ERROR, "parquet_fdw: parquet initialization failed: %s", e.what());
    }

    PG_TRY();
    {
        auto meta = festate->reader->parquet_reader()->metadata();
        int ratio = meta->num_rows() / targrows;

        /* Set ratio to at least 1 to avoid devision by zero issue */
        ratio = ratio < 1 ? 1 : ratio;

        /* We need to scan all rowgroups */
        for (int i = 0; i < meta->num_row_groups(); ++i)
            festate->rowgroups.push_back(i);

        slot = MakeSingleTupleTableSlot(tupleDesc);
        initialize_castfuncs(festate, tupleDesc);

        while (true)
        {
            CHECK_FOR_INTERRUPTS();

            if (cnt >= targrows)
                break;

            /* recycle old segments if any */
            if (!festate->garbage_segments.empty())
            {
                for (auto it : festate->garbage_segments)
                    pfree(it);
                festate->garbage_segments.clear();
                elog(DEBUG1, "parquet_fdw: garbage segments recycled");
            }

            if (festate->row >= festate->num_rows)
            {
                /* Read next row group */
                try
                {
                    if (!read_next_rowgroup(festate, tupleDesc))
                        break;
                }
                catch(const std::exception& e)
                {
                    elog(ERROR,
                         "parquet_fdw: failed to read row group %d: %s",
                         festate->row_group, e.what());
                }
            }

            bool fake = (festate->row % ratio) != 0;

            populate_slot(festate, slot, fake);

            if (!fake)
            {
                rows[cnt++] = heap_form_tuple(tupleDesc,
                                              slot->tts_values,
                                              slot->tts_isnull);
            }

            festate->row++;
        }

        *totalrows = meta->num_rows();
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
parquetAnalyzeForeignTable (Relation relation,
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
    List       *rowgroups;
    ListCell   *lc;
    StringInfoData str;
    bool        is_first = true;

    initStringInfo(&str);

	fdw_private = ((ForeignScan *) node->ss.ps.plan)->fdw_private;
    rowgroups = (List *) llast(fdw_private);

    foreach(lc, rowgroups)
    {
        /*
         * As parquet-tools use 1 based indexing for row groups it's probably
         * a good idea to output row groups numbers in the same way.
         */
        int rowgroup = lfirst_int(lc) + 1;

        if (is_first)
        {
            appendStringInfo(&str, "%i", rowgroup);
            is_first = false;
        }
        else
            appendStringInfo(&str, ", %i", rowgroup);
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

    pg_atomic_write_u32(&coord->next_rowgroup, 0);
    festate = (ParquetFdwExecutionState *) node->fdw_state;
    festate->coordinator = coord;
}

extern "C" void
parquetReInitializeDSMForeignScan(ForeignScanState *node,
                                  ParallelContext *pcxt, void *coordinate)
{
    ParallelCoordinator    *coord = (ParallelCoordinator *) coordinate;

    pg_atomic_write_u32(&coord->next_rowgroup, 0);
}

extern "C" void
parquetInitializeWorkerForeignScan(ForeignScanState *node,
                                   shm_toc *toc,
                                   void *coordinate)
{
    ParallelCoordinator        *coord   = (ParallelCoordinator *) coordinate;
    ParquetFdwExecutionState   *festate;

    festate = (ParquetFdwExecutionState *) node->fdw_state;
    festate->coordinator = coord;
}

extern "C" void
parquetShutdownForeignScan(ForeignScanState *node)
{
    ParquetFdwExecutionState   *festate;

    festate = (ParquetFdwExecutionState *) node->fdw_state;
    festate->coordinator = NULL;
}

