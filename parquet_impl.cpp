/*
 * Parquet processing implementation
 */

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/array.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/exception.h"
#include "parquet/file_reader.h"

extern "C"
{
#include "postgres.h"

#include "access/sysattr.h"
#include "access/nbtree.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "executor/tuptable.h"
#include "foreign/foreign.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "nodes/relation.h"
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
#include "utils/timestamp.h"
#include "utils/typcache.h"
}

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
 * Just a plain C struct since we going to keep objects created by postgres
 */
struct ParquetFdwPlanState
{
    char       *filename;
    Bitmapset  *attrs_sorted;
    Bitmapset  *attrs_used;    /* attributes actually used in query */
    List       *filters;       /* list of RowGroupFilter */
};

class ParquetFdwExecutionState
{
public:
    std::unique_ptr<parquet::arrow::FileReader> reader;

    /* Column indices */
    std::vector<int> indices;

    /* Mapping between slot attributes and parquet columns */
    std::vector<int> map;

    std::vector<FmgrInfo *> castfuncs;

    /* Current row group */
    std::shared_ptr<arrow::Table> table;

    bool     initialized;

    uint32_t row_group;  /* current row group index */
    uint32_t row;        /* current row within row group */
    uint32_t row_num;    /* total rows in row group */

    ParquetFdwExecutionState(const char *filename)
        : row_group(0), row(0), row_num(0), initialized(false)
    {
        reader.reset(
                new parquet::arrow::FileReader(
                        arrow::default_memory_pool(),
                        parquet::ParquetFileReader::OpenFile(filename)));
    }
};

static void *
create_parquet_state(ForeignScanState *scanstate,
                     ParquetFdwPlanState *fdw_private)
{
    ParquetFdwExecutionState *festate;

    festate = new ParquetFdwExecutionState(fdw_private->filename);
    scanstate->fdw_state = festate;
    auto schema = festate->reader->parquet_reader()->metadata()->schema();
 
    TupleTableSlot *slot = scanstate->ss.ss_ScanTupleSlot;
    TupleDesc tupleDesc = slot->tts_tupleDescriptor;

    /* Create mapping between tuple descriptor and parquet columns. */
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
                festate->map[i] = festate->indices.size() - 1; /* index of last element */
                break;
            }
        }
    }

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
}

/*
 * extract_rowgroup_filters
 *      Build a list of expressions we can use to filter out row groups.
 */
static List *
extract_rowgroup_filters(PlannerInfo *root,
                         RelOptInfo *baserel,
                         List *scan_clauses)
{
    List     *filters = NIL;
    ListCell *lc;

    foreach (lc, scan_clauses)
    {
        TypeCacheEntry *tce;
        Node           *clause = (Node *) lfirst(lc);
        OpExpr         *expr;
        Expr           *left, *right;
        int				strategy;
        Const *c;
        Var *v;

        /* Isn't opexpr? */
        if (!IsA(clause, OpExpr))
            continue;

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
        }
        else if (IsA(left, Const))
        {
            if (!IsA(right, Var))
                continue;
            v = (Var *) right;
            c = (Const *) left;
        }
        else
            continue;

        /* TODO */
        tce = lookup_type_cache(exprType((Node *) left),
                                TYPECACHE_BTREE_OPFAMILY);
		strategy = get_op_opfamily_strategy(expr->opno, tce->btree_opf);

        /* Not a btree family operator? */
        if (strategy == 0)
            continue;

        elog(LOG, "Strategy: %d", strategy);

        RowGroupFilter *f = (RowGroupFilter *) palloc(sizeof(RowGroupFilter));
        f->attnum = v->varattno;
        f->value = c;
        f->strategy = strategy;

        filters = lappend(filters, f);
    }

    return filters;
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
    ParquetFdwPlanState *fdw_private = (ParquetFdwPlanState *) best_path->fdw_private;

	/*
	 * We have no native ability to evaluate restriction clauses, so we just
	 * put all the scan_clauses into the plan node's qual list for the
	 * executor to check.  So all we have to do here is strip RestrictInfo
	 * nodes from the clauses and ignore pseudoconstants (which will be
	 * handled elsewhere).
	 */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

    fdw_private->filters = extract_rowgroup_filters(root,
                                                    baserel,
                                                    scan_clauses);

	/* Create the ForeignScan node */
	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							NIL,	/* no expressions to evaluate */
							(List *) fdw_private,
							NIL,	/* no custom tlist */
							NIL,	/* no remote quals */
							outer_plan);
}

extern "C" void
parquetBeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan *plan = (ForeignScan *) node->ss.ps.plan;
    
    try
    {
        node->fdw_state = create_parquet_state(node,
                (ParquetFdwPlanState *) plan->fdw_private);
    }
    catch(const std::exception& e)
    {
        elog(ERROR, "parquet_fdw: parquet initialization failed: %s", e.what());
    }
}

static Oid
to_postgres_type(int arrow_type)
{
    switch (arrow_type)
    {
        case arrow::Type::INT32:
            return INT4OID;
        case arrow::Type::INT64:
            return INT8OID;
        case arrow::Type::STRING:
            return TEXTOID;
        case arrow::Type::BINARY:
            return BYTEAOID;
        case arrow::Type::TIMESTAMP:
            return TIMESTAMPTZOID;
        default:
            elog(ERROR,
                 "parquet_fdw: unsupported column type: %d",
                 arrow_type);
    }
}

static int
get_arrow_list_elem_type(std::shared_ptr<arrow::DataType> type)
{
    auto children = type->children();

    Assert(children->length() == 1);
    return children[0]->type()->id();
}

/*
 * initialize_castfuncs
 *      Check wether implicit cast will be required and prepare cast function
 *      call. For arrays find cast functions for its elements.
 */
static void
initialize_castfuncs(ForeignScanState *node)
{
    ParquetFdwExecutionState *festate = (ParquetFdwExecutionState *) node->fdw_state;
	TupleTableSlot     *slot = node->ss.ss_ScanTupleSlot;

    festate->castfuncs.resize(festate->map.size());

    for (int i = 0; i < festate->map.size(); ++i)
    {
        if (festate->map[i] < 0)
        {
            /* Null column */
            festate->castfuncs[i] = NULL;
            continue;
        }

        auto    column = festate->table->column(festate->map[i]);
        int     type_id = column->type()->id();
        int     src_type,
                dst_type;
        bool    src_is_list,
                dst_is_array;
        Oid     funcid;
        TupleDesc tupleDesc = slot->tts_tupleDescriptor;
        CoercionPathType ct;

        /* Find underlying type of list */
        src_is_list = (type_id == arrow::Type::LIST);
        if (src_is_list)
            type_id = get_arrow_list_elem_type(column->type());
        src_type = to_postgres_type(type_id);
        dst_type = TupleDescAttr(tupleDesc, i)->atttypid;

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
                            column->name().c_str()),
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
 * read_primitive_type
 *      Returns primitive type value from array
 */
static Datum
read_primitive_type(std::shared_ptr<arrow::Array> array,
                    int type_id, int64_t i,
                    FmgrInfo *castfunc)
{
    Datum   res;

    /* Get datum depending on the column type */
    switch (type_id)
    {
        case arrow::Type::INT32:
        {
            arrow::Int32Array* intarray = (arrow::Int32Array *) array.get();
            int value = intarray->Value(i);

            res = Int32GetDatum(value);
            break;
        }
        case arrow::Type::INT64:
        {
            arrow::Int64Array* intarray = (arrow::Int64Array *) array.get();
            int64 value = intarray->Value(i);

            res = Int64GetDatum(value);
            break;
        }
        case arrow::Type::STRING:
        {
            arrow::StringArray* stringarray = (arrow::StringArray *) array.get();
            std::string value = stringarray->GetString(i);

            res = CStringGetTextDatum(value.c_str());
            break;
        }
        case arrow::Type::BINARY:
        {
            arrow::BinaryArray* binarray = (arrow::BinaryArray *) array.get();
            std::string value = binarray->GetString(i);

            /* Build bytea */
            bytea *b = cstring_to_text_with_len(value.data(), value.size());

            res = PointerGetDatum(b);
            break;
        }
        case arrow::Type::TIMESTAMP:
        {
            TimestampTz ts;
            arrow::TimestampArray* tsarray = (arrow::TimestampArray *) array.get();

            ts = time_t_to_timestamptz(
                    tsarray->Value(i) / 1000000000);
            res = TimestampTzGetDatum(ts);
            break;
        }
        case arrow::Type::DATE32:
        {
            arrow::Date32Array* tsarray = (arrow::Date32Array *) array.get();
            int32 d = tsarray->Value(i);

            res = DateADTGetDatum(d);
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
 * nested_list_get_datum
 *      Returns postgres array build from elements of array. Only one
 *      dimensional arrays are supported.
 */
static Datum
nested_list_get_datum(std::shared_ptr<arrow::Array> array, int type_id,
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

    values = (Datum *) palloc0(sizeof(Datum) * array->length());
    get_typlenbyvalalign(elem_type, &elem_len, &elem_byval, &elem_align);

    /* Fill values and nulls arrays */
    for (int64_t i = 0; i < array->length(); ++i)
    {
        if (!array->IsNull(i))
            values[i] = read_primitive_type(array, type_id, i, castfunc);
        else
        {
            if (!nulls)
                nulls = (bool *) palloc0(sizeof(bool) * array->length());
            nulls[i] = true;
        }
    }

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
            int  arrow_type_id = column->type()->id();

            if (array->IsNull(festate->row))
            {
                slot->tts_isnull[i] = true;
                continue;
            }

            /* Currently only primitive types and lists are supported */
            if (arrow_type_id != arrow::Type::LIST)
            {
                slot->tts_values[i] = read_primitive_type(array,
                                                          arrow_type_id,
                                                          festate->row,
                                                          festate->castfuncs[i]);
            }
            else
            {
                arrow::ListArray* larray = (arrow::ListArray *) array.get();
                Oid pg_type_id = TupleDescAttr(slot->tts_tupleDescriptor, i)->atttypid;

                if (!type_is_array(pg_type_id))
                    elog(ERROR,
                         "cannot convert parquet column of type LIST to "
                         "postgres column of scalar type");

                /* Figure out the base element types */
                pg_type_id = get_element_type(pg_type_id);
                arrow_type_id = get_arrow_list_elem_type(column->type());

                std::shared_ptr<arrow::Array> slice =
                    larray->values()->Slice(larray->value_offset(festate->row),
                                            larray->value_length(festate->row));

                slot->tts_values[i] = nested_list_get_datum(slice,
                                                            arrow_type_id,
                                                            pg_type_id,
                                                            festate->castfuncs[i]);
            }
            slot->tts_isnull[i] = false;
        }
        else
        {
            slot->tts_isnull[i] = true;
        }
    }
}

static Datum
bytes_to_postgres_type(const char *bytes, int arrow_type)
{
    switch(arrow_type)
    {
        case arrow::Type::INT32:
            return Int32GetDatum(*(int32 *) bytes);
        case arrow::Type::INT64:
            return Int64GetDatum(*(int64 *) bytes);
        case arrow::Type::STRING:
        case arrow::Type::BINARY:
            return CStringGetTextDatum(bytes);
        default:
            return PointerGetDatum(NULL);
    }
}

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
read_next_rowgroup(ForeignScanState *node)
{
    ParquetFdwExecutionState   *festate = (ParquetFdwExecutionState *) node->fdw_state;
	ForeignScan         *plan = (ForeignScan *) node->ss.ps.plan;
    ParquetFdwPlanState *fdw_private = (ParquetFdwPlanState *) plan->fdw_private;
    std::shared_ptr<arrow::Schema> schema;

    parquet::arrow::FromParquetSchema(
            festate->reader->parquet_reader()->metadata()->schema(),
            &schema);

next_rowgroup:

    if (festate->row_group >= festate->reader->num_row_groups())
        return false;

    bool skip = false;
    auto rowgroup = festate->reader->parquet_reader()->metadata()->RowGroup(festate->row_group);
    ListCell *lc;
    
    foreach (lc, fdw_private->filters)
    {
        RowGroupFilter *filter = (RowGroupFilter *) lfirst(lc);
        int             col = festate->map[filter->attnum - 1];
        auto            colchunk = rowgroup->ColumnChunk(col);
        auto            stats = colchunk->statistics();
        std::shared_ptr<arrow::DataType> type = schema->field(col)->type();
        FmgrInfo finfo;
        Datum    val = filter->value->constvalue;
        int      collid = filter->value->constcollid;

        /* TODO: to_postgres_type returns error, have to change this behaviour */
        find_cmp_func(&finfo,
                      to_postgres_type(type->id()),
                      filter->value->consttype);

        switch (filter->strategy)
        {
            case BTLessStrategyNumber:
            case BTLessEqualStrategyNumber:
                {
                    Datum lower = bytes_to_postgres_type(stats->EncodeMin().c_str(), type->id());
                    int cmpres = FunctionCall2Coll(&finfo, collid, lower, val);
                    bool satisfies = (filter->strategy == BTLessStrategyNumber && cmpres < 0)
                        || (filter->strategy == BTLessEqualStrategyNumber && cmpres <= 0);

                    if (!satisfies)
                    {
                        elog(LOG, "skip rowgroup");
                        skip = true;
                    }
                    break;
                }
            case BTGreaterStrategyNumber:
            case BTGreaterEqualStrategyNumber:
                {
                    Datum upper = bytes_to_postgres_type(stats->EncodeMax().c_str(), type->id());
                    int cmpres = FunctionCall2Coll(&finfo, collid, upper, val);

                    bool satisfies = (filter->strategy == BTGreaterStrategyNumber && cmpres > 0)
                        || (filter->strategy == BTGreaterEqualStrategyNumber && cmpres >= 0);

                    if (!satisfies)
                    {
                        elog(LOG, "skip rowgroup");
                        skip = true;
                    }
                    break;
                }
            case BTEqualStrategyNumber:
                {
                    Datum lower = bytes_to_postgres_type(stats->EncodeMin().c_str(), type->id());
                    Datum upper = bytes_to_postgres_type(stats->EncodeMax().c_str(), type->id());

                    int l = FunctionCall2Coll(&finfo, collid, lower, val);
                    int u = FunctionCall2Coll(&finfo, collid, upper, val);

                    if (l > 0 || u < 0)
                    {
                        elog(LOG, "skip rowgroup");
                        skip = true;
                    }
                }
            default:
                continue;
        }

        /* Skip this row group */
        if (skip)
        {
            festate->row_group++;
            goto next_rowgroup;
        }
    }

    arrow::Status status = festate->reader
        ->RowGroup(festate->row_group)
        ->ReadTable(festate->indices, &festate->table);

    if (!status.ok())
        throw std::runtime_error(status.message().c_str());

    if (!festate->table)
        throw std::runtime_error("got empty table");

    festate->row = 0;
    festate->row_num = festate->table->num_rows();
    festate->row_group++;

    return true;
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

        try
        {
            if (!read_next_rowgroup(node))
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
            initialize_castfuncs(node);
    }

    populate_slot(festate, slot);
    festate->row++;
    ExecStoreVirtualTuple(slot);

    return slot;
}

extern "C" void
parquetEndForeignScan(ForeignScanState *node)
{
    delete (ParquetFdwExecutionState *) node->fdw_state;
}

extern "C" void
parquetReScanForeignScan(ForeignScanState *node)
{
    ParquetFdwExecutionState   *festate = (ParquetFdwExecutionState *) node->fdw_state;

    festate->row_group = 0;
    festate->row = 0;
    festate->row_num = 0;
}

