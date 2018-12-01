#include "postgres.h"
#include "fmgr.h"

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "catalog/pg_foreign_table.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/sampling.h"

//#include "parquet_common.h"

PG_MODULE_MAGIC;


//void *create_parquet_state(ForeignScanState *scanstate, const char *filename);
//void release_parquet_state(void *handler);

/* FDW routines */
extern void parquetGetForeignRelSize(PlannerInfo *root,
					  RelOptInfo *baserel,
					  Oid foreigntableid);
extern void parquetGetForeignPaths(PlannerInfo *root,
					RelOptInfo *baserel,
					Oid foreigntableid);
extern ForeignScan *parquetGetForeignPlan(PlannerInfo *root,
                      RelOptInfo *baserel,
                      Oid foreigntableid,
                      ForeignPath *best_path,
                      List *tlist,
                      List *scan_clauses,
                      Plan *outer_plan);
extern TupleTableSlot *parquetIterateForeignScan(ForeignScanState *node);
extern void parquetBeginForeignScan(ForeignScanState *node, int eflags);
extern void parquetEndForeignScan(ForeignScanState *node);
extern void parquetReScanForeignScan(ForeignScanState *node);

//static void parquetBeginForeignScan(ForeignScanState *node, int eflags);
//static TupleTableSlot *parquetIterateForeignScan(ForeignScanState *node);
//static void parquetEndForeignScan(ForeignScanState *node);


PG_FUNCTION_INFO_V1(parquet_fdw_handler);
Datum
parquet_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	fdwroutine->GetForeignRelSize = parquetGetForeignRelSize;
	fdwroutine->GetForeignPaths = parquetGetForeignPaths;
	fdwroutine->GetForeignPlan = parquetGetForeignPlan;
	//fdwroutine->ExplainForeignScan = parquetExplainForeignScan;
	fdwroutine->BeginForeignScan = parquetBeginForeignScan;
	fdwroutine->IterateForeignScan = parquetIterateForeignScan;
	fdwroutine->ReScanForeignScan = parquetReScanForeignScan;
	fdwroutine->EndForeignScan = parquetEndForeignScan;
	//fdwroutine->AnalyzeForeignTable = parquetAnalyzeForeignTable;
	//fdwroutine->IsForeignScanParallelSafe = parquetIsForeignScanParallelSafe;

	PG_RETURN_POINTER(fdwroutine);
}

