#include "postgres.h"
#include "fmgr.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_table.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "optimizer/planmain.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/elog.h"


PG_MODULE_MAGIC;

void _PG_init(void);

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
extern int parquetAcquireSampleRowsFunc(Relation relation, int elevel,
                             HeapTuple *rows, int targrows,
                             double *totalrows,
                             double *totaldeadrows);
extern bool parquetAnalyzeForeignTable (Relation relation,
                            AcquireSampleRowsFunc *func,
                            BlockNumber *totalpages);
extern void parquetExplainForeignScan(ForeignScanState *node, ExplainState *es);
extern bool parquetIsForeignScanParallelSafe(PlannerInfo *root, RelOptInfo *rel,
                                             RangeTblEntry *rte);
extern Size parquetEstimateDSMForeignScan(ForeignScanState *node,
                                          ParallelContext *pcxt);
extern void parquetInitializeDSMForeignScan(ForeignScanState *node,
                                            ParallelContext *pcxt,
                                            void *coordinate);
extern void parquetReInitializeDSMForeignScan(ForeignScanState *node,
                                              ParallelContext *pcxt,
                                              void *coordinate);
extern void parquetInitializeWorkerForeignScan(ForeignScanState *node,
                                               shm_toc *toc,
                                               void *coordinate);
extern void parquetShutdownForeignScan(ForeignScanState *node);
extern List *parquetImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid);
extern Datum parquet_fdw_validator_impl(PG_FUNCTION_ARGS);

/* GUC variable */
extern bool parquet_fdw_use_threads;
extern bool enable_multifile;
extern bool enable_multifile_merge;

void
_PG_init(void)
{
	DefineCustomBoolVariable("parquet_fdw.use_threads",
							"Enables use_thread option",
							NULL,
							&parquet_fdw_use_threads,
							true,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("parquet_fdw.enable_multifile",
							"Enables Multifile reader",
							NULL,
							&enable_multifile,
							true,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("parquet_fdw.enable_multifile_merge",
							"Enables Multifile Merge reader",
							NULL,
							&enable_multifile_merge,
							true,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);
}

PG_FUNCTION_INFO_V1(parquet_fdw_validator);
Datum
parquet_fdw_validator(PG_FUNCTION_ARGS)
{
    return parquet_fdw_validator_impl(fcinfo);
}

PG_FUNCTION_INFO_V1(parquet_fdw_handler);
Datum
parquet_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *fdwroutine = makeNode(FdwRoutine);

    fdwroutine->GetForeignRelSize = parquetGetForeignRelSize;
    fdwroutine->GetForeignPaths = parquetGetForeignPaths;
    fdwroutine->GetForeignPlan = parquetGetForeignPlan;
    fdwroutine->BeginForeignScan = parquetBeginForeignScan;
    fdwroutine->IterateForeignScan = parquetIterateForeignScan;
    fdwroutine->ReScanForeignScan = parquetReScanForeignScan;
    fdwroutine->EndForeignScan = parquetEndForeignScan;
    fdwroutine->AnalyzeForeignTable = parquetAnalyzeForeignTable;
    fdwroutine->ExplainForeignScan = parquetExplainForeignScan;
    fdwroutine->IsForeignScanParallelSafe = parquetIsForeignScanParallelSafe;
    fdwroutine->EstimateDSMForeignScan = parquetEstimateDSMForeignScan;
    fdwroutine->InitializeDSMForeignScan = parquetInitializeDSMForeignScan;
    fdwroutine->ReInitializeDSMForeignScan = parquetReInitializeDSMForeignScan;
    fdwroutine->InitializeWorkerForeignScan = parquetInitializeWorkerForeignScan;
    fdwroutine->ShutdownForeignScan = parquetShutdownForeignScan;
    fdwroutine->ImportForeignSchema = parquetImportForeignSchema;

    PG_RETURN_POINTER(fdwroutine);
}

