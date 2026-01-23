/*
 * Parquet processing implementation
 */
// basename comes from string.h on Linux,
// but from libgen.h on other POSIX systems (see man basename)
// dirname is always from libgen.h
#include <libgen.h>
#include <stdlib.h>  /* for realpath */

#include <sys/stat.h>
#include <math.h>
#include <list>
#include <set>
#include <glob.h>

#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/file_reader.h"
#include "parquet/statistics.h"

#include "exec_state.hpp"
#include "reader.hpp"
#include "common.hpp"

extern "C"
{
#include "postgres.h"

#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
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
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "parser/parse_coerce.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/typcache.h"

#if PG_VERSION_NUM < 120000
#include "nodes/relation.h"
#include "optimizer/var.h"
#else
#include "access/table.h"
#include "optimizer/optimizer.h"
#endif

#if PG_VERSION_NUM < 110000
#include "catalog/pg_am.h"
#else
#include "catalog/pg_am_d.h"
#endif

#if PG_VERSION_NUM >= 120000
#include "nodes/pathnodes.h"
#endif

#if PG_VERSION_NUM >= 180000
#include "commands/explain_format.h"
#include "commands/explain_state.h"
#endif
}


/* from costsize.c */
#define LOG2(x)  (log(x) / 0.693147180559945)

#if PG_VERSION_NUM < 110000
#define PG_GETARG_JSONB_P PG_GETARG_JSONB
#endif

/*
 * PG 18 added disabled_nodes parameter to create_foreignscan_path.
 * Use this macro for the extra argument.
 */
#if PG_VERSION_NUM >= 180000
#define CREATE_FOREIGNSCAN_DISABLED_NODES  0,
#else
#define CREATE_FOREIGNSCAN_DISABLED_NODES
#endif


bool enable_multifile;
bool enable_multifile_merge;
char *parquet_fdw_allowed_directories = NULL;


static void find_cmp_func(FmgrInfo *finfo, Oid type1, Oid type2);
static void destroy_parquet_state(void *arg);
static List * lappend_globbed_filenames(List *filenames, const char *filename);


/*
 * Restriction
 */
struct RowGroupFilter
{
    AttrNumber  attnum;
    bool        is_key; /* for maps */
    Const      *value;
    int         strategy;
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
    int32       max_open_files;
    bool        files_in_order;
    List       *rowgroups;      /* List of Lists (per filename) */
    uint64      matched_rows;
    ReaderType  type;
};

static int
get_strategy(Oid type, Oid opno, Oid am)
{
    Oid opclass;
    Oid opfamily;

    opclass = GetDefaultOpClass(type, am);

    if (!OidIsValid(opclass))
        return 0;

    opfamily = get_opclass_family(opclass);

    return get_op_opfamily_strategy(opno, opfamily);
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
        Expr       *clause = (Expr *) lfirst(lc);
        OpExpr     *expr;
        Expr       *left, *right;
        int         strategy;
        bool        is_key = false;
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

            /* Not a btree family operator? */
            if ((strategy = get_strategy(v->vartype, opno, BTREE_AM_OID)) == 0)
            {
                /*
                 * Maybe it's a gin family operator? (We only support
                 * jsonb 'exists' operator at the moment)
                 */
                if ((strategy = get_strategy(v->vartype, opno, GIN_AM_OID)) == 0
                    || strategy != JsonbExistsStrategyNumber)
                    continue;
                is_key = true;
            }
        }
        else if (IsA(clause, Var))
        {
            /*
             * Trivial expression containing only a single boolean Var. This
             * also covers cases "BOOL_VAR = true"
             */
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
            .is_key = is_key,
            .value = c,
            .strategy = strategy,
        };

        /* potentially inserting elements may throw exceptions */
        bool error = false;
        try {
            filters.push_back(f);
        } catch (std::exception &e) {
            error = true;
        }
        if (error)
            elog(ERROR, "extracting row filters failed");
    }
}

static Const *
convert_const(Const *c, Oid dst_oid)
{
    Oid         funcid;
    CoercionPathType ct;

    ct = find_coercion_pathway(dst_oid, c->consttype,
                               COERCION_EXPLICIT, &funcid);
    switch (ct)
    {
        case COERCION_PATH_FUNC:
            {
                FmgrInfo    finfo;
                Const      *newc;
                int16       typlen;
                bool        typbyval;

                get_typlenbyval(dst_oid, &typlen, &typbyval);

                newc = makeConst(dst_oid,
                                 0,
                                 c->constcollid,
                                 typlen,
                                 0,
                                 c->constisnull,
                                 typbyval);
                fmgr_info(funcid, &finfo);
                newc->constvalue = FunctionCall1(&finfo, c->constvalue);

                return newc;
            }
        case COERCION_PATH_RELABELTYPE:
            /* Cast is not needed */
            break;
        case COERCION_PATH_COERCEVIAIO:
            {
                /*
                 * In this type of cast we need to output the value to a string
                 * and then feed this string to the input function of the
                 * target type.
                 */
                Const  *newc;
                int16   typlen;
                bool    typbyval;
                Oid     input_fn, output_fn;
                Oid     input_param;
                bool    isvarlena;
                char   *str;

                /* Construct a new Const node */
                get_typlenbyval(dst_oid, &typlen, &typbyval);
                newc = makeConst(dst_oid,
                                 0,
                                 c->constcollid,
                                 typlen,
                                 0,
                                 c->constisnull,
                                 typbyval);

                /* Get IO functions */
                getTypeOutputInfo(c->consttype, &output_fn, &isvarlena);
                getTypeInputInfo(dst_oid, &input_fn, &input_param);

                str = OidOutputFunctionCall(output_fn, c->constvalue);
                newc->constvalue = OidInputFunctionCall(input_fn, str,
                                                        input_param, 0);

                return newc;
            }
        default:
            elog(ERROR, "cast function to %s is not found",
                 format_type_be(dst_oid));
    }
    return c;
}

/*
 * row_group_matches_filter
 *      Check if min/max values of the column of the row group match filter.
 */
static bool
row_group_matches_filter(parquet::Statistics *stats,
                         const arrow::DataType *arrow_type,
                         RowGroupFilter *filter)
{
    FmgrInfo finfo;
    Datum    val;
    int      collid = filter->value->constcollid;
    int      strategy = filter->strategy;

    if (arrow_type->id() == arrow::Type::MAP && filter->is_key)
    {
        /*
         * Special case for jsonb `?` (exists) operator. As key is always
         * of text type we need first convert it to the target type (if needed
         * of course).
         */

        /*
         * Extract the key type (we don't check correctness here as we've
         * already done this in `extract_rowgroups_list()`)
         */
        auto strct = arrow_type->fields()[0];
        auto key = strct->type()->fields()[0];
        arrow_type = key->type().get();

        /* Do conversion */
        filter->value = convert_const(filter->value,
                                      to_postgres_type(arrow_type));
    }
    val = filter->value->constvalue;

    find_cmp_func(&finfo,
                  filter->value->consttype,
                  to_postgres_type(arrow_type));

    switch (filter->strategy)
    {
        case BTLessStrategyNumber:
        case BTLessEqualStrategyNumber:
            {
                Datum   lower;
                int     cmpres;
                bool    satisfies;
                std::string min = stats->EncodeMin();

                lower = bytes_to_postgres_type(min.c_str(), min.length(),
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
                std::string max = stats->EncodeMax();

                upper = bytes_to_postgres_type(max.c_str(), max.length(),
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
        case JsonbExistsStrategyNumber:
            {
                Datum   lower,
                        upper;
                std::string min = stats->EncodeMin();
                std::string max = stats->EncodeMax();

                lower = bytes_to_postgres_type(min.c_str(), min.length(),
                                               arrow_type);
                upper = bytes_to_postgres_type(max.c_str(), max.length(),
                                               arrow_type);

                int l = FunctionCall2Coll(&finfo, collid, val, lower);
                int u = FunctionCall2Coll(&finfo, collid, val, upper);

                if (l < 0 || u > 0)
                    return false;
                break;
            }

        default:
            /* should not happen */
            Assert(false);
    }

    return true;
}

typedef enum
{
    PS_START = 0,
    PS_IDENT,
    PS_QUOTE,
    PS_KEY,
    PS_VALUE,
    PS_QKEY,
    PS_QVALUE
} ParserState;

/*
 * is_valid_path_char
 *      Check if a character is valid in a file path.
 *      Rejects control characters (0x00-0x1F) except for reasonable whitespace.
 */
static inline bool
is_valid_path_char(char c)
{
    unsigned char uc = (unsigned char) c;

    /* Reject control characters except tab (0x09) which might appear in quoted paths */
    if (uc < 0x20 && uc != 0x09)
        return false;

    /* Reject DEL character */
    if (uc == 0x7F)
        return false;

    return true;
}

/*
 * is_path_allowed
 *      Check if a file path is within one of the allowed directories.
 *      Returns true if:
 *      - parquet_fdw.allowed_directories is empty AND user is superuser
 *      - The canonical path starts with one of the allowed directories
 *      Returns false otherwise.
 */
static bool
is_path_allowed(const char *path)
{
    char        resolved_path[MAXPGPATH];
    char        resolved_dir[MAXPGPATH];
    char       *allowed_dirs;
    char       *dir;
    char       *saveptr;

    /* If no restrictions set, only superuser can access any path */
    if (parquet_fdw_allowed_directories == NULL ||
        parquet_fdw_allowed_directories[0] == '\0')
    {
        return superuser();
    }

    /* Resolve the actual path */
    if (realpath(path, resolved_path) == NULL)
    {
        /*
         * If the file doesn't exist yet (e.g., during validation),
         * try to resolve the parent directory
         */
        char   *path_copy = pstrdup(path);
        char   *parent = dirname(path_copy);

        if (realpath(parent, resolved_path) == NULL)
        {
            pfree(path_copy);
            return false;  /* Can't resolve path at all */
        }
        pfree(path_copy);
    }

    /* Parse the comma-separated list of allowed directories */
    allowed_dirs = pstrdup(parquet_fdw_allowed_directories);

    for (dir = strtok_r(allowed_dirs, ",", &saveptr);
         dir != NULL;
         dir = strtok_r(NULL, ",", &saveptr))
    {
        /* Skip leading whitespace */
        while (*dir == ' ')
            dir++;

        /* Remove trailing whitespace */
        char *end = dir + strlen(dir) - 1;
        while (end > dir && *end == ' ')
            *end-- = '\0';

        if (*dir == '\0')
            continue;

        /* Resolve the allowed directory path */
        if (realpath(dir, resolved_dir) == NULL)
            continue;  /* Skip directories that don't exist */

        /* Check if resolved_path starts with resolved_dir */
        size_t dir_len = strlen(resolved_dir);
        if (strncmp(resolved_path, resolved_dir, dir_len) == 0)
        {
            /* Make sure it's a proper prefix (followed by / or end of string) */
            if (resolved_path[dir_len] == '/' || resolved_path[dir_len] == '\0')
            {
                pfree(allowed_dirs);
                return true;
            }
        }
    }

    pfree(allowed_dirs);
    return false;
}

/*
 * parse_filenames_list
 *      Parse space separated list of filenames.
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
                        if (!is_valid_path_char(*cur))
                            elog(ERROR, "parquet_fdw: invalid character (0x%02X) in filename",
                                 (unsigned char) *cur);
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
                        filenames = lappend_globbed_filenames(filenames, f);
                        state = PS_START;
                        break;
                    default:
                        if (!is_valid_path_char(*cur))
                            elog(ERROR, "parquet_fdw: invalid character (0x%02X) at position %ld in filename starting with '%.*s'",
                                 (unsigned char) *cur, (long)(cur - f), (int)Min(cur - f + 1, 32), f);
                        break;
                }
                break;
            case PS_QUOTE:
                switch (*cur)
                {
                    case '"':
                        *cur = '\0';
                        filenames = lappend_globbed_filenames(filenames, f);
                        state = PS_START;
                        break;
                    default:
                        if (!is_valid_path_char(*cur))
                            elog(ERROR, "parquet_fdw: invalid character (0x%02X) at position %ld in filename starting with '%.*s'",
                                 (unsigned char) *cur, (long)(cur - f), (int)Min(cur - f + 1, 32), f);
                        break;
                }
                break;
            default:
                elog(ERROR, "parquet_fdw: unknown parse state");
        }
        cur++;
    }
    filenames = lappend_globbed_filenames(filenames, f);

    return filenames;
}


/*
 * lappend_globbed_filenames
 *      The filename can be a globbing pathname matching potentially multiple files.
 *      All the matched file names are added to the list.
 */
static List *
lappend_globbed_filenames(List *filenames,
                          const char *filename)
{
    glob_t  globbuf;

    globbuf.gl_offs = 0;
    int error = glob(filename, GLOB_ERR | GLOB_NOCHECK | GLOB_BRACE, NULL, &globbuf);
    switch (error) {
        case 0:
            for (size_t i = globbuf.gl_offs; i < globbuf.gl_pathc; i++)
            {
                const char *filepath = globbuf.gl_pathv[i];

                /* Validate path against allowed directories */
                if (!is_path_allowed(filepath))
                {
                    globfree(&globbuf);
                    ereport(ERROR,
                            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                             errmsg("parquet_fdw: path \"%s\" is not in allowed directories",
                                    filepath),
                             errhint("Set parquet_fdw.allowed_directories to include the directory, or contact a superuser.")));
                }

                elog(DEBUG1, "parquet_fdw: adding globbed filename %s to list of files", filepath);
                filenames = lappend(filenames, makeString(pstrdup(filepath)));
            }
            break;
        case GLOB_NOSPACE:
            ereport(ERROR,
                    (errcode(ERRCODE_OUT_OF_MEMORY),
                    errmsg("parquet_fdw: running out of memory while globbing Parquet filename \"%s\"",
                           filename)));
            break;
        case GLOB_ABORTED:
            ereport(ERROR,
                    (errcode(ERRCODE_IO_ERROR),
                    errmsg("parquet_fdw: read error while globbing Parquet filename \"%s\". Check file permissions.",
                           filename)));
            break;
        // Should not come here as we use GLOB_NOCHECK flag
        case GLOB_NOMATCH:
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_FILE),
                    errmsg("parquet_fdw: no Parquet filename matches \"%s\". Check path.",
                           filename)));
            break;
        default:
            ereport(ERROR,
                    (errcode(ERRCODE_SYSTEM_ERROR),
                    errmsg("parquet_fdw: unknown error; no Parquet filename matches \"%s\". Check path and permissions.",
                           filename)));
    }
    globfree(&globbuf);

    return filenames;
}


typedef struct TableMapping {
    char    *tablename;
    char    *filenames;
} TableMapping;

/*
 * parse_tables_map
 *      Parse space separated list of tablename=filename[:filename*].
 *      Filename can be globbed path, but it is not resolved in that function.
 *      The colon-separated list of filenames is replaced by a space-separated
 *      list that can be used in the CREATE TABLE statement.
 */
static List *
parse_tables_map(const char *str)
{
    char       *cur = pstrdup(str);
    char       *f = cur;
    ParserState state = PS_START;
    char       *tablename = NULL;
    char       *filenames = NULL;
    TableMapping *tf = NULL;
    List       *tables = NIL;

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
                        state = PS_QKEY;
                        break;
                    case '=':
                        elog(ERROR, "parquet_fdw: missing table name in tables_map");
                        return tables;    
                    default:
                        /* XXX we should check that *cur is a valid path symbol
                         * but let's skip it for now */
                        state = PS_KEY;
                        f = cur;
                        break;
                }
                break;
            case PS_KEY:
                switch (*cur)
                {
                    case '=':
                        *cur = '\0';
                        tablename = pstrdup(f);
                        f = cur + 1;
                        state = PS_VALUE;
                        break;
                    case ' ':
                        elog(ERROR, "parquet_fdw: missing file name in tables_map");
                        return tables;
                    default:
                        break;
                }
                break;
            case PS_VALUE:
                switch (*cur)
                {
                    case ':':
                        // ':' file path separator in tables_map is replaced by ' ' separator
                        // in filename option.
                        *cur = ' ';
                        break;
                    case '"':
                        // We do nothing when we encounter a double-quote as the value
                        // must be transmitted as-is to the CREATE FOREIGN TABLE statement.
                        // We just need to get the other double-quote.
                        state = PS_QVALUE;
                        break;
                    case ' ':
                        *cur = '\0';
                        filenames = pstrdup(f);
                        Assert(tablename && filenames);
                        tf = (TableMapping *) palloc(sizeof(TableMapping));
                        tf->tablename = tablename;
                        tf->filenames = filenames;
                        tables = lappend(tables, tf);
                        f = cur + 1;
                        state = PS_START;
                        tablename = NULL;
                        filenames = NULL;
                        break;
                    default:
                        break;
                }
                break;
            case PS_QKEY:
                switch (*cur)
                {
                    case '"':
                        *cur = '\0';
                        tablename = pstrdup(f);
                        f = cur + 1;
                        state = PS_VALUE;
                        break;
                    default:
                        break;
                }
                break;
            case PS_QVALUE:
                switch (*cur)
                {
                    case '"':
                        // Back to normal value processing
                        state = PS_VALUE;
                        break;
                    default:
                        break;
                }
                break;
            default:
                elog(ERROR, "parquet_fdw: unknown parse state");
        }
        cur++;
    }
    filenames = pstrdup(f);
    if (!tablename || !filenames)
        elog(ERROR, "parquet_fdw: tables_mapping option can't be empty");
    tf = (TableMapping *) palloc(sizeof(TableMapping));
    tf->tablename = tablename;
    tf->filenames = filenames;
    tables = lappend(tables, tf);

    return tables;
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
                       uint64 *matched_rows,
                       uint64 *total_rows) noexcept
{
    std::unique_ptr<parquet::arrow::FileReader> reader;
    List           *rowgroups = NIL;
    std::string     error;

    /* Open parquet file to read meta information */
    try
    {
        auto result = parquet::arrow::FileReader::Make(
                arrow::default_memory_pool(),
                parquet::ParquetFileReader::OpenFile(filename, false));

        if (!result.ok())
            throw Error("failed to open Parquet file: %s ('%s')",
                        result.status().message().c_str(), filename);
        reader = std::move(result).ValueUnsafe();

        auto meta = reader->parquet_reader()->metadata();
        parquet::ArrowReaderProperties  props;
        parquet::arrow::SchemaManifest  manifest;

        arrow::Status status = parquet::arrow::SchemaManifest::Make(meta->schema(), nullptr,
                                                      props, &manifest);
        if (!status.ok())
            throw Error("error creating arrow schema ('%s')", filename);

        /* Check each row group whether it matches the filters */
        for (int r = 0; r < reader->num_row_groups(); r++)
        {
            bool match = true;
            auto rowgroup = meta->RowGroup(r);

            /* Skip empty rowgroups */
            if (!rowgroup->num_rows())
                continue;

            for (auto &filter : filters)
            {
                AttrNumber      attnum;
                char            pg_colname[NAMEDATALEN];

                attnum = filter.attnum - 1;
                tolowercase(NameStr(TupleDescAttr(tupleDesc, attnum)->attname),
                            pg_colname);

                /*
                 * Search for the column with the same name as filtered attribute
                 */
                for (auto &schema_field : manifest.schema_fields)
                {
                    MemoryContext   ccxt = CurrentMemoryContext;
                    bool            error = false;
                    char            errstr[ERROR_STR_LEN];
                    char            arrow_colname[NAMEDATALEN];
                    auto           &field = schema_field.field;
                    int             column_index;

                    /* Skip complex objects (lists, structs except maps) */
                    if (schema_field.column_index == -1
                        && field->type()->id() != arrow::Type::MAP)
                        continue;

                    if (field->name().length() > NAMEDATALEN)
                        throw Error("parquet column name '%s' is too long (max: %d)",
                                    field->name().c_str(), NAMEDATALEN - 1);
                    tolowercase(field->name().c_str(), arrow_colname);

                    if (strcmp(pg_colname, arrow_colname) != 0)
                        continue;

                    if (field->type()->id() == arrow::Type::MAP)
                    {
                        /*
                         * Extract `key` column of the map.
                         * See `create_column_mapping()` for some details on
                         * map structure.
                         */
                        Assert(schema_field.children.size() == 1);
                        auto &strct = schema_field.children[0];

                        Assert(strct.children.size() == 2);
                        auto &key = strct.children[0];
                        column_index = key.column_index;
                    }
                    else
                        column_index = schema_field.column_index;

                    /* Found it! */
                    std::shared_ptr<parquet::Statistics>  stats;
                    auto column = rowgroup->ColumnChunk(column_index);
                    stats = column->statistics();

                    PG_TRY();
                    {
                        /*
                         * If at least one filter doesn't match rowgroup exclude
                         * the current row group and proceed with the next one.
                         */
                        if (stats && !row_group_matches_filter(stats.get(),
                                                               field->type().get(),
                                                               &filter))
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

                        strncpy(errstr, errdata->message, ERROR_STR_LEN - 1);
                        errstr[ERROR_STR_LEN - 1] = '\0';
                        FreeErrorData(errdata);
                    }
                    PG_END_TRY();
                    if (error)
                        throw Error("row group filter match failed: %s", errstr);
                    break;
                }  /* loop over columns */

                if (!match)
                    break;

            }  /* loop over filters */

            /* All the filters match this rowgroup */
            if (match)
            {
                /* TODO: PG_TRY */
                rowgroups = lappend_int(rowgroups, r);
                *matched_rows += rowgroup->num_rows();
            }
            *total_rows += rowgroup->num_rows();
        }  /* loop over rowgroups */
    }
    catch(const std::exception& e) {
        error = e.what();
    }
    if (!error.empty()) {
        elog(ERROR,
             "parquet_fdw: failed to extract row groups from Parquet file: %s ('%s')",
             error.c_str(), filename);
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
    List           *res = NIL;
    std::string     error;

    try
    {
        std::unique_ptr<parquet::arrow::FileReader> reader;
        parquet::ArrowReaderProperties props;
        parquet::arrow::SchemaManifest manifest;
        FieldInfo      *fields;

        auto result = parquet::arrow::FileReader::Make(
                    arrow::default_memory_pool(),
                    parquet::ParquetFileReader::OpenFile(path, false));
        if (!result.ok())
            throw Error("failed to open Parquet file %s ('%s')",
                        result.status().message().c_str(), path);
        reader = std::move(result).ValueUnsafe();

        auto p_schema = reader->parquet_reader()->metadata()->schema();
        if (!parquet::arrow::SchemaManifest::Make(p_schema, nullptr, props, &manifest).ok())
            throw Error("error creating arrow schema ('%s')", path);

        fields = (FieldInfo *) exc_palloc(
                sizeof(FieldInfo) * manifest.schema_fields.size());

        for (auto &schema_field : manifest.schema_fields)
        {
            auto   &field = schema_field.field;
            auto   &type = field->type();
            Oid     pg_type;

            switch (type->id())
            {
                case arrow::Type::LIST:
                {
                    arrow::DataType *subtype;
                    Oid     pg_subtype;
                    bool    error = false;

                    if (type->num_fields() != 1)
                        throw Error("lists of structs are not supported ('%s')", path);

                    subtype = get_arrow_list_elem_type(type.get());
                    pg_subtype = to_postgres_type(subtype);

                    /* This sucks I know... */
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
                        throw Error("failed to get the type of array elements for %d",
                                    pg_subtype);
                    break;
                }
                case arrow::Type::MAP:
                    pg_type = JSONBOID;
                    break;
                default:
                    pg_type = to_postgres_type(type.get());
            }

            if (pg_type != InvalidOid)
            {
                if (field->name().length() >= NAMEDATALEN)
                    throw Error("field name '%s' in '%s' is too long",
                                field->name().c_str(), path);

                memcpy(fields->name, field->name().c_str(), field->name().length() + 1);
                fields->oid = pg_type;
                res = lappend(res, fields++);
            }
            else
            {
                throw Error("cannot convert field '%s' of type '%s' in '%s'",
                            field->name().c_str(), type->name().c_str(), path);
            }
        }
    }
    catch (std::exception &e)
    {
        error = e.what();
    }
    if (!error.empty())
        elog(ERROR, "parquet_fdw: %s", error.c_str());

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
                         quote_identifier(schemaname), quote_identifier(tablename));
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
            appendStringInfo(&str, ", %s %s", quote_identifier(name), type_name);
        else
        {
            appendStringInfo(&str, "%s %s", quote_identifier(name), type_name);
            is_first = false;
        }
    }
    appendStringInfo(&str, ") SERVER %s ", quote_identifier(servername));
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

        // Don't pass files_map option to tables as it is converted to filename option.
        if (strcmp("tables_map", def->defname) != 0)
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
    const char *delim = " ";
    AttrNumber attnum;

    while ((token = strtok(start, delim)) != NULL)
    {
        if ((attnum = get_attnum(relid, token)) == InvalidAttrNumber)
            elog(ERROR, "parquet_fdw: invalid attribute name '%s'", token);
        attrs = lappend_int(attrs, attnum);
        start = NULL;
    }

    return attrs;
}

/*
 * OidFunctionCall1NullableArg
 *      Practically a copy-paste from FunctionCall1Coll with added capability
 *      of passing a NULL argument.
 */
static Datum
OidFunctionCall1NullableArg(Oid functionId, Datum arg, bool argisnull)
{
#if PG_VERSION_NUM < 120000
    FunctionCallInfoData    _fcinfo;
    FunctionCallInfoData    *fcinfo = &_fcinfo;
#else
	LOCAL_FCINFO(fcinfo, 1);
#endif
    FmgrInfo    flinfo;
    Datum		result;

    fmgr_info(functionId, &flinfo);
    InitFunctionCallInfoData(*fcinfo, &flinfo, 1, InvalidOid, NULL, NULL);

#if PG_VERSION_NUM < 120000
    fcinfo->arg[0] = arg;
    fcinfo->argnull[0] = false;
#else
    fcinfo->args[0].value = arg;
    fcinfo->args[0].isnull = argisnull;
#endif

    result = FunctionCallInvoke(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo->isnull)
        elog(ERROR, "function %u returned NULL", flinfo.fn_oid);

    return result;
}

static List *
get_filenames_from_userfunc(const char *funcname, const char *funcarg)
{
#if PG_VERSION_NUM >= 160000
    List       *f = stringToQualifiedNameList(funcname, NULL);
#else
    List       *f = stringToQualifiedNameList(funcname);
#endif
    Jsonb      *j = NULL;
    Oid         funcid;
    Datum       filenames;
    Oid         jsonboid = JSONBOID;
    Datum      *values;
    bool       *nulls;
    int         num;
    List       *res = NIL;
    ArrayType  *arr;

    if (funcarg)
        j = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(funcarg)));

    funcid = LookupFuncName(f, 1, &jsonboid, false);
    filenames = OidFunctionCall1NullableArg(funcid, (Datum) j, funcarg == NULL);

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

    fdw_private->filenames = NIL;
    fdw_private->attrs_sorted = NIL;
    fdw_private->use_mmap = false;
    fdw_private->use_threads = false;
    fdw_private->max_open_files = 0;
    fdw_private->files_in_order = false;
    table = GetForeignTable(relid);

    // When we get there, we are sure that only one of filename, tables_map or
    // files_func option has been defined. All these options set the
    // fdw_private->filenames value.
    foreach(lc, table->options)
    {
		DefElem    *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "filename") == 0)
        {
            fdw_private->filenames = parse_filenames_list(defGetString(def));
        }
        else if (strcmp(def->defname, "tables_map") == 0)
        {
            ListCell   *table;
            List       *tables_map = parse_tables_map(defGetString(def));
            char       *tablename = get_rel_name(relid);

            foreach(table, tables_map)
            {
                if (strcmp(tablename, ((TableMapping *) lfirst(table))->tablename) == 0)
                {
                    fdw_private -> filenames = parse_filenames_list(((TableMapping *) lfirst(table))->filenames);
                    break;
                }
            }
            // As the tables_map option is replaced by a filename one, we
            // don't need it anymore.
            list_free(tables_map);
        }
        else if (strcmp(def->defname, "files_func") == 0)
        {
            funcname = defGetString(def);
        }
        else if (strcmp(def->defname, "files_func_arg") == 0)
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
            fdw_private->use_mmap = defGetBoolean(def);
        }
        else if (strcmp(def->defname, "use_threads") == 0)
        {
            fdw_private->use_threads = defGetBoolean(def);
        }
        else if (strcmp(def->defname, "max_open_files") == 0)
        {
            /* check that int value is valid */
            fdw_private->max_open_files = string_to_int32(defGetString(def));
        }
        else if (strcmp(def->defname, "files_in_order") == 0)
        {
            fdw_private->files_in_order = defGetBoolean(def);
        }
        else
            elog(ERROR, "unknown option '%s'", def->defname);
    }

    if (funcname)
        fdw_private->filenames = get_filenames_from_userfunc(funcname, funcarg);
}

extern "C" void
parquetGetForeignRelSize(PlannerInfo *root,
                         RelOptInfo *baserel,
                         Oid foreigntableid)
{
    ParquetFdwPlanState *fdw_private;
    std::list<RowGroupFilter> filters;
    RangeTblEntry  *rte;
    Relation        rel;
    TupleDesc       tupleDesc;
    List           *filenames_orig;
    ListCell       *lc;
    uint64          matched_rows = 0;
    uint64          total_rows = 0;

    fdw_private = (ParquetFdwPlanState *) palloc0(sizeof(ParquetFdwPlanState));
    get_table_options(foreigntableid, fdw_private);

    /* Analyze query clauses and extract ones that can be of interest to us*/
    extract_rowgroup_filters(baserel->baserestrictinfo, filters);

    rte = root->simple_rte_array[baserel->relid];
#if PG_VERSION_NUM < 120000
    rel = heap_open(rte->relid, AccessShareLock);
#else
    rel = table_open(rte->relid, AccessShareLock);
#endif
    tupleDesc = RelationGetDescr(rel);

    /*
     * Extract list of row groups that match query clauses. Also calculate
     * approximate number of rows in result set based on total number of tuples
     * in those row groups. It isn't very precise but it is best we got.
     */
    filenames_orig = fdw_private->filenames;
    fdw_private->filenames = NIL;
    foreach (lc, filenames_orig)
    {
        char *filename = strVal(lfirst(lc));
        List *rowgroups = extract_rowgroups_list(filename, tupleDesc, filters,
                                                 &matched_rows, &total_rows);

        if (rowgroups)
        {
            fdw_private->rowgroups = lappend(fdw_private->rowgroups, rowgroups);
            fdw_private->filenames = lappend(fdw_private->filenames, lfirst(lc));
        }
    }
#if PG_VERSION_NUM < 120000
    heap_close(rel, AccessShareLock);
#else
    table_close(rel, AccessShareLock);
#endif
    list_free(filenames_orig);

    baserel->fdw_private = fdw_private;
    baserel->tuples = total_rows;
    baserel->rows = fdw_private->matched_rows = matched_rows;
}

static void
estimate_costs(PlannerInfo *root, RelOptInfo *baserel, Cost *startup_cost,
               Cost *run_cost, Cost *total_cost)
{
    auto    fdw_private = (ParquetFdwPlanState *) baserel->fdw_private;
    double  ntuples;

    ntuples = baserel->tuples *
        clauselist_selectivity(root,
                               baserel->baserestrictinfo,
                               0,
                               JOIN_INNER,
                               NULL);

    /*
     * Here we assume that parquet tuple cost is the same as regular tuple cost
     * even though this is probably not true in many cases. Maybe we'll come up
     * with a smarter idea later. Also we use actual number of rows in selected
     * rowgroups to calculate cost as we need to process those rows regardless
     * of whether they're gonna be filtered out or not.
     */
    *run_cost = fdw_private->matched_rows * cpu_tuple_cost;
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
                       Oid /* foreigntableid */)
{
	ParquetFdwPlanState *fdw_private;
    Path       *foreign_path;
	Cost		startup_cost;
	Cost		total_cost;
    Cost        run_cost;
    bool        is_sorted, is_multi;
    List       *pathkeys = NIL;
    List       *source_pathkeys = NIL;
    std::list<RowGroupFilter> filters;
    ListCell   *lc_sorted;
    ListCell   *lc_rootsort;

    fdw_private = (ParquetFdwPlanState *) baserel->fdw_private;

    estimate_costs(root, baserel, &startup_cost, &run_cost, &total_cost);

    /* Collect used attributes to reduce number of read columns during scan */
    extract_used_attributes(baserel);

    is_sorted = fdw_private->attrs_sorted != NIL;
    is_multi = list_length(fdw_private->filenames) > 1;
    fdw_private->type = is_multi ? RT_MULTI :
        (list_length(fdw_private->filenames) == 0 ? RT_TRIVIAL : RT_SINGLE);

    /*
     * Build pathkeys for the foreign table based on attrs_sorted and ORDER BY
     * (or WINDOW FUNCTION) clause passed by user.
     *
     * pathkeys is used by Postgres to sort the result. After we build pathkeys
     * for the foreign table Postgres will assume that the returned data is
     * already sorted. In the function parquetBeginForeignScan() we make sure
     * that the data from parquet files are sorted.
     *
     * We need to make sure that we don't add PathKey for an attribute which is
     * not passed by ORDER BY. We will stop building pathkeys as soon as we see
     * that an attribute on ORDER BY and "sorted" doesn't match, since in that
     * case Postgres will need to sort by remaining attributes by itself.
     */
    source_pathkeys = root->query_pathkeys;
    lc_rootsort = list_head(source_pathkeys);

    foreach (lc_sorted, fdw_private->attrs_sorted)
    {
        Oid         relid = root->simple_rte_array[baserel->relid]->relid;
        int         attnum = lfirst_int(lc_sorted);
        Oid         typid,
                    collid;
        int32       typmod;
        Oid         sort_op;
        Var        *var;
        List       *attr_pathkeys;

        if (lc_rootsort == NULL)
            break;

        /* Build an expression (simple var) for the attribute */
        get_atttypetypmodcoll(relid, attnum, &typid, &typmod, &collid);
        var = makeVar(baserel->relid, attnum, typid, typmod, collid, 0);

        /* Lookup sorting operator for the attribute type */
        get_sort_group_operators(typid,
                                 true, false, false,
                                 &sort_op, NULL, NULL,
                                 NULL);

        /* Create PathKey for the attribute from "sorted" option */
#if PG_VERSION_NUM >= 160000
        attr_pathkeys = build_expression_pathkey(root, (Expr *) var,
                                                sort_op, baserel->relids,
                                                false);
#else
        attr_pathkeys = build_expression_pathkey(root, (Expr *) var, NULL,
                                                sort_op, baserel->relids,
                                                false);
#endif

        if (attr_pathkeys == NIL)
            break;
        else
        {
            PathKey    *attr_pathkey = (PathKey *) linitial(attr_pathkeys);
            bool        is_redundant = false;

            if (EC_MUST_BE_REDUNDANT(attr_pathkey->pk_eclass))
                is_redundant = true;

            if (lc_rootsort != NULL)
            {
                PathKey    *root_pathkey = (PathKey *) lfirst(lc_rootsort);

                /*
                 * Compare the attribute from "sorted" option and the attribute from
                 * ORDER BY clause ("root"). If they don't match stop here and use
                 * whatever pathkeys we've build so far. Postgres will use remaining
                 * attributes from ORDER BY clause to sort data on higher level of
                 * execution.
                 */
                if (!equal(attr_pathkey, root_pathkey))
                {
                    if (!is_redundant)
                        break;
                }
                else
                {
#if PG_VERSION_NUM < 130000
                    lc_rootsort = lnext(lc_rootsort);
#else
                    lc_rootsort = lnext(source_pathkeys, lc_rootsort);
#endif
                }
            }

            if (!is_redundant)
                pathkeys = list_concat(pathkeys, attr_pathkeys);
        }
    }

    foreign_path = (Path *) create_foreignscan_path(root, baserel,
                                                    NULL,      /* pathtarget (default) */
                                                    baserel->rows,
                                                    CREATE_FOREIGNSCAN_DISABLED_NODES
                                                    startup_cost,
                                                    total_cost,
                                                    pathkeys,
                                                    NULL,      /* required_outer */
                                                    NULL,      /* fdw_outerpath */
                                                    NULL,      /* fdw_restrictinfo */
                                                    (List *) fdw_private);
    if (!enable_multifile && is_multi)
        foreign_path->total_cost += disable_cost;

    add_path(baserel, foreign_path);

    if (fdw_private->type == RT_TRIVIAL)
        return;

    /* Create a separate path with pathkeys for sorted parquet files. */
    if (is_sorted)
    {
        Path                   *path;
        ParquetFdwPlanState    *private_sort;

        private_sort = (ParquetFdwPlanState *) palloc(sizeof(ParquetFdwPlanState));
        memcpy(private_sort, fdw_private, sizeof(ParquetFdwPlanState));

        path = (Path *) create_foreignscan_path(root, baserel,
                                                NULL,      /* pathtarget (default) */
                                                baserel->rows,
                                                CREATE_FOREIGNSCAN_DISABLED_NODES
                                                startup_cost,
                                                total_cost,
                                                pathkeys,
                                                NULL,      /* required_outer */
                                                NULL,      /* fdw_outerpath */
                                                NULL,      /* fdw_restrictinfo */
                                                (List *) private_sort);

        /* For multifile case calculate the cost of merging files */
        if (is_multi)
        {
            private_sort->type = private_sort->max_open_files > 0 ?
                RT_CACHING_MULTI_MERGE : RT_MULTI_MERGE;

            cost_merge((Path *) path, list_length(private_sort->filenames),
                       startup_cost, total_cost, path->rows);

            if (!enable_multifile_merge)
                path->total_cost += disable_cost;
        }
        add_path(baserel, path);
    }

    /* Parallel paths */
    if (baserel->consider_parallel > 0)
    {
        ParquetFdwPlanState *private_parallel;
        bool        use_pathkeys = false;
        int         num_workers = max_parallel_workers_per_gather;
        double      rows_per_worker = baserel->rows / (num_workers + 1);

        private_parallel = (ParquetFdwPlanState *) palloc(sizeof(ParquetFdwPlanState));
        memcpy(private_parallel, fdw_private, sizeof(ParquetFdwPlanState));
        private_parallel->type = is_multi ? RT_MULTI : RT_SINGLE;

        /* For mutifile reader only use pathkeys when files are in order */
        use_pathkeys = is_sorted && (!is_multi || (is_multi && fdw_private->files_in_order));

        Path *path = (Path *)
                 create_foreignscan_path(root, baserel,
                                        NULL,      /* pathtarget (default) */
                                        rows_per_worker,
                                        CREATE_FOREIGNSCAN_DISABLED_NODES
                                        startup_cost,
                                        startup_cost + run_cost / (num_workers + 1),
                                        use_pathkeys ? pathkeys : NULL,
                                        NULL,      /* required_outer */
                                        NULL,      /* fdw_outerpath */
                                        NULL,      /* fdw_restrictinfo */
                                        (List *) private_parallel);

        path->parallel_workers = num_workers;
        path->parallel_aware   = true;
        path->parallel_safe    = true;

        if (!enable_multifile)
            path->total_cost += disable_cost;

        add_partial_path(baserel, path);

        /* Multifile Merge parallel path */
        if (is_multi && is_sorted)
        {
            ParquetFdwPlanState *private_parallel_merge;

            private_parallel_merge = (ParquetFdwPlanState *) palloc(sizeof(ParquetFdwPlanState));
            memcpy(private_parallel_merge, fdw_private, sizeof(ParquetFdwPlanState));

            private_parallel_merge->type = private_parallel_merge->max_open_files > 0 ?
                RT_CACHING_MULTI_MERGE : RT_MULTI_MERGE;

            path = (Path *)
                     create_foreignscan_path(root, baserel,
                                            NULL,      /* pathtarget (default) */
                                            rows_per_worker,
                                            CREATE_FOREIGNSCAN_DISABLED_NODES
                                            startup_cost,
                                            total_cost,
                                            pathkeys,
                                            NULL,      /* required_outer */
                                            NULL,      /* fdw_outerpath */
                                            NULL,      /* fdw_restrictinfo */
                                            (List *) private_parallel_merge);

            cost_merge(path, list_length(private_parallel_merge->filenames),
                       startup_cost, total_cost, path->rows);

            path->total_cost = path->startup_cost + path->total_cost / (num_workers + 1);
            path->parallel_workers = num_workers;
            path->parallel_aware   = true;
            path->parallel_safe    = true;

            if (!enable_multifile_merge)
                path->total_cost += disable_cost;

            add_partial_path(baserel, path);
        }
    }
}

extern "C" ForeignScan *
parquetGetForeignPlan(PlannerInfo * /* root */,
                      RelOptInfo *baserel,
                      Oid /* foreigntableid */,
                      ForeignPath *best_path,
                      List *tlist,
                      List *scan_clauses,
                      Plan *outer_plan)
{
    ParquetFdwPlanState *fdw_private = (ParquetFdwPlanState *) best_path->fdw_private;
    if (fdw_private == NULL) {
        fdw_private = (ParquetFdwPlanState *) palloc0(sizeof(ParquetFdwPlanState));
    }

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
    if (fdw_private->attrs_used) 
        while ((attr = bms_next_member(fdw_private->attrs_used, attr)) >= 0)
            attrs_used = lappend_int(attrs_used, attr);

    foreach (lc, fdw_private->attrs_sorted)
        attrs_sorted = lappend_int(attrs_sorted, lfirst_int(lc));

    /* Packing all the data needed by executor into the list */
    params = lappend(params, fdw_private->filenames);
    params = lappend(params, attrs_used);
    params = lappend(params, attrs_sorted);
    params = lappend(params, makeInteger(fdw_private->use_mmap));
    params = lappend(params, makeInteger(fdw_private->use_threads));
    params = lappend(params, makeInteger(fdw_private->type));
    params = lappend(params, makeInteger(fdw_private->max_open_files));
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
parquetBeginForeignScan(ForeignScanState *node, int /* eflags */)
{
    ParquetFdwExecutionState   *festate = NULL;
    MemoryContextCallback      *callback;
    MemoryContext   reader_cxt;
	ForeignScan    *plan = (ForeignScan *) node->ss.ps.plan;
	EState         *estate = node->ss.ps.state;
    List           *fdw_private = plan->fdw_private;
    List           *attrs_list;
    List           *rowgroups_list = NIL;
    ListCell       *lc, *lc2;
    List           *filenames = NIL;
    std::set<int>   attrs_used;
    List           *attrs_sorted = NIL;
    bool            use_mmap = false;
    bool            use_threads = false;
    int             i = 0;
    ReaderType      reader_type = RT_SINGLE;
    int             max_open_files = 0;
    std::string     error;

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
                use_mmap = (bool) intVal(lfirst(lc));
                break;
            case 4:
                use_threads = (bool) intVal(lfirst(lc));
                break;
            case 5:
                reader_type = (ReaderType) intVal(lfirst(lc));
                break;
            case 6:
                max_open_files = intVal(lfirst(lc));
                break;
            case 7:
                rowgroups_list = (List *) lfirst(lc);
                break;
        }
        ++i;
    }

    MemoryContext   cxt = estate->es_query_cxt;
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    TupleDesc       tupleDesc = slot->tts_tupleDescriptor;

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
            error = e.what();
        }
        if (!error.empty())
            elog(ERROR, "parquet_fdw: scan initialization failed: %s", error.c_str());
    }

    try
    {
        festate = create_parquet_execution_state(reader_type, reader_cxt, tupleDesc,
                                                 attrs_used, sort_keys,
                                                 use_threads, use_mmap,
                                                 max_open_files);

        forboth (lc, filenames, lc2, rowgroups_list)
        {
            char *filename = strVal(lfirst(lc));
            List *rowgroups = (List *) lfirst(lc2);

            festate->add_file(filename, rowgroups);
        }
    }
    catch(std::exception &e)
    {
        error = e.what();
    }
    if (!error.empty())
        elog(ERROR, "parquet_fdw: %s", error.c_str());

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
    std::string                 error;

	ExecClearTuple(slot);
    try
    {
        festate->next(slot);
    }
    catch (std::exception &e)
    {
        error = e.what();
    }
    if (!error.empty())
        elog(ERROR, "parquet_fdw: %s", error.c_str());

    return slot;
}

extern "C" void
parquetEndForeignScan(ForeignScanState * /* node */)
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
parquetAcquireSampleRowsFunc(Relation relation, int /* elevel */,
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
    std::string     error;

    MemSet(&fdw_private, 0, sizeof(fdw_private));
    get_table_options(RelationGetRelid(relation), &fdw_private);

    for (int i = 0; i < tupleDesc->natts; ++i)
        attrs_used.insert(i + 1 - FirstLowInvalidHeapAttributeNumber);

    reader_cxt = AllocSetContextCreate(CurrentMemoryContext,
                                       "parquet_fdw tuple data",
                                       ALLOCSET_DEFAULT_SIZES);
    festate = create_parquet_execution_state(RT_MULTI, reader_cxt, tupleDesc,
                                             attrs_used, std::list<SortSupportData>(),
                                             fdw_private.use_threads,
                                             false, 0);

    foreach (lc, fdw_private.filenames)
    {
        char *filename = strVal(lfirst(lc));

        try
        {
            std::unique_ptr<parquet::arrow::FileReader> reader;
            List           *rowgroups = NIL;

            auto result = parquet::arrow::FileReader::Make(
                        arrow::default_memory_pool(),
                        parquet::ParquetFileReader::OpenFile(filename, false));
            if (!result.ok())
                throw Error("failed to open Parquet file: %s",
                                     result.status().message().c_str());
            reader = std::move(result).ValueUnsafe();
            auto meta = reader->parquet_reader()->metadata();
            num_rows += meta->num_rows();

            /* We need to scan all rowgroups */
            for (int i = 0; i < meta->num_row_groups(); ++i)
                rowgroups = lappend_int(rowgroups, i);
            festate->add_file(filename, rowgroups);
        }
        catch(const std::exception &e)
        {
            error = e.what();
        }
        if (!error.empty())
            elog(ERROR, "parquet_fdw: %s", error.c_str());
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
            try {
                if (!festate->next(slot, fake))
                    break;
            } catch(std::exception &e) {
                error = e.what();
            }
            if (!error.empty())
                elog(ERROR, "parquet_fdw: %s", error.c_str());

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

    return cnt;
}

extern "C" bool
parquetAnalyzeForeignTable(Relation /* relation */,
                           AcquireSampleRowsFunc *func,
                           BlockNumber * /* totalpages */)
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
    reader_type = (ReaderType) intVal(list_nth(fdw_private, 5));
    rowgroups_list = (List *) llast(fdw_private);

    switch (reader_type)
    {
        case RT_TRIVIAL:
            ExplainPropertyText("Reader", "Trivial", es);
            return; /* no rowgroups list output required, just return here */
        case RT_SINGLE:
            ExplainPropertyText("Reader", "Single File", es);
            break;
        case RT_MULTI:
            ExplainPropertyText("Reader", "Multifile", es);
            break;
        case RT_MULTI_MERGE:
            ExplainPropertyText("Reader", "Multifile Merge", es);
            break;
        case RT_CACHING_MULTI_MERGE:
            ExplainPropertyText("Reader", "Caching Multifile Merge", es);
            break;
    }

    forboth(lc, filenames, lc2, rowgroups_list)
    {
        char   *filename = strVal(lfirst(lc));
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
parquetIsForeignScanParallelSafe(PlannerInfo * /* root */,
                                 RelOptInfo *rel,
                                 RangeTblEntry * /* rte */)
{
    return true;
}

extern "C" Size
parquetEstimateDSMForeignScan(ForeignScanState *node,
                              ParallelContext * /* pcxt */)
{
    ParquetFdwExecutionState   *festate;

    festate = (ParquetFdwExecutionState *) node->fdw_state;
    return festate->estimate_coord_size();
}

extern "C" void
parquetInitializeDSMForeignScan(ForeignScanState *node,
                                ParallelContext * pcxt,
                                void *coordinate)
{
    ParallelCoordinator        *coord = (ParallelCoordinator *) coordinate;
    ParquetFdwExecutionState   *festate;

    /*
    coord->i.s.next_rowgroup = 0;
    coord->i.s.next_reader = 0;
    SpinLockInit(&coord->lock);
    */
    festate = (ParquetFdwExecutionState *) node->fdw_state;
    festate->set_coordinator(coord);
    festate->init_coord();
}

extern "C" void
parquetReInitializeDSMForeignScan(ForeignScanState *node,
                                  ParallelContext * /* pcxt */,
                                  void * /* coordinate */)
{
    ParquetFdwExecutionState   *festate;

    festate = (ParquetFdwExecutionState *) node->fdw_state;
    festate->init_coord();
}

extern "C" void
parquetInitializeWorkerForeignScan(ForeignScanState *node,
                                   shm_toc * /* toc */,
                                   void *coordinate)
{
    ParallelCoordinator        *coord   = (ParallelCoordinator *) coordinate;
    ParquetFdwExecutionState   *festate;

    coord = new(coordinate) ParallelCoordinator;
    festate = (ParquetFdwExecutionState *) node->fdw_state;
    festate->set_coordinator(coord);
}

extern "C" void
parquetShutdownForeignScan(ForeignScanState * /* node */)
{
}


/*
 * Add a CREATE FOREIGN TABLE statement command for the given table name
 * to the list of tables creation for the foreign schema.
 */
static List *
add_create_foreign_table(List *cmds, ImportForeignSchemaStmt *stmt, char *tablename, char *filenames)
{
    ListCell   *lc;
    List       *fields;
    List       *paths = NULL;
    char       *path = NULL;
    char       *query;
    bool       found = false; // Is the table in the stmt->table_list

    // Check that table name is allowed or not by the LIMIT TO or EXCEPT
    // clauses of the IMPORT FOREIGN SCHEMA command.
    foreach (lc, stmt->table_list)
    {
        RangeVar *rv = (RangeVar *) lfirst(lc);

        if (strcmp(tablename, rv->relname) == 0)
        {
            if (stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT)
            {
                elog(DEBUG1, "parquet_fdw: relation '%s' is listed in IMPORT FOREIGN SCHEMA EXCEPT clause; not creating it", tablename);
                return cmds;
            }
            found = true;
        }
    }
    if (stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO && !found)
    {
        elog(DEBUG1, "parquet_fdw: relation '%s' is not listed into the IMPORT FOREIGN SCHEMA LIMIT TO clause; not creating it", tablename);
        return cmds;
    }

    // Create list of paths
    paths = parse_filenames_list(filenames);

    // Security: Check that all files are within the remote schema path.
    // We don't check yet that the files exist: this will be done
    // when querying the table.
    size_t len = strlen(stmt->remote_schema);
    foreach(lc, paths)
    {
        path = strVal(lfirst(lc));

        if (strlen(path) <= len || strncmp(stmt->remote_schema, path, len) != 0)
            elog(ERROR,
                "parquet_fdw: file %s is outside remote schema path %s", path, stmt->remote_schema);
    }

    path = strVal(linitial(paths));
    fields = extract_parquet_fields(path);

    if (fields == NIL)
        elog(ERROR, "parquet_fdw: one Parquet file at least must be present into %s to extract columns of remote table", path);
    
    // We should check that fields in all Parquet files are identical.
    // Following code is unreliable...
/*
    for_each_from(lc, paths, 1)
    {
        List *fields2 = extract_parquet_fields(strVal(lfirst(lc)));

        ListCell *fc = (ListCell *) linitial(fields);
        ListCell *fc2 = (ListCell *) linitial(fields2);
        FieldInfo *fi = (FieldInfo *) fc;
        FieldInfo *fi2 = (FieldInfo *) fc2;
        while (fc != NULL && fc2 != NULL)
        {
            if (fi->oid != fi2->oid || strcmp(fi->name, fi2->name) != 0)
                elog(ERROR,
                    "parquet_fdw: fields of file %s are different from those of other files (%s vs %s)",
                    strVal(lfirst(lc)), fi->name, fi2->name);

            fc = lnext(fields, fc);
            fc2 = lnext(fields2, fc2);
            fi = (FieldInfo *) fc;
            fi2 = (FieldInfo *) fc2;
        }
        if (fc != NULL || fc2 != NULL)
            elog(ERROR,
                "parquet_fdw: fields of file %s are different from those of other files (%s vs %s)",
                strVal(lfirst(lc)),
                fc != NULL ? fi->name : "NULL",
                fc2 != NULL ? fi2->name: "NULL");
        pfree(fields2);
    }
*/

    // We can now create the CREATE FOREIGN TABLE query...
    query = create_foreign_table_query(tablename, stmt->local_schema,
                                        stmt->server_name, &filenames, 1,
                                        fields, stmt->options);

    pfree(fields);
    
    return lappend(cmds, query);
}


extern "C" List *
parquetImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid /* serverOid */)
{
    struct dirent  *f;
    DIR            *d;
    List           *cmds = NIL;
    List           *tables = NIL;
    ListCell       *lc;

    // Get options to find tables_map and table names.
    foreach(lc, stmt->options)
    {
        DefElem *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "tables_map") == 0)
        {
            tables = parse_tables_map(defGetString(def));
            break;
        }
    }

    if (tables == NIL)
    {
        // We create a table for each file into the foreign schema
        // definition
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
            bool is_regular = (f->d_type == DT_REG);

            /* Some filesystems don't set d_type, fall back to lstat */
            if (f->d_type == DT_UNKNOWN)
            {
                char *full_path = psprintf("%s/%s", stmt->remote_schema, f->d_name);
                struct stat st;

                if (lstat(full_path, &st) == 0 && S_ISREG(st.st_mode))
                    is_regular = true;
                pfree(full_path);
            }

            if (is_regular)
            {
                char    *filename = pstrdup(f->d_name);
                char    *path = psprintf("%s/%s", stmt->remote_schema, filename);

                /* check that file extension is "parquet" */
                char *ext = strrchr(filename, '.');

                if (ext && strcmp(ext + 1, "parquet") != 0)
                {
                    elog(WARNING,
                        "parquet_fdw: non-Parquet file %s in directory %s; not considering it",
                        filename, stmt->remote_schema);
                    continue;
                }

                /*
                * Set terminal symbol so that tablename is taken from filename
                * without file extension.
                */
                *ext = '\0';
                
                cmds = add_create_foreign_table(cmds, stmt, filename, path);

                pfree(filename);
                pfree(path);
            }

        }
        FreeDir(d);
    }
    else
    {
        // The tables to create are given by the tables_map option
        foreach(lc, tables)
        {
            char *tablename = ((TableMapping *) lfirst(lc))->tablename;
            char *filenames = ((TableMapping *) lfirst(lc))->filenames;

            // We must transmit the filenames as-is to the CREATE FOREIGN TABLE
            // so that globbing patterns are evaluated at query execution and
            // not when the table is created.

            cmds = add_create_foreign_table(cmds, stmt, tablename, filenames);
        }
    }
    list_free(tables);

    return cmds;
}


extern "C" Datum
parquet_fdw_validator_impl(PG_FUNCTION_ARGS)
{
    List       *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
    Oid         catalog = PG_GETARG_OID(1);
    ListCell   *opt_lc;
    bool        filename_provided = false;
    bool        func_provided = false;

    /* Only check table options */
    if (catalog != ForeignTableRelationId)
        PG_RETURN_VOID();

    foreach(opt_lc, options_list)
    {
        DefElem    *def = (DefElem *) lfirst(opt_lc);

        if (strcmp(def->defname, "filename") == 0)
        {
            char   *filename = pstrdup(defGetString(def));
            List   *filenames;
            ListCell *file_lc;

            filenames = parse_filenames_list(filename);

            foreach(file_lc, filenames)
            {
                struct stat stat_buf;
                char       *fn = strVal(lfirst(file_lc));

                if (stat(fn, &stat_buf) != 0)
                {
                    int e = errno;

                    ereport(ERROR,
                            (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                             errmsg("parquet_fdw: filename: %s ('%s')", strerror(e), fn)));
                }
            }
            pfree(filename);
            pfree(filenames);
            filename_provided = true;
        }
        else if (strcmp(def->defname, "tables_map") == 0)
        {
            char   *tables_map = pstrdup(defGetString(def));
            List   *tables;
            ListCell *table_lc;

            tables = parse_tables_map(tables_map);

            int i = 0;
            for_each_from(table_lc, tables, i)
            {
                // Check that tables are not defined multiple times in the tables_map
                char   *tablename = ((TableMapping *) lfirst(table_lc))->tablename;
                ListCell *table2_lc;

                int j = i + 1;
                for_each_from(table2_lc, tables, j)
                {
                    char *tablename2 = ((TableMapping *) lfirst(table2_lc))->tablename;
                    elog(DEBUG1, "parquet_fdw: comparing '%s' with '%s' in tables_map", tablename, tablename2);
                    if (strcmp(tablename, tablename2) == 0)
                        elog(ERROR,
                            "parquet_fdw: table name '%s' is not unique in tables_map option", tablename);
                }

                // Check that paths are valid
                char   *filename = ((TableMapping *) lfirst(table_lc))->filenames;
                List   *filenames;
                ListCell *file_lc;

                filenames = parse_filenames_list(filename);

                foreach(file_lc, filenames)
                {
                    struct stat stat_buf;
                    char       *fn = strVal(lfirst(file_lc));
                    elog(DEBUG1, "parquet_fdw: checking existence of file '%s' for table '%s' in tables_map", fn, tablename);
                    if (stat(fn, &stat_buf) != 0)
                    {
                        int e = errno;

                        ereport(ERROR,
                                (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                                errmsg("parquet_fdw: %s ('%s') in tables_map option for table '%s", strerror(e), fn, tablename)));
                    }
                }
                pfree(tablename);
                pfree(filename);
                list_free(filenames);
                i += 1;
            }
            list_free(tables);
            pfree(tables_map);

            // As a tables_map option is translated to a filename option, we now remove
            // the tables_map from the options.
            options_list = foreach_delete_current(options_list, opt_lc);
            filename_provided = true;
        }
        else if (strcmp(def->defname, "files_func") == 0)
        {
#if PG_VERSION_NUM >= 160000
            List       *funcname = stringToQualifiedNameList(defGetString(def), NULL);
#else
            List       *funcname = stringToQualifiedNameList(defGetString(def));
#endif
            Oid     jsonboid = JSONBOID;
            Oid     funcoid;
            Oid     rettype;

            /*
             * Lookup the function with a single JSONB argument and fail
             * if there isn't one.
             */
            funcoid = LookupFuncName(funcname, 1, &jsonboid, false);
            if ((rettype = get_func_rettype(funcoid)) != TEXTARRAYOID)
            {
                elog(ERROR, "return type of '%s' is %s; expected text[]",
                     defGetString(def), format_type_be(rettype));
            }
            func_provided = true;
        }
        else if (strcmp(def->defname, "files_func_arg") == 0)
        {
            /*
             * Try to convert the string value into JSONB to validate it is
             * properly formatted.
             */
            DirectFunctionCall1(jsonb_in, CStringGetDatum(defGetString(def)));
        }
        else if (strcmp(def->defname, "sorted") == 0)
            ;  /* do nothing */
        else if (strcmp(def->defname, "use_mmap") == 0)
        {
            /* Check that bool value is valid */
            (void) defGetBoolean(def);
        }
        else if (strcmp(def->defname, "use_threads") == 0)
        {
            /* Check that bool value is valid */
            (void) defGetBoolean(def);
        }
        else if (strcmp(def->defname, "max_open_files") == 0)
        {
            /* check that int value is valid */
            string_to_int32(defGetString(def));
        }
        else if (strcmp(def->defname, "files_in_order") == 0)
        {
            /* Check that bool value is valid */
			(void) defGetBoolean(def);
        }
        else
        {
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                     errmsg("parquet_fdw: invalid option \"%s\"",
                            def->defname)));
        }
    }

    if (!filename_provided && !func_provided)
        elog(ERROR, "parquet_fdw: filename or files_func option is required");

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
array_to_fields_list(ArrayType *attnames, ArrayType *atttypes)
{
    List   *res = NIL;
    Datum  *names;
    Datum  *types;
    bool   *nulls;
    int     nnames;
    int     ntypes;

    if (!attnames || !atttypes)
        elog(ERROR, "attnames and atttypes arrays must not be NULL");

    if (ARR_HASNULL(attnames))
        elog(ERROR, "attnames array must not contain NULLs");

    if (ARR_HASNULL(atttypes))
        elog(ERROR, "atttypes array must not contain NULLs");

    deconstruct_array(attnames, TEXTOID, -1, false, 'i', &names, &nulls, &nnames);
    deconstruct_array(atttypes, REGTYPEOID, 4, true, 'i', &types, &nulls, &ntypes);

    if (nnames != ntypes)
        elog(ERROR, "attnames and attypes arrays must have same length");

    for (int i = 0; i < nnames; ++i)
    {
        FieldInfo  *field = (FieldInfo *) palloc(sizeof(FieldInfo));
        char       *attname;
        attname = text_to_cstring(DatumGetTextP(names[i]));

        if (strlen(attname) >= NAMEDATALEN)
            elog(ERROR, "attribute name cannot be longer than %i", NAMEDATALEN - 1);

        memcpy(field->name, attname, strlen(attname) + 1);
        field->oid = types[i];

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
                        const char *servername, List *fields, Oid funcid,
                        Jsonb *arg, Jsonb *options) noexcept
{
    Datum       res;
    FmgrInfo    finfo;
    ArrayType  *arr;
    Oid         ret_type;
    List       *optlist;
    char       *query;

    validate_import_args(tablename, servername, funcid);

    if ((ret_type = get_func_rettype(funcid)) != TEXTARRAYOID)
    {
        elog(ERROR,
             "return type of '%s' function is %s; expected text[]",
             get_func_name(funcid), format_type_be(ret_type));
    }

    optlist = jsonb_to_options_list(options);

    /* Call the user provided function */
    fmgr_info(funcid, &finfo);
    res = FunctionCall1(&finfo, (Datum) arg);

    /*
     * In case function returns NULL the ERROR is thrown. So it's safe to
     * assume function returned something. Just for the sake of readability
     * I leave this condition
     */
    if (res != (Datum) 0)
    {
        Datum  *values;
        bool   *nulls;
        int     num;
        int     ret;

        arr = DatumGetArrayTypeP(res);
        deconstruct_array(arr, TEXTOID, -1, false, 'i', &values, &nulls, &num);

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
        {
            if (nulls[i])
                elog(ERROR, "user function returned an array containing NULL value(s)");
            paths[i] = text_to_cstring(DatumGetTextP(values[i]));
        }

        /*
         * If attributes list is provided then use it. Otherwise get the list
         * from the first file provided by the user function. We trust the user
         * to provide a list of files with the same structure.
         */
        fields = fields ? fields : extract_parquet_fields(paths[0]);

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
    ArrayType  *attnames;
    ArrayType  *atttypes;
    Oid         funcid;
    Jsonb      *arg;
    Jsonb      *options;
    List       *fields;

    tablename = PG_ARGISNULL(0) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(0));
    schemaname = PG_ARGISNULL(1) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(1));
    servername = PG_ARGISNULL(2) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(2));
    attnames = PG_ARGISNULL(3) ? NULL : PG_GETARG_ARRAYTYPE_P(3);
    atttypes = PG_ARGISNULL(4) ? NULL : PG_GETARG_ARRAYTYPE_P(4);
    funcid = PG_ARGISNULL(5) ? InvalidOid : PG_GETARG_OID(5);
    arg = PG_ARGISNULL(6) ? NULL : PG_GETARG_JSONB_P(6);
    options = PG_ARGISNULL(7) ? NULL : PG_GETARG_JSONB_P(7);

    fields = array_to_fields_list(attnames, atttypes);

    import_parquet_internal(tablename, schemaname, servername, fields,
                            funcid, arg, options);

    PG_RETURN_VOID();
}

}
