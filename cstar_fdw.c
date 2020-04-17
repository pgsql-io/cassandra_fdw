/*-------------------------------------------------------------------------
 *
 * cstar_fdw.c
 *                cassandra_fdw.
 *
 * Copyright (c) 2014-2020, BigSQL
 * Portions Copyright (c) 2012-2018, PostgreSQL Global Development Group & Others
 *
 * IDENTIFICATION
 *                contrib/cassandra_fdw/cstar_fdw.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <cassandra.h>
#include <inttypes.h>
#include <time.h>

#include "cstar_fdw.h"
#if PG_VERSION_NUM >= 120000
	#include "access/table.h"
#endif
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "executor/spi.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "mb/pg_wchar.h"
#include "optimizer/cost.h"
#include "optimizer/restrictinfo.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#if PG_VERSION_NUM < 120000
	#include "optimizer/var.h"
#else
	#include "optimizer/optimizer.h"
#endif
#include "parser/parsetree.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

/* Default CPU cost to start up a foreign query. */
#define DEFAULT_FDW_STARTUP_COST	100.0

/* Default CPU cost to process 1 row (above and beyond cpu_tuple_cost). */
#define DEFAULT_FDW_TUPLE_COST		0.01

/* The PRIMARY KEY OPTION name */
/* TODO: Add support for multiple comma-separated PK columns */
#define OPT_PK						"primary_key"

#define SMALLINT_NULL_SET_ISSUE_URL	"https://groups.google.com/a/lists.datastax.com/forum/#!topic/cpp-driver-user/b1XRQdnVH6A"

struct CassFdwOption
{
	const char	*optname;
	Oid			optcontext;		/* Oid of catalog in which option may appear */
};

static struct CassFdwOption valid_options[] =
{
	/* Connection options */
	{ "host",			ForeignServerRelationId },
	{ "port",			ForeignServerRelationId },
	{ "protocol",		ForeignServerRelationId },
	{ "username",		UserMappingRelationId },
	{ "password",		UserMappingRelationId },
	{ "query",			ForeignTableRelationId },
	{ "schema_name",	ForeignTableRelationId },
	{ "table_name",	ForeignTableRelationId },
	/* Pre-req for UPDATE and DELETE support */
	{ OPT_PK,	ForeignTableRelationId },
	{ "read_consistency",	ForeignTableRelationId },
	{ "write_consistency",	ForeignTableRelationId },
	/* Sentinel */
	{ NULL,			InvalidOid }
};


/*
 * This enum describes what's kept in the fdw_private list for a ModifyTable
 * node referencing a cassandra_fdw foreign table.  We store:
 *
 * 1) INSERT/UPDATE/DELETE statement text to be sent to the remote server
 * 2) Integer list of target attribute numbers for INSERT/UPDATE
 *	  (NIL for a DELETE)
 * 3) Boolean flag showing if the remote query has a RETURNING clause
 * 4) Integer list of attribute numbers retrieved by RETURNING, if any
 */
enum FdwModifyPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	FdwModifyPrivateUpdateSql,
	/* Integer list of target attribute numbers for INSERT/UPDATE */
	FdwModifyPrivateTargetAttnums,
	/* has-returning flag (as an integer Value node) */
	FdwModifyPrivateHasReturning,
	/* Integer list of attribute numbers retrieved by RETURNING */
	FdwModifyPrivateRetrievedAttrs
};

/*
 * Execution state of a foreign INSERT/UPDATE/DELETE operation.
 */
typedef struct CassFdwModifyState
{
	Relation	rel;			/* relcache entry for the foreign table */
	AttInMetadata *attinmeta;	/* attribute datatype conversion metadata */

	/* for remote query execution */
	CassSession   *cass_conn; /* connection for the modify */
	bool           sql_sent;
	CassStatement *statement;
	CassConsistency write_consistency;

	/* extracted fdw_private data */
	char	   *query;			/* text of INSERT/UPDATE/DELETE command */
	List	   *target_attrs;	/* list of target attribute numbers */
	bool		has_returning;	/* is there a RETURNING clause? */
	List	   *retrieved_attrs;	/* attr numbers retrieved by RETURNING */

	/* info about parameters for prepared statement */
	AttrNumber	keyAttno;		/* attnum of input resjunk key column */
	int			p_nums;			/* number of parameters to transmit */
	Oid   *p_type_oids;		/* Type OIDs for them */

	/* working memory context */
	MemoryContext temp_cxt;		/* context for per-tuple temporary data */
} CassFdwModifyState;

/*
 * FDW-specific information for RelOptInfo.fdw_private.
 */
typedef struct CassFdwPlanState
{
	/* baserestrictinfo clauses, broken down into safe and unsafe subsets. */
	List	   *remote_conds;
	List	   *local_conds;

	/* Bitmap of attr numbers we need to fetch from the remote server. */
	Bitmapset  *attrs_used;

	/* Estimated size and cost for a scan with baserestrictinfo quals. */
	double		rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;
} CassFdwPlanState;

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */
typedef struct CassFdwScanState
{
	Relation	rel;			/* relcache entry for the foreign table */
	AttInMetadata *attinmeta;	/* attribute datatype conversion metadata */

	/* extracted fdw_private data */
	char	   *query;			/* text of SELECT command */
	List	   *retrieved_attrs;	/* list of retrieved attribute numbers */

	int		NumberOfColumns;

	/* for remote query execution */
	CassSession	   *cass_conn;			/* connection for the scan */
	bool			sql_sended;
	CassStatement  *statement;
	CassConsistency read_consistency;

	/* for storing result tuples */
	HeapTuple  *tuples;			/* array of currently-retrieved tuples */
	int			num_tuples;		/* # of tuples in array */
	int			next_tuple;		/* index of next one to return */

	/* batch-level state, for optimizing rewinds and avoiding useless fetch */
	int			fetch_ct_2;		/* Min(# of fetches done, 2) */
	bool		eof_reached;	/* true if last fetch reached EOF */

	/* working memory contexts */
	MemoryContext batch_cxt;	/* context holding current batch of tuples */
	MemoryContext temp_cxt;		/* context for per-tuple temporary data */
} CassFdwScanState;

enum CassFdwScanPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	CassFdwScanPrivateSelectSql,
	/* Integer list of attribute numbers retrieved by the SELECT */
	CassFdwScanPrivateRetrievedAttrs
};

/*
 * SQL functions
 */
extern Datum cstar_fdw_handler(PG_FUNCTION_ARGS);
extern Datum cstar_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(cstar_fdw_handler);
PG_FUNCTION_INFO_V1(cstar_fdw_validator);

static CassConsistency consistency_from_string(const char *s);

/*
 * FDW callback routines
 */
static void cassGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static void cassGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static ForeignScan *cassGetForeignPlan(
							PlannerInfo *root,
							RelOptInfo *baserel,
							Oid foreigntableid,
							ForeignPath *best_path,
							List *tlist,
							List *scan_clauses,
							Plan *outer_plan
);
static void cassExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void cassBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *cassIterateForeignScan(ForeignScanState *node);
static void cassReScanForeignScan(ForeignScanState *node);
static void cassEndForeignScan(ForeignScanState *node);
static List *cassImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid);

static void
cassAddForeignUpdateTargets(Query *parsetree,
							RangeTblEntry *target_rte,
							Relation target_relation);
static List *
cassPlanForeignModify(PlannerInfo *root, ModifyTable *plan,
					  Index resultRelation, int subplan_index);
static void cassBeginForeignModify(ModifyTableState *mtstate,
					   ResultRelInfo *resultRelInfo, List *fdw_private,
					   int subplan_index, int eflags);
static TupleTableSlot *cassExecForeignInsert(EState *estate,
					  ResultRelInfo *resultRelInfo,
					  TupleTableSlot *slot,
					  TupleTableSlot *planSlot);
static TupleTableSlot *cassExecForeignUpdate(EState *estate,
					  ResultRelInfo *resultRelInfo,
					  TupleTableSlot *slot,
					  TupleTableSlot *planSlot);
static TupleTableSlot *cassExecForeignDelete(EState *estate,
					  ResultRelInfo *resultRelInfo,
					  TupleTableSlot *slot,
					  TupleTableSlot *planSlot);
static void cassEndForeignModify(EState *estate, ResultRelInfo *rinfo);
static void cassExplainForeignModify(ModifyTableState *mtstate,
						 ResultRelInfo *resultRelInfo, List *fdw_private,
						 int subplan_index,
						 struct ExplainState *es);
static int	cassIsForeignRelUpdatable(Relation rel);

/*
 * Helper functions
 */
static void estimate_path_cost_size(PlannerInfo *root,
						RelOptInfo *baserel,
						List *join_conds,
						double *p_rows, int *p_width,
						Cost *p_startup_cost, Cost *p_total_cost);
static bool cassIsValidOption(const char *option, Oid context);
static void
cassGetOptions(Oid foreigntableid,
			   char **host, int *port,
			   char **username, char **password,
			   char **query, char **tablename, char **primarykey, CassConsistency *read_consistency,  CassConsistency *write_consistency);
static void
cassGetPKOption(Oid foreigntableid,
				const char **primarykey);
static void
cassGetReadConsistencyOption(Oid foreigntableid,
				CassConsistency *read_consistency);
static void
cassGetWriteConsistencyOption(Oid foreigntableid,
				CassConsistency *write_consistency);
static void create_cursor(ForeignScanState *node);
static void close_cursor(CassFdwScanState *fsstate);
static void fetch_more_data(ForeignScanState *node);
static void pgcass_transferValue(StringInfo buf, const CassValue* value);
static void pgcass_transformDataType(StringInfo buf, CassValueType type);
static HeapTuple make_tuple_from_result_row(const CassRow* row,
										   int ncolumn,
										   Relation rel,
										   AttInMetadata *attinmeta,
										   List *retrieved_attrs,
										   MemoryContext temp_context);

static void cassClassifyConditions(PlannerInfo *root,
				   RelOptInfo *baserel,
				   List *input_conds,
				   List **remote_conds,
				   List **local_conds);
static void
cassStatementBindNull(const CassStatement * stmt,
					  int pindex, Oid ptypeid, const char *opname,
					  EState *estate, ResultRelInfo *resultRelInfo);
static void releaseCassResources(EState *estate, ResultRelInfo *resultRelInfo);
static void
bind_cass_statement_param(Oid type, Datum value,
						  CassStatement * statement, int pindex);
static TupleTableSlot *
cassExecPKPredWrite(EState *estate,
					ResultRelInfo *resultRelInfo,
					TupleTableSlot *slot,
					TupleTableSlot *planSlot,
                    const char *cqlOpName);

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
cstar_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	fdwroutine->GetForeignRelSize = cassGetForeignRelSize;
	fdwroutine->GetForeignPaths = cassGetForeignPaths;
	fdwroutine->GetForeignPlan = cassGetForeignPlan;

	fdwroutine->ExplainForeignScan = cassExplainForeignScan;
	fdwroutine->BeginForeignScan = cassBeginForeignScan;
	fdwroutine->IterateForeignScan = cassIterateForeignScan;
	fdwroutine->ReScanForeignScan = cassReScanForeignScan;
	fdwroutine->EndForeignScan = cassEndForeignScan;
	fdwroutine->AnalyzeForeignTable = NULL;
	fdwroutine->ImportForeignSchema = cassImportForeignSchema;

	fdwroutine->AddForeignUpdateTargets = cassAddForeignUpdateTargets;
	fdwroutine->PlanForeignModify = cassPlanForeignModify;
	fdwroutine->BeginForeignModify = cassBeginForeignModify;
	fdwroutine->ExecForeignInsert = cassExecForeignInsert;
	fdwroutine->ExecForeignUpdate = cassExecForeignUpdate;
	fdwroutine->ExecForeignDelete = cassExecForeignDelete;
	fdwroutine->EndForeignModify = cassEndForeignModify;
	fdwroutine->ExplainForeignModify = cassExplainForeignModify;
	fdwroutine->IsForeignRelUpdatable = cassIsForeignRelUpdatable;
	PG_RETURN_POINTER(fdwroutine);
}

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses file_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
Datum
cstar_fdw_validator(PG_FUNCTION_ARGS)
{
	List		*options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	char		*svr_host = NULL;
	int			svr_port = 0;
	char		*svr_username = NULL;
	char		*svr_password = NULL;
	char		*svr_query = NULL;
	char		*svr_schema = NULL;
	char		*svr_table = NULL;
	char		*primary_key = NULL;
	ListCell	*cell;

	CassConsistency	read_consistency = DEFAULT_CONSISTENCY_LEVEL;
	CassConsistency	write_consistency = DEFAULT_CONSISTENCY_LEVEL;

	/*
	 * Check that only options supported by cassandra_fdw,
	 * and allowed for the current object type, are given.
	*/
	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (!cassIsValidOption(def->defname, catalog))
		{
			const struct CassFdwOption *opt;
			StringInfoData buf;

			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			initStringInfo(&buf);
			for (opt = valid_options; opt->optname; opt++)
			{
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
							 opt->optname);
			}

			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname),
					 buf.len > 0
					 ? errhint("Valid options in this context are: %s",
							   buf.data)
				  : errhint("There are no valid options in this context.")));
		}

		if (strcmp(def->defname, "host") == 0)
		{
			if (svr_host)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			svr_host = defGetString(def);
		}
		if (strcmp(def->defname, "port") == 0)
		{
			if (svr_port)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			svr_port = atoi(defGetString(def));
		}
		else if (strcmp(def->defname, "username") == 0)
		{
			if (svr_username)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			svr_username = defGetString(def);
		}
		else if (strcmp(def->defname, "password") == 0)
		{
			if (svr_password)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			svr_password = defGetString(def);
		}
		else if (strcmp(def->defname, "query") == 0)
		{
			if (svr_table)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: query cannot be used with table")));

			if (svr_query)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));

			svr_query = defGetString(def);
		}
		else if (strcmp(def->defname, "schema_name") == 0)
		{
			if (svr_schema)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));

			svr_schema = defGetString(def);
		}
		else if (strcmp(def->defname, "table_name") == 0)
		{
			if (svr_query)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: table_name cannot be used with query")));

			if (svr_table)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));

			svr_table = defGetString(def);
		}
		if (strcmp(def->defname, OPT_PK) == 0)
		{
			if (primary_key)
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("conflicting or redundant options")));
			primary_key = defGetString(def);
		}
		if (strcmp(def->defname, "read_consistency") == 0)
		{
			read_consistency = consistency_from_string(defGetString(def));
			if (read_consistency == CASS_CONSISTENCY_UNKNOWN)
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("unknown read consistency level")));
			else if (read_consistency == CASS_CONSISTENCY_ANY)
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("ANY is only supported as a write consistency level, it is not a valid read consistency level")));
		}
		if (strcmp(def->defname, "write_consistency") == 0)
		{
			write_consistency = consistency_from_string(defGetString(def));
			if (write_consistency == CASS_CONSISTENCY_UNKNOWN)
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("unknown write consistency level")));
		}
	}

	if (catalog == ForeignServerRelationId && svr_host == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("host must be specified")));

	if (catalog == ForeignTableRelationId &&
		svr_query == NULL && svr_table == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("either table_name or query must be specified")));

	PG_RETURN_VOID();
}

static CassConsistency
consistency_from_string(const char *s)
{
	if (strcmp(s, "ANY") == 0) return CASS_CONSISTENCY_ANY;
	else if (strcmp(s, "ONE") == 0) return CASS_CONSISTENCY_ONE;
	else if (strcmp(s, "TWO") == 0) return CASS_CONSISTENCY_TWO;
	else if (strcmp(s, "THREE") == 0) return CASS_CONSISTENCY_THREE;
	else if (strcmp(s,  "QUORUM") == 0) return CASS_CONSISTENCY_QUORUM;
	else if (strcmp(s, "ALL") == 0) return CASS_CONSISTENCY_ALL;
	else if (strcmp(s, "LOCAL_QUORUM") == 0) return CASS_CONSISTENCY_LOCAL_QUORUM;
	else if (strcmp(s, "EACH_QUORUM") == 0) return CASS_CONSISTENCY_EACH_QUORUM;
	else if (strcmp(s, "SERIAL") == 0) return CASS_CONSISTENCY_SERIAL;
	else if (strcmp(s, "LOCAL_SERIAL") == 0) return CASS_CONSISTENCY_LOCAL_SERIAL;
	else if (strcmp(s, "LOCAL_ONE") == 0) return CASS_CONSISTENCY_LOCAL_ONE;
	else return CASS_CONSISTENCY_UNKNOWN;
}


/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
cassIsValidOption(const char *option, Oid context)
{
	const struct CassFdwOption *opt;

	for (opt = valid_options; opt->optname; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
			return true;
	}
	return false;
}


/*
 * Fetch the options for a fdw foreign table.
 */
static void
cassGetOptions(Oid foreigntableid, char **host, int *port,
				char **username, char **password, char **query,
				char **tablename, char **primarykey, CassConsistency *read_consistency, CassConsistency *write_consistency)
{
	ForeignTable  *table;
	ForeignServer *server;
	UserMapping   *user;
	List	   *options;
	ListCell   *lc;

	/*
	 * Extract options from FDW objects.
	 */
	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(GetUserId(), server->serverid);

	options = NIL;
	options = list_concat(options, table->options);
	options = list_concat(options, server->options);
	options = list_concat(options, user->options);

	/* Loop through the options */
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "username") == 0)
		{
			*username = defGetString(def);
		}
		else if (strcmp(def->defname, "password") == 0)
		{
			*password = defGetString(def);
		}
		else if (strcmp(def->defname, "query") == 0)
		{
			*query = defGetString(def);
		}
		else if (strcmp(def->defname, "table_name") == 0)
		{
			*tablename = defGetString(def);
		}
		else if (strcmp(def->defname, "host") == 0)
		{
			*host = defGetString(def);
		}
		else if (strcmp(def->defname, "port") == 0)
		{
			*port = atoi(defGetString(def));
		}
		else if (strcmp(def->defname, OPT_PK) == 0)
		{
			*primarykey = defGetString(def);
		}
		else if (strcmp(def->defname, "read_consistency") == 0)
		{
			*read_consistency = consistency_from_string(defGetString(def));
		}
		else if (strcmp(def->defname, "write_consistency") == 0)
		{
			*write_consistency = consistency_from_string(defGetString(def));
		}
	}
}

/*
 * Fetch the primary_key option for a FOREIGN TABLE without returning the
 * remaining options; the PK is the only one needed for certain callbacks such
 * as cassAddForeignUpdateTargets().
 */
static void
cassGetPKOption(Oid foreigntableid,
                const char **primarykey)
{
	ForeignTable *table;
	List         *options;
	ListCell     *lc;

	table = GetForeignTable(foreigntableid);
	options = NIL;
	options = list_concat(options, table->options);

	/* Loop through the options to get the primary_key option. */
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, OPT_PK) == 0)
		{
			*primarykey = defGetString(def);
		}
	}
}

/*
 * Fetch the read_consistency option for a FOREIGN TABLE without returning the
 * remaining options; the read_consistency is the only one needed for certain calls
 */
static void
cassGetReadConsistencyOption(Oid foreigntableid,
                CassConsistency *read_consistency)
{
	ForeignTable *table;
	List         *options;
	ListCell     *lc;

	*read_consistency = DEFAULT_CONSISTENCY_LEVEL;

	table = GetForeignTable(foreigntableid);
	options = NIL;
	options = list_concat(options, table->options);

	/* Loop through the options to get the read_consistency option. */
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "read_consistency") == 0)
		{
			*read_consistency = consistency_from_string(defGetString(def));
		}
	}
}

/*
 * Fetch the write_consistency option for a FOREIGN TABLE without returning the
 * remaining options; the write_consistency is the only one needed for certain calls
 */

static void
cassGetWriteConsistencyOption(Oid foreigntableid,
                CassConsistency *write_consistency)
{
	ForeignTable *table;
	List         *options;
	ListCell     *lc;

	*write_consistency = DEFAULT_CONSISTENCY_LEVEL;

	table = GetForeignTable(foreigntableid);
	options = NIL;
	options = list_concat(options, table->options);

	/* Loop through the options to get the write_consistency option. */
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);
		if (strcmp(def->defname, "write_consistency") == 0)
		{
			*write_consistency = consistency_from_string(defGetString(def));
		}
	}
}


/*
 * cassGetForeignRelSize
 *		Obtain relation size estimates for a foreign table
 */
static void
cassGetForeignRelSize(PlannerInfo *root,
					  RelOptInfo *baserel,
					  Oid foreigntableid)
{
	CassFdwPlanState *fpinfo;
	ListCell   *lc;

	elog(DEBUG1, CSTAR_FDW_NAME
	     ": get foreign rel size for relation ID %d", foreigntableid);

	fpinfo = (CassFdwPlanState *) palloc0(sizeof(CassFdwPlanState));
	baserel->fdw_private = (void *) fpinfo;

	/*
	 * Identify which baserestrictinfo clauses can be sent to the remote
	 * server and which can't.
	 */
	cassClassifyConditions(root, baserel, baserel->baserestrictinfo,
					   &fpinfo->remote_conds, &fpinfo->local_conds);

	fpinfo->attrs_used = NULL;
	pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid,
				   &fpinfo->attrs_used);
	foreach(lc, fpinfo->local_conds)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		pull_varattnos((Node *) rinfo->clause, baserel->relid,
					   &fpinfo->attrs_used);
	}

	/* Fetch options  */

	/* Estimate relation size */
	{
		/*
		 * If the foreign table has never been ANALYZEd, it will have relpages
		 * and reltuples equal to zero, which most likely has nothing to do
		 * with reality.  We can't do a whole lot about that if we're not
		 * allowed to consult the remote server, but we can use a hack similar
		 * to plancat.c's treatment of empty relations: use a minimum size
		 * estimate of 10 pages, and divide by the column-datatype-based width
		 * estimate to get the corresponding number of tuples.
		 */
		if (baserel->pages == 0 && baserel->tuples == 0)
		{
			baserel->pages = 10;
			baserel->tuples =
				(10 * BLCKSZ) / (baserel->reltarget->width + sizeof(HeapTupleHeaderData));

		}

		/* Estimate baserel size as best we can with local statistics. */
		set_baserel_size_estimates(root, baserel);

		/* Fill in basically-bogus cost estimates for use later. */
		estimate_path_cost_size(root, baserel, NIL,
								&fpinfo->rows, &fpinfo->width,
								&fpinfo->startup_cost, &fpinfo->total_cost);
	}
}

static void
estimate_path_cost_size(PlannerInfo *root,
						RelOptInfo *baserel,
						List *join_conds,
						double *p_rows, int *p_width,
						Cost *p_startup_cost, Cost *p_total_cost)
{
	*p_rows = baserel->rows;
	*p_width = baserel->reltarget->width;
	*p_startup_cost = DEFAULT_FDW_STARTUP_COST;
	*p_total_cost = DEFAULT_FDW_TUPLE_COST * 100;
}

/*
 * cassGetForeignPaths
 *		(9.2+) Get the foreign paths
 */
static void
cassGetForeignPaths(PlannerInfo *root,
					RelOptInfo *baserel,
					Oid foreigntableid)
{
	CassFdwPlanState *fpinfo = (CassFdwPlanState *) baserel->fdw_private;
	ForeignPath *path;

	elog(DEBUG1, CSTAR_FDW_NAME
	     ": get foreign paths for relation ID %d", foreigntableid);

	/*
	 * Create simplest ForeignScan path node and add it to baserel.  This path
	 * corresponds to SeqScan path of regular tables (though depending on what
	 * baserestrict conditions we were able to send to remote, there might
	 * actually be an indexscan happening there).  We already did all the work
	 * to estimate cost and size of this path.
	 */
	path = create_foreignscan_path(root, baserel,
								   NULL,
	                               fpinfo->rows + baserel->rows,
	                               fpinfo->startup_cost,
	                               fpinfo->total_cost,
	                               NIL, /* no pathkeys */
	                               NULL,		/* no outer rel either */
	                               NULL,		/* no outer path either */
	                               NIL);		/* no fdw_private list */
	add_path(baserel, (Path *) path);

}

/*
 * cassGetForeignPlan
 *		Create ForeignScan plan node which implements selected best path
 */
static ForeignScan *
cassGetForeignPlan(PlannerInfo *root,
					   RelOptInfo *baserel,
					   Oid foreigntableid,
					   ForeignPath *best_path,
					   List *tlist,
					   List *scan_clauses,
					   Plan *outer_plan
)
{
	CassFdwPlanState *fpinfo = (CassFdwPlanState *) baserel->fdw_private;
	Index		scan_relid = baserel->relid;
	List	   *fdw_private;
	List	   *local_exprs = NIL;
	StringInfoData sql;
	List	   *retrieved_attrs;

	elog(DEBUG1, CSTAR_FDW_NAME
	     ": get foreign plan for relation ID %d", foreigntableid);

	local_exprs = extract_actual_clauses(scan_clauses, false);

	/*
	 * Build the query string to be sent for execution, and identify
	 * expressions to be sent as parameters.
	 */
	initStringInfo(&sql);
	cassDeparseSelectSql(&sql, root, baserel, fpinfo->attrs_used,
					 &retrieved_attrs);

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match enum CassFdwScanPrivateIndex, above.
	 */
	fdw_private = list_make2(makeString(sql.data),
							 retrieved_attrs);

	/*
	 * Create the ForeignScan node from target list, local filtering
	 * expressions, remote parameter expressions, and FDW private information.
	 *
	 * Note that the remote parameter expressions are stored in the fdw_exprs
	 * field of the finished plan node; we can't keep them in private state
	 * because then they wouldn't be subject to later planner processing.
	 */
	return make_foreignscan(tlist,
	                        local_exprs,
	                        scan_relid,
	                        NIL,
	                        fdw_private,
	                        NIL,
	                        NIL,
							NULL);
}

/*
 * cassExplainForeignScan
 *		Produce extra output for EXPLAIN
 */
static void
cassExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	List	   *fdw_private;
	char	   *sql;

	char	*svr_host = NULL;
	int		svr_port = 0;
	char	*svr_username = NULL;
	char	*svr_password = NULL;
	char	*svr_query = NULL;
	char	*svr_table = NULL;
	char	*primary_key = NULL;

	CassConsistency	read_consistency = DEFAULT_CONSISTENCY_LEVEL;
	CassConsistency	write_consistency = DEFAULT_CONSISTENCY_LEVEL;


	elog(DEBUG1, CSTAR_FDW_NAME ": explain foreign scan for relation ID %d",
	     RelationGetRelid(node->ss.ss_currentRelation));

	if (es->verbose)
	{
		/* Fetch options  */
		cassGetOptions(RelationGetRelid(node->ss.ss_currentRelation),
					   &svr_host, &svr_port,
					   &svr_username, &svr_password,
					   &svr_query, &svr_table, &primary_key, &read_consistency, &write_consistency);

		fdw_private = ((ForeignScan *) node->ss.ps.plan)->fdw_private;
		sql = strVal(list_nth(fdw_private, CassFdwScanPrivateSelectSql));
		ExplainPropertyText("Remote SQL", sql, es);
	}
}


/*
 * cassBeginForeignScan
 *		Initiate access to the database
 */
static void
cassBeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	EState	   *estate = node->ss.ps.state;
	CassFdwScanState   *fsstate;
	RangeTblEntry *rte;
	Oid			userid;
	ForeignTable *table;
	ForeignServer *server;
	UserMapping *user;

	elog(DEBUG1, CSTAR_FDW_NAME ": begin foreign scan for relation ID %d",
	     RelationGetRelid(node->ss.ss_currentRelation));

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/*
	 * We'll save private state in node->fdw_state.
	 */
	fsstate = (CassFdwScanState *) palloc0(sizeof(CassFdwScanState));
	node->fdw_state = (void *) fsstate;

	/*
	 * Identify which user to do the remote access as.  This should match what
	 * ExecCheckRTEPerms() does.
	 */
	rte = rt_fetch(fsplan->scan.scanrelid, estate->es_range_table);
	userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

	/* Get info about foreign table. */
	fsstate->rel = node->ss.ss_currentRelation;
	table = GetForeignTable(RelationGetRelid(fsstate->rel));
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(userid, server->serverid);

	/*
	 * Get connection to the foreign server.  Connection manager will
	 * establish new connection if necessary.
	 */
	fsstate->cass_conn = pgcass_GetConnection(server, user, false);
	fsstate->sql_sended = false;

	cassGetReadConsistencyOption(RelationGetRelid(fsstate->rel), &fsstate->read_consistency);

	/* Get private info created by planner functions. */
	fsstate->query = strVal(list_nth(fsplan->fdw_private,
									 CassFdwScanPrivateSelectSql));
	fsstate->retrieved_attrs = (List *) list_nth(fsplan->fdw_private,
											   CassFdwScanPrivateRetrievedAttrs);

	/* Create contexts for batches of tuples and per-tuple temp workspace. */
	fsstate->batch_cxt = AllocSetContextCreate(estate->es_query_cxt,
											   "cassandra_fdw tuple data",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);

	fsstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "cassandra_fdw temporary data",
											  ALLOCSET_SMALL_MINSIZE,
											  ALLOCSET_SMALL_INITSIZE,
											  ALLOCSET_SMALL_MAXSIZE);

	/* Get info we'll need for input data conversion. */
	fsstate->attinmeta = TupleDescGetAttInMetadata(RelationGetDescr(fsstate->rel));
}


/*
 * cassIterateForeignScan
 *		Read next record from the data file and store it into the
 *		ScanTupleSlot as a virtual tuple
 */
static TupleTableSlot*
cassIterateForeignScan(ForeignScanState *node)
{
	CassFdwScanState *fsstate = (CassFdwScanState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	/*
	 * If this is the first call after Begin or ReScan, we need to create the
	 * cursor on the remote side.
	 */
	if (!fsstate->sql_sended)
		create_cursor(node);

	/*
	 * Get some more tuples, if we've run out.
	 */
	if (fsstate->next_tuple >= fsstate->num_tuples)
	{
		/* No point in another fetch if we already detected EOF, though. */
		if (!fsstate->eof_reached)
			fetch_more_data(node);
		/* If we didn't get any tuples, must be end of data. */
		if (fsstate->next_tuple >= fsstate->num_tuples)
			return ExecClearTuple(slot);
	}

	/*
	 * Return the next tuple.
	 */
#if PG_VERSION_NUM < 120000
	ExecStoreTuple(
		fsstate->tuples[fsstate->next_tuple++],
		slot, InvalidBuffer, false);
#else
	ExecStoreHeapTuple(
		fsstate->tuples[fsstate->next_tuple++],
		slot, false);
#endif

	return slot;
}

/*
 * cassReScanForeignScan
 *		Rescan table, possibly with new parameters
 */
static void
cassReScanForeignScan(ForeignScanState *node)
{
	CassFdwScanState *fsstate = (CassFdwScanState *) node->fdw_state;

	elog(DEBUG1, CSTAR_FDW_NAME ": re-scan for foreign relation ID %d",
	     RelationGetRelid(node->ss.ss_currentRelation));

	/* If we haven't created the cursor yet, nothing to do. */
	if (!fsstate->sql_sended)
		return;

	{
		/* Easy: just rescan what we already have in memory, if anything */
		fsstate->next_tuple = 0;
		return;
	}

	/* Now force a fresh FETCH. */
	fsstate->tuples = NULL;
	fsstate->num_tuples = 0;
	fsstate->next_tuple = 0;
	fsstate->fetch_ct_2 = 0;
	fsstate->eof_reached = false;
}

/*
 * cassEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
cassEndForeignScan(ForeignScanState *node)
{
	CassFdwScanState *fsstate = (CassFdwScanState *) node->fdw_state;

	elog(DEBUG1, CSTAR_FDW_NAME ": end foreign scan for relation ID %d",
	     RelationGetRelid(node->ss.ss_currentRelation));

	/* if fsstate is NULL, we are in EXPLAIN; nothing to do */
	if (fsstate == NULL)
		return;

	/* Close the cursor if open, to prevent accumulation of cursors */
	if (fsstate->sql_sended)
		close_cursor(fsstate);

	/* Release remote connection */
	pgcass_ReleaseConnection(fsstate->cass_conn);
	fsstate->cass_conn = NULL;

	/* MemoryContexts will be deleted automatically. */
}

/*
 * cassAddForeignUpdateTargets
 * 		Add the PRIMARY KEY column as resjunk entry.
 */
static void
cassAddForeignUpdateTargets(Query *parsetree,
							RangeTblEntry *target_rte,
							Relation target_relation)
{
	Oid         relid        = RelationGetRelid(target_relation);
	TupleDesc   tupdesc      = target_relation->rd_att;
	const char *primary_key = NULL;
	bool		has_PK       = false;
	int         i;

	elog(DEBUG1, CSTAR_FDW_NAME
	     ": add target column(s) for write on relation ID %d", relid);

	cassGetPKOption(relid, &primary_key);

	if (primary_key == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("No PRIMARY KEY specified for the FOREIGN TABLE "
						"'%s.%s'.",
						pstrdup(get_namespace_name(RelationGetNamespace(
														  target_relation))),
						pstrdup(RelationGetRelationName(target_relation))),
				 errdetail("For UPDATE or DELETE, a PRIMARY KEY must be "
						   "defined for the FOREIGN TABLE."),
				 errhint("Set the FOREIGN TABLE OPTION '%s' to a "
						 "PRIMARY KEY column.",
						 OPT_PK)));
	}

	/*
	 * Loop through all columns of the FOREIGN TABLE to determine the PK
	 * attribute to be added as hidden target column for UPDATE and DELETE
	 * statements.
	 */
	for (i = 0; i < tupdesc->natts; ++i)
	{
#if PG_VERSION_NUM < 110000
		Form_pg_attribute att = tupdesc->attrs[i];
#else
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
#endif
		AttrNumber attrno = att->attnum;

		if (strncmp(NameStr(att->attname), primary_key, strlen(primary_key))
			== 0)
		{
			Var *var;
			TargetEntry *tle;

			/* Make a Var representing the desired value */
			var = makeVar(parsetree->resultRelation,
			              attrno,
			              att->atttypid,
			              att->atttypmod,
			              att->attcollation,
			              0);

			/* Wrap it in a resjunk TLE with the right name ... */
			tle = makeTargetEntry((Expr *)var,
			                      list_length(parsetree->targetList) + 1,
			                      pstrdup(NameStr(att->attname)),
			                      true);

			/* ... and add it to the query's targetlist */
			parsetree->targetList = lappend(parsetree->targetList, tle);

			has_PK = true;
		}
	}

	if (!has_PK)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
		         errmsg("The specified PRIMARY KEY '%s' does not exist in the "
		                "FOREIGN TABLE '%s.%s'.", primary_key,
		                pstrdup(get_namespace_name(RelationGetNamespace(
			                                           target_relation))),
		                pstrdup(RelationGetRelationName(target_relation))),
		         errdetail("For UPDATE or DELETE, a valid PRIMARY KEY must be "
		                   "defined for the FOREIGN TABLE."),
		         errhint("Set the FOREIGN TABLE OPTION '%s' to a "
		                 "valid PRIMARY KEY column.",
		                 OPT_PK)));
	}
}


/*
 * cassPlanForeignModify
 *		Plan an INSERT/UPDATE/DELETE operation on a FOREIGN TABLE
 *
 * Note: currently, the plan tree generated for UPDATE/DELETE will always
 * include a ForeignScan that retrieves PKs and then the ModifyTable node will
 * have to execute individual remote UPDATE/DELETE commands.
 */
static List *
cassPlanForeignModify(PlannerInfo *root,
					  ModifyTable *plan,
					  Index resultRelation,
					  int subplan_index)
{
	CmdType		operation       = plan->operation;
	RangeTblEntry  *rte             = planner_rt_fetch(resultRelation, root);
	Relation        rel;
	StringInfoData  sql;
	List           *targetAttrs     = NIL;
	List           *retrieved_attrs = NIL;
	bool            doNothing       = false;
	const char	   *primaryKey;

	elog(DEBUG1, CSTAR_FDW_NAME ": plan foreign modify");

	initStringInfo(&sql);

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */

#if PG_VERSION_NUM < 120000
	rel = heap_open(rte->relid, NoLock);
#else
	rel = table_open(rte->relid, NoLock);
#endif

	/*
	 * In an INSERT, we transmit all columns that are defined in the FOREIGN
	 * TABLE.  In an UPDATE, we transmit only columns that were explicitly
	 * targets of the UPDATE, so as to avoid unnecessary data transmission.
	 * (We can't do that for INSERT since we would miss sending default values
	 * for columns not listed in the source statement.)
	 */
	if (operation == CMD_INSERT)
	{
		TupleDesc	tupdesc = RelationGetDescr(rel);
		int			attnum;

		for (attnum = 1; attnum <= tupdesc->natts; attnum++)
		{
#if PG_VERSION_NUM < 110000
			Form_pg_attribute attr = tupdesc->attrs[attnum - 1];
#else
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
#endif

			if (!attr->attisdropped)
				targetAttrs = lappend_int(targetAttrs, attnum);
		}
	}
	else if (operation == CMD_UPDATE)
	{
		int			col;
		Bitmapset		*updatedCols = rte->updatedCols;

		col = -1;
		while ((col = bms_next_member(updatedCols, col)) >= 0)
		{
			/* bit numbers are offset by FirstLowInvalidHeapAttributeNumber */
			AttrNumber	attno = col + FirstLowInvalidHeapAttributeNumber;

			if (attno <= InvalidAttrNumber)		/* shouldn't happen */
				elog(ERROR, "system-column update is not supported");
			targetAttrs = lappend_int(targetAttrs, attno);
		}
	}

	/*
	 * ON CONFLICT DO UPDATE and DO NOTHING case with inference specification
	 * should have already been rejected in the optimizer, as presently there
	 * is no way to recognize an arbiter index on a foreign table.  Only DO
	 * NOTHING is supported without an inference specification.
	 */
	if (plan->onConflictAction == ONCONFLICT_NOTHING)
		doNothing = true;
	else if (plan->onConflictAction != ONCONFLICT_NONE)
		elog(ERROR, "unexpected ON CONFLICT specification: %d",
			 (int) plan->onConflictAction);

	cassGetPKOption(rte->relid, &primaryKey);
	/*
	 * Construct the SQL command string.
	 */
	switch (operation)
	{
		case CMD_INSERT:
				cassDeparseInsertSql(&sql, root, resultRelation, rel,
									 targetAttrs, doNothing);
				break;
		case CMD_UPDATE:
				cassDeparseUpdateSql(&sql, root, resultRelation, rel,
									 targetAttrs, primaryKey);
				break;
		case CMD_DELETE:
				cassDeparseDeleteSql(&sql, root, resultRelation, rel,
									 &retrieved_attrs, primaryKey);
			break;
		default:
			elog(ERROR, "unexpected operation: %d", (int) operation);
			break;
	}
#if PG_VERSION_NUM < 120000
	heap_close(rel, NoLock);
#else
	table_close(rel, NoLock);
#endif

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match enum FdwModifyPrivateIndex, above.
	 */
	return list_make4(makeString(sql.data),
					  targetAttrs,
					  makeInteger((retrieved_attrs != NIL)),
					  retrieved_attrs);
}

/*
 * cassBeginForeignModify
 *		Begin an INSERT/UPDATE/DELETE operation on a foreign table
 */
static void cassBeginForeignModify(ModifyTableState *mtstate,
                                   ResultRelInfo *resultRelInfo,
                                   List *fdw_private, int subplan_index,
                                   int eflags)
{
	CassFdwModifyState *fmstate;
	EState             *estate    = mtstate->ps.state;
	CmdType             operation = mtstate->operation;
	Relation            rel       = resultRelInfo->ri_RelationDesc;
	RangeTblEntry      *rte;
	Oid                 userid;
	ForeignTable       *table;
	ForeignServer      *server;
	UserMapping        *user;
	AttrNumber          n_params;
	ListCell           *lc;
	const char         *primaryKey;

	elog(DEBUG1, CSTAR_FDW_NAME ": begin foreign modify on relation ID %d",
		RelationGetRelid(resultRelInfo->ri_RelationDesc));

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  resultRelInfo->ri_FdwState
	 * stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* Begin constructing CassFdwModifyState. */
	fmstate = (CassFdwModifyState *) palloc0(sizeof(CassFdwModifyState));
	fmstate->rel = rel;

	/*
	 * Identify which user to do the remote access as.  This should match what
	 * ExecCheckRTEPerms() does.
	 */
	rte = rt_fetch(resultRelInfo->ri_RangeTableIndex, estate->es_range_table);
	userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

	/* Get info about foreign table. */
	table = GetForeignTable(RelationGetRelid(rel));
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(userid, server->serverid);

	/*
	 * Get connection to the foreign server.  Connection manager will
	 * establish new connection if necessary.
	 */
	fmstate->cass_conn = pgcass_GetConnection(server, user, false);
	fmstate->sql_sent = false;

	cassGetWriteConsistencyOption(RelationGetRelid(fmstate->rel), &fmstate->write_consistency);

	/* Deconstruct fdw_private data. */
	fmstate->query = strVal(list_nth(fdw_private,
									 FdwModifyPrivateUpdateSql));
	fmstate->target_attrs = (List *) list_nth(fdw_private,
											  FdwModifyPrivateTargetAttnums);
	fmstate->has_returning = intVal(list_nth(fdw_private,
											 FdwModifyPrivateHasReturning));
	fmstate->retrieved_attrs = (List *) list_nth(fdw_private,
											 FdwModifyPrivateRetrievedAttrs);

	/* Create context for per-tuple temp workspace. */
	fmstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
	                                          "cassandra_fdw temporary data",
	                                          ALLOCSET_SMALL_MINSIZE,
	                                          ALLOCSET_SMALL_INITSIZE,
	                                          ALLOCSET_SMALL_MAXSIZE);

	/* Prepare for input conversion of RETURNING results. */
	if (fmstate->has_returning)
		fmstate->attinmeta = TupleDescGetAttInMetadata(RelationGetDescr(rel));

	/* Prepare for output conversion of parameters used in modify stmt. */
	n_params = list_length(fmstate->target_attrs) + 1;
	fmstate->p_type_oids = (Oid *) palloc0(sizeof(Oid) * n_params);
	fmstate->p_nums = 0;

	if (operation == CMD_INSERT || operation == CMD_UPDATE)
	{
		/* Set up for remaining transmittable parameters */
		foreach(lc, fmstate->target_attrs)
		{
			int			attnum = lfirst_int(lc);
#if PG_VERSION_NUM < 110000
			Form_pg_attribute attr = RelationGetDescr(rel)->attrs[attnum - 1];
#else
			Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(rel), attnum - 1);
#endif

			Assert(!attr->attisdropped);

			fmstate->p_type_oids[fmstate->p_nums] = attr->atttypid;
			fmstate->p_nums++;
		}
	}

	if (operation == CMD_UPDATE || operation == CMD_DELETE)
	{
		/* Find the key resjunk column in the subplan's result */
		Plan              *subplan = mtstate->mt_plans[subplan_index]->plan;
		Form_pg_attribute  attr;
		AttrNumber         attnum;

		cassGetPKOption(rel->rd_id, &primaryKey);

		fmstate->keyAttno = ExecFindJunkAttributeInTlist(subplan->targetlist,
		                                                 primaryKey);

		if (!AttributeNumberIsValid(fmstate->keyAttno))
			ereport(ERROR,
			        (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
			         errmsg("%s: Internal error -- could not find the "
			                "junk attribute in the target list "
			                "and modifying without a key is not possible",
			                CSTAR_FDW_NAME)));

		attnum = get_attnum(rel->rd_id, primaryKey);
		elog(DEBUG5, CSTAR_FDW_NAME ": The PK attribute number is %d", attnum);

#if PG_VERSION_NUM < 110000
		attr = RelationGetDescr(rel)->attrs[attnum - 1];
#else
		attr = TupleDescAttr(RelationGetDescr(rel), attnum - 1);
#endif


		Assert(strncmp(NameStr(attr->attname), primaryKey, strlen(primaryKey))
		       == 0);

		elog(DEBUG5, CSTAR_FDW_NAME
		     ": The PK attribute name after mapping is %s",
		     NameStr(attr->attname));

		fmstate->p_type_oids[fmstate->p_nums] = attr->atttypid;
		fmstate->p_nums++;
	}

	Assert(fmstate->p_nums <= n_params);

	resultRelInfo->ri_FdwState = fmstate;
}

/*
 * releaseCassResources
 *		Release in-use Cassandra statement and connection resources if any.
 */
static
void releaseCassResources(EState *estate, ResultRelInfo *resultRelInfo)
{
	CassFdwModifyState *fmstate = (CassFdwModifyState *)
		resultRelInfo->ri_FdwState;

	/* if fmstate is NULL, we are in EXPLAIN; nothing to do */
	if (fmstate == NULL)
		return;

	elog(DEBUG2, CSTAR_FDW_NAME ": release resources");

	/* Close the statement if open */
	if (fmstate->statement && fmstate->sql_sent)
		cass_statement_free(fmstate->statement);

	/* Release remote connection */
	pgcass_ReleaseConnection(fmstate->cass_conn);
	fmstate->cass_conn = NULL;
}

/*
 * cassStatementBindNull
 *		Bind NULL to a param position while checking for errors and releasing
 *		resources upon error.
 */
static
void cassStatementBindNull(const CassStatement *stmt,
                           int pindex, Oid ptypeid, const char *opname,
                           EState *estate, ResultRelInfo *resultRelInfo)
{
	if (ptypeid == INT2OID)
	{
		releaseCassResources(estate, resultRelInfo);

		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("Failed to execute %s into Cassandra: \n  "
						"Unable to bind NULL to a SMALLINT COLUMN because of "
						"%s", opname, SMALLINT_NULL_SET_ISSUE_URL)));
	}

	cass_statement_bind_null((CassStatement *) stmt, pindex);
}

/*
 * cassExecForeignInsert
 *		Insert one row into a FOREIGN TABLE
 */
static TupleTableSlot *cassExecForeignInsert(EState *estate,
                                             ResultRelInfo *resultRelInfo,
                                             TupleTableSlot *slot,
                                             TupleTableSlot *planSlot)
{
	CassFdwModifyState *fmstate = (CassFdwModifyState *) resultRelInfo->ri_FdwState;
	int                 pindex  = 0;
	MemoryContext       oldcontext;
	CassFuture*         future  = NULL;
	CassError           rc      = CASS_OK;

	elog(DEBUG1, CSTAR_FDW_NAME ": begin foreign INSERT on relation ID %d",
		RelationGetRelid(resultRelInfo->ri_RelationDesc));

	oldcontext = MemoryContextSwitchTo(fmstate->temp_cxt);

	if (!fmstate->sql_sent)
		fmstate->statement = cass_statement_new(fmstate->query,
		                                        fmstate->p_nums);

	if (slot != NULL && fmstate->target_attrs != NIL)
	{
		ListCell   *lc;

		foreach(lc, fmstate->target_attrs)
		{
			int   attnum = lfirst_int(lc);
			Datum value;
			bool  isnull;

			value = slot_getattr(slot, attnum, &isnull);
			if (isnull)
				cassStatementBindNull(fmstate->statement, pindex,
				                      fmstate->p_type_oids[pindex], "INSERT",
				                      estate, resultRelInfo);
			else
				bind_cass_statement_param(fmstate->p_type_oids[pindex],
				                          value, fmstate->statement, pindex);

			pindex++;
		}

		Assert(pindex == fmstate->p_nums);
	}

	cass_statement_set_consistency(fmstate->statement, fmstate->write_consistency);
	future = cass_session_execute(fmstate->cass_conn, fmstate->statement);
	fmstate->sql_sent = true;
	cass_future_wait(future);

	rc = cass_future_error_code(future);

	if (rc != CASS_OK)
	{
		const char* message;
		size_t message_length;
		cass_future_error_message(future, &message, &message_length);
		cass_future_free(future);
		releaseCassResources(estate, resultRelInfo);

		ereport(ERROR,
		        (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
		         errmsg("Failed to execute the INSERT into Cassandra: %.*s",
		                (int)message_length, message)));
	}

	cass_future_free(future);
	MemoryContextSwitchTo(oldcontext);

	MemoryContextReset(fmstate->temp_cxt);

	return slot;
}

/*
 * cassExecForeignUpdate
 *		Update one row in a FOREIGN TABLE
 */
static TupleTableSlot *cassExecForeignUpdate(EState *estate,
                                             ResultRelInfo *resultRelInfo,
                                             TupleTableSlot *slot,
                                             TupleTableSlot *planSlot)
{
	return cassExecPKPredWrite(
		estate, resultRelInfo, slot, planSlot, "UPDATE");
}

/*
 * cassExecForeignDelete
 *		Delete one row from a FOREIGN TABLE
 */
static TupleTableSlot *cassExecForeignDelete(EState *estate,
                                             ResultRelInfo *resultRelInfo,
                                             TupleTableSlot *slot,
                                             TupleTableSlot *planSlot)
{
	return cassExecPKPredWrite(
		estate, resultRelInfo, slot, planSlot, "DELETE");
}

/*
 * cassEndForeignModify
 *		Finish an INSERT/UPDATE/DELETE operation on a FOREIGN TABLE.
 */
static void cassEndForeignModify(EState *estate, ResultRelInfo *resultRelInfo)
{
	elog(DEBUG1, CSTAR_FDW_NAME ": end foreign modify for relation ID %d",
	     RelationGetRelid(resultRelInfo->ri_RelationDesc));

	releaseCassResources(estate, resultRelInfo);
	/* MemoryContexts will be deleted automatically. */
}

static void cassExplainForeignModify(ModifyTableState *mtstate,
                                     ResultRelInfo *rinfo, List *fdw_private,
                                     int subplan_index,
                                     struct ExplainState *es)
{
	elog(DEBUG1, CSTAR_FDW_NAME ": explain foreign modify");
}

/*
 * cassIsForeignRelUpdatable
 *		Determine whether a FOREIGN TABLE supports INSERT, UPDATE and/or
 *		DELETE.
 */
static int	cassIsForeignRelUpdatable(Relation rel)
{
	/*
	 * Cassandra does not provide "read_only" tables and we do not need to
	 * emulate them on the local side presently.
	 */
	return (1 << CMD_UPDATE) | (1 << CMD_INSERT) | (1 << CMD_DELETE);
}

/*
 * Create cursor for node's query with current parameter values.
 */
static void
create_cursor(ForeignScanState *node)
{
	CassFdwScanState *fsstate = (CassFdwScanState *) node->fdw_state;

	/* Build statement and execute query */
	fsstate->statement = cass_statement_new(fsstate->query, 0);

	/* Mark the cursor as created, and show no tuples have been retrieved */
	fsstate->sql_sended = true;
	fsstate->tuples = NULL;
	fsstate->num_tuples = 0;
	fsstate->next_tuple = 0;
	fsstate->fetch_ct_2 = 0;
	fsstate->eof_reached = false;
}

/*
 * Utility routine to close a cursor.
 */
static void
close_cursor(CassFdwScanState *fsstate)
{
	if (fsstate->statement)
		cass_statement_free(fsstate->statement);
}

/*
 * Fetch some more rows from the node's cursor.
 */
static void
fetch_more_data(ForeignScanState *node)
{
	CassFdwScanState *fsstate = (CassFdwScanState *) node->fdw_state;
	MemoryContext oldcontext;
	CassFuture* 	result_future = NULL;

	/*
	 * We'll store the tuples in the batch_cxt.  First, flush the previous
	 * batch.
	 */
	fsstate->tuples = NULL;
	MemoryContextReset(fsstate->batch_cxt);
	oldcontext = MemoryContextSwitchTo(fsstate->batch_cxt);

	{
		cass_statement_set_consistency(fsstate->statement, fsstate->read_consistency);
		result_future = cass_session_execute(fsstate->cass_conn, fsstate->statement);
		if (cass_future_error_code(result_future) == CASS_OK)
		{
			const CassResult* res;
			int			numrows;
			CassIterator* rows;
			int			k;

			/* Retrieve result set and iterate over the rows */
			res = cass_future_get_result(result_future);

			/* Stash away the state info we have already */
			fsstate->NumberOfColumns = cass_result_column_count(res);

			/* Convert the data into HeapTuples */
			numrows = cass_result_row_count(res);
			fsstate->tuples = (HeapTuple *) palloc0(numrows * sizeof(HeapTuple));
			fsstate->num_tuples = numrows;
			fsstate->next_tuple = 0;

			rows = cass_iterator_from_result(res);
			k = 0;
			while (cass_iterator_next(rows))
			{
				const CassRow* row = cass_iterator_get_row(rows);

				fsstate->tuples[k] = make_tuple_from_result_row(row,
															fsstate->NumberOfColumns,
															fsstate->rel,
															fsstate->attinmeta,
															fsstate->retrieved_attrs,
															fsstate->temp_cxt);

				Assert(k < numrows);
				k++;
			}

			fsstate->eof_reached = true;

			cass_result_free(res);
			cass_iterator_free(rows);
		}
		else
		{
			/* On error, report the original query. */
			pgcass_report_error(ERROR, result_future, true, fsstate->query);

			fsstate->eof_reached = true;
		}

		cass_future_free(result_future);
		MemoryContextSwitchTo(oldcontext);
	}
}

static HeapTuple
make_tuple_from_result_row(const CassRow* row,
						   int ncolumn,
						   Relation rel,
						   AttInMetadata *attinmeta,
						   List *retrieved_attrs,
						   MemoryContext temp_context)
{
	HeapTuple	tuple;
	TupleDesc	tupdesc = RelationGetDescr(rel);
	Datum	   *values;
	bool	   *nulls;
	MemoryContext oldcontext;
	ListCell   *lc;
	int			j;
	StringInfoData buf;

	/*
	 * Do the following work in a temp context that we reset after each tuple.
	 * This cleans up not only the data we have direct access to, but any
	 * cruft the I/O functions might leak.
	 */
	oldcontext = MemoryContextSwitchTo(temp_context);

	values = (Datum *) palloc0(tupdesc->natts * sizeof(Datum));
	nulls = (bool *) palloc(tupdesc->natts * sizeof(bool));
	/* Initialize to nulls for any columns not present in result */
	memset(nulls, true, tupdesc->natts * sizeof(bool));

	initStringInfo(&buf);

	/*
	 * i indexes columns in the relation, j indexes columns in the PGresult.
	 */
	j = 0;
	foreach(lc, retrieved_attrs)
	{
		int			i = lfirst_int(lc);
		const char	   *valstr;

		const CassValue* cassVal = cass_row_get_column(row, j);
		if (cass_true == cass_value_is_null(cassVal))
			valstr = NULL;
		else
		{
			pgcass_transferValue(&buf, cassVal);
			valstr = buf.data;
		}

		if (i > 0)
		{
			/* ordinary column */
			Assert(i <= tupdesc->natts);
			nulls[i - 1] = (valstr == NULL);
			/* Apply the input function even to nulls, to support domains */
			values[i - 1] = InputFunctionCall(&attinmeta->attinfuncs[i - 1],
											  (char *) valstr,
											  attinmeta->attioparams[i - 1],
											  attinmeta->atttypmods[i - 1]);
		}

		resetStringInfo(&buf);

		j++;
	}

	/*
	 * Check we got the expected number of columns.  Note: j == 0 and
	 * PQnfields == 1 is expected, since deparse emits a NULL if no columns.
	 */
	if (j > 0 && j != ncolumn)
		elog(ERROR, "remote query result does not match the foreign table");

	/*
	 * Build the result tuple in caller's memory context.
	 */
	MemoryContextSwitchTo(oldcontext);

	tuple = heap_form_tuple(tupdesc, values, nulls);

	/* Clean up */
	MemoryContextReset(temp_context);

	return tuple;
}

static void
pgcass_transferValue(StringInfo buf, const CassValue* value)
{
	CassValueType type = cass_value_type(value);
	switch (type)
	{
	case CASS_VALUE_TYPE_TINY_INT:
	{
		cass_int8_t i;
		cass_value_get_int8(value, &i);
		appendStringInfo(buf, "%d", i);
		break;
	}
	case CASS_VALUE_TYPE_SMALL_INT:
	{
		cass_int16_t i;
		cass_value_get_int16(value, &i);
		appendStringInfo(buf, "%d", i);
		break;
	}
	case CASS_VALUE_TYPE_INT:
	{
		cass_int32_t i;
		cass_value_get_int32(value, &i);
		appendStringInfo(buf, "%d", i);
		break;
	}
	case CASS_VALUE_TYPE_BIGINT:
	case CASS_VALUE_TYPE_COUNTER:
	{
		cass_int64_t i;
		cass_value_get_int64(value, &i);
		appendStringInfo(buf, "%lld ", (long long int)i);
		break;
	}
	case CASS_VALUE_TYPE_BOOLEAN:
	{
		cass_bool_t b;
		cass_value_get_bool(value, &b);
		appendStringInfoString(buf, b ? "true" : "false");
		break;
	}
	case CASS_VALUE_TYPE_FLOAT:
	{
		cass_float_t d;
		cass_value_get_float(value, &d);
		appendStringInfo(buf, "%f", d);
		break;
	}
	case CASS_VALUE_TYPE_DOUBLE:
	{
		cass_double_t d;
		cass_value_get_double(value, &d);
		appendStringInfo(buf, "%f", d);
		break;
	}

	case CASS_VALUE_TYPE_TEXT:
	case CASS_VALUE_TYPE_ASCII:
	case CASS_VALUE_TYPE_VARCHAR:
	{
		const char* s;
		size_t s_length;
		cass_value_get_string(value, &s, &s_length);
		appendStringInfo(buf, "%.*s", (int)s_length, s);
		break;
	}
	case CASS_VALUE_TYPE_TIMESTAMP:
	{
		cass_int64_t timestamp;

		cass_value_get_int64(value, &timestamp);
		/* cassandra stores in milliseconds so convert to seconds */
		timestamp /= MSECS_PER_SEC;
		appendStringInfo(buf, "%s %s",
					  asctime(gmtime(((time_t *) &timestamp))), LITERAL_UTC);
		break;
	}
	case CASS_VALUE_TYPE_UUID:
	{
		CassUuid u;

		cass_value_get_uuid(value, &u);
		cass_uuid_string(u, buf->data + buf->len);
		buf->len += CASS_UUID_STRING_LENGTH;
		buf->data[buf->len] = '\0';
		break;
	}
	case CASS_VALUE_TYPE_INET:
	{
		CassInet i;

		cass_value_get_inet(value, &i);
		cass_inet_string(i, buf->data + buf->len);
		buf->len += CASS_INET_STRING_LENGTH;
		buf->data[buf->len] = '\0';
		break;
	}
	case CASS_VALUE_TYPE_LIST:
	case CASS_VALUE_TYPE_MAP:
	default:
		appendStringInfoString(buf, "<unhandled type>");
		break;
	}
}

static void
pgcass_transformDataType(StringInfo buf, CassValueType type)
{
	char *valid_datatype = NULL, *invalid_datatype = NULL;
	switch (type)
	{
	case CASS_VALUE_TYPE_TINY_INT:
	{
		invalid_datatype = "tinyint";
		break;
	}
	case CASS_VALUE_TYPE_SMALL_INT:
	{
		valid_datatype = "smallint";
		break;
	}
	case CASS_VALUE_TYPE_INT:
	{
		valid_datatype = "integer";
		break;
	}
	case CASS_VALUE_TYPE_BIGINT:
	case CASS_VALUE_TYPE_COUNTER:
	{
		valid_datatype = "bigint";
		break;
	}
	case CASS_VALUE_TYPE_BOOLEAN:
	{
		valid_datatype = "boolean";
		break;
	}
	case CASS_VALUE_TYPE_DOUBLE:
	{
		valid_datatype = "double precision";
		break;
	}
	case CASS_VALUE_TYPE_FLOAT:
	{
		valid_datatype = "real";
		break;
	}
	case CASS_VALUE_TYPE_DECIMAL:
	{
		invalid_datatype = "decimal";
		break;
	}
	case CASS_VALUE_TYPE_TEXT:
	case CASS_VALUE_TYPE_ASCII:
	case CASS_VALUE_TYPE_VARCHAR:
	{
		valid_datatype = "text";
		break;
	}
	case CASS_VALUE_TYPE_TIMESTAMP:
	{
		valid_datatype = "timestamp(0) with time zone";
		break;
	}
	case CASS_VALUE_TYPE_INET:
	{
		valid_datatype = "inet";
		break;
	}
	case CASS_VALUE_TYPE_UUID:
	{
		valid_datatype = "uuid";
		break;
	}
	case CASS_VALUE_TYPE_LIST:
	{
		invalid_datatype = "list";
		break;
	}
	case CASS_VALUE_TYPE_MAP:
		invalid_datatype = "map";
		break;
	default:
		invalid_datatype = "unknown";
		break;
	}

	if (valid_datatype)
		appendStringInfoString(buf, valid_datatype);
	else
		ereport(ERROR,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		         errmsg("Data type %s not supported.", invalid_datatype)));
}

/*
 * Examine each qual clause in input_conds, and classify them into two groups,
 * which are returned as two lists:
 *	- remote_conds contains expressions that can be evaluated remotely
 *	- local_conds contains expressions that can't be evaluated remotely
 */
static void
cassClassifyConditions(PlannerInfo *root,
					   RelOptInfo *baserel,
					   List *input_conds,
					   List **remote_conds,
					   List **local_conds)
{
	ListCell   *lc;

	*remote_conds = NIL;
	*local_conds = NIL;

	foreach(lc, input_conds)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

		*local_conds = lappend(*local_conds, ri);
	}
}

/*
 * bind_cass_statement_param
 *
 * 	Map a parameter to its corresponding bind call for the Cassandra C(++)
 * 	Driver.
 */
static void bind_cass_statement_param(Oid type, Datum value,
                                      CassStatement *statement, int pindex)
{
	switch (type)
	{
		case INT2OID:
		{
			int16 int16_val = DatumGetInt16(value);
			cass_statement_bind_int16(statement, pindex, int16_val);
			break;
		}
		case INT4OID:
		{
			int32 int32_val = DatumGetInt32(value);
			cass_statement_bind_int32(statement, pindex, int32_val);
			break;
		}
		case INT8OID:
		{
			int64 int64_val = DatumGetInt64(value);
			cass_statement_bind_int64(statement, pindex, int64_val);
			break;
		}
		case FLOAT4OID:
		{
			float4 float4_val = DatumGetFloat4(value);
			cass_statement_bind_float(statement, pindex, float4_val);
			break;
		}
		case FLOAT8OID:
		{
			float8 float8_val = DatumGetFloat8(value);
			cass_statement_bind_double(statement, pindex, float8_val);
			break;
		}
		case BOOLOID:
		{
			bool bool_val = DatumGetBool(value);
			cass_statement_bind_bool(statement, pindex, bool_val);
			break;
		}
		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
		{
			char *str_val = NULL;
			Oid output_func_oid = InvalidOid;
			bool type_var_length = false;

			getTypeOutputInfo(type, &output_func_oid, &type_var_length);
			str_val = OidOutputFunctionCall(output_func_oid, value);

			cass_statement_bind_string(statement, pindex, str_val);
			break;
		}
		case TIMESTAMPTZOID:
		case TIMESTAMPOID:
		{
			struct pg_tm datetime_tm;
			int32 tzoffset = 0;
			fsec_t datetime_fsec;
			Timestamp time;

			/* get the parts */
			(void) timestamp2tm(DatumGetTimestampTz(value),
				            &tzoffset, 
				            &datetime_tm,
				            &datetime_fsec,
				            NULL,
				            NULL);
			/*
			 * PostgreSQL stores the timestamp in the datetime format. The unix timestamp
			 * is essentially the difference between this value and the timestamp
			 * representing the epoch datetime. Also PostgreSQL representation is in
			 * microseconds. Since cassandra expects the timestamp in milliseconds
			 * we further convert this into milliseconds.
			 */
			time = (DatumGetTimestampTz(value)- SetEpochTimestamp() + tzoffset) / MSECS_PER_SEC;
			cass_statement_bind_int64(statement, pindex, (int64)time);
			break;
		}
		default:
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("Data type with OID %d not supported.", type)));
			break;
		}
	}
}

/*
 * cassExecPKPredWrite
 *		Execute a PK-predicated write operation against Cassandra.
 *		This is used for the remote UPDATE and DELETE execution.
 */
static TupleTableSlot *
cassExecPKPredWrite(EState *estate,
					ResultRelInfo *resultRelInfo,
					TupleTableSlot *slot,
					TupleTableSlot *planSlot,
					const char *cqlOpName)
{
	CassFdwModifyState *fmstate = (CassFdwModifyState *) resultRelInfo->ri_FdwState;
	int                 pindex  = 0;
	MemoryContext       oldcontext;
	CassFuture*         future  = NULL;
	CassError           rc      = CASS_OK;
	Datum               value;
	bool                isnull;

	elog(DEBUG1, CSTAR_FDW_NAME ": begin foreign %s on relation ID %d",
		 cqlOpName, RelationGetRelid(resultRelInfo->ri_RelationDesc));

	oldcontext = MemoryContextSwitchTo(fmstate->temp_cxt);

	if (!fmstate->sql_sent)
		fmstate->statement = cass_statement_new(fmstate->query,
		                                        fmstate->p_nums);

	if (slot != NULL && fmstate->target_attrs != NIL)
	{
		ListCell   *lc;

		foreach(lc, fmstate->target_attrs)
		{
			int   attnum = lfirst_int(lc);

			value = slot_getattr(slot, attnum, &isnull);
			if (isnull)
				cassStatementBindNull(fmstate->statement, pindex,
				                      fmstate->p_type_oids[pindex], cqlOpName,
				                      estate, resultRelInfo);
			else
				bind_cass_statement_param(fmstate->p_type_oids[pindex],
				                          value, fmstate->statement, pindex);

			pindex++;
		}
	}

	/* Retrieve the key from the resjunk attribute */
	value = ExecGetJunkAttribute(planSlot, fmstate->keyAttno, &isnull);

	if (isnull)					/* PRIMARY KEY value should not be NULL */
	{
		const char *primary_key;
		Relation    relation = resultRelInfo->ri_RelationDesc;
		Oid         rid      = RelationGetRelid(relation);

		cassGetPKOption(rid, &primary_key);

		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("The specified PRIMARY KEY '%s' contains a NULL value"
						"for the FOREIGN TABLE '%s.%s'.", primary_key,
						pstrdup(get_namespace_name(RelationGetNamespace(
																 relation))),
						pstrdup(RelationGetRelationName(relation))),
			   errdetail("For UPDATE or DELETE, a valid PRIMARY KEY must be "
						 "defined for the FOREIGN TABLE."),
				 errhint("Set the FOREIGN TABLE OPTION '%s' to a "
						 "valid PRIMARY KEY column.",
						 OPT_PK)));
	}

	bind_cass_statement_param(fmstate->p_type_oids[pindex], value,
	                          fmstate->statement, pindex);
	pindex++;
	Assert(pindex == fmstate->p_nums);

	cass_statement_set_consistency(fmstate->statement, fmstate->write_consistency);
	future = cass_session_execute(fmstate->cass_conn, fmstate->statement);
	fmstate->sql_sent = true;
	cass_future_wait(future);

	rc = cass_future_error_code(future);

	if (rc != CASS_OK)
	{
		const char* message;
		size_t message_length;
		cass_future_error_message(future, &message, &message_length);
		cass_future_free(future);
		releaseCassResources(estate, resultRelInfo);

		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("Failed to execute the %s into Cassandra: %.*s",
						cqlOpName, (int) message_length, message)));
	}

	cass_future_free(future);
	MemoryContextSwitchTo(oldcontext);

	MemoryContextReset(fmstate->temp_cxt);

	return slot;
}

/*
 * cassImportForeignSchema
 *		Generates CREATE FOREIGN TABLE statements for each of the tables
 *		in the source schema and returns the list of these statements
 *		to the caller.
 */
static List *
cassImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid)
{
	ForeignServer *server;
	const char *tabname,
			   *colname;
	size_t		tab_length,
				col_length;
	UserMapping *user;
	List	   *result = NIL;
	CassSession *session;
	const CassSchemaMeta *schema_meta;
	const CassKeyspaceMeta *keyspace_meta;
	StringInfoData buf;
	CassIterator *column_family_iter;

	/* get the foreign server, the user mapping and the FDW */
	server = GetForeignServer(serverOid);
	user = GetUserMapping(GetUserId(), server->serverid);

	/*
	 * Get connection to the foreign server.  Connection manager will
	 * establish new connection if necessary.
	 */
	session = pgcass_GetConnection(server, user, false);

	initStringInfo(&buf);

	schema_meta = cass_session_get_schema_meta(session);

	keyspace_meta = cass_schema_meta_keyspace_by_name(schema_meta, stmt->remote_schema);
	if (!keyspace_meta)
	{
		ereport(WARNING,
				(errcode(ERRCODE_WARNING),
		  errmsg("remote schema \"%s\" does not exist", stmt->remote_schema),
				 errhint("Enclose the schema name in double quotes to prevent case folding.")));
		return NIL;
	}
	column_family_iter = cass_iterator_tables_from_keyspace_meta(keyspace_meta);

	/* Loop through the tables in the schema */
	while (cass_iterator_next(column_family_iter))
	{
		const CassTableMeta *table_meta = cass_iterator_get_table_meta(column_family_iter);
		size_t		idx = 0;

		resetStringInfo(&buf);
		cass_table_meta_name(table_meta, &tabname, &tab_length);

		appendStringInfo(&buf, "CREATE FOREIGN TABLE \"%.*s\" (", (int) tab_length, tabname);

		/* Loop through the columns in the table */
		for (; idx < cass_table_meta_column_count(table_meta); idx++)
		{
			const CassColumnMeta *column_meta = cass_table_meta_column(table_meta, idx);
			const CassDataType *datatype_info = cass_column_meta_data_type(column_meta);

			if (idx)
				appendStringInfo(&buf, ", ");

			cass_column_meta_name(column_meta, &colname, &col_length);
			appendStringInfo(&buf, "\"%.*s\" ", (int) col_length, colname);
			pgcass_transformDataType(&buf, cass_data_type_type(datatype_info));
		}
		appendStringInfo(&buf, ") SERVER \"%s\" OPTIONS (schema_name '%s', table_name '%.*s')",
		 server->servername, stmt->remote_schema, (int) tab_length, tabname);
		result = lappend(result, pstrdup(buf.data));

		elog(DEBUG1, CSTAR_FDW_NAME "DDL: %.*s\n", (int) buf.len, buf.data);
	}
	cass_iterator_free(column_family_iter);
	cass_schema_meta_free(schema_meta);
	return result;
}
