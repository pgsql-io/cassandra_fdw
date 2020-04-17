/*-------------------------------------------------------------------------
 *
 * cstar_fdw.h
 *                cassandra_fdw includes.
 *
 * Copyright (c) 2014-2020, BigSQL
 * Portions Copyright (c) 2012-2018, PostgreSQL Global Development Group & Others
 *
 * IDENTIFICATION
 *                contrib/cassandra_fdw/cstar_fdw.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef CASSANDRA_FDW_H_
#define CASSANDRA_FDW_H_

#include <cassandra.h>

#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#if PG_VERSION_NUM < 120000
	#include "nodes/relation.h"
#else
	#include "nodes/pathnodes.h"
#endif
#include "utils/rel.h"

/* User-visible name for logging and reporting purposes */
#define CSTAR_FDW_NAME				"cassandra_fdw"
#define MSECS_PER_SEC				1000
#define LITERAL_UTC					"UTC"
#define DEFAULT_CONSISTENCY_LEVEL	CASS_CONSISTENCY_LOCAL_ONE

/* in cstar_connect.c */
extern CassSession *pgcass_GetConnection(ForeignServer *server, UserMapping *user,
			  bool will_prep_stmt);
extern void pgcass_ReleaseConnection(CassSession *session);

extern void pgcass_report_error(int elevel, CassFuture* result_future,
				bool clear, const char *sql);

/* in deparse.c */
extern void
cassDeparseSelectSql(StringInfo buf,
					 PlannerInfo *root,
					 RelOptInfo *baserel,
					 Bitmapset *attrs_used,
					 List **retrieved_attrs);
extern void
cassDeparseInsertSql(StringInfo buf, PlannerInfo *root,
					 Index rtindex, Relation rel,
					 List *targetAttrs, bool doNothing);

extern void
cassDeparseUpdateSql(StringInfo buf, PlannerInfo *root,
					 Index rtindex, Relation rel,
					 List *targetAttrs, const char *primaryKey);

extern void
cassDeparseDeleteSql(StringInfo buf, PlannerInfo *root,
					 Index rtindex, Relation rel,
					 List **retrieved_attrs,
					 const char *primaryKey);
#endif /* CASSANDRA_FDW_H_ */
