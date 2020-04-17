/*-------------------------------------------------------------------------
 *
 * deparse.c
 *		  Query deparser for cassandra_fdw based on the one from postgres_fdw.
 *
 * This file includes functions that examine query WHERE clauses to see
 * whether they're safe to send to the remote server for execution, as well as
 * functions to construct the query text to be sent.  We only need deparse
 * logic for node types that we consider safe to send.
 *
 * We schema-qualify all names in this module.
 *
 * Copyright (c) 2014-2020, BigSQL
 * Portions Copyright (c) 2012-2015, PostgreSQL Global Development Group & Others
 *
 * IDENTIFICATION
 *		  contrib/cassandra_fdw/deparse.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "cstar_fdw.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#if PG_VERSION_NUM < 120000
	#include "optimizer/var.h"
#else
	#include "optimizer/optimizer.h"
#endif
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/*
 * Functions to construct string representation of a node tree.
 */
static void cassDeparseTargetList(StringInfo buf,
					  PlannerInfo *root,
					  Index rtindex,
					  Relation rel,
					  Bitmapset *attrs_used,
					  List **retrieved_attrs);
static void cassDeparseColumnRef(StringInfo buf, int varno, int varattno,
					 PlannerInfo *root);
static void cassDeparseRelation(StringInfo buf, Relation rel);

/*
 * Append remote name of specified foreign table to buf.
 * Use value of table_name FDW option (if any) instead of relation's name.
 * Similarly, schema_name FDW option overrides schema name.
 */
static void
cassDeparseRelation(StringInfo buf, Relation rel)
{
	ForeignTable *table;
	const char *nspname = NULL;
	const char *relname = NULL;
	ListCell   *lc;

	/* obtain additional catalog information. */
	table = GetForeignTable(RelationGetRelid(rel));

	/*
	 * Use value of FDW options if any, instead of the name of object itself.
	 */
	foreach(lc, table->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "schema_name") == 0)
			nspname = defGetString(def);
		else if (strcmp(def->defname, "table_name") == 0)
			relname = defGetString(def);
	}

	if (nspname == NULL)
		nspname = get_namespace_name(RelationGetNamespace(rel));
	if (relname == NULL)
		relname = RelationGetRelationName(rel);

	appendStringInfo(buf, "%s.%s",
					 quote_identifier(nspname), quote_identifier(relname));
}

/*
 * Construct name to use for given column, and emit it into buf.
 * If it has a column_name FDW option, use that instead of attribute name.
 */
static void
cassDeparseColumnRef(StringInfo buf, int varno, int varattno, PlannerInfo *root)
{
	RangeTblEntry *rte;
	char	   *colname = NULL;
	List	   *options;
	ListCell   *lc;

	/* varno must not be any of OUTER_VAR, INNER_VAR and INDEX_VAR. */
	Assert(!IS_SPECIAL_VARNO(varno));

	/* Get RangeTblEntry from array in PlannerInfo. */
	rte = planner_rt_fetch(varno, root);

	/*
	 * If it's a column of a foreign table, and it has the column_name FDW
	 * option, use that value.
	 */
	options = GetForeignColumnOptions(rte->relid, varattno);
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "column_name") == 0)
		{
			colname = defGetString(def);
			break;
		}
	}

	/*
	 * If it's a column of a regular table or it doesn't have column_name FDW
	 * option, use attribute name.
	 */
	if (colname == NULL)
		colname = get_attname(rte->relid, varattno, false);

	appendStringInfoString(buf, quote_identifier(colname));
}

/*
 * Emit a target list that retrieves the columns specified in attrs_used.
 * This is used for SELECT.
 *
 * The tlist text is appended to buf, and we also create an integer List
 * of the columns being retrieved, which is returned to *retrieved_attrs.
 */
static void
cassDeparseTargetList(StringInfo buf,
                  PlannerInfo *root,
                  Index rtindex,
                  Relation rel,
                  Bitmapset *attrs_used,
                  List **retrieved_attrs)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	bool		have_wholerow;
	bool		first;
	int			i;

	*retrieved_attrs = NIL;

	/* If there's a whole-row reference, we'll need all the columns. */
	have_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
	                              attrs_used);

	first = true;
	for (i = 1; i <= tupdesc->natts; i++)
	{
#if PG_VERSION_NUM < 110000
		Form_pg_attribute attr = tupdesc->attrs[i - 1];
#else
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);
#endif

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		if (have_wholerow ||
		    bms_is_member(i - FirstLowInvalidHeapAttributeNumber,
		                  attrs_used))
		{
			if (!first)
				appendStringInfoString(buf, ", ");
			first = false;

			cassDeparseColumnRef(buf, rtindex, i, root);

			*retrieved_attrs = lappend_int(*retrieved_attrs, i);
		}
	}

	/* Don't generate bad syntax if no undropped columns */
	if (first)
		appendStringInfoString(buf, "NULL");
}

/*
 * Construct a simple SELECT statement that retrieves desired columns
 * of the specified foreign table, and append it to "buf".  The output
 * contains just "SELECT ... FROM tablename".
 *
 * We also create an integer List of the columns being retrieved, which is
 * returned to *retrieved_attrs.
 */
void
cassDeparseSelectSql(StringInfo buf,
                 PlannerInfo *root,
                 RelOptInfo *baserel,
                 Bitmapset *attrs_used,
                 List **retrieved_attrs)
{
	RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);
	Relation	rel;

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = heap_open(rte->relid, NoLock);

	/*
	 * Construct SELECT list
	 */
	appendStringInfoString(buf, "SELECT ");
	cassDeparseTargetList(buf, root, baserel->relid, rel, attrs_used,
	                  retrieved_attrs);

	/*
	 * Construct FROM clause
	 */
	appendStringInfoString(buf, " FROM ");
	cassDeparseRelation(buf, rel);

	elog(DEBUG1, CSTAR_FDW_NAME ": built the statement: %s", buf->data);

	heap_close(rel, NoLock);
}

/*
 * deparse remote INSERT statement
 *
 * The statement text is appended to buf.
 */
void
cassDeparseInsertSql(StringInfo buf, PlannerInfo *root,
				 Index rtindex, Relation rel,
				 List *targetAttrs, bool doNothing)
{
	AttrNumber	pindex;
	bool		first;
	ListCell   *lc;

	appendStringInfoString(buf, "INSERT INTO ");
	cassDeparseRelation(buf, rel);

	if (targetAttrs)
	{
		appendStringInfoChar(buf, '(');

		first = true;
		foreach(lc, targetAttrs)
		{
			int			attnum = lfirst_int(lc);

			if (!first)
				appendStringInfoString(buf, ", ");
			first = false;

			cassDeparseColumnRef(buf, rtindex, attnum, root);
		}

		appendStringInfoString(buf, ") VALUES (");

		pindex = 1;
		first = true;
		foreach(lc, targetAttrs)
		{
			if (!first)
				appendStringInfoString(buf, ", ");
			first = false;

			appendStringInfo(buf, "?");
			pindex++;
		}

		appendStringInfoChar(buf, ')');
	}
	else
		appendStringInfoString(buf, " DEFAULT VALUES");

	if (doNothing)
		appendStringInfoString(buf, " ON CONFLICT DO NOTHING");

	elog(DEBUG1, CSTAR_FDW_NAME ": built the statement: %s", buf->data);
}

/*
 * deparse remote UPDATE statement
 *
 * The statement text is appended to buf.
 */
void
cassDeparseUpdateSql(StringInfo buf, PlannerInfo *root,
				 Index rtindex, Relation rel,
				 List *targetAttrs, const char *primaryKey)
{
	bool		first;
	ListCell   *lc;

	appendStringInfoString(buf, "UPDATE ");
	cassDeparseRelation(buf, rel);
	appendStringInfoString(buf, " SET ");

	first = true;

	foreach(lc, targetAttrs)
	{
		int			attnum = lfirst_int(lc);

		if (!first)
			appendStringInfoString(buf, ", ");
		first = false;

		cassDeparseColumnRef(buf, rtindex, attnum, root);
		appendStringInfoString(buf, " = ?");
	}

	appendStringInfo(buf, " WHERE %s = ?", primaryKey);

	elog(DEBUG1, CSTAR_FDW_NAME ": built the statement: %s", buf->data);
}

/*
 * deparse remote DELETE statement
 *
 * The statement text is appended to buf.
 */
void
cassDeparseDeleteSql(StringInfo buf, PlannerInfo *root,
				 Index rtindex, Relation rel,
				 List **retrieved_attrs,
				 const char *primaryKey)
{
	appendStringInfoString(buf, "DELETE FROM ");
	cassDeparseRelation(buf, rel);
	appendStringInfo(buf, " WHERE %s = ?", primaryKey);

	elog(DEBUG1, CSTAR_FDW_NAME ": built the statement: %s", buf->data);
}
