/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2014-2018, BigSQL
 * Portions Copyright (c) 2012-2015, PostgreSQL Global Development Group & Others
 *
 *-------------------------------------------------------------------------
 */

CREATE FUNCTION cstar_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION cstar_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER cassandra_fdw
  HANDLER cstar_fdw_handler
  VALIDATOR cstar_fdw_validator;
