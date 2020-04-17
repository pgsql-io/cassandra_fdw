
MODULE_big = cassandra_fdw
OBJS = cstar_fdw.o cstar_connect.o deparse.o

SHLIB_LINK = -lcassandra

EXTENSION = cassandra_fdw
DATA = cassandra_fdw--3.1.sql

REGRESS = cassandra_fdw

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
