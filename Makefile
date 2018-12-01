MODULE_big = parquet_fdw
OBJS = parquet.o parquet_fdw.o 
PGFILEDESC = "parquet_fdw - foreign data wrapper for parquet"

#PG_CPPFLAGS = -I$(libpq_srcdir)
#PG_CPPFLAGS = -std=c++11 -Wall -Wextra -Wno-unused-parameter -fexcess-precision=fast
#PG_CPPFLAGS = -Wl,--no-undefined
SHLIB_LINK = -lm -lstdc++ -lparquet -larrow

EXTENSION = parquet_fdw
DATA = parquet_fdw--0.1.sql

#REGRESS = parquet_fdw

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

parquet.o:
	g++ $(CPPFLAGS) -O0 -ggdb3 parquet_impl.cpp $(PG_LIBS) -c -fPIC $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)
