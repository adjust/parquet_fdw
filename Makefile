MODULE_big = parquet_fdw
OBJS = parquet.o parquet_fdw.o 
PGFILEDESC = "parquet_fdw - foreign data wrapper for parquet"

SHLIB_LINK = -lm -lstdc++ -lparquet -larrow

EXTENSION = parquet_fdw
DATA = parquet_fdw--0.1.sql parquet_fdw--0.1--0.2.sql

REGRESS = parquet_fdw import

EXTRA_CLEAN = sql/parquet_fdw.sql expected/parquet_fdw.out

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

COMPILE.cxx.bc = $(CLANG) -xc++ -Wno-ignored-attributes $(BITCODE_CXXFLAGS) $(CPPFLAGS) -emit-llvm -c

parquet.bc:
	$(COMPILE.cxx.bc) -o $@ parquet_impl.cpp
	$(LLVM_BINPATH)/opt -module-summary -f $@ -o $@

parquet.o:
	$(CXX) -std=c++11 -O3 $(CPPFLAGS) $(CCFLAGS) parquet_impl.cpp $(PG_LIBS) -c -fPIC $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)
