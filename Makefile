MODULE_big = parquet_fdw
OBJS = src/common.o src/reader.o src/exec_state.o src/parquet_impl.o src/parquet_fdw.o
PGFILEDESC = "parquet_fdw - foreign data wrapper for parquet"

SHLIB_LINK = -lm -lstdc++ -lparquet -larrow

EXTENSION = parquet_fdw
DATA = parquet_fdw--0.1.sql parquet_fdw--0.1--0.2.sql

# Generate names of test files

TEST_SQL_IN = $(sort $(wildcard test/sql/*.sql.in))
TEST_EXPECTED_IN = $(sort $(wildcard test/expected/*.out.in))

TEST_SQL = $(patsubst test/sql/%.sql.in,test/sql/%.sql,$(TEST_SQL_IN))
TEST_EXPECTED = $(patsubst test/expected/%.out.in,test/expected/%.out,$(TEST_EXPECTED_IN))

REGRESS = $(patsubst test/sql/%.sql.in,%,$(TEST_SQL_IN))

EXTRA_CLEAN = $(TEST_SQL) $(TEST_EXPECTED)
REGRESS_OPTS = --inputdir=test --outputdir=test

PG_CONFIG ?= pg_config

ROOT_DIR := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

# parquet_impl.cpp requires C++ 11 and libarrow 10+ requires C++ 17
override PG_CXXFLAGS += -std=c++17 -O3

PGXS := $(shell $(PG_CONFIG) --pgxs)

# pass CCFLAGS (when defined) to both C and C++ compilers.
ifdef CCFLAGS
	override PG_CXXFLAGS += $(CCFLAGS)
	override PG_CFLAGS += $(CCFLAGS)
endif

include $(PGXS)

# XXX: PostgreSQL below 11 does not automatically add -fPIC or equivalent to C++
# flags when building a shared library, have to do it here explicitely.
ifeq ($(shell test $(VERSION_NUM) -lt 110000; echo $$?), 0)
	override CXXFLAGS += $(CFLAGS_SL)
endif

# PostgreSQL uses link time optimization option which may break compilation
# (this happens on travis-ci). Redefine COMPILE.cxx.bc without this option.
#
# We need to use -Wno-register since C++17 raises an error if "register" keyword
# is used. PostgreSQL headers still uses the keyword, particularly:
# src/include/storage/s_lock.h.
COMPILE.cxx.bc = $(CLANG) -xc++ -Wno-ignored-attributes -Wno-register $(BITCODE_CXXFLAGS) $(CPPFLAGS) -emit-llvm -c

# XXX: a hurdle to use common compiler flags when building bytecode from C++
# files. should be not unnecessary, but src/Makefile.global omits passing those
# flags for an unnknown reason.
%.bc : %.cpp
	$(COMPILE.cxx.bc) $(CXXFLAGS) $(CPPFLAGS)  -o $@ $<

# PostgreSQL 15 dropped support of *.source files to generate tests using pg_regress.
# Because of that we generate paths in tests manually now.

installcheck: $(TEST_SQL) $(TEST_EXPECTED)

$(TEST_SQL): test/sql/%.sql: test/sql/%.sql.in
	sed 's,PG_ABS_SRCDIR,$(ROOT_DIR),g' $< > $@

$(TEST_EXPECTED): test/expected/%.out: test/expected/%.out.in
	sed 's,PG_ABS_SRCDIR,$(ROOT_DIR),g' $< > $@
