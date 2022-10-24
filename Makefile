MODULE_big = parquet_fdw
OBJS = src/common.o src/reader.o src/exec_state.o src/parquet_impl.o src/parquet_fdw.o
PGFILEDESC = "parquet_fdw - foreign data wrapper for parquet"

SHLIB_LINK = -lm -lstdc++ -lparquet -larrow

EXTENSION = parquet_fdw
DATA = parquet_fdw--0.1.sql parquet_fdw--0.1--0.2.sql

REGRESS = parquet_fdw import

EXTRA_CLEAN = sql/parquet_fdw.sql expected/parquet_fdw.out

PG_CONFIG ?= pg_config

# parquet_impl.cpp requires C++ 11.
override PG_CXXFLAGS += -std=c++11 -O3

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
COMPILE.cxx.bc = $(CLANG) -xc++ -Wno-ignored-attributes $(BITCODE_CXXFLAGS) $(CPPFLAGS) -emit-llvm -c

# gcc 10.1 enables moutline-atomics per default on aarch64
# including the deprecated flag breaks compilation
GCC_GTEQ_1001 := $(shell expr `gcc -dumpfullversion -dumpversion | sed -e 's/\.\([0-9][0-9]\)/\1/g' -e 's/\.\([0-9]\)/0\1/g' -e 's/^[0-9]\{3,4\}$$/&00/'` \>= 100100)

ifeq "$(GCC_GTEQ_1001)" "1"
override CXXFLAGS := $(filter-out -moutline-atomics, $(CXXFLAGS))
endif

# XXX: a hurdle to use common compiler flags when building bytecode from C++
# files. should be not unnecessary, but src/Makefile.global omits passing those
# flags for an unnknown reason.
%.bc : %.cpp
	$(COMPILE.cxx.bc) $(CXXFLAGS) $(CPPFLAGS)  -o $@ $<
