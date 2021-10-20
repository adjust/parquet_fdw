#!/usr/bin/env bash

# run postgres
service postgresql start

# build extension
make install CFLAGS="${CFLAGS}"

# run regression tests
status=0
make installcheck PGUSER=postgres || status=$?

# show diff if needed
if [[ ${status} -ne 0 ]] && [[ -f test/regression.diffs ]]; then
    cat test/regression.diffs;
fi

exit ${status}

