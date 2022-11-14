#!/bin/bash

# Initialize a PostgreSQL instance
initdb -D $PGDATA

# Build and install the extension
make clean
make
make install

# Check build
status=$?
if [ $status -ne 0 ]; then exit $status; fi

# Start the instance
pg_ctl -D $PGDATA -l /tmp/postgres.log -w start

# Check startup
status=$?
if [ $status -ne 0 ]; then cat /tmp/postgres.log; fi

# Run tests
make installcheck || status=$?

# show diff if it exists
popd
if test -f test/regression.diffs; then cat test/regression.diffs; fi

# check startup
if [ $status -ne 0 ]; then cat /tmp/postgres.log; fi
exit $status
