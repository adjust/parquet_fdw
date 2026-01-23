#!/bin/bash

PG_VERSION=17
PG_CONF="/etc/postgresql/${PG_VERSION}/main"
PG_BIN="/usr/lib/postgresql/${PG_VERSION}/bin/postgres"
PG_DATA="/var/lib/postgresql/${PG_VERSION}/main"

# File must be writable by postgres user
VALGRIND_LOG="/tmp/valgrind.log"
# -s = detailed allocation
VALGRIND_OPTIONS="--show-error-list=yes"

if [ "$USER" != "postgres" ]; then
	echo "Run this script as 'postgres' user with 'sudo -u postgres $0'"
	exit 1
fi

if systemctl is-active --quiet postgresql@17-main; then
	echo "PostgreSQL service is running. Stop it before."
	exit 1
fi

# Create mandatory configuration file via symlinks
#ln -s ${PG_CONF}/posgresql.conf ${PG_DATA}/postgresql.conf
#ln -s ${PG_CONF}/pg_hba.conf ${PG_DATA}/pg_hba.conf
#ln -s ${PG_CONF}/pg_ident.conf ${PG_DATA}/pg_ident.conf


# Run valgrind
valgrind --leak-check=full --track-origins=yes --log-file=${VALGRIND_LOG} ${VALGRIND_OPTIONS} ${PG_BIN} -D ${PG_DATA} -c config_file=${PG_CONF}/postgresql.conf

# Remove symlinks
#if [ -L "${PG_DATA}/postgresql.conf" ]; then
#	rm "${PG_DATA}/postgresql.conf"
#fi

#if [ -L "${PG_DATA}/pg_ident.conf" ]; then
#	rm "${PG_DATA}/pg_ident.conf"
#fi

#if [ -L "${PG_DATA}/pg_ident.conf" ]; then
#	rm "${PG_DATA}/pg_ident.conf"
#fi

