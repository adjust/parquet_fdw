ARG PG_MAJOR
FROM postgres:${PG_MAJOR}-bullseye

ENV LANG=C.UTF-8 PGDATA=/pgdata

COPY . /src
WORKDIR /src

# Prepare the environment
RUN apt-get update && \
	apt-get install -y gcc make g++ postgresql-server-dev-${PG_MAJOR} && \
	mkdir ${PGDATA} && \
	chown postgres:postgres ${PGDATA} && \
	chown -R postgres:postgres /src && \
	chmod a+rwx -R /usr/share/postgresql/$PG_MAJOR/extension && \
	chmod a+rwx -R /usr/lib/postgresql/$PG_MAJOR/lib && \
	bash /src/install_arrow.sh

USER postgres

ENTRYPOINT PGDATA=${PGDATA} bash /src/test/run_tests.sh
