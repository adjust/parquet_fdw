ARG PG_MAJOR
FROM postgres:${PG_MAJOR}-bookworm

ENV LANG=C.UTF-8 PGDATA=/pgdata

COPY . /src
WORKDIR /src

# Prepare the environment
RUN apt-get update && \
	apt-get install -y gcc make g++ postgresql-server-dev-${PG_MAJOR} python3 python3-pip python3-venv && \
	mkdir ${PGDATA} && \
	chown postgres:postgres ${PGDATA} && \
	chown -R postgres:postgres /src && \
	chmod a+rwx -R /usr/share/postgresql/$PG_MAJOR/extension && \
	chmod a+rwx -R /usr/lib/postgresql/$PG_MAJOR/lib && \
	bash /src/install_arrow.sh

# Install Python dependencies for test data generation
RUN python3 -m venv /src/.venv && \
	/src/.venv/bin/pip install -r /src/requirements.txt && \
	cd /src/test/data && /src/.venv/bin/python generate.py

USER postgres

ENTRYPOINT PGDATA=${PGDATA} bash /src/test/run_tests.sh
