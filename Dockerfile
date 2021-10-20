FROM ubuntu:18.04

ARG PG_VERSION
ENV PG_VERSION=${PG_VERSION:-14}
ARG DEBIAN_FRONTEND=noninteractive
ENV LANG=C.UTF-8

# Install essentials
RUN apt-get update && \
    apt-get -y install build-essential gnupg2 ca-certificates lsb-release curl

# Add Apache Arrow repository
RUN curl https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb -o arrow-source.deb && \
    apt-get install -y -V ./arrow-source.deb
 
# Add PostgreSQL repository
RUN curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list

# Install Apache Arrow and PostgreSQL
RUN apt-get update && \
    apt-get install -y -V libarrow-dev libparquet-dev && \
    apt-get install -y -V postgresql-$PG_VERSION postgresql-server-dev-$PG_VERSION postgresql-client-$PG_VERSION

# Enable trust authentication to postgres
RUN echo 'local all all trust' > /etc/postgresql/$PG_VERSION/main/pg_hba.conf

# Make directories
RUN	mkdir /repo
ADD . /repo
RUN chmod -R a+w /repo
WORKDIR /repo

ENTRYPOINT ["/repo/test/run_tests.sh"]
