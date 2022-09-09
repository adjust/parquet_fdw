FROM postgres

RUN apt update
RUN apt install -y build-essential postgresql-server-dev-all

WORKDIR /parquet_fdw
ADD install_arrow.sh /parquet_fdw
RUN bash install_arrow.sh

ADD . /parquet_fdw
RUN make install
