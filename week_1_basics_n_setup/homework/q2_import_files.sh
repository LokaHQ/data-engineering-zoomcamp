#!/bin/bash

# build image
docker build -f Dockerfile -t 2_docker_sql .

# copy & ingest files
docker run -it --network 2_docker_sql_default --mount type=bind,source=$(pwd)/trip.csv,target=/app/trip.csv 2_docker_sql --user root \
--password root \
--host pgdatabase \
--port 5432 \
--db ny_taxi \
--table_name trip \
--url trip.csv

docker run -it --network 2_docker_sql_default --mount type=bind,source=$(pwd)/zone.csv,target=/app/zone.csv 2_docker_sql --user root \
--password root \
--host pgdatabase \
--port 5432 \
--db ny_taxi \
--table_name zone \
--url zone.csv