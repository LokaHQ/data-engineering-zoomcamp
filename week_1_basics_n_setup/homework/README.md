
# network

```
docker network create pg-network
```

#create database
```
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/data/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5431:5432 \
  --network=pg-network \
  --name pg-database1 \
  postgres:13
```

#open pgadmin
```
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --network=pg-network \
  --name pgadmin-2 \
  dpage/pgadmin4
```

#docker compose

```
docker-compose up
```

#ingest data

```
docker build -t taxi_ingest .
```

```
URL="yellow_tripdata_2021-01.csv"

docker run -it \
  --network=pg-network \
  taxi_ingest \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=${URL}
```

```
URL="https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"

docker run -it \
  --network=pg-network \
  taxi_ingest \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name=zones \
    --url=${URL}
```

#Questions:

3.

```
select count(*) from yellow_taxi_trips where tpep_pickup_datetime::date ='2021-01-15'
```

4.
```
select
date_trunc('day', tpep_pickup_datetime) as pickup_day, max(tip_amount) as max_tip
from yellow_taxi_trips
group by 1
order by max_tip desc
limit 1
```

5.
```
select coalesce(dozones."Zone",'Unknown') as zone,
count(*) as cant_trips
from yellow_taxi_trips as taxi
inner join zones as puzones
on taxi."PULocationID" = puzones."LocationID"
left join zones as dozones
on taxi."DOLocationID" = dozones."LocationID"
where puzones."Zone" ilike '%central park%'
and tpep_pickup_datetime::date = '2021-01-14'
group by 1
order by cant_trips desc
limit 1;
```

6.
```
select concat(coalesce(puzones."Zone",'Unknown'),'/',coalesce(dozones."Zone",'Unknown')) as pu_do,
avg(total_amount) as avg_price_ride
from yellow_taxi_trips as taxi
left join zones as puzones
on taxi."PULocationID" = puzones."LocationID"
left join zones as dozones
on taxi."DOLocationID" = dozones."LocationID"
group by pu_do
order by avg_price_ride desc
limit 1;
```