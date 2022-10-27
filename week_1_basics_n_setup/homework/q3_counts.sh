
docker compose exec pgdatabase psql -d ny_taxi -c "select count(*) from trip where CAST(tpep_pickup_datetime AS DATE) = '2021-01-15';"
-> 53024