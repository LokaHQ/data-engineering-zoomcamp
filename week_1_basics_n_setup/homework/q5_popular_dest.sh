docker compose exec pgdatabase psql -d ny_taxi -c "select LocationID from zone where \"zone\"::text ilike '%central park%';"

 index | LocationID |  Borough  |     Zone     | service_zone
-------+------------+-----------+--------------+--------------
    42 |         43 | Manhattan | Central Park | Yellow Zone
(1 row)

docker compose exec pgdatabase psql -d ny_taxi -c "select \"DOLocationID\", \"Zone\", COUNT(\"trip\".\"index\") from trip join zone on \"DOLocationID\" = \"LocationID\" where \"PULocationID\" in (select \"LocationID\" from zone where \"zone\"::text ilike '%central park%') and CAST(tpep_pickup_datetime AS DATE) = '2021-01-14' GROUP BY \"DOLocationID\", \"Zone\" ORDER BY COUNT(\"trip\".\"index\") DESC LIMIT 1;"


 DOLocationID |         Zone          | count
--------------+-----------------------+-------
          237 | Upper East Side South |    97
(1 row)
