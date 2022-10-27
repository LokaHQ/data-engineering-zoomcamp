docker compose exec pgdatabase psql -d ny_taxi -c "select CAST(tpep_pickup_datetime AS DATE) as day, max(tip_amount) from trip group by CAST(tpep_pickup_datetime AS DATE);"

    day     |   max
------------+---------
 2021-01-21 |     166
 2020-12-31 |    4.08
 2021-01-10 |      91
 2021-01-05 |     151
 2021-01-07 |      95
 2021-01-24 |     122
 2009-01-01 |       0
 2021-01-09 |     230
 2021-01-17 |      65
 2021-01-11 |     145
 2021-01-29 |      75
 2021-01-16 |     100
 2021-01-15 |      99
 2021-01-14 |      95
 2021-02-22 |    1.76
 2021-01-27 |     100
 2021-01-30 |  199.12
 2021-02-01 |    1.54
 2021-01-22 |   92.55
 2021-01-25 |  100.16
 2021-01-23 |     100
 2021-01-13 |     100
 2021-01-01 |     158
 2021-01-08 |     100
 2021-01-12 |  192.61
 2021-01-31 |   108.5
 2021-01-06 |     100
 2021-01-04 |  696.48
 2021-01-02 |  109.15
 2021-01-28 |   77.14
 2021-01-26 |     250
 2008-12-31 |       0
 2021-01-20 | 1140.44
 2021-01-03 |   369.4
 2021-01-18 |      90
 2021-01-19 |   200.8
(36 rows)

docker compose exec pgdatabase psql -d ny_taxi -c "select CAST(tpep_pickup_datetime AS DATE) as day, max(tip_amount) from trip where CAST(tpep_pickup_datetime AS DATE) >= '2021-01-01' and CAST(tpep_pickup_datetime AS DATE) < '2021-02-01' group by CAST(tpep_pickup_datetime AS DATE) order by max(tip_amount) DESC LIMIT 1;"

    day     |   max
------------+---------
 2021-01-20 | 1140.44
(1 row)