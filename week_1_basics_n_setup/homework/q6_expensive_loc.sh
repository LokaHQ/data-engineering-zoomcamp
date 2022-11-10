
docker compose exec pgdatabase psql -d ny_taxi -c "select COALESCE(pu_z.\"Zone\", 'Unknown') || ' / ' || COALESCE(do_z.\"Zone\", 'Unknown') as name, AVG(total_amount) as avg_trip from trip t left outer join zone do_z on \"DOLocationID\" = do_z.\"LocationID\" left outer join zone pu_z on \"PULocationID\" = pu_z.\"LocationID\" GROUP BY pu_z.\"Zone\", do_z.\"Zone\" ORDER BY AVG(total_amount) DESC limit 1;"

          name           | avg_trip
-------------------------+----------
 Alphabet City / Unknown |   2292.4
(1 row)