-- https://www.postgresqltutorial.com/postgresql-tutorial/import-csv-file-into-posgresql-table/

-- 1. Download taxi+_zone_lookup.csv
-- curl https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv > taxi+_zone_lookup.csv

-- 2. Create table "zones"
CREATE TABLE zones (
  LocationID SERIAL,
  Borough VARCHAR(50),
  Zone VARCHAR(50),
  service_zone VARCHAR(50)
);

-- 3. Copy "taxi+_zone_lookup.csv" into table "zones"
COPY zones(LocationID, Borough, Zone, service_zone) FROM '/Users/josepereira/git/data-engineering-zoomcamp/week_1_basics_n_setup/taxi+_zone_lookup.csv' DELIMITER ',' CSV HEADER;

-- 4. Download yellow_tripdata_2021-01.csv.gz
-- curl https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz > yellow_tripdata_2021-01.csv.gz

-- 5. Create table "trips"
CREATE TABLE trips (
    VendorID INTEGER,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance NUMERIC,
    RatecodeID INTEGER,
    store_and_fwd_flag BOOLEAN,
    PULocationID INTEGER,
    DOLocationID INTEGER,
    payment_type INTEGER,
    fare_amount NUMERIC,
    extra NUMERIC,
    mta_tax NUMERIC,
    tip_amount NUMERIC,
    tolls_amount NUMERIC,
    improvement_surcharge NUMERIC,
    total_amount NUMERIC,
    congestion_surcharge NUMERIC
);

-- 6. Copy "taxi+yellow_tripdata_2021-01.csv" into table "trips"
COPY trips(VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,congestion_surcharge) FROM '/Users/josepereira/git/data-engineering-zoomcamp/week_1_basics_n_setup/yellow_tripdata_2021-01.csv' DELIMITER ',' CSV HEADER;

-- Q3. How many taxi trips were there on January 15? Consider only trips that started on January 15
SELECT COUNT(1) FROM trips
    WHERE tpep_pickup_datetime::date = '2021-01-15';
-- 53024

-- Q4. Find the largest tip for each day. On which day it was the largest tip in January? Use pick up time
SELECT MAX(tip_amount), tpep_pickup_datetime::date FROM trips
    GROUP BY tpep_pickup_datetime::date
    ORDER BY MAX(tip_amount) DESC
    LIMIT 1;
-- 1140.44 | 2021-01-20

-- Q5. What was the most popular destination for passengers picked up in central park on January 14? Use the pick up time. Use zone name or unknown
SELECT COALESCE(zdo.Zone, 'Unknown'), count(1) FROM trips t
    INNER JOIN zones zpu ON t.PULocationID = zpu.LocationID
    LEFT JOIN zones zdo ON t.DOLocationID = zdo.LocationID
    WHERE tpep_pickup_datetime::date = '2021-01-14'
    AND zpu.Zone ILIKE '%central park%' --ILIKE is case insensitive
    GROUP BY zdo.Zone
    ORDER BY count(1) DESC
    LIMIT 1;
-- Upper East Side South | 97

-- Q6. What's the pickup-dropoff pair with the largest average price for a ride? Use total_amount. Enter two zone names separated by a slash
SELECT CONCAT(COALESCE(zpu.Zone, 'Unknown'), '/', COALESCE(zdo.Zone, 'Unknown')), avg(total_amount) FROM trips t
    LEFT JOIN zones zpu ON t.PULocationID = zpu.LocationID
    LEFT JOIN zones zdo ON t.DOLocationID = zdo.LocationID
    GROUP BY CONCAT(COALESCE(zpu.Zone, 'Unknown'), '/', COALESCE(zdo.Zone, 'Unknown'))
    ORDER BY avg(total_amount) DESC
    LIMIT 1;
-- Alphabet City/NA | 2292.4
