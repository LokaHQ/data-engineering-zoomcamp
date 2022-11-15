-- https://www.postgresqltutorial.com/postgresql-tutorial/import-csv-file-into-posgresql-table/

-- 1. Download zone data
-- curl https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv > zones.csv
CREATE TABLE zones (
  LocationID SERIAL,
  Borough VARCHAR(50),
  Zone VARCHAR(50),
  service_zone VARCHAR(50)
);
COPY zones(LocationID, Borough, Zone, service_zone) FROM 'zones.csv' DELIMITER ',' CSV HEADER;

-- 2. Download fhv_tripdata_2019 data
-- curl https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-XX.csv.gz
-- gunzip fhv_tripdata_2019-XX.csv.gz
CREATE TABLE trips (
    dispatching_base_num VARCHAR(50),
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    PULocationID INTEGER,
    DOLocationID INTEGER,
    SR_Flag VARCHAR(50)
);
COPY trips(dispatching_base_num,pickup_datetime,dropoff_datetime,PULocationID,DOLocationID,SR_Flag) FROM 'fhv_tripdata_2019-XX.csv' DELIMITER ',' CSV HEADER;

-- Q1. What is count for fhv vehicles data for year 2019
SELECT COUNT(1) FROM trips; --42084899

-- Q2. How many distinct dispatching_base_num we have in fhv for 2019
SELECT COUNT(DISTINCT dispatching_base_num) FROM trips; --791

-- Q3. Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num
-- Partition by dropoff_datetime and cluster dispatching_base_num

-- Q4. What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279
-- table with partitioning by dispatching_base_num and optimized clustering to be created
-- SELECT COUNT(1) FROM new_table;
-- estimated data processed on top right corner (big query)
-- actual data processed obtained after query is executed

-- Q5. What will be the best partitioning or clustering strategy when filtering on dispatching_base_num and SR_Flag
-- clustering - multiple criteria

-- Q6. What improvements can be seen by partitioning and clustering for data size less than 1 GB
-- no improvement can be seen
