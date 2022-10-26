## Week 1 Homework

In this homework we'll prepare the environment 
and practice with terraform and SQL


## Question 1. Google Cloud SDK

Install Google Cloud SDK. What's the version you have? 

To get the version, run `gcloud --version`

Google Cloud SDK 407.0.0

## Google Cloud account 

Create an account in Google Cloud and create a project.

Check 

## Question 2. Terraform 

Now install terraform and go to the terraform directory (`week_1_basics_n_setup/1_terraform_gcp/terraform`)

After that, run

* `terraform init`
* `terraform plan`
* `terraform apply` 

Apply the plan and copy the output (after running `apply`) to the form.

It should be the entire output - from the moment you typed `terraform init` to the very end.

Check
## Prepare Postgres 

Run Postgres and load data as shown in the videos

We'll use the yellow taxi trips from January 2021:

```bash
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv
```

You will also need the dataset with zones:

```bash 
wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
```

Download this data and put it to Postgres

## Question 3. Count records 

How many taxi trips were there on January 15?

Consider only trips that started on January 15.

```
select count(*) from yellow_taxi_data where yellow_taxi_data.tpep_pickup_datetime >= '2021-01-15' and yellow_taxi_data.tpep_pickup_datetime < '2021-01-16'
```
```
trips = 53024
```

## Question 4. Largest tip for each day

Find the largest tip for each day. 
On which day it was the largest tip in January?

Use the pick up time for your calculations.

(note: it's not a typo, it's "tip", not "trip")

`````
select tpep_pickup_datetime as starting_time 
from yellow_taxi_data 
group by starting_time 
order by max(tip_amount) desc
limit 1;
````
````
day = "2021-01-20 11:22:05"
````

## Question 5. Most popular destination

What was the most popular destination for passengers picked up 
in central park on January 14?

Use the pick up time for your calculations.

Enter the zone name (not id). If the zone name is unknown (missing), write "Unknown" 

`````

select coalesce(dozones."Zone", 'Unknown') as zone,
count(*) as cant_trips
from yellow_taxi_data as taxi

inner join zones as puzones
on taxi."PULocationID" = puzones."LocationID"

left join zones as dozones
on taxi."DOLocationID" = puzones."LocationID"
where puzones."Zone" ilike '%central park'
and tpep_pickup_datetime::date = '2021-01-14'
group by 1
order by cant_trips desc
limit 1;

````
````
unknown | 1087 trips
````


## Question 6. Most expensive locations

What's the pickup-dropoff pair with the largest 
average price for a ride (calculated based on `total_amount`)?

Enter two zone names separated by a slash

"Port Richmond/Williamsbridge/Olinville"

For example:

"Jamaica Bay / Clinton East"


If any of the zone names are unknown (missing), write "Unknown". For example, "Unknown / Clinton East". 

`````
select concat(coalesce(puzones."Zone", 'Unknown'), '/', coalesce(dozones."Zone", 'Unknown'))
as pickup_dropoff,
avg(total_amount) as avg_price_ride
from yellow_taxi_data as taxi

left join zones as puzones
on taxi."PULocationID" = puzones."LocationID"

left join zones as dozones
on taxi."DOLocationID" = dozones."LocationID"

group by pickup_dropoff
order by avg_price_ride desc
limit 1;
`````
`````
"Alphabet City/Unknown" 2292.4
````
## Submitting the solutions

* Form for submitting: https://forms.gle/yGQrkgRdVbiFs8Vd7
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 26 January (Wednesday), 22:00 CET


## Solution

Here is the solution to questions 3-6: [video](https://www.youtube.com/watch?v=HxHqH2ARfxM&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

