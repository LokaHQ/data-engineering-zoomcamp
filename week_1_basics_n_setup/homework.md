## Week 1 Homework

In this homework we'll prepare the environment 
and practice with terraform and SQL


## Question 1. Google Cloud SDK

Install Google Cloud SDK. What's the version you have? 

To get the version, run `gcloud --version`

### Answer
```
Google Cloud SDK 404.0.0
alpha 2022.09.23
beta 2022.09.23
bq 2.0.78
bundled-python3-unix 3.9.12
core 2022.09.23
gcloud-crc32c 1.0.0
gsutil 5.14
```

## Google Cloud account 

Create an account in Google Cloud and create a project.


## Question 2. Terraform 

Now install terraform and go to the terraform directory (`week_1_basics_n_setup/1_terraform_gcp/terraform`)

After that, run

* `terraform init`
* `terraform plan`
* `terraform apply` 

Apply the plan and copy the output (after running `apply`) to the form.

It should be the entire output - from the moment you typed `terraform init` to the very end.

### Answer
```
$ terraform init

Initializing the backend...

Successfully configured the backend "local"! Terraform will automatically
use this backend unless the backend configuration changes.

Initializing provider plugins...
- Finding latest version of hashicorp/google...
- Installing hashicorp/google v4.41.0...
- Installed hashicorp/google v4.41.0 (signed by HashiCorp)

Terraform has created a lock file .terraform.lock.hcl to record the provider
selections it made above. Include this file in your version control repository
so that Terraform can guarantee to make the same selections by default when
you run "terraform init" in the future.

Terraform has been successfully initialized!

You may now begin working with Terraform. Try running "terraform plan" to see
any changes that are required for your infrastructure. All Terraform commands
should now work.

If you ever set or change modules or backend configuration for Terraform,
rerun this command to reinitialize your working directory. If you forget, other
commands will detect it and remind you to do so if necessary.


$ terraform plan
var.project
  Your GCP Project ID

  Enter a value: leafy-garden-366619


Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following symbols:
  + create

Terraform will perform the following actions:

  # google_bigquery_dataset.dataset will be created
  + resource "google_bigquery_dataset" "dataset" {
      + creation_time              = (known after apply)
      + dataset_id                 = "trips_data_all"
      + delete_contents_on_destroy = false
      + etag                       = (known after apply)
      + id                         = (known after apply)
      + last_modified_time         = (known after apply)
      + location                   = "europe-west6"
      + project                    = "leafy-garden-366619"
      + self_link                  = (known after apply)

      + access {
          + domain         = (known after apply)
          + group_by_email = (known after apply)
          + role           = (known after apply)
          + special_group  = (known after apply)
          + user_by_email  = (known after apply)

          + dataset {
              + target_types = (known after apply)

              + dataset {
                  + dataset_id = (known after apply)
                  + project_id = (known after apply)
                }
            }

          + view {
              + dataset_id = (known after apply)
              + project_id = (known after apply)
              + table_id   = (known after apply)
            }
        }
    }

  # google_storage_bucket.data-lake-bucket will be created
  + resource "google_storage_bucket" "data-lake-bucket" {
      + force_destroy               = true
      + id                          = (known after apply)
      + location                    = "EUROPE-WEST6"
      + name                        = "dtc_data_lake_leafy-garden-366619"
      + project                     = (known after apply)
      + self_link                   = (known after apply)
      + storage_class               = "STANDARD"
      + uniform_bucket_level_access = true
      + url                         = (known after apply)

      + lifecycle_rule {
          + action {
              + type = "Delete"
            }

          + condition {
              + age                   = 30
              + matches_prefix        = []
              + matches_storage_class = []
              + matches_suffix        = []
              + with_state            = (known after apply)
            }
        }

      + versioning {
          + enabled = true
        }
    }

Plan: 2 to add, 0 to change, 0 to destroy.

────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────

Note: You didn't use the -out option to save this plan, so Terraform can't guarantee to take exactly these actions if you run "terraform
apply" now.
```


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

### Answer
`53024`

```
SELECT COUNT("index")
FROM public.yellow_taxi_trips
WHERE EXTRACT(MONTH FROM tpep_pickup_datetime) = '01' AND EXTRACT(DAY FROM tpep_pickup_datetime) = '15';
```


## Question 4. Largest tip for each day

Find the largest tip for each day. 
On which day it was the largest tip in January?

Use the pick up time for your calculations.

(note: it's not a typo, it's "tip", not "trip")

### Answer
`2021-01-20: 1140.44`

```
SELECT tpep_pickup_datetime, MAX(tip_amount)
FROM public.yellow_taxi_trips
WHERE EXTRACT(MONTH FROM tpep_pickup_datetime) = '01'
GROUP BY tpep_pickup_datetime
ORDER BY MAX(tip_amount) DESC
LIMIT 1;
```


## Question 5. Most popular destination

What was the most popular destination for passengers picked up 
in central park on January 14?

Use the pick up time for your calculations.

Enter the zone name (not id). If the zone name is unknown (missing), write "Unknown" 

### Answer

```
97	Upper East Side South
94	Upper East Side North
83	Lincoln Square East
68	Upper West Side North
60	Upper West Side South
59	Central Park
56	Midtown Center
40	Yorkville West
39	Lenox Hill West
36	Lincoln Square West
```

```
SELECT 
	COUNT(t.index) as total, 
	coalesce(zdo."Zone", 'Unknown') AS dest_zone
FROM public.yellow_taxi_trips t
LEFT JOIN zones zpu on zpu."LocationID" = t."PULocationID" 
left JOIN zones zdo on zdo."LocationID" = t."DOLocationID"
WHERE zpu."Zone" = 'Central Park'
	AND EXTRACT(MONTH FROM tpep_pickup_datetime) = '01' AND EXTRACT(DAY FROM tpep_pickup_datetime) = '14'
group by dest_zone
order by total desc
LIMIT 10;
```

## Question 6. Most expensive locations

What's the pickup-dropoff pair with the largest 
average price for a ride (calculated based on `total_amount`)?

Enter two zone names separated by a slash

For example:

"Jamaica Bay / Clinton East"

If any of the zone names are unknown (missing), write "Unknown". For example, "Unknown / Clinton East".

Alphabet City / Unknown	2292.4
Union Sq / Canarsie	262.852
Ocean Hill / Unknown	234.51
Long Island City/Hunters Point / Clinton East	207.61
Boerum Hill / Woodside	200.3
Baisley Park / Unknown	181.44249999999997
Bushwick South / Long Island City/Hunters Point	156.96
Willets Point / Unknown	154.42
Co-Op City / Dyker Heights	151.37
Rossville/Woodrow / Pelham Bay Park	151.0

```
SELECT 
	concat(
		coalesce(zpu."Zone", 'Unknown'), ' / ',
		coalesce(zdo."Zone", 'Unknown')) as pair,
	avg(total_amount) as average
FROM public.yellow_taxi_trips t
LEFT JOIN zones zdo on zdo."LocationID" = t."DOLocationID"
LEFT JOIN zones zpu on zpu."LocationID" = t."PULocationID" 
group by pair
order by average desc
LIMIT 10;
```




## Solution

Here is the solution to questions 3-6: [video](https://www.youtube.com/watch?v=HxHqH2ARfxM&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

