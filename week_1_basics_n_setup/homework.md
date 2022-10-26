## Week 1 Homework

In this homework we'll prepare the environment
and practice with terraform and SQL

## Question 1. Google Cloud SDK

Install Google Cloud SDK. What's the version you have?

```
(base) wilson@MacBook-Pro google_keys % gcloud --version
Google Cloud SDK 407.0.0
bq 2.0.79
core 2022.10.21
gcloud-crc32c 1.0.0
gsutil 5.15

```

## Google Cloud account

Create an account in Google Cloud and create a project.

## Question 2. Terraform

Now install terraform and go to the terraform directory (`week_1_basics_n_setup/1_terraform_gcp/terraform`)

After that, run

- `terraform init`
- `terraform plan`
- `terraform apply`

Apply the plan and copy the output (after running `apply`) to the form.

It should be the entire output - from the moment you typed `terraform init` to the very end.

```
(base) wilson@MacBook-Pro terraform % terraform init

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
(base) wilson@MacBook-Pro terraform % terraform plan
var.project
  Your GCP Project ID

  Enter a value: dtc-de-course-366709


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
      + project                    = "dtc-de-course-366709"
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
      + name                        = "dtc_data_lake_dtc-de-course-366709"
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
(base) wilson@MacBook-Pro terraform % terraform apply
var.project
  Your GCP Project ID

  Enter a value: dtc-de-course-366709


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
      + project                    = "dtc-de-course-366709"
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
      + name                        = "dtc_data_lake_dtc-de-course-366709"
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

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: yes

google_bigquery_dataset.dataset: Creating...
google_storage_bucket.data-lake-bucket: Creating...
google_bigquery_dataset.dataset: Creation complete after 1s [id=projects/dtc-de-course-366709/datasets/trips_data_all]
google_storage_bucket.data-lake-bucket: Creation complete after 1s [id=dtc_data_lake_dtc-de-course-366709]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.
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

```
SELECT
	COUNT(*)
FROM
	yellow_taxi_trips
WHERE
	CAST(tpep_pickup_datetime AS date) = '2021-01-15'
```

```
53024
```

## Question 4. Largest tip for each day

Find the largest tip for each day.
On which day it was the largest tip in January?

Use the pick up time for your calculations.

(note: it's not a typo, it's "tip", not "trip")

```
SELECT
	MAX(tip_amount) as tip,
	tpep_pickup_datetime as pu_date
FROM
	yellow_taxi_trips
GROUP BY pu_date
ORDER by tip desc
limit 1
```

```
1140.44	"2021-01-20 11:22:05"
```

## Question 5. Most popular destination

What was the most popular destination for passengers picked up
in central park on January 14?

Use the pick up time for your calculations.

Enter the zone name (not id). If the zone name is unknown (missing), write "Unknown"

```
¯\_(ツ)_/¯ - My SQL level is not that good - Copied from solutions video

select
	coalesce(dozones."Zone",'Unknown') as zone,
	count(*) as cant_trips
from yellow_taxi_trips as taxi
inner join
	zones as puzones
	on taxi."PULocationID" = puzones."LocationID"
left join
	zones as dozones
	on taxi."DOLocationID" = dozones."LocationID"
where
	puzones."Zone" ilike '%central park%'
	and tpep_pickup_datetime::date = '2021-01-14'
group by 1
order by cant_trips desc
limit 1;
```

## Question 6. Most expensive locations

What's the pickup-dropoff pair with the largest
average price for a ride (calculated based on `total_amount`)?

Enter two zone names separated by a slash

For example:

"Jamaica Bay / Clinton East"

If any of the zone names are unknown (missing), write "Unknown". For example, "Unknown / Clinton East".

```
ಠ_ಠ - My SQL level is not that good - Copied from solutions video

select
	concat(coalesce(puzones."Zone",'Unknown'),'/',coalesce(dozones."Zone",'Unknown')) as pu_do,
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

## Submitting the solutions

- Form for submitting: https://forms.gle/yGQrkgRdVbiFs8Vd7
- You can submit your homework multiple times. In this case, only the last submission will be used.

Deadline: 26 January (Wednesday), 22:00 CET

## Solution

Here is the solution to questions 3-6: [video](https://www.youtube.com/watch?v=HxHqH2ARfxM&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
