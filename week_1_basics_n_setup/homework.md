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

Note: You didn't use the -out option to save this plan, so Terraform can't
guarantee to take exactly these actions if you run "terraform apply" now.
➜  ~/repositories/data-engineering-zoomcamp/week_1_basics_n_setup/1_terraform_gcp/terraform git:(de-2022-eliofreitas) terraform init

Initializing the backend...

Initializing provider plugins...
- Reusing previous version of hashicorp/google from the dependency lock file
- Using previously-installed hashicorp/google v4.41.0

Terraform has been successfully initialized!

You may now begin working with Terraform. Try running "terraform plan" to see
any changes that are required for your infrastructure. All Terraform commands
should now work.

If you ever set or change modules or backend configuration for Terraform,
rerun this command to reinitialize your working directory. If you forget, other
commands will detect it and remind you to do so if necessary.
➜  ~/repositories/data-engineering-zoomcamp/week_1_basics_n_setup/1_terraform_gcp/terraform git:(de-2022-eliofreitas) terraform plan -var="project=data-bootcamp-efreitas"

Terraform used the selected providers to generate the following execution plan.
Resource actions are indicated with the following symbols:
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
      + project                    = "data-bootcamp-efreitas"
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
      + name                        = "dtc_data_lake_data-bootcamp-efreitas"
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

───────────────────────────────────────────────────────────────────────────────

Note: You didn't use the -out option to save this plan, so Terraform can't
guarantee to take exactly these actions if you run "terraform apply" now.
➜  ~/repositories/data-engineering-zoomcamp/week_1_basics_n_setup/1_terraform_gcp/terraform git:(de-2022-eliofreitas) terraform apply
var.project
  Your GCP Project ID

  Enter a value: ^C
Interrupt received.
Please wait for Terraform to exit or data loss may occur.
Gracefully shutting down...


╷
│ Error: No value for required variable
│ 
│   on variables.tf line 5:
│    5: variable "project" {
│ 
│ The root module input variable "project" is not set, and has no default
│ value. Use a -var or -var-file command line argument to provide a value for
│ this variable.
╵
➜  ~/repositories/data-engineering-zoomcamp/week_1_basics_n_setup/1_terraform_gcp/terraform git:(de-2022-eliofreitas) terraform plan -var="project=data-bootcamp-efreitas"

Terraform used the selected providers to generate the following execution plan.
Resource actions are indicated with the following symbols:
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
      + project                    = "data-bootcamp-efreitas"
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
      + name                        = "dtc_data_lake_data-bootcamp-efreitas"
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

───────────────────────────────────────────────────────────────────────────────

Note: You didn't use the -out option to save this plan, so Terraform can't
guarantee to take exactly these actions if you run "terraform apply" now.
➜  ~/repositories/data-engineering-zoomcamp/week_1_basics_n_setup/1_terraform_gcp/terraform git:(de-2022-eliofreitas) terraform apply -var="project=data-bootcamp-efreitas"

Terraform used the selected providers to generate the following execution plan.
Resource actions are indicated with the following symbols:
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
      + project                    = "data-bootcamp-efreitas"
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
      + name                        = "dtc_data_lake_data-bootcamp-efreitas"
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
google_storage_bucket.data-lake-bucket: Creation complete after 2s [id=dtc_data_lake_data-bootcamp-efreitas]
╷
│ Error: Error creating Dataset: googleapi: Error 403: Access Denied: Project data-bootcamp-efreitas: User does not have bigquery.datasets.create permission in project data-bootcamp-efreitas., accessDenied
│ 
│   with google_bigquery_dataset.dataset,
│   on main.tf line 45, in resource "google_bigquery_dataset" "dataset":
│   45: resource "google_bigquery_dataset" "dataset" {
│ 
╵
➜  ~/repositories/data-engineering-zoomcamp/week_1_basics_n_setup/1_terraform_gcp/terraform git:(de-2022-eliofreitas) terraform apply -var="project=data-bootcamp-efreitas"
google_storage_bucket.data-lake-bucket: Refreshing state... [id=dtc_data_lake_data-bootcamp-efreitas]

Terraform used the selected providers to generate the following execution plan.
Resource actions are indicated with the following symbols:
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
      + project                    = "data-bootcamp-efreitas"
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

Plan: 1 to add, 0 to change, 0 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: yes

google_bigquery_dataset.dataset: Creating...
google_bigquery_dataset.dataset: Creation complete after 3s [id=projects/data-bootcamp-efreitas/datasets/trips_data_all]

Apply complete! Resources: 1 added, 0 changed, 0 destroyed.
➜  ~/repositories/data-engineering-zoomcamp/week_1_basics_n_setup/1_terraform_gcp/terraform git:(de-2022-eliofreitas) cd ..
➜  ~/repositories/data-engineering-zoomcamp/week_1_basics_n_setup/1_terraform_gcp git:(de-2022-eliofreitas) cd ..        
➜  ~/repositories/data-engineering-zoomcamp/week_1_basics_n_setup git:(de-2022-eliofreitas) cd 2_docker_sql 
➜  ~/repositories/data-engineering-zoomcamp/week_1_basics_n_setup/2_docker_sql git:(de-2022-eliofreitas) docker build -t test:pandas
"docker build" requires exactly 1 argument.
See 'docker build --help'.

Usage:  docker build [OPTIONS] PATH | URL | -

Build an image from a Dockerfile
➜  ~/repositories/data-engineering-zoomcamp/week_1_basics_n_setup/2_docker_sql git:(de-2022-eliofreitas) docker build -t test:pandas
"docker build" requires exactly 1 argument.
See 'docker build --help'.

Usage:  docker build [OPTIONS] PATH | URL | -

Build an image from a Dockerfile
➜  ~/repositories/data-engineering-zoomcamp/week_1_basics_n_setup/2_docker_sql git:(de-2022-eliofreitas) docker build -t test:pandas .
[+] Building 67.5s (10/10) FINISHED                                             
 => [internal] load build definition from Dockerfile                       0.0s
 => => transferring dockerfile: 223B                                       0.0s
 => [internal] load .dockerignore                                          0.0s
 => => transferring context: 2B                                            0.0s
 => [internal] load metadata for docker.io/library/python:3.9.1            2.7s
 => [internal] load build context                                          0.0s
 => => transferring context: 2.44kB                                        0.0s
 => [1/5] FROM docker.io/library/python:3.9.1@sha256:ca8bd3c91af8b12c2d0  49.7s
 => => resolve docker.io/library/python:3.9.1@sha256:ca8bd3c91af8b12c2d04  0.0s
 => => sha256:7fbd49098d8eebfdb20ed5a074e0af80951e13ec88f 8.33kB / 8.33kB  0.0s
 => => sha256:8b846e1b73901174c506ae5e6219ac2f356ef71eaa5 9.98MB / 9.98MB  6.4s
 => => sha256:ca8bd3c91af8b12c2d042ade99f7c8f578a9f80a0db 2.36kB / 2.36kB  0.0s
 => => sha256:3cb0ab4a815457fb78fb84a41c66981420aaee5d067 2.22kB / 2.22kB  0.0s
 => => sha256:c78c297fb0d010ee085f95ae439636ecb167b050 49.18MB / 49.18MB  13.5s
 => => sha256:06af62193c25241eb123af8cf115c7a6298e834976f 7.69MB / 7.69MB  6.4s
 => => sha256:fb44d26a138a8d064a4ab8f9b472c64e7136c248 52.17MB / 52.17MB  23.0s
 => => sha256:195488cfc78f0e257698fa052494b5340338b5 183.89MB / 183.89MB  44.8s
 => => extracting sha256:c78c297fb0d010ee085f95ae439636ecb167b050c1acb427  1.2s
 => => sha256:e91064730500746be97b6f35ae9c33bf1d9b7b0b4e 6.26MB / 6.26MB  18.3s
 => => extracting sha256:06af62193c25241eb123af8cf115c7a6298e834976fe148a  0.2s
 => => extracting sha256:8b846e1b73901174c506ae5e6219ac2f356ef71eaa5896df  0.2s
 => => sha256:e88c09aad7fe916f528741afa2a4072f4b851472 18.59MB / 18.59MB  25.4s
 => => sha256:7c492206888c962a8cd02d08cdbdc962f2a10b7e2373d1 232B / 232B  23.5s
 => => extracting sha256:fb44d26a138a8d064a4ab8f9b472c64e7136c2482ec5af19  1.5s
 => => sha256:32ebfb02ddcff9e4c8d2b993f909b47b7141ba9fb8 2.16MB / 2.16MB  25.3s
 => => extracting sha256:195488cfc78f0e257698fa052494b5340338b5acfdec4df1  3.8s
 => => extracting sha256:e91064730500746be97b6f35ae9c33bf1d9b7b0b4e9fd5f7  0.2s
 => => extracting sha256:e88c09aad7fe916f528741afa2a4072f4b8514725f54c9a9  0.4s
 => => extracting sha256:7c492206888c962a8cd02d08cdbdc962f2a10b7e2373d1ac  0.0s
 => => extracting sha256:32ebfb02ddcff9e4c8d2b993f909b47b7141ba9fb8aa2719  0.1s
 => [2/5] RUN apt-get install wget                                         0.5s
 => [3/5] RUN pip install pandas sqlalchemy psycopg2                      13.9s
 => [4/5] WORKDIR /app                                                     0.0s
 => [5/5] COPY ingest_data.py ingest_data.py                               0.0s
 => exporting to image                                                     0.5s
 => => exporting layers                                                    0.5s
 => => writing image sha256:9d41d76176886068771717e87b5229801486e8c4aee35  0.0s
 => => naming to docker.io/library/test:pandas                             0.0s

Use 'docker scan' to run Snyk tests against images to find vulnerabilities and learn how to fix them
➜  ~/repositories/data-engineering-zoomcamp/week_1_basics_n_setup/2_docker_sql git:(de-2022-eliofreitas) docker run -it test:pandas
usage: ingest_data.py [-h] --user USER --password PASSWORD --host HOST --port
                      PORT --db DB --table_name TABLE_NAME --url URL
ingest_data.py: error: the following arguments are required: --user, --password, --host, --port, --db, --table_name, --url
➜  ~/repositories/data-engineering-zoomcamp/week_1_basics_n_setup/2_docker_sql git:(de-2022-eliofreitas) docker run -it test:pandas 2022-10-26
usage: ingest_data.py [-h] --user USER --password PASSWORD --host HOST --port
                      PORT --db DB --table_name TABLE_NAME --url URL
ingest_data.py: error: the following arguments are required: --user, --password, --host, --port, --db, --table_name, --url
➜  ~/repositories/data-engineering-zoomcamp/week_1_basics_n_setup/2_docker_sql git:(de-2022-eliofreitas) docker run -it test:pandas 2022-10-26 123 hello
usage: ingest_data.py [-h] --user USER --password PASSWORD --host HOST --port
                      PORT --db DB --table_name TABLE_NAME --url URL
ingest_data.py: error: the following arguments are required: --user, --password, --host, --port, --db, --table_name, --url
➜  ~/repositories/data-engineering-zoomcamp/week_1_basics_n_setup/2_docker_sql git:(de-2022-eliofreitas) docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
➜  ~/repositories/data-engineering-zoomcamp/week_1_basics_n_setup/2_docker_sql git:(de-2022-eliofreitas) 
➜  ~/repositories/data-engineering-zoomcamp/week_1_basics_n_setup/2_docker_sql git:(de-2022-eliofreitas) 
➜  ~/repositories/data-engineering-zoomcamp/week_1_basics_n_setup/2_docker_sql git:(de-2022-eliofreitas) wget https://s3.amazonaws.com/nyc-tlc/csv_backup/yellow_tripdata_2021-01.csv
--2022-10-26 21:26:17--  https://s3.amazonaws.com/nyc-tlc/csv_backup/yellow_tripdata_2021-01.csv
Resolving s3.amazonaws.com (s3.amazonaws.com)... 52.216.42.128
Connecting to s3.amazonaws.com (s3.amazonaws.com)|52.216.42.128|:443... connected.
HTTP request sent, awaiting response... 403 Forbidden
2022-10-26 21:26:18 ERROR 403: Forbidden.

➜  ~/repositories/data-engineering-zoomcamp/week_1_basics_n_setup/2_docker_sql git:(de-2022-eliofreitas) wget https://s3.amazonaws.com/nyc-tlc/csv_backup/yellow_tripdata_2021-01.csv
--2022-10-26 21:28:16--  https://s3.amazonaws.com/nyc-tlc/csv_backup/yellow_tripdata_2021-01.csv
Resolving s3.amazonaws.com (s3.amazonaws.com)... 52.216.147.150
Connecting to s3.amazonaws.com (s3.amazonaws.com)|52.216.147.150|:443... connected.
HTTP request sent, awaiting response... 403 Forbidden
2022-10-26 21:28:17 ERROR 403: Forbidden.

➜  ~/repositories/data-engineering-zoomcamp/week_1_basics_n_setup/2_docker_sql git:(de-2022-eliofreitas) docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
Unable to find image 'postgres:13' locally
13: Pulling from library/postgres
dd6189d6fc13: Pull complete 
e83a243abe4a: Pull complete 
e1e5d0e9701b: Pull complete 
b8172b349685: Pull complete 
83dad09f3014: Pull complete 
c4849d0ca437: Pull complete 
7642d443be37: Pull complete 
a40f15729374: Pull complete 
6f3cbbcd9a7a: Pull complete 
63102aef13a0: Pull complete 
cb0ff2a1d75c: Pull complete 
d62b1d3172bc: Pull complete 
29ed5a147831: Pull complete 
Digest: sha256:2b31dc28ab2a687bb191e66e69c2534c9c74107ddb3192ff22a04de386425905
Status: Downloaded newer image for postgres:13
The files belonging to this database system will be owned by user "postgres".
This user must also own the server process.

The database cluster will be initialized with locale "en_US.utf8".
The default database encoding has accordingly been set to "UTF8".
The default text search configuration will be set to "english".

Data page checksums are disabled.

initdb: error: directory "/var/lib/postgresql/data" exists but is not empty
If you want to create a new database system, either remove or empty
the directory "/var/lib/postgresql/data" or run initdb
with an argument other than "/var/lib/postgresql/data".
➜  ~/repositories/data-engineering-zoomcamp/week_1_basics_n_setup/2_docker_sql git:(de-2022-eliofreitas) docker run -it \

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

