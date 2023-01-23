## Data engineering LokaCamp Project

From source to dashboards - USA flights data insights (1987 to 2008).

The goal of the project is ingestion and analysis of a large dataset for flight delays
data in the USA. We will try to answer some questions:
- What's the best time of the year to fly to minimise delays?
- Do older planes suffer more delays?
- How does increased air-traffic impact on-time schedules?
- Are there any flight delays patterns? etc.

### Datasource
We will ingest, process the data and serve data insights for the "Airline on-time performance" dataset:

Have you ever been stuck in an airport because your flight was
delayed or cancelled and wondered if you could have predicted it if you'd had more data? This
is your chance to find out. The data: The data consists of flight arrival and departure details
for all commercial flights within the USA, from October 1987 to April 2008. This is a large
dataset: there are nearly 120 million records in total, and takes up 1.6 gigabytes of space
compressed and 12 gigabytes when uncompressed. The data comes originally from RITA where it is
described in detail.

Source:

https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/HG7NV7
https://community.amstat.org/jointscsg-section/dataexpo/dataexpo2009

The flight entries in the dataset contain only airport codes, without any airport info.
To provide more details to analysts, we will extend each flight record with airport details from 
https://github.com/datasets/airport-codes (the csv 
https://github.com/datasets/airport-codes/blob/master/data/airport-codes.csv)


### Cloud Resources
We will be using GCP resources to host datalake and BigQuery as a Data Warehouse.
The resources are deployed with Terraform. For more details, follow the instructions
in the [terraform_gcp](./terraform_gcp) directory README.

Once we have the resources deployed, we can proceed with the ingestion pipeline.

### Ingestion Pipeline
Airflow is used as workflow orchestrator. We can run the airflow instance locally with
docker-compose. 

1. Update the variables in the airflow/.env.gcp file appropriately.
2. Create a GCP service account and download the JSON credentials file. The service account should have full
   GCS and full BigQuery access.
3. Store the file under ~/.config/gcloud/google_credentials.json

```shell
cd airflow
docker-compose up -d
```

Now open the airflow web UI on http://localhost:8080/

- username: airflow
- password: airflow

Use the tags filter to find the 2 Airflow DAGs we need - tag `zoom-camp`. Now we can see the two DAGs:

- IngestionTriggerDAG - ingestion controller DAG, reads the source_files.csv metadata file and triggers the
main IngestionDag per file
- IngestionDAG - downloads, processes, uploads the files to GCS datalake, ingests the data in BigQuery and creates
BigQuery tables. The final step commits the file - marks the file as ingested.
  
The IngestionTriggerDAG is scheduled for daily runs. Once a day it starts, checks for new file entries, and if new
file entries are detected, it starts a IngestionDAG run per file.

### Data Analytics
We are using dbt to create and deploy the analytical views and models for the data. For more details, 
follow the instructions in the [dbt_airlines](./dbt_airlines) directory README.

### Dashboards
Looker Studio is used to create the dashboards.
