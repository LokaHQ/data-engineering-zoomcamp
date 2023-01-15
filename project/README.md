https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/HG7NV7

1. Airflow listener for a meta file, once a file update is detected, 
   checks for un-ingested files and ingests them, 
   then marks the file as ingested
2. The files are initially ingested in the raw data-lake (gcs)
3. This picks the files and ingests the data in Big Query
4. Partitioned and clustered tables with dbt
5. Create views with dbt
6. Google Data Explorer for visuals
7. Dockerized Airflow + Terraform for gcp resources + dbt
