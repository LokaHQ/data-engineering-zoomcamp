import os

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")


def ingest_gcs_callable(bucket: str, object_name: str, file_to_upload: str):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python

    :param bucket: GCS bucket name
    :param object_name: GCS target path & file-name
    :param file_to_upload: Source path & file-name
    :return:
    """
    from google.cloud import storage

    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(file_to_upload)


def commit_file(year: str, download_link: str):
    import pandas as pd

    df_commit_log = pd.read_csv(
        AIRFLOW_HOME + "/dags/source_files_commit_log.csv", index_col=None
    )
    new_entry = pd.DataFrame([[year, download_link]], columns=["year", "download_link"])

    df_commit_log = pd.concat([df_commit_log, new_entry])
    df_commit_log.to_csv(
        AIRFLOW_HOME + "/dags/source_files_commit_log.csv", index=False
    )


def format_to_parquet(csv_file: str):
    import pyarrow.csv as pv
    import pyarrow.parquet as pq

    table = pv.read_csv(csv_file)
    pq.write_table(table, csv_file.replace(".csv", ".parquet"))
