import os
import pandas as pd
from google.cloud import storage

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")


def ingest_gcs_callable(bucket: str, object_name: str, csv_file: str):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python

    :param bucket: GCS bucket name
    :param object_name: GCS target path & file-name
    :param csv_file: Source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(csv_file)


def commit_file(output_file: str, download_link: str):
    df_commit_log = pd.read_csv(
        AIRFLOW_HOME + "/dags/source_files_commit_log.csv", index_col=None
    )
    new_entry = pd.DataFrame(
        [
            [
                output_file.split("_")[1].split(".")[0],
                download_link,
            ]
        ],
        columns=["year", "download_link"],
    )

    df_commit_log = pd.concat([df_commit_log, new_entry])
    df_commit_log.to_csv(AIRFLOW_HOME + "/dags/source_files_commit_log.csv", index=False)
