from google.cloud import storage


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
