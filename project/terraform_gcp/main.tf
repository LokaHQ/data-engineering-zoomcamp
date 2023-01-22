terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "3.5.0"
    }
  }
}

provider "google" {
  project = var.project
  region = var.region
  // credentials = file(var.credentials)  # Use this if you do not want to set env-var GOOGLE_APPLICATION_CREDENTIALS
}

# Datalake Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "data-lake-bucket" {
  name          = "${local.data_lake_bucket}_${var.project}" # Concatenating DL bucket & Project name for unique naming
  location      = var.region

  # Optional, but recommended settings:
  storage_class = var.storage_class

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }

  force_destroy = true
}

# Datalake metadata files seed
resource "google_storage_bucket_object" "source_file" {
  name   = "metadata/source_files.csv"
  source = "datalake_metadata/source_files_sample.csv"
  bucket = "${local.data_lake_bucket}_${var.project}"
}

resource "google_storage_bucket_object" "source_file_commit_log" {
  name   = "metadata/source_files_commit_log.csv"
  source = "datalake_metadata/source_files_commit_log_sample.csv"
  bucket = "${local.data_lake_bucket}_${var.project}"
}

# Data Warehouse (BigQuery Dataset)
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.BQ_DATASET
  project    = var.project
  location   = var.region
}