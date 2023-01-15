## Terraform for GCP Resources

To create a new deployment run the commands (replace PROJECT_ID with you GCP Project ID):

```shell
# gcloud CLI - Refresh service-account's auth-token for this session
gcloud auth application-default login
```

```shell
terraform init
terraform plan -var="project=dataengineeringzoomcamp-368119"
terraform apply -var="project=dataengineeringzoomcamp-368119"
```

Update an existing stack:

1. Inspect the updates to be applied
```shell
terraform plan -var="project=dataengineeringzoomcamp-368119"
```

2. Apply the updates
```shell
terraform apply -var="project=dataengineeringzoomcamp-368119"
```

Destroy all resources:
```shell
terraform destroy
```