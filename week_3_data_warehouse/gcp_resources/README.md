gcloud CLI:
```shell
# Refresh service-account's auth-token for this session
gcloud auth application-default login
```

Terraform commands:
```shell
terraform plan -var="project=PROJECT_ID_HERE"
terraform apply -var="project=PROJECT_ID_HERE"
terraform destroy
```
