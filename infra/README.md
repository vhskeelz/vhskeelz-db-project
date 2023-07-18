# Infrastructure

## Prerequisites

* Terraform
* Login to Terraform Cloud with permissions to the relevant workspace
* Make sure the Terraform Cloud workspace is configured for Local execution mode
* Google Cloud SDK installed and authenticated with relevant permissions

## Deploy

```
terraform -chdir=infra init
terraform -chdir=infra apply
```
