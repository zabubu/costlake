# Deploying the AWS infrastructure

Nothing in this repository creates cloud resources automatically: CI only runs
`terraform validate` and `terraform test` with mocked providers.

## What `envs/aws` creates

| Resource | Purpose | Cost |
|---|---|---|
| 2 S3 buckets (landing, lake) | Versioning (lake), SSE-S3 or SSE-KMS, public access block, TLS-only policy, lifecycle rules | S3 storage and requests: a few cents per month for one organisation's billing data |
| `aws_bcmdataexports_export` | Daily FOCUS export in Parquet to `landing/aws/focus/` | Free (AWS Data Exports); the S3 storage of the files is billed |
| IAM role `costlake-pipeline-<env>` | Read landing, read/write lake (and the KMS key, if any) | Free |

The export is created in `us-east-1` (Data Exports API region); the buckets in `var.region`.

## Procedure

```bash
cd infrastructure/terraform/envs/aws
cp backend.hcl.example backend.hcl            # remote state bucket (create it once, separately)
cp terraform.tfvars.example terraform.tfvars  # owner and globally unique bucket names
terraform init -backend-config=backend.hcl
terraform plan -out=costlake.tfplan          # review
terraform apply costlake.tfplan
```

Then run costlake with the role from the `pipeline_role_arn` output and:

```bash
COSTLAKE_ENV=prod
COSTLAKE_STORAGE_BACKEND=s3                 # no endpoint URL: real AWS, credentials from the role
COSTLAKE_LANDING_BUCKET=<landing_bucket output>
COSTLAKE_LAKE_BUCKET=<lake_bucket output>
COSTLAKE_DATABASE_URL=postgresql+psycopg://...
COSTLAKE_SERVING_DATABASE_URL=postgresql+psycopg://...
```

The first FOCUS export is delivered within 24 hours of its creation. Azure and Google Cloud exports are configured in their consoles to write to the `azure/focus/` and `gcp/focus/` prefixes (or to their own storage, synchronised to the landing bucket).

## Destroy

```bash
terraform destroy
```

Buckets are created with `force_destroy = false`: empty them first (`aws s3 rm --recursive`, including object versions of the lake bucket). This is deliberate: a `destroy` must not silently delete billing history.
