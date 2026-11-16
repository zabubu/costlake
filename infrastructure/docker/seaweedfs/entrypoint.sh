#!/bin/sh
# Renders the S3 identities from environment variables, then starts SeaweedFS.
# Three identities, least privilege:
#   admin     bucket administration (Terraform only)
#   exporter  writes billing exports to landing (stands in for the cloud provider)
#   pipeline  reads landing, reads/writes the lake
set -eu
: "${S3_ADMIN_ACCESS_KEY:?}" "${S3_ADMIN_SECRET_KEY:?}"
: "${S3_EXPORTER_ACCESS_KEY:?}" "${S3_EXPORTER_SECRET_KEY:?}"
: "${S3_PIPELINE_ACCESS_KEY:?}" "${S3_PIPELINE_SECRET_KEY:?}"
LANDING="${LANDING_BUCKET:-costlake-landing}"
LAKE="${LAKE_BUCKET:-costlake-lake}"
umask 077
mkdir -p /etc/seaweedfs-rendered
cat > /etc/seaweedfs-rendered/s3.json <<JSON
{
  "identities": [
    {"name": "admin",
     "credentials": [{"accessKey": "${S3_ADMIN_ACCESS_KEY}", "secretKey": "${S3_ADMIN_SECRET_KEY}"}],
     "actions": ["Admin", "Read", "Write", "List", "Tagging"]},
    {"name": "exporter",
     "credentials": [{"accessKey": "${S3_EXPORTER_ACCESS_KEY}", "secretKey": "${S3_EXPORTER_SECRET_KEY}"}],
     "actions": ["Write:${LANDING}", "List:${LANDING}", "Read:${LANDING}"]},
    {"name": "pipeline",
     "credentials": [{"accessKey": "${S3_PIPELINE_ACCESS_KEY}", "secretKey": "${S3_PIPELINE_SECRET_KEY}"}],
     "actions": ["Read:${LANDING}", "List:${LANDING}", "Read:${LAKE}", "Write:${LAKE}", "List:${LAKE}"]}
  ]
}
JSON
# Each bucket is a collection that reserves several volumes: the default limit (8)
# is exhausted by two buckets, hence -volume.max.
exec weed server -dir=/data -ip.bind=0.0.0.0 -s3 -s3.port=8333 \
  -s3.config=/etc/seaweedfs-rendered/s3.json -metricsPort=9327 \
  -master.volumeSizeLimitMB=256 -volume.max=100
