# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub: **Security → Report a vulnerability** on this repository. Do not open a public issue. You will get an answer within a week.

## Scope and design

* Billing data reveals an organisation's infrastructure and spending. Treat the buckets and databases as confidential.
* The local platform is a development environment: services listen on 127.0.0.1 only, and secrets are random values generated into a git-ignored `.env` by `scripts/init-env.sh`. Do not expose it on a network without TLS and authentication in front of Dagster (which has no authentication of its own).
* Least privilege is applied to S3 identities, PostgreSQL roles and the AWS IAM role (see `docs/architecture.md#security-model`).
* Dependencies are hash-pinned (`requirements/*.lock`). CI runs pip-audit, gitleaks on the git history, trivy on the IaC and Dockerfile and on the built image.
