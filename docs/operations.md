# Operations

## Day to day

| Task | Command |
|---|---|
| Start / stop (keeps data) | `make up` / `make down` |
| Follow pipeline logs | `make logs`, or Grafana → Explore → Loki: `{service="costlake-code"} \| json \| run_id="<id>"` |
| Trigger a run now | `make run` (otherwise the sensor checks the landing bucket every 60 s, and a full refresh runs daily at 06:00 UTC) |
| Lake state | `docker compose exec costlake-code costlake status` |
| Recent deliveries | `docker compose exec costlake-code costlake deliveries list` |
| Validate configuration | `docker compose exec costlake-code costlake validate-config` |

Interfaces (bound to 127.0.0.1): Dagster `:3000`, Grafana `:3001` (user `admin`, password `GRAFANA_ADMIN_PASSWORD` in `.env`), Prometheus `:9090`, S3 endpoint `:8333`.

## Alert runbook

| Alert | Meaning | Action |
|---|---|---|
| `CostlakePipelineStepFailed` | The last run of a step failed | Open the failed run in Dagster; the error and the step's structured logs are in Loki with the same `run_id`. Retries already happened (3 for I/O steps). Fix, then `make run`. |
| `CostlakeDeliveryQuarantined` | The quality gate rejected a delivery | `costlake deliveries list` shows the failed rules. Data problem at the source: ask for a re-export (a new delivery replaces the period). Adapter problem: fix the code, deploy, then `costlake deliveries requeue <id>`. |
| `CostlakeQualityErrors` | Blocking rules failed in the last 24 h | Same as above. |
| `CostlakeSourceStale` | No export from a source for 36 h | Check the provider export job and its write permission on the landing bucket. |
| `CostlakeCostAnomaly(Critical)` | Unexpected spend change | FinOps dashboard → anomaly table (actual, expected, z-score); drill into `serving.fct_daily_cost` by sub-account and owner. |
| `CostlakeForecastOverrun` | Run-rate > 120 % of last period | Check the showback panel for the owner driving the increase. |
| `CostlakeTagCoverageLow` | < 80 % of usage cost is allocatable | Tag the resources listed under owner `unallocated`. |
| `CostlakeExporterDown` / `CostlakeLedgerUnreachable` | Monitoring blind | `docker compose ps`, then the PostgreSQL logs. |

Alerts are evaluated by Prometheus (`:9090/alerts`) and visible in the *Platform health* dashboard. Routing to a pager or chat (Alertmanager) is on the roadmap.

## Recovery

* **Rebuild gold and serving**: gold is derived from silver on every run; `make run` rebuilds everything downstream of silver.
* **Rebuild silver from bronze**: bronze is immutable. To replay a delivery, set it back to `bronze` (`costlake deliveries requeue` for quarantined ones). The lake bucket is versioned in the AWS environment, so an overwritten silver object can also be restored.
* **Backups**: PostgreSQL holds the ledger (reconstructible from bronze) and the Dagster history. Use `pg_dump` of the `costlake` and `dagster` databases; on AWS, enable bucket replication if the lake must survive a region loss.

## Clean-up

* `make down`: stop containers, keep volumes.
* `make destroy`: `terraform destroy` of the local buckets, then remove containers **and volumes** (all local data).
* `make clean`: remove local build and test artefacts.
