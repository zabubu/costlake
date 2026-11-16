#!/usr/bin/env bash
# End-to-end test of the running platform (`make up` first). Exercises the real path:
# exports land in S3 -> the Dagster sensor starts a run -> serving tables, metrics,
# alerts, logs and dashboards are checked. Exits non-zero on the first failure.
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE=${COMPOSE:-docker compose}
DAGSTER=${DAGSTER_URL:-http://127.0.0.1:3000}
PROM=${PROMETHEUS_URL:-http://127.0.0.1:9090}
GRAFANA=${GRAFANA_URL:-http://127.0.0.1:3001}
TIMEOUT=${E2E_TIMEOUT_SECONDS:-900}
# shellcheck disable=SC1091
set -a; . ./.env; set +a

step() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
fail() { printf '\033[31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }
ok() { printf '\033[32mok\033[0m  %s\n' "$*"; }
psql_q() { $COMPOSE exec -T -e PGPASSWORD="$GRAFANA_PG_PASSWORD" postgres psql -h 127.0.0.1 -U grafana -d costlake -tAc "$1"; }

wait_for() { # wait_for <description> <seconds> <command...>
  local what=$1 limit=$2; shift 2
  local start=$SECONDS
  until "$@" >/dev/null 2>&1; do
    (( SECONDS - start > limit )) && fail "timed out after ${limit}s waiting for: $what"
    sleep 5
  done
  ok "$what ($(( SECONDS - start ))s)"
}

graphql() {
  curl -fsS "$DAGSTER/graphql" -H 'Content-Type: application/json' \
    -d "{\"query\": $(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$1")}"
}

# "<runId>:<status>" for every pipeline run, newest first.
runs() {
  graphql '{ runsOrError(filter: {pipelineName: "costlake_pipeline"}) { ... on Runs { results { runId status } } } }' \
    | python3 -c 'import json,sys; print("\n".join(r["runId"]+":"+r["status"] for r in json.load(sys.stdin)["data"]["runsOrError"]["results"]))'
}

# Statuses of the runs started after the snapshot in $BASELINE.
new_run_statuses() { runs | grep -vFf <(printf '%s\n' "$BASELINE" | cut -d: -f1 | grep . || echo none) | cut -d: -f2 || true; }

new_run_finished() {
  local s; s=$(new_run_statuses)
  [[ -n $s ]] && ! grep -qE 'QUEUED|NOT_STARTED|STARTING|STARTED' <<<"$s"
}

step "Platform health"
for svc in seaweedfs postgres costlake-code dagster-webserver costlake-exporter prometheus grafana; do
  state=$($COMPOSE ps --format '{{.Health}}' "$svc")
  [[ $state == healthy ]] || fail "$svc is $state"
done
ok "all services with a healthcheck are healthy"
targets=$(curl -fsS "$PROM/api/v1/query" --data-urlencode 'query=min(up)' | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["result"][0]["value"][1])')
[[ $targets == 1 ]] || fail "a Prometheus scrape target is down"
ok "every Prometheus target is up"

step "Landing synthetic billing exports (exporter identity)"
BASELINE=$(runs)
$COMPOSE --profile tools run --rm generator

step "Dagster sensor detects the deliveries and runs the pipeline"
wait_for "the triggered run(s) to finish" "$TIMEOUT" new_run_finished
statuses=$(new_run_statuses | sort | uniq -c | tr -s ' ')
echo "    runs:$statuses"
if grep -qvw SUCCESS <(new_run_statuses); then fail "a pipeline run did not succeed: see $DAGSTER/runs"; fi
ok "pipeline run succeeded"

step "Serving layer (read with the grafana read-only role)"
for table in fct_daily_cost agg_daily_service_cost mart_owner_monthly_cost mart_tag_coverage_daily mart_commitment_savings_monthly mart_month_to_date cost_anomalies; do
  rows=$(psql_q "select count(*) from serving.$table")
  (( rows > 0 )) || fail "serving.$table is empty"
  ok "serving.$table: $rows rows"
done
anomalies=$(psql_q "select string_agg(provider || ' / ' || service_name || ' ' || direction, ', ' order by charge_date) from serving.cost_anomalies")
echo "    anomalies: $anomalies"
[[ $anomalies == *"AWS / Amazon Elastic Compute Cloud spike"* && $anomalies == *"Google Cloud / BigQuery spike"* && $anomalies == *"Microsoft / Azure Kubernetes Service drop"* ]] \
  || fail "the three injected incidents were not all detected"
ok "the three injected incidents are detected"
if psql_q "create table serving.should_fail(x int)" 2>/dev/null; then fail "grafana role can write"; fi
ok "grafana role is read-only"

step "Ledger"
statuses=$($COMPOSE exec -T costlake-code costlake deliveries list --limit 100 | awk '{print $2}' | sort | uniq -c | tr -s ' ')
echo "    $statuses"
[[ $statuses == *published* ]] || fail "no published delivery"

step "Metrics and alerts"
exporter_has() {
  curl -fsS "$PROM/api/v1/query" --data-urlencode "query=$1" | python3 -c 'import json,sys; sys.exit(0 if json.load(sys.stdin)["data"]["result"] else 1)'
}
wait_for "costlake_mtd_cost scraped" 120 exporter_has 'costlake_mtd_cost'
wait_for "pipeline step telemetry pushed" 60 exporter_has 'costlake_step_last_run_success{step="publish"} == 1'
# Only the AKS incident (4 days ago) is inside the 7-day "recent" window of the exporter.
wait_for "CostlakeCostAnomaly alert firing for the AKS drop" 180 exporter_has \
  'ALERTS{alertname="CostlakeCostAnomaly",alertstate="firing",service="Azure Kubernetes Service",direction="drop"}'
if exporter_has 'ALERTS{alertname=~"CostlakePipelineStepFailed|CostlakeDeliveryQuarantined|CostlakeLedgerUnreachable",alertstate="firing"}'; then
  fail "a platform alert is firing"
fi
ok "no platform alert firing"

step "Centralised logs"
loki_has_run_logs() {
  $COMPOSE exec -T grafana wget -qO- \
    "http://loki:3100/loki/api/v1/query_range?query=%7Bservice%3D%22costlake-code%22%7D%20%7C%3D%20%22step.succeeded%22&limit=5&since=1h" \
    | python3 -c 'import json,sys; sys.exit(0 if json.load(sys.stdin)["data"]["result"] else 1)'
}
wait_for "pipeline logs queryable in Loki" 120 loki_has_run_logs

step "Grafana"
for uid in costlake-pg prometheus loki; do
  curl -fsS -u "admin:$GRAFANA_ADMIN_PASSWORD" "$GRAFANA/api/datasources/uid/$uid/health" >/dev/null || fail "datasource $uid unhealthy"
  ok "datasource $uid healthy"
done
for uid in costlake-finops costlake-platform; do
  curl -fsS -u "admin:$GRAFANA_ADMIN_PASSWORD" "$GRAFANA/api/dashboards/uid/$uid" >/dev/null || fail "dashboard $uid missing"
  ok "dashboard $uid provisioned"
done

step "Restatement: a fresh export of the same periods replaces them (second run, republish)"
published_before=$(psql_q "select round(sum(billed_cost)::numeric, 2) from serving.fct_daily_cost")
BASELINE=$(runs)
$COMPOSE --profile tools run --rm generator >/dev/null
wait_for "the restatement run to finish" "$TIMEOUT" new_run_finished
if grep -qvw SUCCESS <(new_run_statuses); then fail "the restatement run did not succeed"; fi
published_after=$(psql_q "select round(sum(billed_cost)::numeric, 2) from serving.fct_daily_cost")
[[ $published_before == "$published_after" ]] || fail "totals changed after re-export of identical data: $published_before -> $published_after"
ok "same totals after restatement ($published_after): no double counting"

step "Idempotence: a sensor tick without new exports starts nothing"
before=$(runs | wc -l)
sleep 90
after=$(runs | wc -l)
[[ $before == "$after" ]] || fail "a run started without new data ($before -> $after)"
ok "no new run ($after runs total)"

printf '\n\033[32mEnd-to-end test passed.\033[0m Dagster %s  Grafana %s  Prometheus %s\n' "$DAGSTER" "$GRAFANA" "$PROM"
