#!/usr/bin/env bash
# Prints a consumer group's current lag as Grafana sees it (through its Prometheus datasource),
# i.e. the value behind the "Consumer lag by group" panel.
# Usage: scripts/grafana-lag.sh <consumer-group>
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a
group=${1:?usage: scripts/grafana-lag.sh <consumer-group>}
# kafka-exporter reports -1 for partitions without a committed offset; count those as 0.
query="sum(clamp_min(kafka_consumergroup_lag{consumergroup=\"${group}\"}, 0))"
curl -fsS -u "admin:${GRAFANA_ADMIN_PASSWORD}" -G \
  "http://localhost:${GRAFANA_HOST_PORT:-3002}/api/datasources/proxy/uid/prometheus/api/v1/query" \
  --data-urlencode "query=${query}" |
  python3 -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(r[0]["value"][1] if r else "no data")'
