#!/usr/bin/env bash
# Flash-sale load test with optional failure drills.
#
#   load-tests/run.sh baseline    10,000 users, 500 seats, no faults
#   load-tests/run.sh failures    same, while: killing a booking-saga instance, stopping the outbox
#                                 relay for 60 s, restarting Redis + redis:rebuild, and stopping then
#                                 restarting ticket-service
#
# Needs: infra running (npm run infra:up), migrations applied, topics created, `npm run build` done.
# Results go to load-tests/results/<mode>-<timestamp>/.
set -uo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
MODE=${1:-baseline}
USERS=${USERS:-10000}
VUS=${VUS:-400}
set -a; source .env; set +a

# Load-test timings: short enough that every hold and deadline passes within the test.
export LOG_LEVEL=warn
export HOLD_TTL_MS=60000 PAYMENT_DEADLINE_MS=30000 HOLD_AFTER_DEADLINE_MS=10000
# Each payment holds its partition for PAYMENT_DELAY_MS, so throughput is about
# 6 partitions / delay. 100 ms gives ~60 payments/s, enough to pay a 500-seat burst well inside the
# 30 s deadline (at 500 ms, ~12/s, many bookings expire and get refunded).
export PAYMENT_DELAY_MS=100 PAYMENT_FAILURE_RATE=0.1
export ADMISSION_INTERVAL_MS=500 ADMISSION_BATCH_SIZE=500 ADMISSION_TTL_MS=180000
export SWEEP_INTERVAL_MS=1000 SWEEPER_MAX_OUTBOX_BACKLOG=100
export CONSUMER_CONCURRENCY=6

STAMP=$(date +%Y%m%d-%H%M%S)
OUT="$ROOT/load-tests/results/$MODE-$STAMP"
mkdir -p "$OUT"
TIMELINE="$OUT/timeline.log"
declare -A PIDS

now() { date +%s.%N; }
mark() { printf '%s %s\n' "$(date +%s)" "$*" | tee -a "$TIMELINE"; }

# start <name> <app> [ENV=VALUE...]: runs apps/<app>/dist/main.js in the background.
start() {
  local name=$1 app=$2; shift 2
  env "$@" node "apps/$app/dist/main.js" >> "$OUT/$name.log" 2>&1 &
  PIDS[$name]=$!
}
stop() { local sig=${2:-TERM}; kill "-$sig" "${PIDS[$1]}" 2>/dev/null; wait "${PIDS[$1]}" 2>/dev/null; }

cleanup() {
  for name in "${!PIDS[@]}"; do kill "${PIDS[$name]}" 2>/dev/null; done
  wait 2>/dev/null
  docker start ticketsurge-kafka-ui-1 ticketsurge-grafana-1 > /dev/null 2>&1
}
trap cleanup EXIT

prom() { # prom <query> [time]: first sample value of an instant query
  curl -s -G "localhost:${PROMETHEUS_HOST_PORT:-9090}/api/v1/query" --data-urlencode "query=$1" \
    ${2:+--data-urlencode "time=$2"} |
    python3 -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(r[0]["value"][1] if r else "0")'
}
total_lag() { prom 'sum(clamp_min(kafka_consumergroup_lag{consumergroup!~"test.*"}, 0))'; }
psql_q() { docker exec -i ticketsurge-postgres-1 psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "$1"; }

# Free memory for the test: Kafka UI and Grafana are not needed while it runs (Prometheus keeps
# recording; Grafana shows the same data afterwards).
docker stop ticketsurge-kafka-ui-1 ticketsurge-grafana-1 > /dev/null

mark "prepare $USERS users"
PREP=$(npm run --silent loadtest:prepare -- "$USERS")
EVENT_ID=$(echo "$PREP" | python3 -c 'import json,sys; print(json.loads(sys.stdin.read().strip().splitlines()[-1])["eventId"])')
echo "$PREP" > "$OUT/prepare.json"

start api api
start admission admission-worker
# booking-worker split by role, so single parts can be killed or stopped.
start relay booking-worker BOOKING_WORKER_ROLES=relay,sweeper METRICS_PORT=9102
start saga-a booking-worker BOOKING_WORKER_ROLES=saga METRICS_PORT=9112
start saga-b booking-worker BOOKING_WORKER_ROLES=saga METRICS_PORT=9122
start seat-sync booking-worker BOOKING_WORKER_ROLES=seat-sync METRICS_PORT=9132
start payment payment-service
start tickets ticket-service
start notifications notification-service
start analytics analytics-service
for _ in $(seq 1 60); do curl -sf localhost:3000/health > /dev/null && break; sleep 1; done
sleep 15 # consumers join their groups
mark "services started; event $EVENT_ID"

# Memory sampler: the minimum available memory goes into the summary.
( while :; do free -m | awk '/Mem/ {print $7}' >> "$OUT/memory.log"; sleep 2; done ) &
MEM_SAMPLER=$!

LOAD_START=$(date +%s)
mark "load start"
docker run --rm --network host --user "$(id -u):$(id -g)" \
  -v "$ROOT/load-tests:/scripts" -v "$OUT:/results" \
  -e USERS="$USERS" -e VUS="$VUS" -e RESULT_NAME=k6 \
  grafana/k6:2.3.0 run --quiet /scripts/flash-sale.js > "$OUT/k6.log" 2>&1 &
K6=$!

LAST_FAULT_END=$LOAD_START
if [ "$MODE" = failures ]; then
  sleep 5;  mark "FAULT kill -9 booking-saga instance A"; stop saga-a KILL
  sleep 5;  mark "FAULT stop outbox relay (60 s)"; stop relay
  sleep 10; mark "FAULT restart Redis"; T0=$(now)
            docker restart ticketsurge-redis-1 > /dev/null
            until docker exec ticketsurge-redis-1 redis-cli -a "$REDIS_PASSWORD" --no-auth-warning ping 2>/dev/null | grep -q PONG; do sleep 0.2; done
            mark "redis back after $(python3 -c "print(round($(now)-$T0,2))") s; running redis:rebuild"
            npm run --silent redis:rebuild >> "$TIMELINE" 2>&1
            mark "redis rebuilt $(python3 -c "print(round($(now)-$T0,2))") s after the restart began"
  sleep 10; mark "FAULT stop ticket-service"; stop tickets
  sleep 30; mark "restart ticket-service"; start tickets ticket-service
  sleep 10; mark "restart outbox relay (stopped $(( $(date +%s) - LOAD_START - 10 )) s)"; start relay booking-worker BOOKING_WORKER_ROLES=relay,sweeper METRICS_PORT=9102
  LAST_FAULT_END=$(date +%s)
fi

wait "$K6"
LOAD_END=$(date +%s)
mark "load end ($(( LOAD_END - LOAD_START )) s)"
# Baseline has no fault: recovery is measured from the end of the load.
[ "$MODE" = failures ] || LAST_FAULT_END=$LOAD_END

# Drain: wait for every hold and payment deadline to pass, the sweeper to finish, the outboxes to
# empty, and every consumer to catch up.
DEADLINE_END=$(( LOAD_END + 75 ))
while :; do
  pending=$(psql_q "SELECT count(*) FROM booking.bookings WHERE event_id = '$EVENT_ID' AND status = 'PENDING_PAYMENT'")
  backlog=$(psql_q "SELECT (SELECT count(*) FROM booking.outbox WHERE published_at IS NULL) + (SELECT count(*) FROM payment.outbox WHERE published_at IS NULL) + (SELECT count(*) FROM ticket.outbox WHERE published_at IS NULL)")
  lag=$(total_lag)
  if [ "$pending" = 0 ] && [ "$backlog" = 0 ] && [ "${lag%.*}" = 0 ]; then
    [ -z "${DRAINED:-}" ] && DRAINED=$(date +%s) && mark "drained: no pending bookings, outboxes empty, lag 0"
    [ "$(date +%s)" -ge "$DEADLINE_END" ] && sleep 10 && [ "$(total_lag)" = 0 ] && break
  else
    DRAINED=
  fi
  [ $(( $(date +%s) - LOAD_END )) -gt 900 ] && mark "drain timed out (pending=$pending backlog=$backlog lag=$lag)" && break
  sleep 2
done

kill "$MEM_SAMPLER" 2>/dev/null
mark "consistency check"
npm run --silent consistency:check -- "$EVENT_ID" > "$OUT/consistency.json" 2>&1
tail -1 "$OUT/consistency.json" | tee -a "$TIMELINE"

# Measurements from Prometheus over the test window.
WINDOW=$(( $(date +%s) - LOAD_START + 30 ))s
PEAK_RPS=$(prom "max_over_time(sum(rate(ticketsurge_http_requests_total{route!=\"/metrics\"}[10s]))[$WINDOW:5s])")
MAX_LAG=$(prom "max_over_time(sum(clamp_min(kafka_consumergroup_lag{consumergroup!~\"test.*\"}, 0))[$WINDOW:5s])")
MAX_LAG_GROUP=$(curl -s -G "localhost:${PROMETHEUS_HOST_PORT:-9090}/api/v1/query" --data-urlencode \
  "query=max_over_time(sum by (consumergroup) (clamp_min(kafka_consumergroup_lag{consumergroup!~\"test.*\"}, 0))[$WINDOW:5s])" |
  python3 -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(json.dumps({x["metric"]["consumergroup"]: float(x["value"][1]) for x in r}))')
# Recovery: from the end of the last fault (or of the load, for baseline) to the moment lag and
# outbox backlog were both back at 0 for good, at Prometheus' 5 s resolution.
RECOVERY=$(curl -s -G "localhost:${PROMETHEUS_HOST_PORT:-9090}/api/v1/query_range" \
  --data-urlencode 'query=sum(clamp_min(kafka_consumergroup_lag{consumergroup!~"test.*"}, 0)) + sum(max by (table) (ticketsurge_outbox_backlog))' \
  --data-urlencode "start=$LAST_FAULT_END" --data-urlencode "end=$(date +%s)" --data-urlencode step=5 |
  python3 -c "
import json,sys
vals=[(float(t),float(v)) for t,v in json.load(sys.stdin)['data']['result'][0]['values']]
nonzero=[t for t,v in vals if v>0]
print(round(nonzero[-1]+5-$LAST_FAULT_END) if nonzero else 0)")

python3 - "$OUT" "$MODE" "$EVENT_ID" "$PEAK_RPS" "$MAX_LAG" "$MAX_LAG_GROUP" "$RECOVERY" "$(( LOAD_END - LOAD_START ))" <<'PY'
import json, sys
out, mode, event, peak, max_lag, lag_groups, recovery, duration = sys.argv[1:]
k6 = json.load(open(f"{out}/k6.json"))
m = k6["metrics"]
cons_lines = open(f"{out}/consistency.json").read()
cons = json.loads(cons_lines[cons_lines.index('{'):cons_lines.rindex('}') + 1])
outcomes = {k.removeprefix("outcome_"): int(v["values"]["count"]) for k, v in m.items() if k.startswith("outcome_")}
summary = {
    "mode": mode,
    "eventId": event,
    "users": int(m["iterations"]["values"]["count"]),
    "userOutcomes": outcomes,
    "loadDurationSeconds": int(duration),
    "httpRequests": int(m["http_reqs"]["values"]["count"]),
    "averageRps": round(m["http_reqs"]["values"]["rate"], 1),
    "peakRps": round(float(peak), 1),
    "holdLatencyMs": {k: round(v, 1) for k, v in m["hold_latency"]["values"].items()},
    "bookingLatencyMs": {k: round(v, 1) for k, v in m["booking_latency"]["values"].items()},
    "seatMapLatencyMs": {k: round(v, 1) for k, v in m["seat_map_latency"]["values"].items()},
    "admissionWaitMs": {k: round(v, 1) for k, v in m["admission_wait"]["values"].items()},
    "httpFailedRate": round(m["http_req_failed"]["values"]["rate"], 4),
    "maxConsumerLag": float(max_lag),
    "maxConsumerLagByGroup": json.loads(lag_groups),
    "recoverySeconds": int(recovery),
    "consistency": {k: cons[k] for k in ["seatsWithMultipleActiveBookings", "confirmedWithoutSuccessfulPayment",
                                         "confirmedSeatsWithoutTicket", "overduePendingBookings", "redisSoldMismatches"]},
    "totals": cons["totals"],
    "minAvailableMemoryMb": min(int(x) for x in open(f"{out}/memory.log") if x.strip()),
}
json.dump(summary, open(f"{out}/summary.json", "w"), indent=2)
print(json.dumps(summary, indent=2))
PY
mark "done: $OUT"
