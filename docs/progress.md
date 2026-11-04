# Progress

| Phase | Name                          | Status |
| ----- | ----------------------------- | ------ |
| 1     | Project setup                 | Done   |
| 2     | Events and seats              | Done   |
| 3     | Auth                          | Done   |
| 4     | Seat holds                    | Done   |
| 5     | Bookings and final protection | Done   |
| 6     | Virtual queue and rate limits | Done   |
| 7     | Kafka and outbox              | Done   |
| 8     | Payment and saga              | Done   |
| 9     | Expiry and refunds            | Done   |
| 10    | Seat sync and live updates    | Done   |
| 11    | Tickets and notifications     | Done   |
| 12    | Retries and dead letters      | Done   |
| 13    | Analytics and replay          | Done   |
| 14    | Monitoring                    | Done   |
| 15    | Load and failure tests        | Done   |

## Phase 1: Project setup (done 2026-11-04)

- npm workspaces monorepo: `packages/config`, `apps/api`.
- TypeScript 6.0 strict (`tsc -b` project references), ESLint 10 with type-aware typescript-eslint, Prettier.
- Docker Compose: PostgreSQL 18.6, Redis 8.10.3, Apache Kafka 4.4.0 (KRaft), kafbat Kafka UI v1.5.0,
  all with health checks.
- `@ticketsurge/config`: Zod env validation that reports every problem at once, `.env` loading, pino logger.
- `GET /health`: checks PostgreSQL, Redis, and Kafka in parallel with a per-check timeout; 200 or 503.
- Tests: 19 unit tests and 4 integration tests (healthy, plus each dependency unreachable).

Decisions and deviations:

- Node 24 LTS (installed; v26 is the newest LTS). Pinned in `.nvmrc`.
- TypeScript 6.0.x instead of 7.0, because typescript-eslint 8.72 supports only `<6.1.0`.
- `kafbat/kafka-ui` instead of `provectuslabs/kafka-ui`, which is no longer maintained.
- Host ports default to 5433 (PostgreSQL) and 6380 (Redis) to avoid other local instances; configurable in `.env`.
- Kafka and Kafka UI JVM heaps are capped, because the WSL VM has about 3.9 GB RAM.
- `npm audit` reports 12 moderate advisories, all in dev-only lint tooling (minimatch via eslint); none in runtime dependencies.

## Phase 2: Events and seats (done 2026-11-04)

- `migrations/0001_events_and_seats.sql`: `booking` schema, `booking.events`, `booking.seats`
  (unique seat position per event, prices in minor units).
- `scripts` workspace: migration runner (`npm run db:migrate`; ordered, one transaction per file,
  advisory lock, checksum check against edited migrations) and idempotent seed (`npm run db:seed`,
  demo event `7c1f0b8e-5d2a-4c3e-9f4a-0d1e2b3c4a50` with 5 sections x 10 rows x 10 seats = 500 seats).
- `GET /events`, `GET /events/:id`, `GET /events/:id/seats`, `POST /admin/events` (Zod-validated layout,
  event + seats inserted in one transaction, max 20,000 seats).
- Consistent error body `{ error: { code, message, details? } }`; Zod errors become 400.
- Integration tests use a separate `<db>_test` database and Redis DB 1, created and migrated by Vitest global setup.

Decisions:

- `POST /admin/events` is protected by an `x-admin-key` header (`ADMIN_API_KEY`) until auth exists.
- Kafka client logs go to pino at debug level, because librdkafka logs every reconnect while a broker is down.

## Phase 3: Auth (done 2026-11-04)

- `migrations/0002_users.sql`: `booking.users` (lowercased unique email, password hash).
- `POST /auth/register` (201, 409 on duplicate email) and `POST /auth/login` (200, identical 401 for wrong
  password and unknown email; unknown emails still run one scrypt verification so timing does not leak).
- Passwords: Node's built-in scrypt (N=16384, r=8, p=1), random salt, parameters stored in the hash.
- Access tokens: HS256 JWT via `jose` with issuer, audience, and expiry; only HS256 is accepted.
- `authenticate` and `requireAdmin` guards. Admins are users whose email is in `ADMIN_EMAILS`.
- Test: every protected route returns 401 for no header, a non-Bearer scheme, an empty token, garbage,
  an expired token, and a token signed with another secret.

Decisions:

- The brief's `booking.users` has no role column, so admin access uses an `ADMIN_EMAILS` allowlist. This
  replaces the Phase 2 `x-admin-key` header.

## Phase 4: Seat holds (done 2026-11-04)

- `packages/redis-scripts`: Lua scripts `holdSeats` (all-or-nothing, returns conflicts), `extendHold`
  (only if the user still owns every seat), `releaseHold` (deletes a seat only while it is still
  `hold:<holdId>`), loaded via EVALSHA with EVAL fallback, plus typed wrappers.
- `POST /events/:id/holds` (201 holdId + expiresAt; 400 unknown/duplicate seats; 403 sale not open or
  not admitted; 404 unknown event; 409 conflicts with the taken seat IDs; 503 Redis unavailable).
- `DELETE /holds/:holdId` (204; 404 for someone else's or an expired hold; 503 Redis unavailable).
- Seat map shows live `available` / `held` / `sold` from Redis; if Redis is down it is served with `degraded: true`.
- Temporary `ADMISSION_BYPASS` flag (removed in Phase 6).
- Test: 100 parallel HTTP requests for one seat give exactly one 201 and 99 x 409, and the seat is free
  after its TTL. Also 300 concurrent overlapping multi-seat holds never double-hold a seat.

Decisions:

- Added the Redis key `holdref:<holdId>` -> eventId. DELETE /holds/:holdId and POST /bookings only
  receive a hold ID, but hold and seat keys are partitioned by `{eventId}`. It is written before the hold
  and extended before it, so it always outlives the hold.
- Holds are rejected with 403 `SALE_NOT_OPEN` before `sale_starts_at` or for non-published events.
- The Redis client fails fast while disconnected (offline queue off, 1s command timeout), so holds
  return 503 instead of waiting.

## Phase 5: Bookings and final protection (done 2026-11-04)

- `migrations/0003_bookings.sql`: `booking.bookings` (unique `hold_id`, unique `(user_id, idempotency_key)`)
  and `booking.booking_seats` with the partial unique index `one_active_booking_per_seat`. Composite
  foreign keys keep a booked seat inside its booking's event.
- `POST /bookings` (Idempotency-Key header required): checks the hold exists and belongs to the user,
  runs extendHold to the payment deadline + 1 minute (409 if gone), then inserts the PENDING_PAYMENT
  booking and its active seats in one transaction, priced from the database. A unique-index rejection
  returns 409 `SEAT_TAKEN` and releases the hold. Returns 202 with bookingId.
- Idempotency: the same key + hold returns the same booking (also under 20 concurrent requests); the same
  key with a different hold returns 422; a second key for an already booked hold returns 409.
- `GET /bookings/:id` (owner only, 404 otherwise) with its seats.
- The seat map now takes booked seats from PostgreSQL (CONFIRMED = sold, PENDING_PAYMENT = held), so it
  stays correct even when Redis is down. `DELETE /holds/:holdId` returns 409 once the hold is booked.
- Tests: a repeated key returns one booking; a direct duplicate active seat insert fails with
  `one_active_booking_per_seat`; after Redis loses its state, a second buyer's booking is rejected by PostgreSQL.

Notes:

- The `booking.requested` outbox row (brief step 3c) is added in Phase 7 together with the outbox table.

## Phase 6: Virtual queue and rate limits (done 2026-11-04)

- Lua scripts `joinQueue` (one place per user, strictly increasing sequence), `admitNext` (pops the next
  N users and sets `admit:{eventId}:{userId}` with a 5-minute TTL in one step), and `rateLimit`
  (fixed window per user and route).
- `POST /events/:id/queue` (position, or admitted with `admissionExpiresAt`) and
  `GET /events/:id/queue/me` (404 if not queued). Joining is allowed before the sale opens.
- `apps/admission-worker`: every `ADMISSION_INTERVAL_MS`, admits up to `ADMISSION_BATCH_SIZE` users per
  event. A per-event `SET NX PX` lock keeps the rate correct when several worker instances run.
- Rate limits (429 with `Retry-After`) on queue joins, holds, and bookings; all limits are configurable.
- `ADMISSION_BYPASS` removed: `holdSeats` always requires admission.
- Tests: users are admitted in join order (8 users, batches of 3); holds without admission (never joined,
  still waiting, expired, or admitted to another event) return 403; 300 concurrent joins get positions
  1-300; two concurrent workers admit only one batch; rate limits return 429; queue endpoints return 503
  when Redis is down.

Decisions:

- Added a `joinQueue` Lua script (not in the brief's list) so joining is atomic.
- The worker finds events through a Redis set `queues:active`, so it needs no access to the booking
  schema.

## Phase 7: Kafka and outbox (done 2026-11-04)

- `packages/events`: topic names, the versioned envelope, and a Zod schema per event type; validated on
  publish (`createEnvelope`) and on consume (`parseEnvelope`). All messages are keyed by bookingId.
- `packages/kafka`: client factory, producer with `acks=all` and idempotence, topic creation, and a
  consumer runner that commits offsets manually after the handler succeeds.
- `packages/outbox`: `insertOutboxMessage` (same transaction as the data change), `OutboxRelay`
  (publishes unpublished rows in id order, then sets `published_at`; an advisory lock keeps it to one
  active relay per table), and `outboxBacklog`.
- `migrations/0004_booking_outbox.sql`: `booking.outbox` as in the brief, plus a partial index on
  unpublished rows.
- `POST /bookings` now writes the `booking.requested` outbox row in the booking transaction.
- `apps/booking-worker`: runs the booking outbox relay.
- `npm run kafka:topics`: creates the six topics (6 partitions, 7-day retention, replication factor from
  `KAFKA_REPLICATION_FACTOR`; `min.insync.replicas` becomes 2 at replication factor 3).
- Tests: a booking's message appears in `booking.requested` with the right key, headers, and data;
  with the relay stopped it waits in the outbox and is delivered once the relay starts; a Kafka failure
  leaves rows unpublished; two racing relays publish each row once; a rejected booking writes no message.
  Also verified live: booking created with booking-worker stopped, published after it started, read
  back with `kafka-console-consumer`.

Decisions:

- `KAFKA_TOPIC_PREFIX` (empty by default) prefixes topics and consumer groups. Each Kafka test file
  creates its own `test.<random>.` topics, so tests never mix with development traffic.
- Until Phase 12, a failing handler is retried in place with backoff and a malformed message is
  logged and skipped; Phase 12 replaces both with bounded retries and dead-letter topics.

## Phase 8: Payment and saga (done 2026-11-04)

- `migrations/0005_payments_and_saga.sql`: `booking.processed_events`, and the `payment` schema
  (`payments` with unique `booking_id`, `outbox`, `processed_events`).
- `packages/outbox`: `processOnce`, the brief's idempotent-consumer pattern (claim the messageId,
  apply the change and new outbox rows, commit; duplicates roll back).
- `apps/payment-service`: consumes `booking.requested`, waits `PAYMENT_DELAY_MS` (outside any
  transaction), fails `PAYMENT_FAILURE_RATE` of charges, and writes the payment plus
  `payment.completed` in one transaction. It runs its own outbox relay.
- booking saga (in booking-worker, role `saga`): consumes `payment.completed`. Success confirms with
  `WHERE status = 'PENDING_PAYMENT' AND expires_at > now()` and publishes `booking.confirmed`. Failure
  cancels, deactivates the booking's seats in the same transaction, and publishes `booking.cancelled`.
- `BOOKING_WORKER_ROLES` picks which parts run in a process (relay, saga).
- Tests: success confirms (payment SUCCEEDED, seats active, `booking.confirmed` published); failure
  cancels and deactivates seats (`booking.cancelled`, seats bookable again in PostgreSQL); a
  duplicated `payment.completed` is applied once and a later conflicting result cannot undo a
  confirmation; a redelivered `booking.requested` charges once. Also verified live with all four
  processes (6 bookings at a 50% failure rate: 4 confirmed, 2 cancelled, lag 0).

Fixed during this phase:

- The live run found a bug the tests had hidden: `pg` returns BIGINT as a string, the API installs a
  global parser but the worker does not, so the saga's `booking.confirmed` failed validation and
  retried forever. The saga now casts in SQL, and the test pipeline uses pg's default parsing so
  the tests behave like the standalone service (verified: the old code now fails the test).
- Several workspaces had lost declared dependencies during sequential `npm install -w` calls (hidden
  by hoisting). Every package.json now declares exactly what it imports.
- Each service now uses its own Kafka client ID (removed the shared `KAFKA_CLIENT_ID` from `.env`).

## Phase 9: Expiry and refunds (done 2026-11-04)

- `migrations/0006_expiry_and_refunds.sql`: partial index on overdue-candidate bookings; payments can
  be `REFUNDED` with `refunded_at`.
- Expiry sweeper (booking-worker role `sweeper`): every `SWEEP_INTERVAL_MS`, locks overdue
  PENDING_PAYMENT bookings with `FOR UPDATE SKIP LOCKED`, sets EXPIRED, deactivates seats, and adds
  `booking.cancelled` (reason EXPIRED) in one transaction. It pauses while the booking outbox has more
  than `SWEEPER_MAX_OUTBOX_BACKLOG` unpublished rows (a lagging relay means the payment may not have
  had a chance to arrive).
- Late payments: a successful `payment.completed` that cannot confirm locks the booking. If it is still
  pending but overdue, the saga expires it exactly like the sweeper. For EXPIRED or CANCELLED bookings it
  publishes `payment.refund-requested`. Seats are never restored.
- payment-service consumes `payment.refund-requested` and marks the payment REFUNDED (conditional on
  SUCCEEDED, so a repeat cannot refund twice).
- Saga and sweeper share one `closePendingBooking` helper with the conditional update.
- Tests: a delayed payment (3s against a 1s deadline) expires the booking, frees its seats, publishes
  `booking.cancelled`, then requests and records a refund; a late payment before the sweeper runs is
  expired and refunded by the saga; the sweeper pauses above the backlog limit and resumes after it; ten
  failed payments racing two sweeps close each booking exactly once.

Notes:

- Topics must exist (`npm run kafka:topics`) before services start: auto-creation is disabled, and a
  send to a missing topic blocks the relay until librdkafka's delivery timeout.

## Phase 10: Seat sync and live updates (done 2026-11-04)

- `markSold` Lua script: seat value becomes `sold:<bookingId>` with no TTL and the hold hash is deleted;
  safe to repeat, and it overrides a stale Redis hold because PostgreSQL already decided.
- seat-sync (booking-worker role `seat-sync`, group `seat-sync`): `booking.confirmed` runs markSold;
  `booking.cancelled` runs releaseHold without an owner check. It updates Redis only (no
  processed_events), and if Redis is down the consumer retries until it is back.
- Redis Pub/Sub: `seats:{eventId}` carries `seats.updated` (held / available / sold) from the API (hold,
  release) and seat-sync (sold, released). `user:{userId}` carries `booking.updated` (API: PENDING_PAYMENT;
  seat-sync: CONFIRMED / CANCELLED / EXPIRED) and `queue.updated` (admission-worker, on admission).
  The API's publishes are best effort: a lost notification never fails a request.
- `apps/realtime-gateway`: WebSocket at `/ws?token=<access token>` (JWT checked before the upgrade, 401
  otherwise). Clients send `subscribe.event` / `unsubscribe.event`; the gateway PSUBSCRIBEs once to
  `seats:*` and `user:*` and routes messages to subscribed sockets. It has a subscription limit, ping
  heartbeat, and `GET /health`.
- Tests: two connected clients both see a seat go held, available, held, then sold (after payment and
  seat-sync); booking status reaches only its owner; a failed payment frees the seat for both and the
  other user can hold it; unsubscribed clients get nothing; bad tokens get 401; markSold is idempotent and
  cannot be undone by a stale hold's release.

Notes:

- Pub/Sub keeps no history. Clients must reload `GET /events/:id/seats` and `GET /bookings/:id` after
  every (re)connect (documented in the README).
- Holds that simply expire (TTL) send no message; clients see them as available on the next reload.
- Waiting users get `queue.updated` only when admitted; their position comes from polling
  `GET /events/:id/queue/me` (pushing positions to everyone each round would be expensive).

## Phase 11: Tickets and notifications (done 2026-11-04)

- `migrations/0007_tickets_and_notifications.sql`: `ticket` schema (`tickets` with unique code and unique
  `(event_id, seat_id)`, `outbox`, `processed_events`) and `notification` schema (`notifications` with
  unique `(booking_id, type)`, `processed_events`; no outbox because it publishes nothing).
- `apps/ticket-service`: consumes `booking.confirmed`, issues one ticket per seat with a random
  readable code (`TS-XXXX-XXXX-XXXX`), and publishes `ticket.issued` through its own outbox, only when
  it actually created tickets.
- `apps/notification-service`: consumes `ticket.issued` (TICKETS_ISSUED, so the message goes out after
  tickets exist) and `booking.cancelled` (BOOKING_CANCELLED / BOOKING_EXPIRED), records the notification,
  then "sends" it (a log line; real email/SMS is out of scope).
- Two layers of idempotency in both: `processed_events` for redelivered messages (same messageId), and
  unique constraints for replays that arrive with a new messageId.
- Test: replaying `booking.confirmed` and `ticket.issued`, both as exact redeliveries and with new
  message IDs, creates no second ticket, no second `ticket.issued`, and no second notification; a
  replayed `booking.cancelled` notifies once. Also verified live with all six processes (5 bookings at a
  30% failure rate: tickets and notifications match outcomes, every consumer group at lag 0).

## Phase 12: Retries and dead letters (done 2026-11-04)

- `packages/kafka` consumer: a failing handler is retried with exponential backoff up to
  `maxAttempts` (default 5, 200 ms doubling to 5 s). Malformed messages (JSON or schema errors) and
  `PermanentError`s skip retries. Anything that cannot be processed goes to `dlq.<consumer-group>` with
  its original key, value, and headers, plus `dlq-error`, `dlq-original-topic`, `-partition`,
  `-offset`, `dlq-consumer-group`, `dlq-attempts`, and `dlq-failed-at`. The consumer then commits and
  moves on. If the dead-letter write fails, the offset is not committed and the message is retried.
- Every service passes its producer as the dead-letter writer (notification-service gained a producer
  for this alone). `npm run kafka:topics` also creates one `dlq.<group>` topic per consumer group.
- Tests: a malformed message (bad JSON, and valid JSON failing the schema) reaches the dead-letter topic
  with error headers while the next message is processed; transient failures succeed on retry; an
  exhausted retry budget and a permanent error are dead-lettered (3 and 1 attempts) and later messages
  continue; at the service level, garbage on `booking.requested` lands in `dlq.payment-service` and a
  real booking behind it still confirms.

Fixed during this phase:

- An intermittent test failure: a consumer that subscribes before a brand-new topic appears in broker
  metadata skips that topic until its next metadata refresh (minutes). Test topic creation now waits
  until every partition has a leader. In production, topics exist before services start.

## Phase 13: Analytics and replay (done 2026-11-04)

- `migrations/0008_analytics.sql`: `analytics.sales_by_event` (confirmed bookings, seats sold, revenue,
  cancelled, expired per event) and `analytics.processed_events`.
- `apps/analytics-service`: consumes `booking.confirmed` and `booking.cancelled` (group `analytics`) and
  increments the totals, exactly once per message via `processed_events`.
- `npm run kafka:reset-offsets -- <group>`: resets a stopped consumer group to the earliest retained
  offsets. The admin API has no "set offsets" call, so it deletes the group's committed offsets; our
  consumers start a group without offsets at the earliest offset. It refuses while the group has members.
- `npm run analytics:rebuild`: resets the analytics group first (so it fails safely if the service is
  still running), then empties both analytics tables. Starting the service replays the topics.
- Test: sales totals for 6 bookings (4 confirmed, 2 cancelled) match the booking tables; after stopping
  analytics, resetting offsets, and clearing the tables, the replay produces identical totals. A reset
  while the service runs is refused. Also verified live on the dev data: 19 messages replayed into the
  same totals.

Notes:

- A rebuild only sees messages Kafka still retains (7 days by default). For longer history, raise
  `KAFKA_RETENTION_MS` or snapshot the totals.

## Phase 14: Monitoring (done 2026-11-04)

- `packages/metrics` (new shared package, prom-client): per-service registries with Node process
  metrics; HTTP request count and latency per route template; Kafka message count by result and
  processing time per consumer group; outbox backlog (read at scrape time) and rows published.
- Every process serves `GET /metrics`: the API on 3000, realtime-gateway on 3001 (connections, messages
  forwarded), and the workers on `METRICS_PORT` (admission 9101 with queue length and admissions,
  booking-worker 9102, payment 9103, ticket 9104, notification 9105, analytics 9106).
- Trace IDs: the API's request ID is the trace ID (taken from a valid `X-Request-Id` or generated),
  logged as `traceId`, returned in `X-Request-Id`, and stored in every event envelope. Consumers run each
  handler inside an AsyncLocalStorage trace context, so every log line includes `traceId` and
  `messageId`.
- Compose: kafka-exporter v1.11.0 (consumer lag), Prometheus v3.15.1 (5 s scrapes of the exporter and
  all host services), Grafana 13.2.5 on port 3002 with a provisioned datasource and the "TicketSurge
  overview" dashboard (`infra/grafana/build-dashboard.mjs`): throughput, p95 latency, and errors by
  route; queue; consumer lag, throughput, p95 processing time, and dead letters by group; outbox
  backlog; services up.
- `scripts/grafana-lag.sh <group>` prints a group's lag as Grafana's Prometheus datasource reports it.
- Test (live, all eight services): with ticket-service stopped and bookings continuing, its lag in Grafana
  went 2, 3, 5, 6; after restarting it, lag returned to 0 within 25 s. One booking's traceId appeared in
  the logs of api, booking-worker, payment, ticket, and notification services. Automated tests cover
  route-template metrics, X-Request-Id handling, trace propagation into events, and trace context in
  consumer handlers.

Fixed during this phase:

- Kafka data was not persisted (found while testing): the apache/kafka image writes to
  `/tmp/kraft-combined-logs` unless `KAFKA_LOG_DIRS` is set, so recreating the container lost all topics
  and consumer offsets. `KAFKA_LOG_DIRS=/var/lib/kafka/data` now puts the log on the named volume
  (verified across a forced recreate).

Notes:

- kafka-exporter reports -1 lag for partitions without a committed offset; dashboards clamp it to 0.

## Phase 15: Load and failure tests (done 2026-11-04)

- `npm run redis:rebuild`: sold seats from CONFIRMED bookings (markSold), holds from PENDING_PAYMENT
  bookings (TTL = deadline + grace). Redis now runs without persistence, so a restart really loses state.
- `npm run consistency:check`: the four zero-counts from the brief plus Redis sold seats equal to
  PostgreSQL. Each schema is queried separately and compared in code.
- `load-tests/flash-sale.js` (k6 2.3.0) and `load-tests/run.sh baseline|failures`: prepares 10,000 users
  and a 500-seat event, runs every service (booking-worker split by role), injects the drills, waits for
  drain, runs the consistency check, and collects measurements from k6 and Prometheus.
- Both runs passed the consistency check. Measured 2026-11-04 on an Intel Core i5-1235U (12 threads),
  WSL2 with 3.7 GB RAM; 10,000 users (200 at a time), 500 seats, 10% abandoned holds, 10% failed payments:

  | Measurement              | Baseline | With failure drills |
  | ------------------------ | -------- | ------------------- |
  | Peak requests per second | 593      | 329                 |
  | p95 hold latency         | 1.8 s    | 4.9 s               |
  | p95 booking latency      | 2.8 s    | 5.0 s               |
  | Max consumer lag         | 52       | 563                 |
  | Recovery time            | 0 s      | 170 s               |
  | Seats sold               | 497      | 493                 |

  Drills: kill -9 one of two saga instances, stop the relay (78 s), restart Redis + `redis:rebuild`,
  stop ticket-service for 30 s. Recovery time runs from the last fault ending until lag and outbox
  backlog stayed at 0, while the load was still running.

Findings:

- Payment throughput is about 6 partitions / `PAYMENT_DELAY_MS`, because a simulated charge holds its
  partition. At 500 ms (~12/s) many bookings in a 300-user burst expired before payment (then refunded);
  the load runs use 100 ms.
- The relay was stopped for 78 s instead of 60 s because the Redis restart drill ran inside that window.
- Memory is the limit on this machine (3.7 GB WSL): Kafka UI and Grafana are stopped during runs, and
  k6 runs 200 users at a time.
