# Progress

| Phase | Name                          | Status      |
| ----- | ----------------------------- | ----------- |
| 1     | Project setup                 | Done        |
| 2     | Events and seats              | Done        |
| 3     | Auth                          | Done        |
| 4     | Seat holds                    | Done        |
| 5     | Bookings and final protection | Done        |
| 6     | Virtual queue and rate limits | Done        |
| 7     | Kafka and outbox              | Done        |
| 8     | Payment and saga              | Done        |
| 9     | Expiry and refunds            | Done        |
| 10    | Seat sync and live updates    | Not started |
| 11    | Tickets and notifications     | Not started |
| 12    | Retries and dead letters      | Not started |
| 13    | Analytics and replay          | Not started |
| 14    | Monitoring                    | Not started |
| 15    | Load and failure tests        | Not started |

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
