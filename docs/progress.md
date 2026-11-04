# Progress

| Phase | Name                          | Status      |
| ----- | ----------------------------- | ----------- |
| 1     | Project setup                 | Done        |
| 2     | Events and seats              | Done        |
| 3     | Auth                          | Done        |
| 4     | Seat holds                    | Done        |
| 5     | Bookings and final protection | Done        |
| 6     | Virtual queue and rate limits | Done        |
| 7     | Kafka and outbox              | Not started |
| 8     | Payment and saga              | Not started |
| 9     | Expiry and refunds            | Not started |
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
