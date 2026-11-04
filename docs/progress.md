# Progress

| Phase | Name                          | Status      |
| ----- | ----------------------------- | ----------- |
| 1     | Project setup                 | Done        |
| 2     | Events and seats              | Done        |
| 3     | Auth                          | Not started |
| 4     | Seat holds                    | Not started |
| 5     | Bookings and final protection | Not started |
| 6     | Virtual queue and rate limits | Not started |
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
