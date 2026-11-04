You are building the backend of a flash-sale ticketing platform in this empty folder. It is a portfolio project that must demonstrate production-style use of Kafka and Redis. The core guarantee is that a seat is never sold twice, even during traffic spikes, retries, and service crashes.

Before writing code:
1. Save this entire brief unchanged to docs/architecture.md.
2. Create CLAUDE.md with the working rules below and a pointer to docs/architecture.md.
3. Create docs/progress.md with all 15 phases marked as not started.
4. Check my environment. I may be on Windows with WSL2, so confirm Node.js, Docker, and Docker Compose work from this shell. Report any problem before continuing.

# Working rules

- Build ONLY the current phase. Start with Phase 1. Do not begin the next phase until I reply "continue".
- Every phase must leave the project runnable and locally testable.
- Run the build, linter, and automated tests yourself, and fix failures before reporting completion.
- At the end of each phase, report: what changed, exact commands for me to run, the expected result of each command, and a suggested commit message. Then update docs/progress.md and STOP.
- Keep changes inside the current phase's scope. Do not add placeholder services for future phases.
- Do not change the architecture silently. If something in this brief is wrong or blocks you, explain the problem and proposed change, then wait for my decision.
- Do not commit or push unless I ask.
- Never hardcode secrets. Use .env files, commit only .env.example, and validate configuration on startup.
- Use TypeScript strict mode. Keep SQL and Lua scripts readable, with short comments that explain why where it is not obvious.
- Add tests with each behavior, especially for concurrency and failure handling.

# Stack

- Monorepo: npm workspaces
- Runtime: Node.js LTS with TypeScript
- HTTP API: Fastify
- Database: PostgreSQL using pg and plain SQL migrations
- Redis client: ioredis
- Kafka client: @confluentinc/kafka-javascript, using its KafkaJS-compatible API
- Kafka: Apache Kafka in KRaft mode, with Kafka UI for local inspection
- Validation: Zod
- Tests: Vitest, plus k6 for load tests
- Monitoring: Prometheus and Grafana
- Local infrastructure: Docker Compose
- Check the current stable versions when installing. Do not rely on versions from memory.

Real payment providers, real email or SMS, Kubernetes, and multi-region deployment are out of scope. The frontend is a separate later project.

# Repository structure

ticketing/
  apps/
    api/                    HTTP API
    booking-worker/         Outbox relay, booking saga, expiry sweeper, seat sync
    admission-worker/       Moves users out of the virtual queue
    payment-service/        Simulated payments and refunds
    ticket-service/         Ticket creation
    notification-service/   Simulated notifications
    analytics-service/      Rebuildable sales totals
    realtime-gateway/       WebSocket server
  packages/
    events/                 Topic names and event schemas
    kafka/                  Producer, consumer, retry, and DLQ helpers
    outbox/                 Shared relay logic
    redis-scripts/          Lua scripts and typed wrappers
    config/                 Environment validation and logging
  migrations/
  infra/
    docker-compose.yml
    prometheus/
    grafana/
  load-tests/
  scripts/                  Topic creation, seeding, redis:rebuild, offset reset

api and booking-worker form one logical booking service and share the booking schema. They are separate processes because they scale differently. Do not join across service schemas in application code.

# Architecture

Redis handles the live contention, PostgreSQL makes the final decision, and Kafka runs the steps after a booking is created.

- Client -> API over HTTP. Client <-> realtime gateway over WebSocket.
- API -> Redis for rate limits, queue, admission checks, and holds.
- API -> PostgreSQL for bookings.
- admission-worker -> Redis to admit waiting users at a configured rate.
- booking-worker <-> PostgreSQL and Kafka. It also updates Redis through seat sync.
- payment, ticket, notification, and analytics services consume and publish through Kafka. Each owns a PostgreSQL schema.
- realtime-gateway subscribes to Redis Pub/Sub and forwards updates to connected clients.

# Booking flow

Synchronous:
1. A user joins the queue and receives a position.
2. An admitted user holds seats. holdSeats either holds all requested seats or changes nothing.
3. POST /bookings:
   a. Check that the hold exists and belongs to the user.
   b. Run extendHold. Return 409 if the hold is gone.
   c. In one transaction, insert the booking as PENDING_PAYMENT, its active seats, and a booking.requested outbox row.
   d. If the unique seat index rejects the insert, return 409 and release the hold.
   e. Return 202 with bookingId.

Asynchronous:
4. The outbox relay publishes booking.requested.
5. payment-service simulates the result and publishes payment.completed.
6. booking-worker runs a conditional update on the pending booking:
   - Paid in time: set CONFIRMED and publish booking.confirmed. Seat sync marks seats sold, ticket-service issues tickets, and notification-service sends a simulated message after ticket.issued.
   - Payment failed: set CANCELLED, set its booking_seats rows inactive in the same transaction, and publish booking.cancelled. Seat sync releases the hold, and notification-service notifies the user.
   - No result by expires_at: the sweeper sets EXPIRED, deactivates seats in the same transaction, and publishes booking.cancelled.
   - Payment succeeded after expiry: publish payment.refund-requested. payment-service records the refund. Do not restore the seats.

Status changes must use this pattern, so a payment result and the sweeper cannot both win:

    UPDATE booking.bookings
    SET status = 'CONFIRMED', updated_at = now()
    WHERE id = $1 AND status = 'PENDING_PAYMENT';

# PostgreSQL

Use one PostgreSQL instance with one schema per service. Store money in minor units.

- booking.users: id, email unique, password_hash
- booking.events: id, name, venue, starts_at, sale_starts_at, status
- booking.seats: id, event_id, section, row_label, seat_number, price_minor; each position is unique within an event
- booking.bookings: id, event_id, user_id, hold_id unique, status, amount_minor, idempotency_key, expires_at, created_at, updated_at; unique (user_id, idempotency_key)
- booking.booking_seats: booking_id, event_id, seat_id, active
- payment.payments: id, booking_id unique, status, amount_minor, provider_ref
- ticket.tickets: id, booking_id, event_id, seat_id, code unique, issued_at; unique (event_id, seat_id)
- notification.notifications: id, user_id, booking_id, type, sent_at; unique (booking_id, type)
- analytics.sales_by_event: rebuildable totals by event

Booking statuses: PENDING_PAYMENT, CONFIRMED, CANCELLED, EXPIRED.

Every service that publishes has its own outbox table. Every service that consumes in a transaction has its own processed_events table.

    CREATE UNIQUE INDEX one_active_booking_per_seat
      ON booking.booking_seats (event_id, seat_id)
      WHERE active = true;

    CREATE TABLE booking.outbox (
      id           BIGSERIAL PRIMARY KEY,
      message_id   UUID NOT NULL UNIQUE,
      topic        TEXT NOT NULL,
      message_key  TEXT NOT NULL,
      payload      JSONB NOT NULL,
      created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
      published_at TIMESTAMPTZ
    );

    CREATE TABLE booking.processed_events (
      consumer     TEXT NOT NULL,
      message_id   UUID NOT NULL,
      processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (consumer, message_id)
    );

# Redis

Redis holds fast, temporary state. PostgreSQL remains the source of truth.

Keys:
- queue:{eventId}: sorted set of waiting users, ordered by join sequence
- queue:{eventId}:seq: increasing queue counter
- admit:{eventId}:{userId}: admission marker, 5-minute TTL
- seat:{eventId}:{seatId}: hold:<holdId> with TTL, or sold:<bookingId> without TTL
- hold:{eventId}:{holdId}: hash with owner and seat IDs, same TTL as the hold
- rl:{route}:{userId}: rate-limit state
- seats:{eventId}: Pub/Sub channel for seat changes
- user:{userId}: Pub/Sub channel for booking status

Keep the {eventId} hash tag so an event's keys stay in one Redis Cluster slot.

Atomic Lua scripts:
- holdSeats: check admission and every seat; hold all if free, otherwise change nothing and return conflicts
- extendHold: extend only if the user still owns every seat
- releaseHold: delete a seat key only if its value matches this hold
- markSold: replace the seat value with sold:<bookingId> and remove the TTL
- admitNext: move the next N users from the queue to admitted

Configurable defaults: hold 10 minutes; payment deadline 5 minutes after booking creation; reset the hold to expire 1 minute after that deadline.

scripts/redis:rebuild restores sold seats from CONFIRMED bookings and holds from PENDING_PAYMENT bookings. Unbooked holds and queue places are not recovered; users select again or rejoin.

If Redis is unavailable, queue and hold endpoints return 503. Do not allow holds without Redis.

# Kafka

Delivery is at-least-once. Consumers must make duplicate messages harmless.

Topics:
- booking.requested: published by booking-worker; consumed by payment-service
- payment.completed: published by payment-service; consumed by booking-saga
- booking.confirmed: published by booking-worker; consumed by ticket-service, seat-sync, analytics
- booking.cancelled: published by booking-worker; consumed by seat-sync, notification-service, analytics
- payment.refund-requested: published by booking-worker; consumed by payment-service
- ticket.issued: published by ticket-service; consumed by notification-service
- dlq.<consumer-group>: messages that keep failing after retries

Key booking and payment messages by bookingId, with 6 partitions to start. Do NOT key by the show's eventId, because a flash sale would send nearly all messages to one partition.

Producer: acks=all, idempotence on. Consumer: commit offsets manually after the database transaction commits. Local replication factor: 1. Retention: 7 days. Document replication factor 3 and min.insync.replicas 2 as the production-style setting.

Event envelope, validated with shared Zod schemas when publishing and consuming:

    {
      "messageId": "uuid",
      "type": "booking.confirmed",
      "version": 1,
      "occurredAt": "ISO-8601 timestamp",
      "traceId": "string",
      "data": { "bookingId": "uuid", "eventId": "uuid", "seatIds": ["uuid"] }
    }

messageId deduplicates Kafka messages. eventId identifies the concert or show.

Outbox: save the data change and outbox row in one transaction. Run one relay per service, publish unpublished rows in id order, then set published_at. A crash may publish a row twice, and consumers must handle that.

Idempotent consumer:

    BEGIN
      INSERT INTO processed_events (consumer, message_id) ON CONFLICT DO NOTHING
      if 0 rows inserted: ROLLBACK, commit the offset, and stop
      apply the change
      add any new events to the outbox
    COMMIT
    commit the Kafka offset

seat-sync updates Redis only and does not need processed_events, because markSold and releaseHold are safe to repeat.

Retries: use backoff for transient errors, then send the original message and error details to dlq.<consumer-group> and continue.

# API

- POST /auth/register
- POST /auth/login
- GET /events
- GET /events/:id
- GET /events/:id/seats
- POST /events/:id/queue
- GET /events/:id/queue/me
- POST /events/:id/holds with seatIds: 201 holdId and expiresAt; 403 not admitted; 409 conflicts; 429 rate limited; 503 Redis unavailable
- DELETE /holds/:holdId: 204
- POST /bookings with holdId and an Idempotency-Key header: 202 bookingId; 409 hold expired or seat taken
- GET /bookings/:id
- POST /admin/events: creates an event and seats
- GET /health

WebSocket at /ws:
- subscribe.event from client: eventId
- queue.updated to client: position, admitted
- seats.updated to client: seatIds, status
- booking.updated to client: bookingId, status

The gateway forwards from Redis Pub/Sub. Pub/Sub does not store missed messages, so document that clients must reload seats and booking status after reconnecting.

# Phases

For each phase, the listed test must pass before you report completion.

Phase 1: Project setup
Build the monorepo, TypeScript and lint configuration, Docker Compose for PostgreSQL, Redis, Kafka, and Kafka UI, config validation, and GET /health.
Test: all containers are healthy, and GET /health returns 200 after checking PostgreSQL, Redis, and Kafka.

Phase 2: Events and seats
Build migrations, a seed script, GET /events, GET /events/:id, GET /events/:id/seats, and POST /admin/events.
Test: a seeded 500-seat event returns the correct seat map.

Phase 3: Auth
Build registration, login, password hashing, and JWT checks.
Test: protected routes return 401 without a valid token.

Phase 4: Seat holds
Build holdSeats, extendHold, releaseHold, and the hold endpoints. Use a temporary admission bypass flag that only Phase 6 removes.
Test: with 100 parallel requests for one seat, exactly one succeeds, and the seat is free after its TTL.

Phase 5: Bookings and final protection
Build POST /bookings, GET /bookings/:id, idempotency keys, and the partial unique index.
Test: a repeated key returns one booking, and a direct duplicate active seat insert fails.

Phase 6: Virtual queue and rate limits
Build queue endpoints, admission-worker, admitNext, and rate limiting. Remove the bypass flag.
Test: users are admitted in order, and holds without admission return 403.

Phase 7: Kafka and outbox
Build topic creation, event schemas, the booking outbox write, and the relay.
Test: a new booking appears in booking.requested in Kafka UI, and stopping the relay only delays it.

Phase 8: Payment and saga
Build payment-service with configurable delay and failure rate, and the booking-saga consumer.
Test: success confirms the booking; failure cancels it and deactivates its seats.

Phase 9: Expiry and refunds
Build the sweeper and late-payment path. Pause the sweeper when the outbox backlog passes a configurable limit.
Test: a delayed payment expires the booking and creates a refund request.

Phase 10: Seat sync and live updates
Build seat-sync, markSold, Redis Pub/Sub messages, and realtime-gateway.
Test: two connected WebSocket clients see each seat change.

Phase 11: Tickets and notifications
Build both services with idempotent consumers.
Test: replaying a booking event creates no second ticket or notification.

Phase 12: Retries and dead letters
Build backoff and dead-letter topics in the shared Kafka package.
Test: a malformed message reaches the dead-letter topic while later messages continue.

Phase 13: Analytics and replay
Build analytics-service and an offset reset script.
Test: sales totals match before and after a rebuild from Kafka.

Phase 14: Monitoring
Build Prometheus metrics, trace IDs in logs, and Grafana dashboards for throughput, latency, and consumer lag.
Test: stopping a consumer shows growing lag in Grafana.

Phase 15: Load and failure tests
Build k6 scenarios, scripts/redis:rebuild, and consistency checks. Simulate 10,000 users competing for 500 seats, with some abandoned holds and failed payments. Repeat while killing a booking-saga instance, stopping the relay for a minute, restarting Redis and running redis:rebuild, and stopping then restarting ticket-service.
Test: after holds and deadlines pass and consumer lag reaches zero, all of these are zero: seats with more than one active booking, confirmed bookings without a successful payment, active confirmed seats without a ticket, and overdue pending bookings. Sold seats in Redis must match PostgreSQL.
Write the machine specs and measured results to README.md: peak requests per second, p95 hold and booking latency, maximum consumer lag, recovery time, and seats sold. Do not invent or estimate results.

Start now with the setup steps, then Phase 1 only.
