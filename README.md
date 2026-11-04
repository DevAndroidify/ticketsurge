# TicketSurge

Backend for a flash-sale ticketing platform, built to show production-style use of **Kafka** and
**Redis**. The core guarantee: a seat is never sold twice, even during traffic spikes, retries, and
service crashes.

- Architecture brief: [docs/architecture.md](docs/architecture.md)
- Build progress: [docs/progress.md](docs/progress.md)

Redis handles the live contention, PostgreSQL makes the final decision, and Kafka runs the steps
after a booking is created.

## Stack

Node.js 24 LTS · TypeScript (strict) · Fastify · PostgreSQL 18 (`pg`, plain SQL) · Redis 8 (`ioredis`) ·
Apache Kafka 4 in KRaft mode (`@confluentinc/kafka-javascript`) · Kafka UI · Zod · Vitest ·
Docker Compose

## Getting started

Requirements: Node.js 24 (`nvm use`), Docker with Compose v2+. On Windows, run everything inside WSL2.

```bash
nvm use
npm install
cp .env.example .env        # then change the passwords
npm run infra:up            # PostgreSQL, Redis, Kafka, Kafka UI; waits until all are healthy
npm run build
npm run db:migrate          # apply SQL migrations
npm run db:seed             # demo event with 500 seats
npm run kafka:topics        # create the Kafka topics (6 partitions each)
npm run dev:api             # http://localhost:3000
# each of these in its own terminal:
npm run dev:admission       # lets users out of the virtual queue
npm run dev:booking-worker  # outbox relay, booking saga, expiry sweeper, seat sync
npm run dev:payment         # simulated payments and refunds
npm run dev:tickets         # issues tickets for confirmed bookings
npm run dev:notifications   # simulated email notifications
npm run dev:realtime        # WebSocket gateway on :3001
npm run dev:analytics       # sales totals per event (rebuild: stop it, npm run analytics:rebuild, start)
curl localhost:3000/health
```

## Buying a seat (current API)

| Step | Request                                           | Result                                          |
| ---- | ------------------------------------------------- | ----------------------------------------------- |
| 1    | `POST /auth/register` `{ email, password }`       | `accessToken` (send as `Authorization: Bearer`) |
| 2    | `POST /events/:id/queue`                          | queue position                                  |
| 3    | `GET /events/:id/queue/me` until `admitted: true` | admitted for 5 minutes                          |
| 4    | `GET /events/:id/seats`                           | seat map with `available` / `held` / `sold`     |
| 5    | `POST /events/:id/holds` `{ seatIds }`            | 201 `holdId`, `expiresAt` (all seats or none)   |
| 6    | `POST /bookings` `{ holdId }` + `Idempotency-Key` | 202 `bookingId`, status `PENDING_PAYMENT`       |
| 7    | `GET /bookings/:id`                               | booking with its seats                          |

Admins (emails in `ADMIN_EMAILS`) create events with `POST /admin/events`.

## Live updates (WebSocket)

Connect to `ws://localhost:3001/ws?token=<accessToken>` (realtime-gateway) and send
`{ "type": "subscribe.event", "eventId": "<id>" }`. You then receive:

| Message           | Fields                        | Sent to                      |
| ----------------- | ----------------------------- | ---------------------------- |
| `seats.updated`   | `eventId, seatIds, status`    | everyone subscribed to event |
| `booking.updated` | `bookingId, status`           | the booking's owner          |
| `queue.updated`   | `eventId, position, admitted` | the admitted user            |

The gateway forwards Redis Pub/Sub, which does not store messages. **After every connect or
reconnect, reload the seat map (`GET /events/:id/seats`) and your booking (`GET /bookings/:id`)**;
anything that happened while you were disconnected is not replayed. Holds that expire on their own
send no message either; they show as available on the next reload.

Kafka UI: http://localhost:8080 · Grafana: http://localhost:3002 · Prometheus: http://localhost:9090

| Command                                           | What it does                                                |
| ------------------------------------------------- | ----------------------------------------------------------- |
| `npm run infra:up`                                | Start the containers and wait for health checks             |
| `npm run infra:down`                              | Stop the containers (data is kept)                          |
| `npm run infra:reset`                             | Stop the containers and delete their volumes                |
| `npm run build`                                   | Compile every workspace (`tsc -b`)                          |
| `npm run db:migrate`                              | Apply pending SQL migrations                                |
| `npm run db:seed`                                 | Create the 500-seat demo event (idempotent)                 |
| `npm run dev:api`                                 | API with watch mode                                         |
| `npm run dev:admission`                           | Admission worker with watch mode                            |
| `npm run typecheck`                               | Type-check sources and tests                                |
| `npm run lint`                                    | ESLint (type-aware) and Prettier check                      |
| `npm test`                                        | Unit tests                                                  |
| `npm run test:integration`                        | Tests against the running containers                        |
| `npm run loadtest:baseline` / `loadtest:failures` | 10,000-user flash sale (k6), optionally with failure drills |
| `npm run redis:rebuild`                           | Restore sold seats and booked holds from PostgreSQL         |
| `npm run consistency:check`                       | Verify the end-of-test invariants                           |

## Load test results

Measured on 2026-11-04 with `load-tests/run.sh`: 10,000 users (200 at a time) compete for 500 seats;
10% of holds are abandoned and 10% of payments fail. Everything runs on one laptop: Intel Core
i5-1235U (12 threads), WSL2 with 3.7 GB RAM, Docker 29.7.2, Node 24.

| Measurement                   | Baseline | With failure drills |
| ----------------------------- | -------- | ------------------- |
| Peak requests per second      | 593      | 329                 |
| p95 hold latency              | 1.8 s    | 4.9 s               |
| p95 booking latency           | 2.8 s    | 5.0 s               |
| Max consumer lag (all groups) | 52       | 563                 |
| Recovery time                 | 0 s      | 170 s               |
| Seats sold                    | 497      | 493                 |
| Consistency check             | all 0    | all 0               |

Failure drills, during the load: kill -9 one of two booking-saga instances; stop the outbox relay
(78 s); restart Redis (back after 7.7 s) and run `redis:rebuild` (0.6 s); stop ticket-service for 30 s.
Recovery time is from the last fault ending until consumer lag and outbox backlog stayed at 0; the load
was still running for most of it. With the relay stopped, many bookings missed their 30 s payment
deadline: they expired and their late payments were refunded, as designed. The consistency check
(no seat with two active bookings, no confirmed booking without a successful payment, no sold seat
without a ticket, no overdue pending booking, Redis sold seats equal to PostgreSQL) passed after both
runs.

## Local vs production-style Kafka settings

Locally there is one broker, so topics use replication factor 1. A production-style cluster would use
replication factor 3 with `min.insync.replicas=2`, so `acks=all` writes survive the loss of one broker.
