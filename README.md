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
npm run dev:api             # http://localhost:3000
npm run dev:admission       # in a second terminal: lets users out of the queue
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

Kafka UI: http://localhost:8080

| Command                    | What it does                                    |
| -------------------------- | ----------------------------------------------- |
| `npm run infra:up`         | Start the containers and wait for health checks |
| `npm run infra:down`       | Stop the containers (data is kept)              |
| `npm run infra:reset`      | Stop the containers and delete their volumes    |
| `npm run build`            | Compile every workspace (`tsc -b`)              |
| `npm run db:migrate`       | Apply pending SQL migrations                    |
| `npm run db:seed`          | Create the 500-seat demo event (idempotent)     |
| `npm run dev:api`          | API with watch mode                             |
| `npm run dev:admission`    | Admission worker with watch mode                |
| `npm run typecheck`        | Type-check sources and tests                    |
| `npm run lint`             | ESLint (type-aware) and Prettier check          |
| `npm test`                 | Unit tests                                      |
| `npm run test:integration` | Tests against the running containers            |

## Local vs production-style Kafka settings

Locally there is one broker, so topics use replication factor 1. A production-style cluster would use
replication factor 3 with `min.insync.replicas=2`, so `acks=all` writes survive the loss of one broker.
