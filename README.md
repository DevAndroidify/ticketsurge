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
npm run dev:api             # http://localhost:3000
curl localhost:3000/health
```

Kafka UI: http://localhost:8080

| Command                    | What it does                                     |
| -------------------------- | ------------------------------------------------ |
| `npm run infra:up`         | Start the containers and wait for health checks  |
| `npm run infra:down`       | Stop the containers (data is kept)               |
| `npm run infra:reset`      | Stop the containers and delete their volumes     |
| `npm run build`            | Compile every workspace (`tsc -b`)               |
| `npm run typecheck`        | Type-check sources and tests                     |
| `npm run lint`             | ESLint (type-aware) and Prettier check           |
| `npm test`                 | Unit tests                                       |
| `npm run test:integration` | Tests against the running containers             |

## Local vs production-style Kafka settings

Locally there is one broker, so topics use replication factor 1. A production-style cluster would use
replication factor 3 with `min.insync.replicas=2`, so `acks=all` writes survive the loss of one broker.
