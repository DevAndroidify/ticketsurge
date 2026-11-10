# TicketSurge

**Live demo: [ticketsurge-web.vercel.app](https://ticketsurge-web.vercel.app)**. Open it and press
**Start a sale** to watch a flash sale run, no setup needed.
([Demo site source](https://github.com/DevAndroidify/ticketsurge-web))

TicketSurge is the backend of a flash-sale ticketing platform: thousands of people try to buy a
limited number of seats at the same moment, and every seat must be sold exactly once.

Users wait in a virtual queue, hold seats, and book them. Redis handles the rush (queue, seat holds,
rate limits), PostgreSQL makes the final decision on who gets each seat, and Kafka carries each booking
through payment, ticketing, and notifications. Live seat changes reach clients over WebSocket.

Built with Node.js, TypeScript, Fastify, PostgreSQL, Redis, Kafka, Prometheus, and Grafana.

## Run it

```bash
npm install
cp .env.example .env
npm run infra:up
npm run build && npm run db:migrate && npm run db:seed && npm run kafka:topics
npm run dev:all        # all eight services in one terminal, logs prefixed by service
```

The live demo runs a simulation in your browser. With the commands above running, press **Connect
my backend** on it to drive these real services instead; `.env.example` already allows the site in
`CORS_ORIGINS`. When the demo event sells out, `npm run demo:new-event` opens a fresh one.

More detail: [docs/architecture.md](docs/architecture.md) and [docs/progress.md](docs/progress.md).
