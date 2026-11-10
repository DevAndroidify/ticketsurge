# TicketSurge — working rules

Flash-sale ticketing backend (portfolio project) demonstrating production-style Kafka and Redis.
Core guarantee: a seat is never sold twice, even during traffic spikes, retries, and crashes.

**The full architecture brief is in [docs/architecture.md](docs/architecture.md). Read it before changing anything.**
Phase status is tracked in [docs/progress.md](docs/progress.md).

## Working rules

- Build ONLY the current phase. Do not begin the next phase until the user replies "continue".
- Every phase must leave the project runnable and locally testable.
- Run the build, linter, and automated tests yourself, and fix failures before reporting completion.
- At the end of each phase, report: what changed, exact commands to run, the expected result of each
  command, and a suggested commit message. Then update docs/progress.md and STOP.
- Keep changes inside the current phase's scope. Do not add placeholder services for future phases.
- Do not change the architecture silently. If something in the brief is wrong or blocks you, explain the
  problem and proposed change, then wait for the user's decision.
- Do not commit or push unless the user asks.
- Never hardcode secrets. Use .env files, commit only .env.example, and validate configuration on startup.
- Use TypeScript strict mode. Keep SQL and Lua scripts readable, with short comments that explain why where
  it is not obvious.
- Add tests with each behavior, especially for concurrency and failure handling.

## Environment notes

- Development happens inside WSL2 (Ubuntu) at `~/projects/ticketsurge`. Run every command from a WSL shell.
- Node is managed by nvm and only loads in interactive shells; non-interactive scripts must
  `source ~/.nvm/nvm.sh && nvm use` (the version is pinned in `.nvmrc`).
- WSL has ~3 GB RAM; keep JVM heaps for Kafka and Kafka UI capped in `infra/docker-compose.yml`.
- Git author for this repo: Sagar Pandey <pandeeysr175@gmail.com> (set in local git config).
  Remote: github.com/DevAndroidify/ticketsurge.

## Common commands

```bash
npm run infra:up          # start PostgreSQL, Redis, Kafka, Kafka UI and wait until healthy
npm run build             # tsc -b across all workspaces
npm run lint              # eslint + prettier check
npm test                  # unit tests
npm run test:integration  # tests against the running containers
npm run dev:all           # run all eight services with watch mode in one terminal
npm run dev:api           # run the API with watch mode
npm run dev:admission     # run the admission worker with watch mode
npm run db:migrate        # apply SQL migrations; npm run db:seed for the 500-seat demo event
npm run demo:new-event    # open a fresh 500-seat event when the demo one has sold out
```

Integration tests use a separate `<db>_test` database and Redis DB 1 (see scripts/src/testing).
