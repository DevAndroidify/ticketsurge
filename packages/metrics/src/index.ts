import { createServer, type Server } from 'node:http';
import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from 'prom-client';

export { Counter, Gauge, Histogram, Registry };

/** A registry with Node process metrics, every series labelled with the service name. */
export function createRegistry(service: string): Registry {
  const registry = new Registry();
  registry.setDefaultLabels({ service });
  collectDefaultMetrics({ register: registry });
  return registry;
}

/**
 * Serves GET /metrics for Prometheus. Used by services that have no HTTP server of their own.
 * Port 0 disables it (for example a second instance on the same host).
 */
export async function startMetricsServer(
  registry: Registry,
  port: number,
  host = '0.0.0.0',
): Promise<Server | undefined> {
  if (port === 0) return undefined;
  const server = createServer((request, response) => {
    if (request.url !== '/metrics') {
      response.writeHead(404).end();
      return;
    }
    registry.metrics().then(
      (body) => {
        response.writeHead(200, { 'content-type': registry.contentType }).end(body);
      },
      (error: unknown) => {
        response.writeHead(500).end(String(error));
      },
    );
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, host, resolve);
  });
  return server;
}

// Latency buckets in seconds, from 5 ms to 10 s.
const LATENCY_BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10];

/** Kafka consumer metrics: throughput, outcome, and processing time per group and topic. */
export function consumerMetrics(registry: Registry) {
  const messages = new Counter({
    name: 'ticketsurge_messages_total',
    help: 'Kafka messages handled, by consumer group, topic, and result',
    labelNames: ['group', 'topic', 'result'] as const,
    registers: [registry],
  });
  const duration = new Histogram({
    name: 'ticketsurge_message_duration_seconds',
    help: 'Time to handle one Kafka message, including retries',
    labelNames: ['group', 'topic'] as const,
    buckets: LATENCY_BUCKETS,
    registers: [registry],
  });
  /** Returns the `onOutcome` callback for one consumer group. */
  return (group: string) => (outcome: { topic: string; result: string; durationMs: number }) => {
    messages.inc({ group, topic: outcome.topic, result: outcome.result });
    duration.observe({ group, topic: outcome.topic }, outcome.durationMs / 1000);
  };
}

/** Outbox metrics: unpublished rows (read at scrape time) and rows published. */
export function outboxMetrics(registry: Registry, table: string, backlog: () => Promise<number>) {
  new Gauge({
    name: 'ticketsurge_outbox_backlog',
    help: 'Outbox rows not yet published to Kafka',
    labelNames: ['table'] as const,
    registers: [registry],
    async collect() {
      try {
        this.set({ table }, await backlog());
      } catch {
        // Database down: leave the last value; the scrape itself still succeeds.
      }
    },
  });
  const published = new Counter({
    name: 'ticketsurge_outbox_published_total',
    help: 'Outbox rows published to Kafka',
    labelNames: ['table'] as const,
    registers: [registry],
  });
  return (count: number) => {
    published.inc({ table }, count);
  };
}

/** Everything a Kafka service reports, built on one registry. */
export class ServiceMetrics {
  readonly registry: Registry;
  private readonly consumers: ReturnType<typeof consumerMetrics>;

  constructor(service: string) {
    this.registry = createRegistry(service);
    this.consumers = consumerMetrics(this.registry);
  }

  /** `onOutcome` for startConsumer. */
  consumer(group: string) {
    return this.consumers(group);
  }

  /** `onPublished` for an OutboxRelay, plus a backlog gauge read at scrape time. */
  outbox(table: string, backlog: () => Promise<number>) {
    return outboxMetrics(this.registry, table, backlog);
  }
}

/** HTTP metrics, recorded per route template (not raw URL) to keep cardinality bounded. */
export function httpMetrics(registry: Registry) {
  const requests = new Counter({
    name: 'ticketsurge_http_requests_total',
    help: 'HTTP requests, by method, route, and status code',
    labelNames: ['method', 'route', 'status'] as const,
    registers: [registry],
  });
  const duration = new Histogram({
    name: 'ticketsurge_http_request_duration_seconds',
    help: 'HTTP request duration',
    labelNames: ['method', 'route', 'status'] as const,
    buckets: LATENCY_BUCKETS,
    registers: [registry],
  });
  return (method: string, route: string, status: number, seconds: number) => {
    const labels = { method, route, status: String(status) };
    requests.inc(labels);
    duration.observe(labels, seconds);
  };
}
