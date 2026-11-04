import { describe, expect, it } from 'vitest';
import {
  consumerMetrics,
  createRegistry,
  httpMetrics,
  outboxMetrics,
  startMetricsServer,
} from './index.js';

describe('metrics', () => {
  it('records consumer outcomes, outbox backlog, and HTTP requests', async () => {
    const registry = createRegistry('unit');
    consumerMetrics(registry)('payment-service')({
      topic: 'booking.requested',
      result: 'processed',
      durationMs: 12,
    });
    const published = outboxMetrics(registry, 'booking.outbox', () => Promise.resolve(7));
    published(3);
    httpMetrics(registry)('POST', '/bookings', 202, 0.04);

    const text = await registry.metrics();
    expect(text).toContain(
      'ticketsurge_messages_total{group="payment-service",topic="booking.requested",result="processed",service="unit"} 1',
    );
    expect(text).toContain('ticketsurge_outbox_backlog{table="booking.outbox",service="unit"} 7');
    expect(text).toContain(
      'ticketsurge_outbox_published_total{table="booking.outbox",service="unit"} 3',
    );
    expect(text).toContain(
      'ticketsurge_http_requests_total{method="POST",route="/bookings",status="202",service="unit"} 1',
    );
    expect(text).toContain('process_cpu_user_seconds_total');
  });

  it('serves /metrics over HTTP', async () => {
    const registry = createRegistry('unit');
    const server = await startMetricsServer(registry, 0);
    expect(server).toBeUndefined();

    const real = await startMetricsServer(registry, 39_517, '127.0.0.1');
    try {
      const response = await fetch('http://127.0.0.1:39517/metrics');
      expect(response.status).toBe(200);
      expect(await response.text()).toContain('process_cpu_user_seconds_total');
      expect((await fetch('http://127.0.0.1:39517/other')).status).toBe(404);
    } finally {
      real?.close();
    }
  });
});
