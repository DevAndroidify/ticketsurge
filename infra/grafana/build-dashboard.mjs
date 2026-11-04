// Generates dashboards/ticketsurge.json. Edit the panel list here, then run:
//   node infra/grafana/build-dashboard.mjs
import { writeFileSync } from 'node:fs';

const ds = { type: 'prometheus', uid: 'prometheus' };
let id = 0;

function panel(title, unit, targets, gridPos, options = {}) {
  return {
    id: ++id,
    type: options.type ?? 'timeseries',
    title,
    description: options.description ?? '',
    datasource: ds,
    gridPos,
    fieldConfig: {
      defaults: { unit, min: 0, custom: { lineWidth: 2, fillOpacity: 10 } },
      overrides: [],
    },
    options: {
      legend: { displayMode: 'table', placement: 'right', calcs: ['lastNotNull', 'max'] },
    },
    targets: targets.map(([expr, legendFormat], i) => ({
      datasource: ds,
      refId: String.fromCharCode(65 + i),
      expr,
      legendFormat,
    })),
  };
}

const row = (title, y) => ({
  id: ++id,
  type: 'row',
  title,
  collapsed: false,
  gridPos: { h: 1, w: 24, x: 0, y },
  panels: [],
});

const dashboard = {
  uid: 'ticketsurge-overview',
  title: 'TicketSurge overview',
  tags: ['ticketsurge'],
  timezone: 'browser',
  schemaVersion: 41,
  refresh: '5s',
  time: { from: 'now-15m', to: 'now' },
  panels: [
    row('HTTP API', 0),
    panel(
      'Throughput by route',
      'reqps',
      [
        [
          'sum by (route) (rate(ticketsurge_http_requests_total{route!="/metrics"}[1m]))',
          '{{route}}',
        ],
      ],
      { h: 8, w: 12, x: 0, y: 1 },
    ),
    panel(
      'p95 latency by route',
      's',
      [
        [
          'histogram_quantile(0.95, sum by (le, route) (rate(ticketsurge_http_request_duration_seconds_bucket{route!="/metrics"}[1m])))',
          '{{route}}',
        ],
      ],
      { h: 8, w: 12, x: 12, y: 1 },
    ),
    panel(
      'Error responses by status',
      'reqps',
      [
        [
          'sum by (status) (rate(ticketsurge_http_requests_total{status=~"[45].."}[1m]))',
          '{{status}}',
        ],
      ],
      { h: 7, w: 12, x: 0, y: 9 },
      {
        description: '409 = seat taken / hold expired, 429 = rate limited, 503 = Redis unavailable',
      },
    ),
    panel(
      'Virtual queue',
      'short',
      [
        ['sum by (event_id) (ticketsurge_queue_waiting_users)', 'waiting {{event_id}}'],
        [
          'sum by (event_id) (rate(ticketsurge_admitted_users_total[1m]))',
          'admitted/s {{event_id}}',
        ],
      ],
      { h: 7, w: 12, x: 12, y: 9 },
    ),

    row('Kafka consumers', 16),
    panel(
      'Consumer lag by group',
      'short',
      [
        [
          // kafka-exporter reports -1 for partitions without a committed offset; count those as 0.
          'sum by (consumergroup) (clamp_min(kafka_consumergroup_lag{consumergroup!~"test.*"}, 0))',
          '{{consumergroup}}',
        ],
      ],
      { h: 8, w: 12, x: 0, y: 17 },
      {
        description:
          'Messages published but not yet processed. Grows while a consumer is stopped or too slow.',
      },
    ),
    panel(
      'Messages processed by group',
      'ops',
      [['sum by (group) (rate(ticketsurge_messages_total{result="processed"}[1m]))', '{{group}}']],
      { h: 8, w: 12, x: 12, y: 17 },
    ),
    panel(
      'p95 message processing time',
      's',
      [
        [
          'histogram_quantile(0.95, sum by (le, group) (rate(ticketsurge_message_duration_seconds_bucket[1m])))',
          '{{group}}',
        ],
      ],
      { h: 7, w: 12, x: 0, y: 25 },
    ),
    panel(
      'Dead-lettered messages (5m)',
      'short',
      [
        [
          'sum by (group) (increase(ticketsurge_messages_total{result="dead_lettered"}[5m]))',
          '{{group}}',
        ],
      ],
      { h: 7, w: 12, x: 12, y: 25 },
    ),

    row('Outbox and services', 32),
    panel(
      'Outbox backlog (unpublished rows)',
      'short',
      [
        ['max by (table) (ticketsurge_outbox_backlog)', '{{table}}'],
        ['sum by (table) (rate(ticketsurge_outbox_published_total[1m]))', 'published/s {{table}}'],
      ],
      { h: 8, w: 12, x: 0, y: 33 },
      {
        description:
          'A growing backlog means the relay is stopped or Kafka is unreachable. The sweeper pauses above its limit.',
      },
    ),
    panel(
      'Services up',
      'short',
      [['up{job="ticketsurge"}', '{{app}} {{instance}}']],
      { h: 8, w: 12, x: 12, y: 33 },
      { type: 'state-timeline' },
    ),
  ],
};

writeFileSync(
  new URL('./dashboards/ticketsurge.json', import.meta.url),
  JSON.stringify(dashboard, null, 2) + '\n',
);
console.log(`wrote ${dashboard.panels.length} panels`);
