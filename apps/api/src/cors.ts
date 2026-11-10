import type { FastifyInstance } from 'fastify';

/** Headers the demo site sends: bearer token, JSON bodies, idempotency keys and trace IDs. */
const ALLOWED_HEADERS = 'authorization, content-type, idempotency-key, x-request-id';
/** Headers the browser may read: 429 back-off and the trace ID. */
const EXPOSED_HEADERS = 'retry-after, x-request-id';

/**
 * Lets the listed browser origins call the API directly, so a hosted copy of the demo site can
 * connect to a backend running on the visitor's own machine. Off when the list is empty, which is
 * the default: same-origin callers (curl, k6, the site's dev-server proxy) never need it.
 */
export function registerCors(app: FastifyInstance, origins: readonly string[]): void {
  if (origins.length === 0) return;
  const allowed = new Set(origins);

  app.addHook('onRequest', async (request, reply) => {
    const origin = request.headers.origin;
    if (origin === undefined || !allowed.has(origin)) return;
    void reply.header('access-control-allow-origin', origin);
    void reply.header('vary', 'Origin');
    void reply.header('access-control-expose-headers', EXPOSED_HEADERS);
  });

  // Preflight for every route. An origin that is not listed gets no CORS headers, so the browser
  // blocks the real request.
  app.options('*', { logLevel: 'warn' }, async (request, reply) => {
    const origin = request.headers.origin;
    if (origin !== undefined && allowed.has(origin)) {
      void reply.header('access-control-allow-methods', 'GET, POST, DELETE');
      void reply.header('access-control-allow-headers', ALLOWED_HEADERS);
      void reply.header('access-control-max-age', '600');
      // Chrome asks before a public site may reach localhost (Private Network Access).
      if (request.headers['access-control-request-private-network'] === 'true') {
        void reply.header('access-control-allow-private-network', 'true');
      }
    }
    return reply.code(204).send();
  });
}
