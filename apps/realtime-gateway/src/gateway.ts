import websocket from '@fastify/websocket';
import type { Logger } from '@ticketsurge/config';
import { channelId } from '@ticketsurge/redis-scripts';
import Fastify, { type FastifyBaseLogger } from 'fastify';
import type { Redis } from 'ioredis';
import { jwtVerify } from 'jose';
import type { WebSocket } from 'ws';
import { z } from 'zod';
import type { GatewayConfig } from './config.js';

declare module 'fastify' {
  interface FastifyRequest {
    wsUserId: string | null;
  }
}

const clientMessage = z.discriminatedUnion('type', [
  z.object({ type: z.literal('subscribe.event'), eventId: z.uuid() }),
  z.object({ type: z.literal('unsubscribe.event'), eventId: z.uuid() }),
]);

interface Client {
  socket: WebSocket;
  userId: string;
  events: Set<string>;
  alive: boolean;
}

/** Which sockets want which messages. */
class Registry {
  private readonly byEvent = new Map<string, Set<Client>>();
  private readonly byUser = new Map<string, Set<Client>>();

  add(client: Client) {
    addTo(this.byUser, client.userId, client);
  }
  subscribe(client: Client, eventId: string) {
    client.events.add(eventId);
    addTo(this.byEvent, eventId, client);
  }
  unsubscribe(client: Client, eventId: string) {
    client.events.delete(eventId);
    removeFrom(this.byEvent, eventId, client);
  }
  remove(client: Client) {
    for (const eventId of client.events) removeFrom(this.byEvent, eventId, client);
    removeFrom(this.byUser, client.userId, client);
  }
  forEvent(eventId: string) {
    return this.byEvent.get(eventId) ?? new Set<Client>();
  }
  forUser(userId: string) {
    return this.byUser.get(userId) ?? new Set<Client>();
  }
  *all(): Generator<Client> {
    for (const clients of this.byUser.values()) yield* clients;
  }
  get size() {
    let n = 0;
    for (const clients of this.byUser.values()) n += clients.size;
    return n;
  }
}

function addTo(map: Map<string, Set<Client>>, key: string, client: Client) {
  const set = map.get(key) ?? new Set<Client>();
  set.add(client);
  map.set(key, set);
}
function removeFrom(map: Map<string, Set<Client>>, key: string, client: Client) {
  const set = map.get(key);
  if (!set) return;
  set.delete(client);
  if (set.size === 0) map.delete(key);
}

export interface GatewayDeps {
  logger: Logger;
  /** A dedicated connection: in subscriber mode it can run no other commands. */
  subscriber: Redis;
  config: Pick<
    GatewayConfig,
    'JWT_SECRET' | 'JWT_ISSUER' | 'JWT_AUDIENCE' | 'WS_MAX_SUBSCRIPTIONS' | 'WS_PING_INTERVAL_MS'
  >;
}

/**
 * WebSocket gateway at /ws. Clients connect with `?token=<access token>`, send
 * `{ "type": "subscribe.event", "eventId": ... }`, and receive:
 * - seats.updated (seat changes of subscribed events, from Redis channel seats:{eventId});
 * - booking.updated and queue.updated (their own, from Redis channel user:{userId}).
 *
 * Redis Pub/Sub keeps no history: anything published while a client is disconnected is lost, so
 * clients must reload the seat map and their booking status after every (re)connect.
 */
export async function buildGateway({ logger, subscriber, config }: GatewayDeps) {
  const loggerInstance: FastifyBaseLogger = logger;
  const app = Fastify({ loggerInstance });
  const registry = new Registry();
  const key = new TextEncoder().encode(config.JWT_SECRET);

  await app.register(websocket, { options: { maxPayload: 4096 } });
  app.decorateRequest('wsUserId', null);

  // Route every Pub/Sub message to the sockets that want it. The payload is already JSON.
  await subscriber.psubscribe('seats:*', 'user:*');
  subscriber.on('pmessage', (_pattern: string, channel: string, message: string) => {
    const target = channelId(channel);
    if (!target) return;
    const clients =
      target.kind === 'seats' ? registry.forEvent(target.id) : registry.forUser(target.id);
    for (const client of clients) {
      if (client.socket.readyState === client.socket.OPEN) client.socket.send(message);
    }
  });

  app.get('/health', async (_request, reply) => {
    const ready = subscriber.status === 'ready';
    return reply.code(ready ? 200 : 503).send({
      status: ready ? 'ok' : 'unavailable',
      redis: subscriber.status,
      connections: registry.size,
    });
  });

  app.get(
    '/ws',
    {
      websocket: true,
      // Authenticate before the HTTP upgrade, so a bad token gets a plain 401.
      preValidation: async (request, reply) => {
        const token = (request.query as { token?: unknown }).token;
        if (typeof token !== 'string' || token.length === 0) {
          return reply
            .code(401)
            .send({ error: { code: 'UNAUTHORIZED', message: 'token required' } });
        }
        try {
          const { payload } = await jwtVerify(token, key, {
            algorithms: ['HS256'],
            issuer: config.JWT_ISSUER,
            audience: config.JWT_AUDIENCE,
          });
          if (typeof payload.sub !== 'string') throw new Error('no subject');
          request.wsUserId = payload.sub;
        } catch {
          return reply
            .code(401)
            .send({ error: { code: 'UNAUTHORIZED', message: 'invalid token' } });
        }
      },
    },
    (socket, request) => {
      const client: Client = {
        socket,
        userId: request.wsUserId ?? '',
        events: new Set(),
        alive: true,
      };
      registry.add(client);
      const send = (message: object) => {
        socket.send(JSON.stringify(message));
      };

      socket.on('pong', () => {
        client.alive = true;
      });
      socket.on('message', (raw: Buffer) => {
        let parsed: z.infer<typeof clientMessage>;
        try {
          parsed = clientMessage.parse(JSON.parse(raw.toString('utf8')));
        } catch {
          send({ type: 'error', message: 'invalid message' });
          return;
        }
        if (parsed.type === 'subscribe.event') {
          if (
            !client.events.has(parsed.eventId) &&
            client.events.size >= config.WS_MAX_SUBSCRIPTIONS
          ) {
            send({ type: 'error', message: 'too many subscriptions' });
            return;
          }
          registry.subscribe(client, parsed.eventId);
          send({ type: 'subscribed', eventId: parsed.eventId });
        } else {
          registry.unsubscribe(client, parsed.eventId);
          send({ type: 'unsubscribed', eventId: parsed.eventId });
        }
      });
      socket.on('close', () => {
        registry.remove(client);
      });
    },
  );

  // Heartbeat: close connections that did not answer the previous ping.
  const heartbeat = setInterval(() => {
    for (const client of registry.all()) {
      if (!client.alive) {
        client.socket.terminate();
        continue;
      }
      client.alive = false;
      client.socket.ping();
    }
  }, config.WS_PING_INTERVAL_MS);
  app.addHook('onClose', async () => {
    clearInterval(heartbeat);
    await subscriber.punsubscribe().catch(() => undefined);
  });

  return { app, registry };
}
