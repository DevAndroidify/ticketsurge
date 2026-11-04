import { AsyncLocalStorage } from 'node:async_hooks';

export interface TraceContext {
  /** Follows one booking across services: the HTTP request ID, carried in every event envelope. */
  traceId: string;
  /** The Kafka message being handled, when there is one. */
  messageId?: string;
}

const storage = new AsyncLocalStorage<TraceContext>();

/** Runs `fn` with a trace context. Every log line written inside it carries the IDs. */
export function runWithTrace<T>(context: TraceContext, fn: () => T): T {
  return storage.run(context, fn);
}

export function currentTrace(): TraceContext | undefined {
  return storage.getStore();
}
