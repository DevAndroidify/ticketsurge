import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import type { Redis } from 'ioredis';

/**
 * A Lua script run with EVALSHA, falling back to EVAL when Redis does not have it cached yet
 * (first call, or after a Redis restart flushed the script cache).
 */
export class RedisScript {
  readonly sha: string;

  constructor(
    readonly name: string,
    readonly lua: string,
  ) {
    this.sha = createHash('sha1').update(lua).digest('hex');
  }

  /** Loads lua/<file> relative to the package root (works from both src/ and dist/). */
  static fromFile(name: string, file: string): RedisScript {
    return new RedisScript(name, readFileSync(new URL(`../lua/${file}`, import.meta.url), 'utf8'));
  }

  async run(redis: Redis, keys: readonly string[], args: readonly (string | number)[]) {
    try {
      return await redis.evalsha(this.sha, keys.length, ...keys, ...args);
    } catch (error) {
      if (error instanceof Error && error.message.startsWith('NOSCRIPT')) {
        return redis.eval(this.lua, keys.length, ...keys, ...args);
      }
      throw error;
    }
  }
}
