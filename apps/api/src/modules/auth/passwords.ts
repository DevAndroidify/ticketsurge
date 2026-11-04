import { randomBytes, scrypt, timingSafeEqual, type ScryptOptions } from 'node:crypto';

/**
 * Password hashing with Node's built-in scrypt (memory-hard, no native dependency).
 * Parameters are stored inside the hash, so they can be raised later without breaking old hashes.
 * Format: scrypt$N$r$p$<salt base64>$<hash base64>
 */
const DEFAULT_PARAMS = { N: 16_384, r: 8, p: 1 } as const;
const KEY_LENGTH = 32;
const SALT_LENGTH = 16;

function deriveKey(password: string, salt: Buffer, options: ScryptOptions): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    // maxmem must exceed 128 * N * r bytes; give headroom for raised parameters.
    scrypt(password, salt, KEY_LENGTH, { ...options, maxmem: 256 * 1024 * 1024 }, (error, key) => {
      if (error) reject(error);
      else resolve(key);
    });
  });
}

export async function hashPassword(password: string): Promise<string> {
  const salt = randomBytes(SALT_LENGTH);
  const { N, r, p } = DEFAULT_PARAMS;
  const key = await deriveKey(password, salt, { N, r, p });
  return `scrypt$${N}$${r}$${p}$${salt.toString('base64')}$${key.toString('base64')}`;
}

/** Constant-time check. Returns false (never throws) for malformed hashes. */
export async function verifyPassword(password: string, stored: string): Promise<boolean> {
  const parts = stored.split('$');
  if (parts.length !== 6 || parts[0] !== 'scrypt') return false;
  const [, n, r, p, saltB64, keyB64] = parts as [string, string, string, string, string, string];
  const params = { N: Number(n), r: Number(r), p: Number(p) };
  if (!Object.values(params).every((v) => Number.isInteger(v) && v > 0)) return false;

  const expected = Buffer.from(keyB64, 'base64');
  if (expected.length !== KEY_LENGTH) return false;
  try {
    const actual = await deriveKey(password, Buffer.from(saltB64, 'base64'), params);
    return timingSafeEqual(actual, expected);
  } catch {
    return false;
  }
}

/**
 * A valid hash of a random password. Login verifies against it when the email is unknown, so
 * response time does not reveal which emails are registered.
 */
export const DUMMY_HASH_PROMISE: Promise<string> = hashPassword(randomBytes(16).toString('hex'));
