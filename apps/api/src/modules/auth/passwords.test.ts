import { describe, expect, it } from 'vitest';
import { hashPassword, verifyPassword } from './passwords.js';

describe('password hashing', () => {
  it('verifies the right password and rejects a wrong one', async () => {
    const hash = await hashPassword('correct horse battery');
    expect(hash).toMatch(/^scrypt\$16384\$8\$1\$[^$]+\$[^$]+$/);
    expect(await verifyPassword('correct horse battery', hash)).toBe(true);
    expect(await verifyPassword('correct horse batter', hash)).toBe(false);
  });

  it('salts every hash', async () => {
    const [a, b] = await Promise.all([hashPassword('same'), hashPassword('same')]);
    expect(a).not.toBe(b);
  });

  it.each([
    '',
    'plain-text',
    'bcrypt$10$abc',
    'scrypt$0$8$1$c2FsdA==$aGFzaA==',
    'scrypt$16384$8$1$c2FsdA==$dG9vLXNob3J0',
  ])('returns false for malformed hash %j', async (stored) => {
    expect(await verifyPassword('anything', stored)).toBe(false);
  });
});
