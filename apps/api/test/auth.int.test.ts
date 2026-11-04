import { SignJWT } from 'jose';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import {
  TEST_ADMIN_EMAIL,
  bearer,
  createTestApp,
  registerUser,
  resetDatabase,
  type TestApp,
} from './support/integration.js';

/**
 * Every route that requires a token. Each later phase adds its protected routes here, so the
 * "401 without a valid token" guarantee is checked for all of them.
 */
const PROTECTED_ROUTES: { method: 'GET' | 'POST' | 'DELETE'; url: string }[] = [
  { method: 'POST', url: '/admin/events' },
  { method: 'POST', url: '/events/7c1f0b8e-5d2a-4c3e-9f4a-0d1e2b3c4a50/holds' },
  { method: 'DELETE', url: '/holds/7c1f0b8e-5d2a-4c3e-9f4a-0d1e2b3c4a50' },
];

let t: TestApp;

beforeAll(async () => {
  t = await createTestApp();
});
afterAll(async () => {
  await t.close();
});
beforeEach(async () => {
  await resetDatabase(t.clients.pg);
});

describe('POST /auth/register and /auth/login', () => {
  it('registers, then logs in with the same credentials', async () => {
    const register = await t.app.inject({
      method: 'POST',
      url: '/auth/register',
      payload: { email: '  Alice@Example.COM ', password: 'correct horse battery' },
    });
    expect(register.statusCode, register.body).toBe(201);
    const registered = register.json<{
      user: { id: string; email: string };
      accessToken: string;
    }>();
    expect(registered.user.email).toBe('alice@example.com');
    expect(registered.accessToken.split('.')).toHaveLength(3);

    const { rows } = await t.clients.pg.query<{ password_hash: string }>(
      'SELECT password_hash FROM booking.users WHERE id = $1',
      [registered.user.id],
    );
    expect(rows[0]?.password_hash).toMatch(/^scrypt\$/);
    expect(rows[0]?.password_hash).not.toContain('correct horse battery');

    const login = await t.app.inject({
      method: 'POST',
      url: '/auth/login',
      payload: { email: 'ALICE@example.com', password: 'correct horse battery' },
    });
    expect(login.statusCode).toBe(200);
    expect(login.json()).toMatchObject({ user: registered.user, expiresIn: 3600 });
  });

  it('rejects a duplicate email with 409, regardless of case', async () => {
    await registerUser(t, 'bob@example.com');
    const response = await t.app.inject({
      method: 'POST',
      url: '/auth/register',
      payload: { email: 'BOB@example.com', password: 'another password' },
    });
    expect(response.statusCode).toBe(409);
    expect(response.json()).toMatchObject({ error: { code: 'EMAIL_TAKEN' } });
  });

  it('gives the same 401 for a wrong password and an unknown email', async () => {
    await registerUser(t, 'carol@example.com');
    const wrong = await t.app.inject({
      method: 'POST',
      url: '/auth/login',
      payload: { email: 'carol@example.com', password: 'wrong password' },
    });
    const unknown = await t.app.inject({
      method: 'POST',
      url: '/auth/login',
      payload: { email: 'nobody@example.com', password: 'wrong password' },
    });
    expect(wrong.statusCode).toBe(401);
    expect(unknown.statusCode).toBe(401);
    expect(wrong.json()).toEqual(unknown.json());
  });

  it.each([
    ['invalid email', { email: 'not-an-email', password: 'long enough' }],
    ['short password', { email: 'dave@example.com', password: 'short' }],
    ['missing fields', {}],
  ])('rejects %s with 400', async (_label, payload) => {
    const response = await t.app.inject({ method: 'POST', url: '/auth/register', payload });
    expect(response.statusCode).toBe(400);
  });
});

describe('protected routes', () => {
  async function invalidTokens(): Promise<[string, Record<string, string>][]> {
    const secret = new TextEncoder().encode(t.config.JWT_SECRET);
    const base = () =>
      new SignJWT({ email: 'x@test.local' })
        .setProtectedHeader({ alg: 'HS256' })
        .setSubject('3f0e8f8a-1111-4222-8333-444455556666')
        .setIssuer(t.config.JWT_ISSUER)
        .setAudience(t.config.JWT_AUDIENCE);
    const now = Math.floor(Date.now() / 1000);
    const expired = await base()
      .setIssuedAt(now - 7200)
      .setExpirationTime(now - 3600)
      .sign(secret);
    const wrongSecret = await base()
      .setIssuedAt()
      .setExpirationTime('1h')
      .sign(new TextEncoder().encode('a-completely-different-secret-value-xx'));
    return [
      ['no Authorization header', {}],
      ['a non-Bearer scheme', { authorization: 'Basic dXNlcjpwYXNz' }],
      ['an empty bearer token', { authorization: 'Bearer ' }],
      ['a garbage token', { authorization: 'Bearer not.a.token' }],
      ['an expired token', { authorization: `Bearer ${expired}` }],
      ['a token signed with another secret', { authorization: `Bearer ${wrongSecret}` }],
    ];
  }

  it('return 401 without a valid token', async () => {
    const cases = await invalidTokens();
    for (const route of PROTECTED_ROUTES) {
      for (const [label, headers] of cases) {
        const response = await t.app.inject({ ...route, headers, payload: {} });
        expect(response.statusCode, `${route.method} ${route.url} with ${label}`).toBe(401);
        expect(response.json()).toMatchObject({ error: { code: 'UNAUTHORIZED' } });
      }
    }
  });

  it('POST /admin/events returns 403 for a non-admin and 201 for an admin', async () => {
    const payload = {
      name: 'Admin Only',
      venue: 'Hall',
      startsAt: '2030-01-01T20:00:00Z',
      saleStartsAt: '2029-12-01T10:00:00Z',
      sections: [{ name: 'A', priceMinor: 1000, rows: ['1'], seatsPerRow: 10 }],
    };
    const user = await registerUser(t);
    const asUser = await t.app.inject({
      method: 'POST',
      url: '/admin/events',
      headers: bearer(user),
      payload,
    });
    expect(asUser.statusCode).toBe(403);

    const admin = await registerUser(t, TEST_ADMIN_EMAIL);
    const asAdmin = await t.app.inject({
      method: 'POST',
      url: '/admin/events',
      headers: bearer(admin),
      payload,
    });
    expect(asAdmin.statusCode, asAdmin.body).toBe(201);
  });

  it('public routes stay public', async () => {
    expect((await t.app.inject('/events')).statusCode).toBe(200);
    expect((await t.app.inject('/health')).statusCode).toBe(200);
  });
});
