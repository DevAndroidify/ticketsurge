import { SignJWT, UnsecuredJWT } from 'jose';
import { describe, expect, it } from 'vitest';
import { TokenService } from './tokens.js';

const settings = {
  secret: 'test-secret-that-is-at-least-32-chars!!',
  issuer: 'ticketsurge-api',
  audience: 'ticketsurge',
  ttlSeconds: 60,
};
const tokens = new TokenService(settings);
const user = { id: '0b6f6c4e-7d33-4b8e-9d55-1c2a3b4c5d6e', email: 'a@example.com' };

describe('TokenService', () => {
  it('round-trips a user', async () => {
    const { token, expiresIn } = await tokens.sign(user);
    expect(expiresIn).toBe(60);
    expect(await tokens.verify(token)).toEqual(user);
  });

  it('rejects an expired token', async () => {
    const { token } = await tokens.sign(user, new Date(Date.now() - 120_000));
    expect(await tokens.verify(token)).toBeUndefined();
  });

  it('rejects a token signed with another secret', async () => {
    const other = new TokenService({
      ...settings,
      secret: 'another-secret-that-is-32-chars-long!',
    });
    const { token } = await other.sign(user);
    expect(await tokens.verify(token)).toBeUndefined();
  });

  it('rejects a token for another audience or issuer', async () => {
    for (const patch of [{ audience: 'elsewhere' }, { issuer: 'evil' }]) {
      const { token } = await new TokenService({ ...settings, ...patch }).sign(user);
      expect(await tokens.verify(token)).toBeUndefined();
    }
  });

  it('rejects an unsigned (alg: none) token', async () => {
    const token = new UnsecuredJWT({ email: user.email })
      .setSubject(user.id)
      .setIssuer(settings.issuer)
      .setAudience(settings.audience)
      .setIssuedAt()
      .setExpirationTime('1h')
      .encode();
    expect(await tokens.verify(token)).toBeUndefined();
  });

  it('rejects a tampered payload', async () => {
    const { token } = await tokens.sign(user);
    const [header, , signature] = token.split('.');
    const forged = Buffer.from(JSON.stringify({ sub: 'someone-else', email: 'x@y.z' })).toString(
      'base64url',
    );
    expect(await tokens.verify(`${header ?? ''}.${forged}.${signature ?? ''}`)).toBeUndefined();
  });

  it('rejects a token without an email claim', async () => {
    const token = await new SignJWT({})
      .setProtectedHeader({ alg: 'HS256' })
      .setSubject(user.id)
      .setIssuer(settings.issuer)
      .setAudience(settings.audience)
      .setIssuedAt()
      .setExpirationTime('1h')
      .sign(new TextEncoder().encode(settings.secret));
    expect(await tokens.verify(token)).toBeUndefined();
  });

  it('rejects garbage', async () => {
    expect(await tokens.verify('not.a.jwt')).toBeUndefined();
  });
});
