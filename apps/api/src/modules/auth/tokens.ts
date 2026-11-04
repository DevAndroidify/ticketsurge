import { SignJWT, jwtVerify, errors } from 'jose';

export interface AuthUser {
  id: string;
  email: string;
}

export interface TokenSettings {
  secret: string;
  issuer: string;
  audience: string;
  ttlSeconds: number;
}

/** Signs and verifies HS256 access tokens. Only HS256 is accepted, so `alg: none` is rejected. */
export class TokenService {
  private readonly key: Uint8Array;

  constructor(private readonly settings: TokenSettings) {
    this.key = new TextEncoder().encode(settings.secret);
  }

  async sign(
    user: AuthUser,
    now: Date = new Date(),
  ): Promise<{ token: string; expiresIn: number }> {
    const issuedAt = Math.floor(now.getTime() / 1000);
    const token = await new SignJWT({ email: user.email })
      .setProtectedHeader({ alg: 'HS256', typ: 'JWT' })
      .setSubject(user.id)
      .setIssuer(this.settings.issuer)
      .setAudience(this.settings.audience)
      .setIssuedAt(issuedAt)
      .setExpirationTime(issuedAt + this.settings.ttlSeconds)
      .sign(this.key);
    return { token, expiresIn: this.settings.ttlSeconds };
  }

  /** Returns the user, or undefined for any invalid, expired, or foreign token. */
  async verify(token: string): Promise<AuthUser | undefined> {
    try {
      const { payload } = await jwtVerify(token, this.key, {
        algorithms: ['HS256'],
        issuer: this.settings.issuer,
        audience: this.settings.audience,
        requiredClaims: ['sub', 'exp', 'iat'],
      });
      if (typeof payload.sub !== 'string' || typeof payload.email !== 'string') return undefined;
      return { id: payload.sub, email: payload.email };
    } catch (error) {
      if (error instanceof errors.JOSEError) return undefined;
      throw error;
    }
  }
}
