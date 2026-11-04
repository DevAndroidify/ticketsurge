import type { FastifyInstance } from 'fastify';
import type pg from 'pg';
import { z } from 'zod';
import { PG_UNIQUE_VIOLATION, pgErrorInfo } from '../../db.js';
import { conflict, unauthorized } from '../../errors.js';
import { DUMMY_HASH_PROMISE, hashPassword, verifyPassword } from './passwords.js';
import type { TokenService } from './tokens.js';

const credentials = z.object({
  // Emails are case-insensitive in practice; store and compare them lowercased.
  email: z.string().trim().toLowerCase().pipe(z.email().max(254)),
  // Upper bound stops very long inputs from being used to burn CPU in scrypt.
  password: z.string().min(8).max(128),
});

export interface AuthRoutesOptions {
  db: pg.Pool;
  tokens: TokenService;
}

export function authRoutes(app: FastifyInstance, { db, tokens }: AuthRoutesOptions): void {
  app.post('/auth/register', async (request, reply) => {
    const { email, password } = credentials.parse(request.body);
    const passwordHash = await hashPassword(password);
    let id: string;
    try {
      const { rows } = await db.query<{ id: string }>(
        'INSERT INTO booking.users (email, password_hash) VALUES ($1, $2) RETURNING id',
        [email, passwordHash],
      );
      id = rows[0]?.id ?? '';
    } catch (error) {
      const info = pgErrorInfo(error);
      if (info.code === PG_UNIQUE_VIOLATION && info.constraint === 'users_email_unique') {
        throw conflict('EMAIL_TAKEN', 'An account with this email already exists');
      }
      throw error;
    }
    const { token, expiresIn } = await tokens.sign({ id, email });
    return reply.code(201).send({ user: { id, email }, accessToken: token, expiresIn });
  });

  app.post('/auth/login', async (request) => {
    // Login only checks length bounds, so older accounts still work if the policy changes.
    const { email, password } = credentials.parse(request.body);
    const { rows } = await db.query<{ id: string; password_hash: string }>(
      'SELECT id, password_hash FROM booking.users WHERE email = $1',
      [email],
    );
    const user = rows[0];
    // Always run one scrypt verification, so timing does not reveal unknown emails.
    const valid = await verifyPassword(password, user?.password_hash ?? (await DUMMY_HASH_PROMISE));
    if (!user || !valid) throw unauthorized('Invalid email or password');

    const { token, expiresIn } = await tokens.sign({ id: user.id, email });
    return { user: { id: user.id, email }, accessToken: token, expiresIn };
  });
}
