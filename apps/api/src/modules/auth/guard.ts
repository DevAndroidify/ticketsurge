import type { FastifyInstance, FastifyRequest, preHandlerAsyncHookHandler } from 'fastify';
import { forbidden, unauthorized } from '../../errors.js';
import type { AuthUser, TokenService } from './tokens.js';

declare module 'fastify' {
  interface FastifyRequest {
    authUser: AuthUser | null;
  }
}

export interface AuthGuards {
  /** Requires a valid `Authorization: Bearer <token>` header; otherwise 401. */
  authenticate: preHandlerAsyncHookHandler;
  /** Requires a valid token whose email is in ADMIN_EMAILS; 401 without a token, 403 otherwise. */
  requireAdmin: preHandlerAsyncHookHandler;
}

export function registerAuth(
  app: FastifyInstance,
  tokens: TokenService,
  adminEmails: readonly string[],
): AuthGuards {
  app.decorateRequest('authUser', null);
  const admins = new Set(adminEmails.map((email) => email.toLowerCase()));

  const authenticate: preHandlerAsyncHookHandler = async (request) => {
    const header = request.headers.authorization;
    const match = header ? /^Bearer ([A-Za-z0-9._~+/=-]+)$/.exec(header) : null;
    if (!match?.[1]) throw unauthorized('Missing or malformed bearer token');
    const user = await tokens.verify(match[1]);
    if (!user) throw unauthorized('Invalid or expired token');
    request.authUser = user;
  };

  const requireAdmin: preHandlerAsyncHookHandler = async function (request, reply) {
    await authenticate.call(this, request, reply);
    if (!request.authUser || !admins.has(request.authUser.email)) {
      throw forbidden('NOT_ADMIN', 'Admin access required');
    }
  };

  return { authenticate, requireAdmin };
}

/** The authenticated user of a route protected by `authenticate`. */
export function currentUser(request: FastifyRequest): AuthUser {
  if (!request.authUser) throw unauthorized();
  return request.authUser;
}
