import { describe, expect, it } from 'vitest';
import { ticketCode } from './tickets.js';

describe('ticketCode', () => {
  it('has the expected readable format', () => {
    expect(ticketCode()).toMatch(
      /^TS-[0-9A-HJKMNP-TV-Z]{4}-[0-9A-HJKMNP-TV-Z]{4}-[0-9A-HJKMNP-TV-Z]{4}$/,
    );
  });

  it('does not repeat across many codes', () => {
    const codes = new Set(Array.from({ length: 10_000 }, () => ticketCode()));
    expect(codes.size).toBe(10_000);
  });
});
