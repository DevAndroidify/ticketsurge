import { describe, expect, it } from 'vitest';
import { MAX_SEATS_PER_EVENT, createEventBody } from './schemas.js';

const valid = {
  name: 'Show',
  venue: 'Hall',
  startsAt: '2030-01-01T20:00:00Z',
  saleStartsAt: '2029-12-01T10:00:00+01:00',
  sections: [{ name: 'A', priceMinor: 100, rows: ['1', '2'], seatsPerRow: 5 }],
};

describe('createEventBody', () => {
  it('accepts a valid body and defaults status to PUBLISHED', () => {
    expect(createEventBody.parse(valid).status).toBe('PUBLISHED');
  });

  it.each([
    ['sale after show', { saleStartsAt: '2030-02-01T00:00:00Z' }],
    ['negative price', { sections: [{ ...valid.sections[0], priceMinor: -1 }] }],
    ['fractional price', { sections: [{ ...valid.sections[0], priceMinor: 1.5 }] }],
    ['duplicate section', { sections: [valid.sections[0], valid.sections[0]] }],
    ['no sections', { sections: [] }],
    ['timestamp without zone', { startsAt: '2030-01-01T20:00:00' }],
    [
      'too many seats',
      {
        sections: [
          {
            name: 'Huge',
            priceMinor: 1,
            rows: Array.from({ length: 200 }, (_, i) => `R${i}`),
            seatsPerRow: 500,
          },
        ],
      },
    ],
  ])('rejects %s', (_label, patch) => {
    expect(createEventBody.safeParse({ ...valid, ...patch }).success).toBe(false);
  });

  it('caps an event at the configured seat limit', () => {
    expect(MAX_SEATS_PER_EVENT).toBe(20_000);
  });
});
