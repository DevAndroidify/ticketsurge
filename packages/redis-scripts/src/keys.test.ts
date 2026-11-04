import { describe, expect, it } from 'vitest';
import { keys, parseSeatValue } from './keys.js';

describe('keys', () => {
  it('wraps the event ID in a hash tag so an event stays in one cluster slot', () => {
    expect(keys.seat('e1', 's1')).toBe('seat:{e1}:s1');
    expect(keys.hold('e1', 'h1')).toBe('hold:{e1}:h1');
    expect(keys.admit('e1', 'u1')).toBe('admit:{e1}:u1');
  });
});

describe('parseSeatValue', () => {
  it.each([
    ['hold:abc', { kind: 'hold', holdId: 'abc' }],
    ['sold:xyz', { kind: 'sold', bookingId: 'xyz' }],
    [null, undefined],
    ['garbage', undefined],
  ])('parses %j', (value, expected) => {
    expect(parseSeatValue(value)).toEqual(expected);
  });
});
