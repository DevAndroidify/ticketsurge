import { describe, expect, it } from 'vitest';
import { channelId, keys, parseSeatValue } from './keys.js';

describe('channelId', () => {
  it('parses seat and user channels and rejects anything else', () => {
    expect(channelId(keys.seatsChannel('e1'))).toEqual({ kind: 'seats', id: 'e1' });
    expect(channelId(keys.userChannel('u1'))).toEqual({ kind: 'user', id: 'u1' });
    expect(channelId('seats:e1')).toBeUndefined();
    expect(channelId('other:{x}')).toBeUndefined();
  });
});

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
