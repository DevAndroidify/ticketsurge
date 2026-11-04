// Resets a consumer group to the earliest offsets so its next start replays every retained message.
// Usage: npm run kafka:reset-offsets -- <consumer-group>     (stop that service first)
import { CONSUMER_GROUPS } from '@ticketsurge/events';
import { resetGroupToEarliest } from '@ticketsurge/kafka';
import { withAdmin } from '../lib/kafka-admin.js';

const group = process.argv[2];
const known = Object.values(CONSUMER_GROUPS) as string[];
if (!group || !known.includes(group)) {
  console.error(`usage: npm run kafka:reset-offsets -- <group>\ngroups: ${known.join(', ')}`);
  process.exit(1);
}

try {
  const result = await withAdmin((admin, prefix) =>
    resetGroupToEarliest(admin, `${prefix}${group}`),
  );
  console.log(
    result === 'reset'
      ? `${group}: reset to earliest; it replays all retained messages on its next start`
      : `${group}: no committed offsets; it already starts from the earliest offset`,
  );
} catch (error) {
  console.error((error as Error).message);
  process.exitCode = 1;
}
