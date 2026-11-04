import { z } from 'zod';

export const MAX_SEATS_PER_EVENT = 20_000;

export const eventIdParams = z.object({ id: z.uuid() });

export const listEventsQuery = z.object({
  limit: z.coerce.number().int().min(1).max(100).default(50),
  offset: z.coerce.number().int().min(0).default(0),
});

const sectionSchema = z.object({
  name: z.string().trim().min(1).max(50),
  priceMinor: z.number().int().min(0).max(100_000_000),
  rows: z.array(z.string().trim().min(1).max(10)).min(1).max(200),
  seatsPerRow: z.number().int().min(1).max(500),
});

/** Body of POST /admin/events: rectangular sections, each with its own price. */
export const createEventBody = z
  .object({
    name: z.string().trim().min(1).max(200),
    venue: z.string().trim().min(1).max(200),
    startsAt: z.iso.datetime({ offset: true }),
    saleStartsAt: z.iso.datetime({ offset: true }),
    status: z.enum(['DRAFT', 'PUBLISHED']).default('PUBLISHED'),
    sections: z.array(sectionSchema).min(1).max(100),
  })
  .superRefine((body, ctx) => {
    if (Date.parse(body.saleStartsAt) > Date.parse(body.startsAt)) {
      ctx.addIssue({
        code: 'custom',
        path: ['saleStartsAt'],
        message: 'must not be after startsAt',
      });
    }
    const names = new Set<string>();
    let total = 0;
    body.sections.forEach((section, index) => {
      if (names.has(section.name)) {
        ctx.addIssue({ code: 'custom', path: ['sections', index, 'name'], message: 'duplicate' });
      }
      names.add(section.name);
      if (new Set(section.rows).size !== section.rows.length) {
        ctx.addIssue({
          code: 'custom',
          path: ['sections', index, 'rows'],
          message: 'duplicate row',
        });
      }
      total += section.rows.length * section.seatsPerRow;
    });
    if (total > MAX_SEATS_PER_EVENT) {
      ctx.addIssue({
        code: 'custom',
        path: ['sections'],
        message: `at most ${MAX_SEATS_PER_EVENT} seats per event`,
      });
    }
  });

export type CreateEventInput = z.infer<typeof createEventBody>;
