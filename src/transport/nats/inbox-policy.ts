export const DEFAULT_INBOX_CONSUMER_INACTIVE_MS = 7 * 24 * 60 * 60 * 1_000;
export const MIN_INBOX_CONSUMER_INACTIVE_MS = 60 * 1_000;
export const MAX_INBOX_CONSUMER_INACTIVE_MS = 90 * 24 * 60 * 60 * 1_000;

/** Runtime durables must expire, with an exact millisecond-to-nanosecond conversion. */
export const isValidInboxConsumerInactiveMs = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) &&
  value >= MIN_INBOX_CONSUMER_INACTIVE_MS && value <= MAX_INBOX_CONSUMER_INACTIVE_MS &&
  Number.isSafeInteger(value * 1_000_000);

export const DEFAULT_INBOX_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1_000;
export const MIN_INBOX_MAX_AGE_MS = 60 * 1_000;
export const MAX_INBOX_MAX_AGE_MS = 90 * 24 * 60 * 60 * 1_000;

/** Keep max_age finite and its millisecond-to-nanosecond conversion exact. */
export const isValidInboxMaxAgeMs = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) &&
  value >= MIN_INBOX_MAX_AGE_MS && value <= MAX_INBOX_MAX_AGE_MS &&
  Number.isSafeInteger(value * 1_000_000);
