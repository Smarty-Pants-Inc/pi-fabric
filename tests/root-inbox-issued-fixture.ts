import { randomUUID } from "node:crypto";
import {
  confirmedRootInboxSession as confirmedSession,
  rootInboxMessage as message,
  rootInboxSession as session,
  sessionHoldsInboxBatch as holdsBatch,
} from "../src/topology/root-inbox.js";
import { rootInboxCarrierDigest, type RootInboxReceiptSnapshot, type RootInboxReceiptVerifier } from "../src/topology/root-inbox-shared.js";

// Test-owned issuer: readers never mint authority from arbitrary session history.
const issued = new Map<string, RootInboxReceiptSnapshot>();
export const registerIssuedInbox = (details: object, content?: unknown): void => {
  const fields = details as { ids: string[]; receipts?: string[]; nonce?: string };
  const snapshot = Object.freeze({
    ids: Object.freeze([...fields.ids]),
    receipts: Object.freeze([...(fields.receipts ?? [])]),
    digest: rootInboxCarrierDigest({ content }),
  });
  fields.nonce = randomUUID();
  issued.set(fields.nonce, snapshot);
};
export const verifyIssuedInbox: RootInboxReceiptVerifier = (details, carrier, accepted) => {
  if (!details || typeof details !== "object") return undefined;
  const nonce = (details as { nonce?: unknown }).nonce;
  if (typeof nonce !== "string" || accepted.has(nonce)) return undefined;
  const snapshot = issued.get(nonce);
  if (!snapshot || snapshot.digest !== rootInboxCarrierDigest(carrier)) return undefined;
  accepted.add(nonce);
  return snapshot;
};
export const rootInboxMessage = (...args: Parameters<typeof message>) => {
  const result = message(...args);
  registerIssuedInbox(result.details, result.content);
  return result;
};
export const rootInboxSession = (entries: readonly unknown[], lookback = 500) =>
  session(entries, lookback, verifyIssuedInbox);
export const confirmedRootInboxSession = (manager: Parameters<typeof confirmedSession>[0]) =>
  confirmedSession(manager, verifyIssuedInbox);
export const sessionHoldsInboxBatch = (entries: readonly unknown[], ids: readonly string[], lookback = 500) =>
  holdsBatch(entries, ids, lookback, verifyIssuedInbox);
