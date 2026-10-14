import { randomUUID } from "node:crypto";
import {
  confirmedRootInboxSession as confirmedSession,
  rootInboxMessage as message,
  rootInboxSession as session,
  sessionHoldsInboxBatch as holdsBatch,
} from "../src/topology/root-inbox.js";

// Test-owned issuer: readers never mint authority from arbitrary session history.
const issued = new Map<string, { readonly ids: readonly string[]; readonly receipts: readonly string[] }>();
export const registerIssuedInbox = (details: object): void => {
  const fields = details as { ids: string[]; receipts?: string[]; nonce?: string };
  const snapshot = Object.freeze({
    ids: Object.freeze([...fields.ids]),
    receipts: Object.freeze([...(fields.receipts ?? [])]),
  });
  fields.nonce = randomUUID();
  issued.set(fields.nonce, snapshot);
};
export const verifyIssuedInbox = (details: unknown) => {
  if (!details || typeof details !== "object") return undefined;
  const nonce = (details as { nonce?: unknown }).nonce;
  return typeof nonce === "string" ? issued.get(nonce) : undefined;
};
export const rootInboxMessage = (...args: Parameters<typeof message>) => {
  const result = message(...args);
  registerIssuedInbox(result.details);
  return result;
};
export const rootInboxSession = (entries: readonly unknown[], lookback = 500) =>
  session(entries, lookback, verifyIssuedInbox);
export const confirmedRootInboxSession = (manager: Parameters<typeof confirmedSession>[0]) =>
  confirmedSession(manager, verifyIssuedInbox);
export const sessionHoldsInboxBatch = (entries: readonly unknown[], ids: readonly string[], lookback = 500) =>
  holdsBatch(entries, ids, lookback, verifyIssuedInbox);
