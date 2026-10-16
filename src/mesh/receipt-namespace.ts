import fs from "node:fs";
import { isNamespaceRace, namespaceBarrierAvailable, namespaceKey, syncPathNamespace } from "../core/atomic-write.js";

/**
 * A checked preparation of the `event-receipts` namespace, made BEFORE `.lock` (fv2-coldpub C).
 * The complete reopenable namespace (every ancestor and symlink parent through the root) was
 * fsynced while `key` named it, so its entries are durable while the key still matches. It is
 * NOT a file or leaf receipt: every installed or unlinked receipt file still owes a fresh file
 * fsync and a fresh leaf-directory fsync. A mismatched key (recreated or retargeted directory,
 * replaced ancestor, changed symlink) uses the complete existing barrier instead.
 * Same-boot invariant: cooperating mesh writers never remove or relocate `event-receipts`,
 * the mesh root, or its ancestors; they only create, replace or unlink receipt files in it.
 */
export interface ReceiptNamespace { directory: string; key: string }

/** Idempotent. Fails closed (throws) on an I/O failure, so no append follows a failed preparation.
 * A namespace race or an unsupported platform yields no preparation: the full barrier applies. */
export const prepareReceiptNamespace = (directory: string): ReceiptNamespace | undefined => {
  if (!namespaceBarrierAvailable()) return undefined;
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  try {
    const key = namespaceKey(directory);
    syncPathNamespace(directory);
    return namespaceKey(directory) === key ? { directory, key } : undefined;
  } catch (error) { if (isNamespaceRace(error)) return undefined; throw error; }
};

const unchanged = (prepared: ReceiptNamespace | undefined, directory: string): prepared is ReceiptNamespace => {
  if (!prepared || prepared.directory !== directory) return false;
  // Any lookup failure denies the fast path; the complete barrier then reports it.
  try { return namespaceKey(directory) === prepared.key; } catch { return false; }
};

/** Fresh fsync of the leaf directory itself, then a recheck that the prepared namespace still
 * names it. False means the caller must run the complete barrier. */
const syncLeaf = (prepared: ReceiptNamespace): boolean => {
  const fd = fs.openSync(prepared.directory, fs.constants.O_RDONLY);
  try {
    const opened = fs.fstatSync(fd), named = fs.statSync(prepared.directory);
    if (!opened.isDirectory() || opened.dev !== named.dev || opened.ino !== named.ino) return false;
    fs.fsyncSync(fd);
  } finally { fs.closeSync(fd); }
  return unchanged(prepared, prepared.directory);
};

/**
 * Directory barrier after a receipt-file rename or unlink in `directory`: a fresh leaf fsync when
 * the prepared namespace still matches before and after it, else the complete existing barrier
 * (whose I/O failures and namespace races propagate unchanged). With no preparation this is
 * exactly syncPathNamespace, the barrier these callers used before.
 */
export const receiptDirectoryBarrier = (prepared: ReceiptNamespace | undefined) => (directory: string): void => {
  if (!unchanged(prepared, directory) || !syncLeaf(prepared)) syncPathNamespace(directory);
};
