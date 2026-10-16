// E17 structural seam only, NOT a second validator or receipt writer.
// Rollback calls A1's BUILT writer. Source validation belongs to A1's public reader.
import fs from "node:fs";
import path from "node:path";
import { blocked, seenRoot, receiverOwner, type SeenRecord } from "./control.js";
export interface DrainReceipt {
  version: 1;
  hostId: string;
  flippedAt: number;
  completedAt: number;
  complete: boolean;
  consumer: { stream: string; numPending: number; numAckPending: number } | null;
  dispositions: Array<{ commandEventId: string; disposition: "relay" | "completed" }>;
}
export interface DrainApi {
  readNatsDrainReceipt(storeDir: string): DrainReceipt | undefined;
  writeNatsDrainReceipt(storeDir: string, receipt: DrainReceipt): void;
  natsDrainReceiptPath(storeDir: string): string;
}
export async function drainApi(kind: "source" | "dist" = "dist"): Promise<DrainApi> {
  const url = new URL(kind === "dist" ? "../../../dist/topology/nats-drain-receipt.js"
    : "../../../src/topology/nats-drain-receipt.ts", import.meta.url);
  blocked(fs.existsSync(url), `A1 E17 ${kind}/topology/nats-drain-receipt module is absent; parent must integrate/build the frozen public API`);
  const module = await import(url.href) as Partial<DrainApi>;
  for (const name of ["readNatsDrainReceipt", "writeNatsDrainReceipt", "natsDrainReceiptPath"] as const) {
    blocked(typeof module[name] === "function", `A1 E17 ${kind} module must export ${name}`);
  }
  return module as DrainApi;
}
/** Evidence comes from actual generated records; no fake writtenAt fallback. */
export function lastWrite(records: readonly SeenRecord[]): number {
  for (const record of records) blocked(typeof record.writtenAt === "number" && Number.isFinite(record.writtenAt),
    "A1 E17 every NATS claim/outcome write must set writtenAt");
  return Math.max(...records.map(record => record.writtenAt!));
}
export function receiptFor(records: readonly SeenRecord[], patch: Partial<DrainReceipt> = {}): DrainReceipt {
  const completedAt = Math.max(Date.now(), lastWrite(records));
  return { version: 1, hostId: receiverOwner.hostId, flippedAt: completedAt, completedAt, complete: true,
    consumer: { stream: "FABRIC_IN_machine-b", numPending: 0, numAckPending: 0 }, dispositions: [], ...patch };
}
export async function writeDrain(root: string, receipt: DrainReceipt): Promise<{ api: DrainApi; file: string }> {
  const api = await drainApi("dist"), directory = seenRoot(root, receipt.hostId);
  blocked(api.natsDrainReceiptPath(directory) === path.join(directory, "nats-drain-receipt.json"), "A1 E17 receipt pathname drift");
  api.writeNatsDrainReceipt(directory, receipt);
  return { api, file: api.natsDrainReceiptPath(directory) };
}
