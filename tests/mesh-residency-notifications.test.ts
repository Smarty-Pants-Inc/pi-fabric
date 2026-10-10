import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { RESIDENCY_NOTIFICATION_DIR, readResidencyNotification, residencyNotificationName } from "../src/mesh/residency-notifications.js";

const roots: string[] = [], stores: MeshStore[] = [];
afterEach(() => {
  for (const store of stores.splice(0)) store.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const writer: MeshIdentity = { id: "resident", name: "resident", kind: "main" };
describe("residency key notifications", () => {
  it.each(["file", "shadow", "sqlite"] as const)("signals committed delivery/claim keys only, including prepared batches on %s", async stateBackend => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "residency-key-signals-")); roots.push(root);
    const mesh = new MeshStore(root, 65_536, 1_000, { stateBackend }); stores.push(mesh);
    const directory = path.join(root, RESIDENCY_NOTIFICATION_DIR);
    const key = "residency/deliveries/root/item", claim = "residency/completion-claims/item";
    await mesh.put({ key: "topology/hosts/lease", identity: writer, value: { updatedAt: 1 } });
    expect(fs.existsSync(directory)).toBe(false);
    const initial = await mesh.put({ key, identity: writer, value: "pending" });
    const file = path.join(directory, residencyNotificationName(key)), signal = fs.readFileSync(file, "utf8");
    expect(readResidencyNotification(root, path.basename(file))).toBe(key);
    await mesh.writeBatch({ identity: writer, ops: [{ kind: "put", key: "topology/hosts/lease", value: { updatedAt: 2 } }] });
    expect(fs.readFileSync(file, "utf8")).toBe(signal);
    await expect(mesh.delete({ key, ifVersion: initial.version + 1 })).rejects.toThrow(/compare-and-swap/);
    expect(fs.readFileSync(file, "utf8")).toBe(signal);
    await mesh.writeBatch({ identity: writer, ops: [], prepare: () => [
      { kind: "put", key, value: "changed", ifVersion: initial.version }, { kind: "put", key: claim, value: "receipt" },
    ] });
    expect(fs.readFileSync(file, "utf8")).not.toBe(signal); expect(readResidencyNotification(root, residencyNotificationName(claim))).toBe(claim);
    const unchanged = fs.readFileSync(file, "utf8");
    await mesh.writeBatch({ identity: writer, ops: [{ kind: "delete", key, ifVersion: 0, onConflict: "skip" }] });
    expect(fs.readFileSync(file, "utf8")).toBe(unchanged);
    await expect(mesh.writeBatch({ identity: writer, ops: [{ kind: "put", key, value: "aborted", ifVersion: 0 }] })).rejects.toThrow();
    expect(fs.readFileSync(file, "utf8")).toBe(unchanged);
    await expect(mesh.writeBatch({ identity: writer, ops: [{ kind: "put", key, value: "committed" }],
      commitOutbox: () => { throw new Error("outbox failure"); } })).rejects.toThrow("outbox failure");
    expect(mesh.get(key, { fresh: true })?.value).toBe("committed"); expect(fs.readFileSync(file, "utf8")).not.toBe(unchanged);
    await mesh.writeBatch({ identity: writer, ops: [{ kind: "delete", key }, { kind: "delete", key: claim }] });
    expect(fs.readdirSync(directory)).toEqual([]);
  });
  it("does not trust malformed, misnamed or oversized advisory files", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "residency-key-hints-")); roots.push(root);
    const directory = path.join(root, RESIDENCY_NOTIFICATION_DIR); fs.mkdirSync(directory);
    const key = "residency/deliveries/root/item", filename = residencyNotificationName(key), file = path.join(directory, filename);
    for (const text of ["{", JSON.stringify({ key: "topology/hosts/lease" }), JSON.stringify({ key: "residency/deliveries/root/other" }), "x".repeat(2_049)]) {
      fs.writeFileSync(file, text); expect(readResidencyNotification(root, filename)).toBeUndefined();
    }
    expect(readResidencyNotification(root, "../state.json")).toBeUndefined();
  });
});
