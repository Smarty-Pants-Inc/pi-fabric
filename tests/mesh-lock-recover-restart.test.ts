import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { acquireMeshCustodyLock } from "../src/mesh/custody-lock.js";
import { recoverMeshLock } from "../src/mesh/lock-recover.js";
import { localFilesystemTestHooks, pidNamespaceTestHooks } from "../src/core/pid-namespace.js";

// Data-safety/operator boundary: retain every real probe directory, and never call release/delete cleanup.
const roots: string[] = [];
const root = () => { const directory = fs.mkdtempSync(path.join(os.tmpdir(), "operator-stale-restart-")); roots.push(directory); return directory; };
afterAll(() => {
  if (process.env.STALELOCK_TEST_TRACE_DIR) for (const directory of roots) {
    const entries: Record<string, string> = {};
    const snapshot = (relative: string): void => {
      for (const entry of fs.readdirSync(path.join(directory, relative), { withFileTypes: true })) {
        const name = path.join(relative, entry.name);
        if (entry.isDirectory()) { entries[`${name}/`] = "directory"; snapshot(name); }
        else entries[name] = fs.readFileSync(path.join(directory, name)).toString("base64");
      }
    };
    snapshot("");
    fs.writeFileSync(path.join(process.env.STALELOCK_TEST_TRACE_DIR, `${path.basename(directory)}.trace.json`), JSON.stringify({ root: directory, encoding: "base64", entries }));
  }
});
beforeEach(() => { vi.spyOn(fs, "rmSync").mockImplementation(() => {}); });
afterEach(() => { vi.restoreAllMocks(); localFilesystemTestHooks.reset(); pidNamespaceTestHooks.reset(); });
const productionOwner = async (): Promise<string> => {
  const directory = root(); await acquireMeshCustodyLock(directory, 1_000);
  return fs.readFileSync(path.join(directory, "custody.lock", "owner"), "utf8");
};
const deadPid = (): number => {
  const pid = Number(fs.readFileSync("/proc/sys/kernel/pid_max", "utf8").trim()) + 1;
  expect(() => process.kill(pid, 0)).toThrow(expect.objectContaining({ code: "ESRCH" })); return pid;
};
const mutate = (owner: string, reason: string): string => {
  const fields = owner.split("\n");
  if (reason === "other-boot") {
    fields[5] = fields[5] === "ffffffff-ffff-ffff-ffff-ffffffffffff" ? "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee" : "ffffffff-ffff-ffff-ffff-ffffffffffff";
    const ns = fields[6]!.split(":"); ns[2] = fields[5]; fields[6] = ns.join(":");
  } else if (reason === "other-namespace") {
    const ns = fields[6]!.split(":"); ns[3] = String(BigInt(ns[3]!) + 1n); fields[6] = ns.join(":");
  } else if (reason === "dead-pid") fields[1] = String(deadPid());
  else fields[3] = String(BigInt(fields[3]!) + 1n);
  return fields.join("\n");
};
const plant = (directory: string, kind: "mesh" | "custody", owner: string): string => {
  const lock = path.join(directory, kind === "mesh" ? ".lock" : "custody.lock");
  fs.mkdirSync(lock); fs.writeFileSync(path.join(lock, "owner"), owner); return lock;
};

describe.skipIf(process.platform !== "linux")("operator recovery accepts restart-aware owners", () => {
  it.each(["mesh", "custody"] as const)("safely recovers every complete new stale %s owner on overlay, with audit and retained evidence", async kind => {
    const source = await productionOwner(); localFilesystemTestHooks.set(0x794c7630);
    for (const reason of ["other-boot", "other-namespace", "dead-pid", "reused-pid"]) {
      const directory = root(), owner = mutate(source, reason), lock = plant(directory, kind, owner);
      const result = await recoverMeshLock(directory, { lock: kind, holder: owner.split("\n")[0]! });
      expect(result).toMatchObject({ recovered: true, mode: "safe", lock, holderPid: Number(owner.split("\n")[1]) });
      expect(result.warning).toBeUndefined(); expect(fs.existsSync(lock)).toBe(false);
      expect(result.fence).toMatch(/\.dead\.\d{4}-\d\d-\d\dT.+\.[a-f0-9]{64}$/);
      expect(fs.readFileSync(path.join(result.fence, "owner"), "utf8")).toBe(owner);
      expect(JSON.parse(fs.readFileSync(`${result.fence}.recovery.json`, "utf8"))).toMatchObject({
        kind: "stale-lock-observation", source: lock, aside: result.fence, reason, owner,
      });
      expect(JSON.parse(fs.readFileSync(path.join(directory, "lock-recoveries.jsonl"), "utf8"))).toMatchObject({ mode: "safe", holder: owner.split("\n")[0] });
    }
  });

  it.each(["mesh", "custody"] as const)("the safe operator form never steals a live %s holder", async kind => {
    const directory = root(), owner = await productionOwner(), lock = plant(directory, kind, owner);
    await expect(recoverMeshLock(directory, { lock: kind, holder: owner.split("\n")[0]! })).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_RECOVER_REFUSED" });
    await expect(recoverMeshLock(directory, { lock: kind, holder: owner.split("\n")[0]!, unsafeHolderStopped: true }))
      .rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_RECOVER_REFUSED" });
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(owner);
    expect(fs.existsSync(path.join(directory, "lock-recoveries.jsonl"))).toBe(false);
  });

  it("recognizes a production-written unknown new owner and permits the audited unsafe override", async () => {
    pidNamespaceTestHooks.set(undefined);
    const source = await productionOwner();
    expect(source.split("\n")).toHaveLength(9); expect(source.split("\n")[6]).toBe("");
    const directory = root(), lock = plant(directory, "mesh", source), holder = source.split("\n")[0]!;
    await expect(recoverMeshLock(directory, { lock: "mesh", holder })).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_RECOVER_REFUSED" });
    const result = await recoverMeshLock(directory, { lock: "mesh", holder, unsafeHolderStopped: true });
    expect(result.mode).toBe("unsafe"); expect(result.warning).toMatch(/UNSAFE override/);
    expect(fs.existsSync(lock)).toBe(false);
    expect(fs.readFileSync(path.join(result.fence, "owner"), "utf8")).toBe(source);
    expect(JSON.parse(fs.readFileSync(`${result.fence}.recovery.json`, "utf8"))).toMatchObject({ reason: "operator-stopped", owner: source });
  });

  it("safe degraded recovery uses the same dated fence and a truthful dead-pid observation", async () => {
    const fields = (await productionOwner()).split("\n"); fields[3] = ""; fields[1] = String(deadPid());
    const owner = fields.join("\n"), directory = root(), lock = plant(directory, "mesh", owner);
    localFilesystemTestHooks.set(0xef53); // Preserve the incomplete/legacy namespace policy's local-root requirement.
    const result = await recoverMeshLock(directory, { lock: "mesh", holder: fields[0]! });
    expect(result.mode).toBe("safe"); expect(fs.existsSync(lock)).toBe(false);
    expect(fs.readFileSync(path.join(result.fence, "owner"), "utf8")).toBe(owner);
    expect(JSON.parse(fs.readFileSync(`${result.fence}.recovery.json`, "utf8"))).toMatchObject({ reason: "dead-pid", owner });
  });

  it("requires the exact holder token for a complete stale new owner", async () => {
    const directory = root(), owner = mutate(await productionOwner(), "dead-pid"), lock = plant(directory, "mesh", owner);
    await expect(recoverMeshLock(directory, { lock: "mesh", holder: "wrong" })).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_RECOVER_REFUSED" });
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(owner);
    expect(fs.existsSync(path.join(directory, "lock-recoveries.jsonl"))).toBe(false);
  });
});
