import fs from "node:fs";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ResidentHost } from "../src/residency/host.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { LIVENESS_POLICY_KEY } from "../src/topology/host-leases.js";

const states: Array<{ root: string; host: ResidentHost }> = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const { root, host } of states.splice(0)) {
    await host.close(); fs.rmSync(root, { recursive: true, force: true });
  }
});
const fixture = async (files: boolean) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-registry-hold-wait-"));
  const config: ResidentHostConfig = {
    format: 1, rootId: "session:registry-probe", sessionId: "registry-probe", cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), sessionActorRoot: path.join(root, "session-actors"),
    residencyRoot: residentRoot(path.join(root, "mesh"), "session:registry-probe"), fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, nice: 19 }, mesh: DEFAULT_FABRIC_CONFIG.mesh,
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "unused", claudeBinary: "unused", vedaBinary: "unused",
  };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  const now = Date.now();
  const records = Array.from({ length: 40 }, (_, i) => ({
    id: (i + 1).toString(16).padStart(32, "0"), name: `idle-${i}`, instructions: "wait", createdAt: now, updatedAt: now,
    rootId: config.rootId, residency: "durable", runner: "pi", status: "idle", scope: i < 20 ? "project" : "session",
    events: [], topics: [], messages: [],
  }));
  for (const [actorRoot, rows] of [[config.actorRoot, records.slice(0, 20)], [config.sessionActorRoot!, records.slice(20)]] as const) {
    fs.mkdirSync(actorRoot, { recursive: true });
    fs.writeFileSync(path.join(actorRoot, "actors.json"), JSON.stringify({ format: 1, actors: rows }));
  }
  const host = new ResidentHost(config); states.push({ root, host });
  await host.start();
  if (files) await host.mesh.put({ key: LIVENESS_POLICY_KEY, value: { version: 1, hostLeases: "files", participants: "files" }, identity: host.identity });
  await host.participants.refresh();
  expect(host.actors.listOwned()).toHaveLength(40);
  return { host, config, records };
};
const holdMesh = async (meshRoot: string) => {
  const child = spawn(process.execPath, [path.resolve("tests/fixtures/hold-mesh-lock.mjs"), meshRoot, "3000"],
    { stdio: ["ignore", "pipe", "pipe"] });
  let stderr = "";
  child.stderr.on("data", chunk => { stderr += chunk; });
  const exited = new Promise<number | null>((resolve, reject) => { child.once("error", reject); child.once("close", resolve); });
  await new Promise<void>((resolve, reject) => {
    child.stdout.once("data", () => resolve()); child.once("error", reject);
    child.once("exit", () => reject(new Error(`holder exited before ready: ${stderr}`)));
  });
  return { exited };
};

describe("RC3.1 registry hold-and-wait regression (#4383)", () => {
  it.each([false, true])("releases registry custody during a 3 s external mesh hold (files=%s)", async files => {
    const { host, config, records } = await fixture(files);
    const { exited } = await holdMesh(config.meshRoot);
    let refresh: Promise<unknown> | undefined, mutation: Promise<unknown> | undefined;
    try {
      // RC3.1 has sequential per-key writes, not main's #531 presence batch.
      // Both writeBatch and the files-only confirm path must retain #504 custody.
      let entered!: () => void;
      const selected = new Promise<void>(resolve => { entered = resolve; });
      const checkCustody = () => {
        for (const root of [config.actorRoot, config.sessionActorRoot!])
          expect(fs.existsSync(path.join(root, "actors.json.lock", "owner"))).toBe(true);
        entered();
      };
      const batch = host.mesh.writeBatch.bind(host.mesh);
      vi.spyOn(host.mesh, "writeBatch").mockImplementationOnce(input => { checkCustody(); return batch(input); });
      const confirm = host.mesh.confirmWritable.bind(host.mesh);
      vi.spyOn(host.mesh, "confirmWritable").mockImplementationOnce(callback => { checkCustody(); return confirm(callback); });
      refresh = host.participants.refresh().catch(error => error);
      await selected;
      const registry = new ActorRegistryStore(config.actorRoot);
      const started = performance.now();
      mutation = registry.withLock(() => registry.write(registry.records().map(row => ({ ...row, marker: "concurrent mutation" }))));
      await mutation;
      const registryMutationMs = performance.now() - started;
      const setterStarted = performance.now();
      mutation = host.actors.setInstructions(records[0]!.id, "changed while mesh is busy");
      await mutation;
      const setterMs = performance.now() - setterStarted;
      process.stdout.write(JSON.stringify({ regression: "external-mesh-hold", files, registryMutationMs, setterMs }) + "\n");
      expect(registryMutationMs).toBeLessThan(1000);
      expect(setterMs).toBeLessThan(1000);
      expect(await refresh).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      expect(await exited).toBe(0);
      await host.participants.refresh();
      expect(new ActorRegistryStore(config.actorRoot).records()[0]).toMatchObject({ instructions: "changed while mesh is busy" });
    } finally { await exited; await refresh; await mutation; }
  }, 10000);

  it("unwinds registry custody when dead participant-key recovery finds a busy mesh", async () => {
    const { host, config, records } = await fixture(true);
    const keyLock = path.join(config.meshRoot, "participants", ".locks", createHash("sha256").update(records[0]!.id).digest("hex"));
    fs.mkdirSync(keyLock, { recursive: true });
    fs.writeFileSync(path.join(keyLock, "owner"), "2147483647\n\ndead-key\n");
    const { exited } = await holdMesh(config.meshRoot);
    try {
      const started = performance.now();
      await expect(host.participants.refresh()).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      await new ActorRegistryStore(config.actorRoot).withLock(() => undefined);
      expect(performance.now() - started).toBeLessThan(1000);
      expect(fs.existsSync(keyLock)).toBe(true);
      expect(await exited).toBe(0);
      await host.participants.refresh();
      expect(fs.existsSync(keyLock)).toBe(false);
    } finally { await exited; }
  }, 10000);
});
