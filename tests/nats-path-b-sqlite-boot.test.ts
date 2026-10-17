// Regression for NATS activation with SQLite mesh state: first open must finish
// before Main file custody, then the public Path B followUp/ACK path must work.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { Bucket } from "@nats-io/kv";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig, type FabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { NatsKvStore } from "../src/transport/nats/kv.js";
import { runTransportAdmin } from "../src/transport/nats/admin.js";
import { createPathBHarness, PATH_B_MACHINES, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1: SQLite boot regression requires NATS_BIN");
let broker: PathBHarness | undefined;

beforeAll(async () => {
  if (!binary) return;
  broker = await createPathBHarness({ binary, tls: {} });
  for (const [name, value] of Object.entries(broker.adminEnv(broker.users.bootstrap))) vi.stubEnv(name, value);
  try {
    expect(await runTransportAdmin(["provision", "--ca-file", broker.tls!.caFile, "--machines", PATH_B_MACHINES.join(",")])).toBe(0);
  } finally { vi.unstubAllEnvs(); }
}, 30_000);

beforeEach(() => {
  // Keep this boot test record-only for broker KV deletion paths.
  vi.spyOn(Bucket.prototype, "delete").mockImplementation(async () => { throw new Error("real broker delete forbidden in SQLite boot regression"); });
  vi.spyOn(Bucket.prototype, "purge").mockImplementation(async () => { throw new Error("real broker purge forbidden in SQLite boot regression"); });
  vi.spyOn(NatsKvStore.prototype, "delete").mockImplementation(async () => false);
});

afterAll(async () => { await broker?.stop(); vi.restoreAllMocks(); vi.unstubAllEnvs(); });

function configFor(root: string, machine: "machine-a" | "machine-b"): FabricConfig {
  const user = machine === "machine-a" ? broker!.users.machineA : broker!.users.machineB;
  return normalizeFabricConfig({
    fullCodeMode: false,
    mesh: { enabled: true, transport: "nats", hostTransport: "nats", root: path.join(root, "mesh"), stateBackend: "sqlite",
      nats: { ...broker!.runtimeOptions(user), operatorMachine: "fabric-ops" } },
    agents: { enabled: false }, residency: { enabled: false }, mcp: { enabled: false }, memory: { enabled: false },
    jev: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false },
  });
}

function main(root: string, sessionId: string, label: string) {
  const messages: Array<{ message: unknown; options: unknown }> = [];
  const pi = { on: () => () => {}, events: { emit() {}, on: () => () => {} }, getThinkingLevel: () => "off",
    getSessionName: () => label, appendEntry() {}, sendMessage(message: unknown, options: unknown) { messages.push({ message, options }); },
  } as unknown as ExtensionAPI;
  const context = { cwd: root, hasUI: false, mode: "rpc", isProjectTrusted: () => true,
    isIdle: () => true, hasPendingMessages: () => false,
    modelRegistry: { getAvailable: () => [], find: () => undefined, getApiKeyAndHeaders: async () => undefined },
    sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined, getBranch: () => [], getLeafId: () => null, getEntries: () => [] },
    ui: { setStatus() {}, notify() {} },
  } as unknown as ExtensionContext;
  const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
    extension: path.resolve("src/index.ts"), worker: path.join(root, "unused-worker.mjs"),
    residentHost: path.join(root, "unused-resident.mjs"), skills: root,
  } });
  const invoke = (ref: string, args: Record<string, unknown>) => runtime.registry.invoke(ref, args, {
    cwd: root, signal: undefined, parentToolCallId: "sqlite-boot", nestedToolCallId: ref, extensionContext: context,
    update() {}, approve: async () => {}, audits: [], maxResultChars: 10_000,
  });
  return { runtime, context, messages, invoke };
}

describe.skipIf(!binary || process.platform !== "linux")("NATS Path B SQLite activation", () => {
  it("boots actual SQLite state before custody and admits a public followUp", async () => {
    for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
    vi.stubEnv("SMARTY_AGENT", undefined);
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "nats-sqlite-boot-"));
    fs.mkdirSync(path.join(root, "agent"), { recursive: true });
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
    vi.stubEnv("PI_FABRIC_PROJECT_ROOT", root);
    const ownerSession = "sqlite-boot-owner-0000-4000-8000-000000000001";
    const senderSession = "sqlite-boot-sender-0000-4000-8000-000000000002";
    const owner = main(root, ownerSession, "sqlite-boot-owner");
    const sender = main(root, senderSession, "sqlite-boot-sender");
    try {
      await owner.runtime.initialize(owner.context, configFor(root, "machine-a"));
      await sender.runtime.initialize(sender.context, configFor(root, "machine-b"));
      expect(owner.runtime.mesh.stateBackend).toBe("sqlite");
      expect(sender.runtime.mesh.stateBackend).toBe("sqlite");
      expect(fs.existsSync(path.join(root, "mesh", "state.db"))).toBe(true);

      const receipt = await sender.invoke("agents.followUp", { id: `session:${ownerSession}`, message: "SQLITE_BOOT_ACCEPTED_CANARY" });
      expect(receipt).toMatchObject({ acknowledged: true });
      await vi.waitFor(() => expect(JSON.stringify(owner.messages)).toContain("SQLITE_BOOT_ACCEPTED_CANARY"), { timeout: 5_000 });
    } finally {
      await Promise.allSettled([owner.runtime.shutdown("exit"), sender.runtime.shutdown("exit")]);
      fs.rmSync(root, { recursive: true, force: true });
      vi.unstubAllEnvs();
    }
  }, 60_000);
});
