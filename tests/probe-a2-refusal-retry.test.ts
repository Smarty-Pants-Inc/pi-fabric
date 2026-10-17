import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { MainAgentController } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { armLateDirectoryReadFault } from "./fixtures/a2-probe-cli-provider.js";

// Keep: the A2 trust boundary is the late directory lookup immediately before control publication.
// This in-process mesh reaches that refusal without a live-host race, then proves explicit same-key
// retry and owner-side command deduplication. The post-install script covers the two-process path.
const roots: string[] = [];
const stores: MeshStore[] = [];
const directories: ParticipantDirectory[] = [];
const planes: FabricControlPlane[] = [];
const mains: MainAgentController[] = [];
const who = (id: string): MeshIdentity => ({ id, name: id, kind: "main" });
type RouterPorts = ConstructorParameters<typeof AgentMessageRouter>;
const unknownManager = { status: (id: string) => { throw new Error(`Unknown Fabric agent: ${id}`); } } as unknown as RouterPorts[0];
const actors = (identity: MeshIdentity) => ({
  identity,
  status: (id: string) => { throw new Error(`Unknown Fabric actor: ${id}`); },
  validateDirectMessage: () => undefined,
} as unknown as RouterPorts[1]);

const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-a2-probe-")); roots.push(root);
  const meshRoot = path.join(root, "mesh");
  const senderId = "session:a2-sender", receiverId = "session:a2-receiver";
  const mesh = () => { const store = new MeshStore(meshRoot, 65_536, 1_000); stores.push(store); return store; };
  const senderDirectory = new ParticipantDirectory(mesh(), {
    enabled: true, hostId: senderId, rootId: senderId, identity: who(senderId),
    heartbeatMs: 60_000, leaseMs: 120_000, reapDeadHosts: false,
  });
  const receiverDirectory = new ParticipantDirectory(mesh(), {
    enabled: true, hostId: receiverId, rootId: receiverId, identity: who(receiverId),
    heartbeatMs: 60_000, leaseMs: 120_000, reapDeadHosts: false,
  });
  directories.push(senderDirectory, receiverDirectory);
  const receiverIncarnation = receiverDirectory.ownerIncarnation;
  if (!receiverIncarnation) throw new Error("receiver directory did not allocate an incarnation");
  const presence: FabricParticipantRecord = {
    format: 1, id: receiverId, rootId: receiverId, kind: "root",
    ownerHostId: receiverId, ownerIdentityId: receiverId,
    ownerIncarnation: receiverIncarnation,
    name: "a2-probe-receiver", status: "running", runner: "pi", transport: "host",
    capabilities: ["followUp", "steer"], controlProtocol: "v1", startedAt: 1, updatedAt: Date.now(),
  };
  receiverDirectory.registerSource(() => [presence]);
  await receiverDirectory.refresh();
  await senderDirectory.refresh();
  expect(senderDirectory.get(receiverId, undefined, { fresh: true })).toMatchObject({ id: receiverId, stale: false });

  const main = new MainAgentController({
    on: () => () => {}, sendMessage: vi.fn(), getThinkingLevel: () => "off",
  } as unknown as ExtensionAPI, receiverId, true, root, "a2-probe-receiver");
  mains.push(main);
  main.attachFollowUpDrain({
    isIdle: () => false,
    hasPendingMessages: () => false,
    sessionManager: { getEntries: () => [] },
  } as never, 60_000, path.join(root, "receiver-followups.json"));
  const receiver = new FabricControlPlane(mesh(), who(receiverId), {
    enabled: true, hostId: receiverId, ownerIncarnation: receiverIncarnation,
    pollMs: 5, acknowledgementTimeoutMs: 1_000,
  });
  const sender = new FabricControlPlane(mesh(), who(senderId), {
    enabled: true, hostId: senderId, pollMs: 5, acknowledgementTimeoutMs: 1_000,
  });
  planes.push(receiver, sender);
  const receiverRouter = new AgentMessageRouter(unknownManager, actors(who(receiverId)), main, receiverDirectory, receiver, binding => binding);
  const accept = vi.fn((...args: Parameters<AgentMessageRouter["acceptControl"]>) => receiverRouter.acceptControl(...args));
  receiver.start(accept);
  sender.start(() => ({ accepted: false }));
  const senderMain = { id: senderId, local: true, matches: (id: string) => id === senderId } as unknown as RouterPorts[2];
  const route = new AgentMessageRouter(unknownManager, actors(who(senderId)), senderMain, senderDirectory, sender, binding => binding);
  const commands = () => sender.mesh.read({ topic: "fabric.control.command", limit: 100 })
    .filter(event => event.kind === "followUp" && (event.data as { message?: string } | undefined)?.message?.includes("a2probe-") === true);
  return { root, senderDirectory, route, sender, receiver, accept, commands, receiverId, receiverDirectory, presence };
};

afterEach(async () => {
  await Promise.all(planes.splice(0).map(plane => plane.close()));
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  for (const main of mains.splice(0)) main.closeFollowUpDrain();
  for (const store of stores.splice(0)) store.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  vi.restoreAllMocks();
});

// Keep: installed-release provenance is a trust boundary. Exercise the public verifier against
// real nested artifacts and the CLI's fail-closed receipt without starting live Pi processes.
const probeScript = fileURLToPath(new URL("../scripts/probe-a2-refusal-retry.mjs", import.meta.url));
const releaseSha = "a".repeat(40);
const releaseFixture = () => {
  const release = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-a2-release-")); roots.push(release);
  const sources = {
    "dist/index.js": "export const entry = true;\n",
    "dist/chunks/nested/optional.js": "export const optional = true;\n",
  };
  for (const [relative, source] of Object.entries(sources)) {
    const file = path.join(release, relative);
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, source);
  }
  fs.writeFileSync(path.join(release, "package.json"), JSON.stringify({ name: "pi-fabric", type: "module" }));
  const metadata = {
    commit: releaseSha,
    files: Object.fromEntries(Object.entries(sources).map(([file, source]) => [file, createHash("sha256").update(source).digest("hex")])),
  };
  const writeMetadata = (value: unknown = metadata) => fs.writeFileSync(path.join(release, "RELEASE.json"), JSON.stringify(value));
  writeMetadata();
  return { release, metadata, writeMetadata, chunk: path.join(release, "dist/chunks/nested/optional.js") };
};
const probeModule = () => import(`../scripts/${"probe-a2-refusal-retry"}.mjs`);

describe("A2 installed release provenance", () => {
  it("requires an explicit --release before starting Pi and prints only a FAIL receipt", () => {
    const result = spawnSync(process.execPath, [probeScript, "--release-sha", releaseSha, "--pi", path.join(os.tmpdir(), "a2probe-no-pi")], {
      encoding: "utf8", timeout: 3_000,
    });
    expect(result.error).toBeUndefined();
    expect(result.status).toBe(1);
    expect(result.stderr).toMatch(/--release\b/);
    expect(result.stdout.trim().split("\n")).toHaveLength(1);
    expect(result.stdout).toMatch(/^A2PROBE .* FAIL \d+ms\n$/);
    expect(result.stdout).not.toContain(" PASS ");
  });

  it("accepts matching commit metadata and the complete recursive JS manifest", async () => {
    const { verifyRelease } = await probeModule();
    const f = releaseFixture();
    fs.writeFileSync(path.join(f.release, "dist/chunks/nested/optional.js.map"), "not JavaScript");
    expect(verifyRelease(f.release, releaseSha)).toMatchObject({ identity: "release-json" });
  });

  it("refuses absent metadata outside the installer's content-addressed releases path", async () => {
    const { verifyRelease } = await probeModule();
    const f = releaseFixture();
    fs.unlinkSync(path.join(f.release, "RELEASE.json"));
    expect(() => verifyRelease(f.release, releaseSha)).toThrow();
  });

  it("accepts legacy installs only through their real releases/<sha> path", async () => {
    const { verifyRelease } = await probeModule();
    const f = releaseFixture();
    fs.unlinkSync(path.join(f.release, "RELEASE.json"));
    const installed = path.join(f.release, "releases", releaseSha);
    fs.mkdirSync(installed, { recursive: true });
    fs.renameSync(path.join(f.release, "dist"), path.join(installed, "dist"));
    fs.renameSync(path.join(f.release, "package.json"), path.join(installed, "package.json"));
    expect(verifyRelease(installed, releaseSha)).toMatchObject({ identity: "release-path", releaseSha });
    expect(() => verifyRelease(installed, "b".repeat(40))).toThrow(/path mismatch/);
    const alias = path.join(f.release, "current");
    fs.symlinkSync(installed, alias, "junction");
    expect(verifyRelease(alias, releaseSha)).toMatchObject({ identity: "release-path" });
    // Even at an accepted path, present but mismatched metadata cannot fall back to the path.
    fs.writeFileSync(path.join(installed, "RELEASE.json"), JSON.stringify({ ...f.metadata, commit: "b".repeat(40) }));
    expect(() => verifyRelease(alias, releaseSha)).toThrow(/SHA mismatch/);
  });

  it("never trusts a symlink's releases/<sha> spelling over its real path", async () => {
    const { verifyRelease } = await probeModule();
    const f = releaseFixture();
    fs.unlinkSync(path.join(f.release, "RELEASE.json"));
    const alias = path.join(f.release, "releases", releaseSha);
    fs.mkdirSync(path.dirname(alias));
    fs.symlinkSync(f.release, alias, "junction");
    expect(() => verifyRelease(alias, releaseSha)).toThrow(/path mismatch/);
  });

  it("refuses metadata for another commit even when every artifact hash matches", async () => {
    const { verifyRelease } = await probeModule();
    const f = releaseFixture();
    f.writeMetadata({ ...f.metadata, commit: "b".repeat(40) });
    expect(() => verifyRelease(f.release, releaseSha)).toThrow();
  });

  it.each([
    ["missing commit", { files: {} }],
    ["missing files", { commit: releaseSha }],
    ["null files", { commit: releaseSha, files: null }],
    ["array files", { commit: releaseSha, files: [] }],
  ])("refuses malformed metadata: %s", async (_name, metadata) => {
    const { verifyRelease } = await probeModule();
    const f = releaseFixture();
    f.writeMetadata(metadata);
    expect(() => verifyRelease(f.release, releaseSha)).toThrow();
  });

  it("refuses an altered nested dist chunk while the entrypoint is unchanged", async () => {
    const { verifyRelease } = await probeModule();
    const f = releaseFixture();
    fs.appendFileSync(f.chunk, "// changed after packaging\n");
    expect(() => verifyRelease(f.release, releaseSha)).toThrow();
  });

  it.each(["unlisted artifact", "missing artifact", "unlisted manifest entry"])("requires an exact recursive JS manifest: %s", async defect => {
    const { verifyRelease } = await probeModule();
    const f = releaseFixture();
    if (defect === "unlisted artifact") fs.writeFileSync(path.join(f.release, "dist/chunks/nested/extra.js"), "export {};\n");
    else if (defect === "missing artifact") fs.unlinkSync(f.chunk);
    else f.writeMetadata({ ...f.metadata, files: { ...f.metadata.files, "dist/absent.js": "0".repeat(64) } });
    expect(() => verifyRelease(f.release, releaseSha)).toThrow();
  });

  it("turns an otherwise passing result into FAIL when final release revalidation fails", async () => {
    const { verifyRelease, finalVerdict } = await probeModule();
    const f = releaseFixture();
    expect(() => verifyRelease(f.release, releaseSha)).not.toThrow();
    expect(finalVerdict(undefined)).toBe("PASS");
    fs.appendFileSync(f.chunk, "// replaced while the probe was running\n");
    let failure: unknown;
    try { verifyRelease(f.release, releaseSha); } catch (error) { failure = error; }
    expect(failure).toBeInstanceOf(Error);
    expect(finalVerdict(failure)).toBe("FAIL");
  });

  it.each(["probe residue remains", "scratch removal failed"])("does not preserve provisional PASS after cleanup failure: %s", async reason => {
    const { finalVerdict } = await probeModule();
    expect(finalVerdict(undefined)).toBe("PASS");
    expect(finalVerdict(new Error(reason))).toBe("FAIL");
  });
});

describe("A2 late directory refusal and same-key retry", () => {
  it("publishes no refusal attempt, then delivers one receiver message across two same-key retries", async () => {
    const f = await fixture();
    const key = "a2probe-test-20261017T000000Z";
    const message = `A2PROBE ${key}`;
    const refresh = vi.spyOn(f.senderDirectory, "refreshRoutingView");
    const fault = armLateDirectoryReadFault(path.join(f.root, "mesh"));
    try {
      await expect(f.route.routeMessage(f.receiverId, message, undefined, "followUp", undefined, { idempotencyKey: key }))
        .rejects.toMatchObject({
          name: "FabricDirectoryUnavailableError",
          code: "FABRIC_DIRECTORY_UNAVAILABLE",
          retryable: true,
          cause: { code: "EIO", message: "A2PROBE one-shot late directory read" },
        });
    } finally { fault.restore(); }
    expect(fault.injections).toBe(1);
    expect(refresh).not.toHaveBeenCalled();
    expect(f.commands()).toHaveLength(0);

    await expect(f.route.routeMessage(f.receiverId, message, undefined, "followUp", undefined, { idempotencyKey: key }))
      .resolves.toMatchObject({ queued: true, acknowledged: true, routed: "mesh" });
    expect(f.commands()).toHaveLength(1);
    expect(f.accept).toHaveBeenCalledTimes(1);

    await expect(f.route.routeMessage(f.receiverId, message, undefined, "followUp", undefined, { idempotencyKey: key }))
      .resolves.toMatchObject({ queued: true, acknowledged: true, routed: "mesh" });
    expect(f.accept).toHaveBeenCalledTimes(1);
    expect(fault.injections).toBe(1);
    expect(refresh).not.toHaveBeenCalled();
    expect(new Set(f.commands().map(event => (event.data as { commandId: string }).commandId)).size).toBe(1);
  });
});
