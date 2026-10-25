import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { MIRROR_FRESH_MS, writeHostLease } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

// Keep reason: smarty-dev#16723's recorded cross-repository return address and the
// directory's lease/collision trust boundary. Real provider, resolver and on-disk directory;
// no workers, native deletion or live fleet state. All fixtures stay in the isolated TMPDIR.
// File-based launch authority requires POSIX ownership; explicit env metadata works everywhere.
describe.skipIf(process.getuid === undefined)("#16723 projectAgent launch binding", () => {
  const roots: string[] = [];
  const directories: ParticipantDirectory[] = [];
  const providers: AgentsProvider[] = [];
  afterEach(async () => {
    await Promise.all(providers.splice(0).map(provider => provider.close()));
    await Promise.all(directories.splice(0).map(directory => directory.close()));
    vi.unstubAllEnvs();
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  });
  const leadId = "session:11111111-1111-4111-8111-111111111111";
  const otherId = "session:22222222-2222-4222-8222-222222222222";
  const repository = "code.example/team/lane";
  const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
  const fixture = async (binding: "marker" | "env" | "none" = "marker", remote = false) => {
    vi.stubEnv("SMARTY_LEAD_SESSION", binding === "env" ? leadId : "");
    vi.stubEnv("PI_FABRIC_PROJECT", "");
    const lane = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-launch-lead-"));
    roots.push(lane);
    execFileSync("git", ["init", "-q", lane], { stdio: "ignore" });
    execFileSync("git", ["-C", lane, "remote", "add", "origin", `https://${repository}.git`], { stdio: "ignore" });
    const cwd = path.join(lane, "sub");
    fs.mkdirSync(cwd);
    if (binding === "marker") {
      fs.mkdirSync(path.join(lane, ".local"), { mode: 0o700 });
      fs.writeFileSync(path.join(lane, ".local", "lead"), `${leadId}\n`);
    }
    const mesh = new MeshStore(path.join(lane, "mesh"), 64 * 1024, 1_000);
    const identity: MeshIdentity = { id: "session:observer", name: "main", kind: "main" };
    const directory = new ParticipantDirectory(mesh, {
      enabled: true, hostId: identity.id, rootId: identity.id, identity, reapDeadHosts: false,
    });
    directories.push(directory);
    const agents = new AgentManager(cwd, DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(lane, "runs") });
    // This test only discovers roots; delivery is deliberately not implemented.
    const mainAgent: FabricMainAgentTarget = {
      id: identity.id, local: true, matches: id => id === identity.id,
      info: () => { throw new Error("unexpected Main info lookup"); },
      deliverAgent: () => { throw new Error("unexpected delivery"); },
    };
    const actors = new ActorManager("observer", identity, mesh, DEFAULT_FABRIC_CONFIG.mesh, agents, () => {}, {
      actorRoot: path.join(lane, "actors"), mainAgent, rootId: identity.id,
    });
    const lifecycle = new LifecycleBroker(mesh, identity, directory,
      { enabled: false, pollMs: 20, maxReadEvents: 100 }, async () => {});
    const provider = new AgentsProvider(agents, actors, new GlobalActorRegistry(lane, 64 * 1024),
      mainAgent, directory, undefined, lifecycle);
    providers.push(provider);
    const context: FabricInvocationContext = {
      cwd, signal: undefined, parentToolCallId: "test", nestedToolCallId: "nested",
      extensionContext: {} as ExtensionContext, update() {}, activity() {},
    };
    const call = () => provider.invoke("projectAgent", {}, context);
    // An explicit source-clock sample, not just a recent projected host deadline.
    const mirror = (observedAt: number, sourceExpiresAt = observedAt + 120_000) => writeHostLease(mesh.root, {
      id: leadId, rootId: leadId, identityId: leadId, startedAt: 1, updatedAt: Date.now(), expiresAt: Date.now() + 120_000,
      mirrorLease: 1, mirror: { remoteHost: "remote", observedAt, sourceObservedAt: observedAt,
        sourceUpdatedAt: observedAt, sourceExpiresAt },
    });
    const publish = async (id: string, patch: Partial<FabricParticipantRecord> = {}, expiresAt = Date.now() + 120_000) => {
      const author: MeshIdentity = { id, name: "main", kind: "main" };
      const mirrored = id === leadId && remote;
      const now = Date.now();
      await mesh.put({ key: key("topology/hosts/", id), identity: author, value: {
        format: 1, id, rootId: id, identity: author, startedAt: 1, updatedAt: now, expiresAt,
        ...(mirrored ? { remoteHost: "remote", mirrorLease: 1 } : {}),
      } });
      await mesh.put({ key: key("topology/participants/", id), identity: author, value: {
        format: 1, id, rootId: id, ownerHostId: id, ownerIdentityId: id, kind: "root", name: "main",
        status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp", "fabric"],
        interactive: true, role: "project-agent", repository, project: path.resolve("/synthetic/lead"),
        startedAt: 1, updatedAt: now, controlProtocol: "v1", ...(mirrored ? { remoteHost: "remote" } : {}), ...patch,
      } });
      if (mirrored) mirror(now);
    };
    await publish(otherId); // A valid native fallback must never replace a failed recorded id.
    return { mesh, directory, call, publish, mirror };
  };
  const unavailable = {
    code: "FABRIC_PROJECT_AGENT_UNRESOLVED", message: expect.stringContaining("is absent or stale"),
  };

  it.each(["marker", "env"] as const)("resolves the exact same-repo then cross-repo recorded lead via %s", async binding => {
    const f = await fixture(binding);
    await f.publish(leadId);
    await expect(f.call()).resolves.toMatchObject({ id: leadId });
    await f.publish(leadId, { repository: "code.example/team/control" });
    await expect(f.call()).resolves.toMatchObject({ id: leadId });
  });

  it.each(["absent", "expired", "reloading"] as const)("refuses the %s recorded lead without falling back", async kind => {
    const f = await fixture();
    if (kind === "expired") await f.publish(leadId, {}, Date.now() - 1);
    if (kind === "reloading") await f.publish(leadId, { status: "reloading", reloadUntil: Date.now() - 1 });
    expect(f.directory.sessions().map(root => root.id)).toEqual([otherId]);
    await expect(f.call()).rejects.toMatchObject(unavailable);
  });

  it.each<Partial<FabricParticipantRecord>>([
    { interactive: false }, { capabilities: ["fabric"] }, { capabilities: ["followUp"] }, { capabilities: ["steer"] },
  ])("refuses a present non-interactive recorded lead: %j", async patch => {
    const f = await fixture();
    await f.publish(leadId, patch);
    expect(f.directory.sessions().find(root => root.id === leadId)).toBeDefined();
    await expect(f.call()).rejects.toMatchObject({
      code: "FABRIC_PROJECT_AGENT_UNRESOLVED", message: expect.stringContaining("is non-interactive or lacks steer/followUp"),
    });
  });

  it("diagnoses repository mismatch only for unbound native election", async () => {
    const f = await fixture("none", true);
    await f.publish(otherId, { repository: "code.example/team/other" });
    await f.publish(leadId); // A fresh same-repository mirror still cannot be elected without a binding.
    expect(f.directory.sessions().find(root => root.id === leadId)).toMatchObject({ remoteHost: "remote" });
    await expect(f.call()).rejects.toMatchObject({
      code: "FABRIC_PROJECT_AGENT_UNRESOLVED", message: expect.stringContaining("No recorded launch lead; live native roots belong to another repository"),
    });
    await f.publish(otherId);
    await expect(f.call()).resolves.toMatchObject({ id: otherId });
  });

  it.each(["bridge", "source-lease"] as const)("accepts a cross-repo recorded mirror only while its %s is fresh", async kind => {
    const f = await fixture("marker", true);
    await f.publish(leadId, { repository: "code.example/team/control" });
    await expect(f.call()).resolves.toMatchObject({ id: leadId, remoteHost: "remote" });
    if (kind === "bridge") f.mirror(Date.now() - MIRROR_FRESH_MS - 1);
    else f.mirror(Date.now(), Date.now() - 1);
    if (kind === "bridge") expect(f.directory.sessions().find(root => root.id === leadId)).toMatchObject({ bridgeStale: true });
    else expect(f.directory.sessions().find(root => root.id === leadId)).toBeUndefined();
    await expect(f.call()).rejects.toMatchObject(unavailable);
  });

  it("refuses a colliding recorded mirror instead of electing a native root", async () => {
    const f = await fixture("marker", true);
    await f.publish(leadId, { repository: "code.example/team/control" });
    const identity: MeshIdentity = { id: "session:native-owner", name: "main", kind: "main" };
    await f.mesh.put({ key: key("topology/hosts/", identity.id), identity, value: {
      format: 1, id: identity.id, rootId: leadId, identity, startedAt: 1, updatedAt: Date.now(), expiresAt: Date.now() + 120_000,
    } });
    expect(f.directory.sessions().map(root => root.id)).toEqual([otherId]);
    await expect(f.call()).rejects.toMatchObject(unavailable);
  });
});
