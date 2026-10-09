import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { claimMainName, mainNameBindingKey, mainNameOwnerDead, principalMainNames,
  readMainNameBinding, MAIN_NAME_REBINDING_PREFIX, type MainNameBinding } from "../src/topology/main-name-binding.js";
import type { FabricParticipantInfo } from "../src/topology/types.js";

const dirs: string[] = [];
const temp = () => { const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-name-binding-")); dirs.push(dir); return dir; };
afterEach(() => { vi.restoreAllMocks(); for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true }); });
const root = (n: number, name = "lead"): FabricParticipantInfo => {
  const sessionId = `aaaaaaaa-0000-4000-8000-${String(n).padStart(12, "0")}`;
  const id = `session:${sessionId}`;
  return { format: 1, id, rootId: id, ownerHostId: id, ownerIdentityId: id,
    sessionId, herdrPane: `w${n}:p1`, kind: "root", name, status: "idle", runner: "pi", transport: "host",
    capabilities: ["steer", "followUp", "fabric"], interactive: true, controlProtocol: "v1",
    startedAt: 1, updatedAt: Date.now(), local: false, stale: false };
};
const identity = (p: FabricParticipantInfo): MeshIdentity => ({ id: p.id, name: p.name, kind: "main", sessionId: p.sessionId! });
const mesh = () => new MeshStore(path.join(temp(), "mesh"), 64 * 1024, 1000);
const router = (store: MeshStore, listing: () => FabricParticipantInfo[], cwd?: string) => {
  const request = vi.fn().mockResolvedValue({ routed: "mesh", acknowledged: true });
  const ports = { get: (id: string) => listing().find(p => p.id === id), list: listing,
    scheduleRefresh() {}, mainNameBinding: (name: string) => readMainNameBinding(store, name),
    principalName: (name: string) => principalMainNames(cwd).has(name) };
  type Ports = ConstructorParameters<typeof AgentMessageRouter>;
  const value = new AgentMessageRouter({ status: () => { throw new Error("Unknown Fabric agent"); } } as unknown as Ports[0],
    { identity: { id: "sender", kind: "main", name: "sender" } } as unknown as Ports[1],
    { id: "session:sender", local: true, matches: () => false } as unknown as Ports[2], ports, { request }, b => b);
  const send = (name = "name:lead") => value.routeMessage(name, "must stay bound", undefined, "steer");
  return { send, request };
};

describe("durable Main name selector custody (same-UID is fleet trust, not authentication)", () => {
  it("CAS binds the first publisher and keeps sole impostor unaddressable during absence or reload", async () => {
    const store = mesh(); const lead = root(1); const newcomer = root(2);
    await claimMainName(store, identity(lead), lead);
    const first = readMainNameBinding(store, "lead");
    await claimMainName(store, identity(newcomer), newcomer);
    expect(readMainNameBinding(store, "lead")).toEqual(first);
    expect(first).toMatchObject({ sessionId: lead.sessionId, herdrPane: lead.herdrPane, hostId: lead.ownerHostId });
    let listing = [lead, newcomer]; const send = router(store, () => listing);
    await expect(send.send()).rejects.toMatchObject({ code: "FABRIC_NAME_TARGET_AMBIGUOUS" });
    for (const status of ["reloading", "stopping", "absent"]) {
      listing = status === "absent" ? [newcomer] : [{ ...lead, status }, newcomer];
      const before = store.read({ topic: "fabric.control.command", limit: 100 });
      await expect(send.send()).rejects.toMatchObject({ code: "FABRIC_NAME_TARGET_ABSENT", boundSessionId: lead.id });
      expect(store.read({ topic: "fabric.control.command", limit: 100 })).toEqual(before);
    }
    expect(send.request).not.toHaveBeenCalled();
    // A new directory/store incarnation still sees the durable reservation.
    expect(readMainNameBinding(new MeshStore(store.root, 64 * 1024, 1000), "lead")).toEqual(first);
    listing = [lead]; await expect(send.send()).resolves.toMatchObject({ acknowledged: true });
    expect(send.request).toHaveBeenCalledOnce();
    await expect(send.send("name:absent")).rejects.toMatchObject({ code: "FABRIC_NAME_TARGET_ABSENT" });
  });

  it("rebinds after a real owner pid exits and logs the transition atomically", async () => {
    const store = mesh(); const lead = root(1); const newcomer = root(2);
    await claimMainName(store, identity(lead), lead);
    const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], { stdio: "ignore" });
    await once(child, "spawn");
    const close = once(child, "close");
    try {
      const original = readMainNameBinding(store, "lead")!;
      const owner = { host: os.hostname(), pid: child.pid!, processStartedAt: processStartTime(child.pid!) };
      await store.put({ key: mainNameBindingKey("lead"), identity: identity(lead), value: { ...original, owner } });
      await claimMainName(store, identity(newcomer), newcomer);
      expect(readMainNameBinding(store, "lead")?.sessionId).toBe(lead.sessionId);
      child.kill("SIGKILL"); await close;
      await claimMainName(store, identity(newcomer), newcomer);
      const rebound = readMainNameBinding(store, "lead")!;
      expect(rebound).toMatchObject({ sessionId: newcomer.sessionId, hostId: newcomer.ownerHostId });
      expect(store.listAll(MAIN_NAME_REBINDING_PREFIX)).toEqual([expect.objectContaining({ value: expect.objectContaining({
        previous: expect.objectContaining({ sessionId: lead.sessionId }), next: rebound, reason: "owner-process-dead",
      }) })]);
      const send = router(store, () => [newcomer]);
      await expect(send.send()).resolves.toMatchObject({ acknowledged: true });
      expect(send.request).toHaveBeenCalledWith(newcomer.ownerHostId, newcomer.id, "steer", expect.anything(), newcomer.ownerIdentityId, expect.anything());
    } finally { if (child.exitCode === null && child.signalCode === null) { child.kill("SIGKILL"); await close; } }
  });

  it("only one concurrent claimant wins initial binding or a dead owner's rebinding", async () => {
    const store = mesh(); const a = root(1); const b = root(2);
    await Promise.all([claimMainName(store, identity(a), a), claimMainName(store, identity(b), b)]);
    const original = readMainNameBinding(store, "lead")!;
    expect([a.sessionId, b.sessionId]).toContain(original.sessionId);
    if (process.platform === "linux") {
      await store.put({ key: mainNameBindingKey("lead"), identity: identity(a),
        value: { ...original, owner: { ...original.owner, processStartedAt: "0" } } });
      const c = root(3); const d = root(4);
      await Promise.all([claimMainName(store, identity(c), c), claimMainName(store, identity(d), d)]);
      expect([c.sessionId, d.sessionId]).toContain(readMainNameBinding(store, "lead")?.sessionId);
      expect(store.listAll(MAIN_NAME_REBINDING_PREFIX)).toHaveLength(1);
    }
  });

  it("does not use a remote/unknown owner, expired presence, EPERM, or unreadable start time as death", async () => {
    const store = mesh(); const lead = root(1); const newcomer = root(2);
    await claimMainName(store, identity(lead), lead);
    const original = readMainNameBinding(store, "lead")!;
    expect(mainNameOwnerDead(original)).toBe(false);
    expect(mainNameOwnerDead({ ...original, owner: { ...original.owner, host: "another-host", pid: 2147483647 } })).toBe(false);
    expect(mainNameOwnerDead({ ...original, owner: { host: original.owner.host, pid: original.owner.pid } })).toBe(false);
    const kill = vi.spyOn(process, "kill").mockImplementation(() => { throw Object.assign(new Error("unknown"), { code: "EIO" }); });
    expect(mainNameOwnerDead(original)).toBe(false);
    kill.mockImplementation(() => { throw Object.assign(new Error("denied"), { code: "EPERM" }); });
    expect(mainNameOwnerDead(original)).toBe(false); kill.mockRestore();
    const read = fs.readFileSync;
    vi.spyOn(fs, "readFileSync").mockImplementation(((file: unknown, ...args: unknown[]) => {
      if (String(file) === `/proc/${process.pid}/stat`) throw new Error("EIO");
      return Reflect.apply(read, fs, [file, ...args]);
    }) as typeof fs.readFileSync);
    expect(mainNameOwnerDead({ ...original, owner: { ...original.owner, processStartedAt: "0" } })).toBe(false);
    await claimMainName(store, identity(newcomer), { ...newcomer, status: "idle", updatedAt: 0 });
    expect(readMainNameBinding(store, "lead")?.sessionId).toBe(lead.sessionId);
  });

  it("keeps corrupt bindings fail closed and renews only the same session's incarnation", async () => {
    const store = mesh(); const lead = root(1);
    await claimMainName(store, identity(lead), lead);
    await claimMainName(store, identity(lead), { ...lead, herdrPane: "moved:p1" });
    expect(readMainNameBinding(store, "lead")?.herdrPane).toBe("moved:p1");
    expect(store.listAll(MAIN_NAME_REBINDING_PREFIX)).toEqual([]);
    await store.put({ key: mainNameBindingKey("lead"), identity: identity(lead), value: { format: 1, name: "lead" } });
    await expect(claimMainName(store, identity(root(2)), root(2))).rejects.toThrow("Invalid durable Main name binding");
  });

  it("refuses every fixed principal name even with one live Main, but permits its exact session", async () => {
    const store = mesh();
    for (const name of ["org", "org-kate", "org-marisela", "org-deputy", "org-preview-paul"]) {
      const p = root(1, name); const send = router(store, () => [p]);
      await expect(send.send(`name:${name}`)).rejects.toMatchObject({ code: "FABRIC_NAME_TARGET_PRINCIPAL" });
      expect(send.request).not.toHaveBeenCalled();
      await expect(send.send(name)).rejects.toMatchObject({ code: "FABRIC_NAME_TARGET_PRINCIPAL" });
      await expect(send.send(p.id)).resolves.toMatchObject({ acknowledged: true });
      expect(send.request).toHaveBeenCalledOnce();
    }
  });

  it("reads additional principal orgInstance names from setup/org.json", async () => {
    const cwd = temp(); fs.mkdirSync(path.join(cwd, "setup"));
    fs.writeFileSync(path.join(cwd, "setup", "org.json"), JSON.stringify({ principals: [
      { orgInstance: { herdrAgent: "org-new-principal" } }, { orgInstance: { herdrAgent: "bad/name" } },
    ] }));
    expect(principalMainNames(cwd).has("org-new-principal")).toBe(true);
    const p = root(1, "org-new-principal"); const send = router(mesh(), () => [p], cwd);
    await expect(send.send("name:org-new-principal")).rejects.toMatchObject({ code: "FABRIC_NAME_TARGET_PRINCIPAL" });
    expect(send.request).not.toHaveBeenCalled();
  });
});
