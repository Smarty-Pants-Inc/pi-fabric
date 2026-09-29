import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { main, resolveMeshRoot } from "../src/participants-cli.js";

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const scratch = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-participants-"));
  roots.push(root);
  return root;
};
const hash = (id: string): string => createHash("sha256").update(id).digest("hex");

// A root (and its host lease) as the participant directory, or the mesh bridge for remoteHost, writes it.
const addRoot = async (
  store: MeshStore,
  name: string,
  options: { expiresIn?: number; remoteHost?: string; rootId?: string } = {},
): Promise<string> => {
  const now = Date.now();
  const id = `session:${name}`;
  const identity: MeshIdentity = { id, name: "main", kind: "main", sessionId: name };
  const remote = options.remoteHost ? { remoteHost: options.remoteHost } : {};
  await store.put({ key: `topology/hosts/${hash(id)}`, identity, value: {
    format: 1, id, rootId: options.rootId ?? id, identity, startedAt: now, updatedAt: now,
    expiresAt: now + (options.expiresIn ?? 60_000), ...remote,
  } });
  await store.put({ key: `topology/participants/${hash(id)}`, identity, value: {
    format: 1, id, kind: "root", rootId: options.rootId ?? id, ownerHostId: id, ownerIdentityId: id, name: "main",
    label: `PF-${name}`, status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp", "fabric"],
    cwd: `/work/${name}`, sessionId: name, model: "anthropic/claude", thinking: "medium",
    startedAt: now, updatedAt: now, pendingMessages: false, controlProtocol: "v1", ...remote,
  } });
  return id;
};

const run = async (argv: string[]): Promise<{ code: number; out: string; err: string }> => {
  let out = "";
  let err = "";
  const code = await main(argv, (text) => { out += text; }, (text) => { err += text; });
  return { code, out, err };
};
const ids = (out: string): string[] => (JSON.parse(out) as Array<{ id: string }>).map((entry) => entry.id).sort();

describe("fabric-participants", () => {
  it("lists live roots with mirrored remotes, adds stale ones on request, and never writes", async () => {
    const root = scratch();
    const store = new MeshStore(root, 64 * 1024, 1_000);
    const live = await addRoot(store, "live");
    const stale = await addRoot(store, "stale", { expiresIn: -60_000 });
    const mirrored = await addRoot(store, "forge", { remoteHost: "forge" });
    // A mirror that claims the live root's lineage is refused; the refusal is not published here.
    await addRoot(store, "shadow", { remoteHost: "forge", rootId: live });
    const before = fs.readdirSync(root).map((name) => [name, fs.statSync(path.join(root, name)).mtimeMs]);

    const current = await run(["participants", "--json", "--mesh", root]);
    expect(current).toMatchObject({ code: 0, err: "" });
    expect(ids(current.out)).toEqual([mirrored, live].sort());
    const entries = JSON.parse(current.out) as Array<Record<string, unknown>>;
    expect(entries.find((entry) => entry.id === mirrored)).toMatchObject({
      kind: "root", remoteHost: "forge", label: "PF-forge", sessionId: "forge", local: false, stale: false,
    });
    expect(entries.find((entry) => entry.id === live)).toMatchObject({
      kind: "root", rootId: live, cwd: "/work/live", status: "idle", model: "anthropic/claude", stale: false,
    });
    expect(entries.find((entry) => entry.id === live)).not.toHaveProperty("remoteHost");

    const all = await run(["--mesh", root, "--include-stale"]);
    expect(ids(all.out)).toEqual([mirrored, live, stale].sort());
    expect((JSON.parse(all.out) as Array<Record<string, unknown>>).find((entry) => entry.id === stale))
      .toMatchObject({ stale: true });

    expect(JSON.parse((await run(["--mesh", root, "--kind", "actor"])).out)).toEqual([]);
    expect(fs.readdirSync(root).map((name) => [name, fs.statSync(path.join(root, name)).mtimeMs])).toEqual(before);
  });

  it("prints [] for an empty mesh and exits 2 with a named error for a missing one", async () => {
    const root = scratch();
    expect(await run(["--mesh", root])).toEqual({ code: 0, out: "[]\n", err: "" });
    const missing = await run(["--mesh", path.join(root, "absent")]);
    expect(missing).toMatchObject({ code: 2, out: "" });
    expect(missing.err).toContain("FABRIC_MESH_MISSING");
    expect(fs.existsSync(path.join(root, "absent"))).toBe(false);
    expect((await run(["--kind", "peer"])).code).toBe(2);
  });

  it("resolves the mesh root as Fabric does: env, project config over agent config, default", () => {
    const project = scratch();
    const agentDir = scratch();
    const env = { PI_CODING_AGENT_DIR: agentDir };
    expect(resolveMeshRoot(env, project)).toBe(path.join(project, ".pi", "fabric", "mesh"));
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ mesh: { root: "/fleet/global" } }));
    expect(resolveMeshRoot(env, project)).toBe(path.resolve("/fleet/global"));
    fs.mkdirSync(path.join(project, ".pi"));
    fs.writeFileSync(path.join(project, ".pi", "fabric.json"), JSON.stringify({ mesh: { root: "mesh-here" } }));
    expect(resolveMeshRoot(env, project)).toBe(path.join(project, "mesh-here"));
    expect(resolveMeshRoot({ ...env, PI_FABRIC_MESH_ROOT: "/fleet/env" }, project)).toBe("/fleet/env");
  });
});
