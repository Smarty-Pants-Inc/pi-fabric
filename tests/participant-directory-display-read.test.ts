import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshReadOptions } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";

// smarty-dev#4250: the dashboard's participant/peer listing (background) is display-only and
// stays incremental; every other listing keeps the bound read, and a display view never
// publishes a collision refusal without an authoritative re-read first.
const roots: string[] = [];
afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const hash = (id: string): string => createHash("sha256").update(id).digest("hex");
const writerIdentity = { id: "session:writer", name: "writer", kind: "main" as const };
const identity = { id: "session:local", name: "main", kind: "main" as const, sessionId: "local" };

const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "directory-display-read-")); roots.push(root);
  const writer = new MeshStore(root, 256 * 1024, 1000);
  // ~1 MB canonical payload, so a whole-file read or hash is unmistakable.
  await writer.writeBatch({ identity: writerIdentity, ops: Array.from({ length: 100 }, (_, i) => ({ kind: "put" as const,
    key: `bulk/${String(i).padStart(3, "0")}`, value: "x".repeat(10_000) })) });
  const mesh = new MeshStore(root, 256 * 1024, 1000);
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity,
    heartbeatMs: 60_000, leaseMs: 120_000, reapDeadHosts: false });
  const tokens = vi.spyOn(mesh, "stateToken");
  const options = () => tokens.mock.calls.map(([read]) => (read ?? {}) as MeshReadOptions);
  return { root, writer, mesh, directory, tokens, options };
};

describe("participant directory display reads (smarty-dev#4250)", () => {
  it("only background listings read displayOnly; ordinary and fresh listings stay bound", async () => {
    const f = await fixture();
    // Every state read one listing makes (its scan and its self fallback) uses one mode.
    const modes = (list: () => unknown) => {
      f.tokens.mockClear();
      list();
      return [...new Set(f.options().map((read) => read.displayOnly === true))];
    };
    expect(modes(() => f.directory.list({ scope: "project", background: true }))).toEqual([true]);
    expect(modes(() => f.directory.peers(Date.now(), { background: true }))).toEqual([true]);
    expect(modes(() => f.directory.list({ scope: "project" }))).toEqual([false]);
    expect(modes(() => f.directory.list({ scope: "project", fresh: true }))).toEqual([false]);
    expect(modes(() => f.directory.list({ scope: "project", background: true, fresh: true }))).toEqual([false]);
  });

  it("a background listing follows changes without reading the whole payload", async () => {
    const f = await fixture();
    f.directory.list({ scope: "project", background: true });
    const file = path.join(f.root, "state.json");
    const readSync = vi.spyOn(fs, "readSync"), readFile = vi.spyOn(fs, "readFileSync");
    const bytes = () => readSync.mock.results.reduce((sum, r) => sum + (r.type === "return" ? Number(r.value) : 0), 0);
    const parses = () => readFile.mock.calls.filter(([target]) => String(target) === file).length;
    for (let n = 0; n < 10; n++) {
      await f.writer.put({ key: `leases/tick-${n % 3}`, value: { n }, identity: writerIdentity });
      // Only the listing is measured: the writer itself reads the state for its commit.
      const before = bytes(), parsed = parses();
      expect(f.directory.list({ scope: "project", background: true })).toBeDefined();
      expect(parses()).toBe(parsed);
      // The 1 MB payload is never read or hashed again: the appended record and headers only.
      expect(bytes() - before).toBeLessThan(16 * 1024);
    }
  });

  it("a collision seen in a display view is re-read authoritatively before the refusal is published", async () => {
    const f = await fixture();
    const remote = { id: "session:forge", name: "main", kind: "main" as const, sessionId: "forge" };
    // A mirrored root claiming this host's own root id collides (smarty-dev#2004).
    await f.writer.writeBatch({ identity: remote, ops: [
      { kind: "put", key: "topology/participants/" + hash(identity.id), value: {
        format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: remote.id, ownerIdentityId: remote.id,
        name: "main", status: "idle", runner: "pi", transport: "host", capabilities: ["fabric"], cwd: "/x",
        startedAt: 1, updatedAt: Date.now(), controlProtocol: "v1", remoteHost: "forge" } },
      { kind: "put", key: "topology/hosts/" + hash(remote.id), value: {
        format: 1, id: remote.id, rootId: remote.id, identity: remote, startedAt: 1, updatedAt: Date.now(),
        expiresAt: Date.now() + 60_000, remoteHost: "forge" } },
    ] });
    const publish = vi.spyOn(f.mesh, "publish");
    f.directory.list({ scope: "project", background: true });
    // The display view saw the collision; the listing was rebuilt from a bound read.
    const modes = f.options().map((read) => read.displayOnly === true);
    expect(modes[0]).toBe(true);
    expect(modes.length).toBeGreaterThan(1);
    expect(modes.slice(1).every((mode) => !mode)).toBe(true);
    await vi.waitFor(() => expect(publish).toHaveBeenCalledTimes(1));
    expect(publish.mock.calls[0]![0]).toMatchObject({ kind: "refused" });
  });
});
