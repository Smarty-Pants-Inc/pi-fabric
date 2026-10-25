import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { runSessionPendingCli } from "../src/cli/session-pending.js";

// Keep reason: data safety at an offline read boundary. Exercise real files and the CLI export,
// including a real registry writer; corrupt/unreadable state must not authorize a move as empty.
const SESSION = "12345678-1234-1234-1234-123456789abc";
const OTHER = "87654321-4321-4321-4321-cba987654321";
const ROOT = `session:${SESSION}`;
const ID = "a".repeat(32);
const FOREIGN = "b".repeat(32);
const SECRET = "PRIVATE MESSAGE / TASK / INSTRUCTIONS MUST NOT APPEAR";
const fixtures: string[] = [];
afterEach(() => { for (const directory of fixtures.splice(0)) fs.rmSync(directory, { recursive: true, force: true }); });
const fixture = (): string => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-session-pending-"));
  fixtures.push(directory); return directory;
};
const json = (file: string, value: unknown): void => {
  fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, JSON.stringify(value));
};
const mailbox = (mesh: string): string => path.join(mesh, "main-followups", `${SESSION}.json`);
// MainAgentController.#save, src/main-agent.ts:1063-1071. The writer is private and requires
// a live Main context, so use its exact version-1 journal envelope without constructing a runtime.
const followUps = (mesh: string, count: number): void => json(mailbox(mesh), {
  version: 1, items: Array.from({ length: count }, (_, index) => ({
    id: `followup-${index}`, from: { id: "sender", name: "Sender", kind: "main" }, sentAt: 1000 + index,
    message: SECRET,
  })),
});
const actor = (id = ID, rootId = ROOT, status = "idle") => ({
  id, rootId, name: `actor-${id[0]}`, status, instructions: SECRET, messages: [],
  residency: "session", createdAt: 1000, updatedAt: 1001,
});
const registry = (directory: string, rows: ReturnType<typeof actor>[]): void => {
  fs.mkdirSync(directory, { recursive: true });
  const store = new ActorRegistryStore(directory);
  const prepared = store.prepare(rows);
  try { prepared.commit(); } finally { prepared.dispose(); }
};
// ActorManager.#queueFile/#persistQueue: lineage hash and format-1 items, resumed marks
// in-flight/restored custody. Payload is deliberately present to test output minimization.
const queueFile = (directory: string, rootId = ROOT): string => path.join(directory, ID,
  `queue-${createHash("sha256").update([rootId, "session"].join("\0")).digest("hex").slice(0, 16)}.json`);
const item = (id: string, resumed = false) => ({ id, source: "message", createdAt: 1000, payload: { task: SECRET }, ...(resumed ? { resumed } : {}) });
const run = async (args: string[], env: NodeJS.ProcessEnv = {}) => {
  let stdout = "", stderr = "";
  const code = await runSessionPendingCli(args, {
    env, stdout: { write: (chunk: string) => { stdout += chunk; return true; } },
    stderr: { write: (chunk: string) => { stderr += chunk; return true; } },
  });
  return { code, stdout, stderr, value: stdout.startsWith("{") ? JSON.parse(stdout) : undefined };
};
const snapshot = (root: string): unknown[] => {
  const result: unknown[] = [];
  const visit = (file: string): void => {
    const stat = fs.lstatSync(file, { bigint: true });
    result.push({ path: path.relative(root, file), size: stat.size, mode: stat.mode, mtimeNs: stat.mtimeNs });
    if (stat.isDirectory()) for (const name of fs.readdirSync(file).sort()) visit(path.join(file, name));
  };
  visit(root); return result;
};

describe("session pending CLI recorded-state inventory", () => {
  it("accepts absent mailbox and actor namespace without creating anything", async () => {
    const mesh = fixture(); const before = snapshot(mesh);
    const result = await run([SESSION, "--mesh", mesh]);
    expect(result.code).toBe(0); expect(result.stderr).toBe("");
    expect(result.value).toMatchObject({ format: 1, sessionId: SESSION, rootId: ROOT, complete: true,
      followUps: { file: mailbox(mesh), present: false, count: 0 }, actors: [], unknown: [],
      totals: { actors: 0, queued: 0, inFlight: 0, followUps: 0 } });
    expect(snapshot(mesh)).toEqual(before);
  });

  it("reports N journal records, ids and oldest timestamp, never message bodies", async () => {
    const mesh = fixture(); followUps(mesh, 3);
    const result = await run([`session:${SESSION}`, "--mesh", mesh, "--json"]);
    expect(result.code).toBe(0);
    expect(result.value.followUps).toMatchObject({ present: true, count: 3, oldestAt: 1000,
      ids: ["followup-0", "followup-1", "followup-2"] });
    expect(result.stdout).not.toContain(SECRET); expect(result.stdout.trim().split("\n")).toHaveLength(1);
  });

  it.each(["{ invalid PRIVATE", JSON.stringify({ version: 2, items: [] }), JSON.stringify({ version: 1, items: [null] }), "null"])(
    "fails closed for a corrupt or unknown mailbox: %s", async contents => {
      const mesh = fixture(); fs.mkdirSync(path.dirname(mailbox(mesh))); fs.writeFileSync(mailbox(mesh), contents);
      const before = snapshot(mesh); const result = await run([SESSION, "--mesh", mesh]);
      expect(result.code).toBe(1); expect(result.value.complete).toBe(false);
      expect(result.value.followUps).toMatchObject({ present: true, count: null });
      expect(result.value.unknown.length).toBeGreaterThan(0); expect(result.value.totals.followUps).toBeNull();
      expect(result.stdout).not.toContain("PRIVATE"); expect(snapshot(mesh)).toEqual(before);
    },
  );

  it("reads real actor registry generations, excludes foreign ownership and inventories queues across roots", async () => {
    const mesh = fixture(); const project = path.join(mesh, "actors");
    // Runtime PI_FABRIC_SESSION_ID can differ from the Pi session UUID: scan registries,
    // select rootId rather than assuming the folder names prove ownership.
    const session = path.join(project, "fabric-session-override");
    registry(project, [actor(ID), actor(FOREIGN, `session:${OTHER}`)]);
    registry(session, [actor("c".repeat(32))]);
    json(queueFile(project), { format: 1, items: [item("queued-1"), item("queued-2"), item("in-flight", true)] });
    const result = await run([SESSION, "--mesh", mesh]);
    expect(result.code).toBe(0);
    expect(result.value.actors.map((entry: { id: string }) => entry.id).sort()).toEqual([ID, "c".repeat(32)]);
    expect(result.value.actors).toEqual(expect.arrayContaining([
      expect.objectContaining({ id: ID, scope: "project", status: "idle", queued: 2, inFlight: 1 }),
      expect.objectContaining({ id: "c".repeat(32), scope: "session", queued: 0, inFlight: 0 }),
    ]));
    expect(result.value.totals).toMatchObject({ actors: 2, queued: 2, inFlight: 1 });
    expect(result.stdout).not.toContain(SECRET);
  });

  it("deduplicates retained predecessor copies with in-flight custody taking precedence", async () => {
    const mesh = fixture(); const directory = path.join(mesh, "actors", SESSION);
    registry(directory, [actor()]);
    json(queueFile(directory), { format: 1, items: [item("same")] });
    json(queueFile(directory, `session:${OTHER}`), { format: 1, items: [item("same", true), item("predecessor-only")] });
    const result = await run([SESSION, "--mesh", mesh]);
    expect(result.code).toBe(0); expect(result.value.totals).toMatchObject({ queued: 1, inFlight: 1 });
  });

  it("never treats unreadable queue state as zero", async () => {
    const mesh = fixture(); const directory = path.join(mesh, "actors"); registry(directory, [actor()]);
    fs.mkdirSync(path.dirname(queueFile(directory)), { recursive: true }); fs.writeFileSync(queueFile(directory), "bad PRIVATE");
    const result = await run([SESSION, "--mesh", mesh]);
    expect(result.code).toBe(1); expect(result.value.actors[0]).toMatchObject({ queued: null, inFlight: null });
    expect(result.value.totals.queued).toBeNull(); expect(result.stdout).not.toContain("PRIVATE");
  });

  it.each([{ format: 1, actors: [null] }, { format: 9, actors: [] }, { format: 1, actors: [{ id: ID }] }])(
    "refuses unknown actor shapes instead of silently filtering them", async value => {
      const mesh = fixture(); json(path.join(mesh, "actors", "actors.json"), value);
      const result = await run([SESSION, "--mesh", mesh]);
      expect(result.code).toBe(1); expect(result.value.totals.actors).toBeNull(); expect(result.value.unknown.length).toBeGreaterThan(0);
    },
  );

  it("reports owner and routing metadata without owner paths or routed content", async () => {
    const mesh = fixture(); followUps(mesh, 1);
    json(mailbox(mesh).replace(/\.json$/, ".owner.json"), { rootId: ROOT, sessionId: SESSION, ownerIdentityId: ROOT,
      pid: 123, host: "host", activationId: "activation", sessionFile: SECRET, name: SECRET, retired: true });
    json(path.join(mesh, "main-followups", "successors", `${createHash("sha256").update(ROOT).digest("hex")}.json`), { oldRoot: ROOT, newRoot: `session:${OTHER}` });
    json(path.join(mesh, "main-followups", "routes", `${"f".repeat(64)}.json`), { oldRoot: ROOT, messageId: "followup-0", newRoot: `session:${OTHER}`, item: { message: SECRET } });
    const result = await run([SESSION, "--mesh", mesh]);
    expect(result.code).toBe(0); expect(result.value.followUps.owner).toMatchObject({ pid: 123, retired: true });
    expect(result.value.routing).toEqual({ successorRootId: `session:${OTHER}`, routes: 1, unfinishedRoutes: 1 });
    expect(result.stdout).not.toContain(SECRET);
  });

  it.each([
    { format: 1, items: {} },
    { format: 1, items: [{ ...item("bad"), resumed: "yes" }] },
    { format: 1, items: [], settledWindows: [null] },
    { format: 1, items: [], pendingDeliveryOutcomes: "unknown" },
  ])("refuses unknown queue checkpoint shapes", async value => {
    const mesh = fixture(); const directory = path.join(mesh, "actors"); registry(directory, [actor()]);
    json(queueFile(directory), value);
    const result = await run([SESSION, "--mesh", mesh]);
    expect(result.code).toBe(1); expect(result.value.actors[0]).toMatchObject({ queued: null, inFlight: null });
  });

  it.each(["owner", "successor", "route"])("refuses corrupt %s metadata", async kind => {
    const mesh = fixture();
    const file = kind === "owner" ? mailbox(mesh).replace(/\.json$/, ".owner.json") : kind === "successor"
      ? path.join(mesh, "main-followups", "successors", `${createHash("sha256").update(ROOT).digest("hex")}.json`)
      : path.join(mesh, "main-followups", "routes", `${"f".repeat(64)}.json`);
    json(file, { unexpected: SECRET });
    const before = snapshot(mesh); const result = await run([SESSION, "--mesh", mesh]);
    expect(result.code).toBe(1); expect(result.value.totals.queued).toBeNull();
    expect(result.stdout).not.toContain(SECRET); expect(snapshot(mesh)).toEqual(before);
  });

  it("refuses nonempty child custody rather than invoking the mutating pending() reader", async () => {
    const mesh = fixture(); const directory = path.join(mesh, "actors"); registry(directory, [actor()]);
    json(path.join(directory, ID, "child-completions", `${FOREIGN}.json`), { format: 1, result: { id: FOREIGN, text: SECRET } });
    const before = snapshot(mesh); const result = await run([SESSION, "--mesh", mesh]);
    expect(result.code).toBe(1); expect(result.value.actors[0].childCompletionEntries).toBe(1);
    expect(result.value.unknown.join(" ")).toContain("owner reconciliation"); expect(snapshot(mesh)).toEqual(before);
    expect(result.stdout).not.toContain(SECRET);
  });

  it("proves read-only paths, sizes, modes and nanosecond mtimes for populated state", async () => {
    const mesh = fixture(); const directory = path.join(mesh, "actors", SESSION);
    followUps(mesh, 2); registry(directory, [actor()]);
    json(queueFile(directory), { format: 1, items: [item("queued"), item("running", true)],
      settledWindows: [{ sourceId: "event", pending: { task: SECRET } }], pendingDeliveryOutcomes: [{ outcome: "failed", text: SECRET }] });
    const before = snapshot(mesh); const result = await run([SESSION, "--mesh", mesh]);
    expect(result.code).toBe(0); expect(result.value.totals).toMatchObject({ queued: 1, inFlight: 1, settledWindows: 1, pendingDeliveryOutcomes: 1 });
    expect(snapshot(mesh)).toEqual(before); expect(result.stdout).not.toContain(SECRET);
  });

  it("supports env root and lets --mesh take precedence", async () => {
    const mesh = fixture();
    expect((await run([SESSION], { PI_FABRIC_MESH_ROOT: mesh })).code).toBe(0);
    expect((await run([SESSION, "--mesh", mesh], { PI_FABRIC_MESH_ROOT: "/does-not-exist" })).code).toBe(0);
    const missing = await run([SESSION]); expect(missing.code).toBe(1); expect(missing.value.error).toBe("mesh-required");
  });

  it("refuses a missing mesh root or non-directory namespace", async () => {
    const mesh = fixture();
    const result = await run([SESSION, "--mesh", path.join(mesh, "missing")]);
    expect(result.code).toBe(1); expect(result.value.followUps.count).toBeNull(); expect(result.value.totals.queued).toBeNull();
    fs.writeFileSync(path.join(mesh, "actors"), "not a directory");
    expect((await run([SESSION, "--mesh", mesh])).code).toBe(1);
  });

  it.skipIf(process.platform === "win32")("rejects a dangling mailbox symlink instead of reporting absence", async () => {
    const mesh = fixture(); fs.mkdirSync(path.dirname(mailbox(mesh)));
    fs.symlinkSync(path.join(mesh, "missing"), mailbox(mesh)); const before = snapshot(mesh);
    const result = await run([SESSION, "--mesh", mesh]);
    expect(result.code).toBe(1); expect(result.value.followUps).toMatchObject({ present: true, count: null });
    expect(snapshot(mesh)).toEqual(before);
  });

  it.each([[], ["not-a-uuid"], ["../../escape"], [SESSION, "extra"], [SESSION, "--unknown"], [SESSION, "--mesh"],
    [SESSION, "--mesh", "--json"], [SESSION, "--json", "--json"], [SESSION, "--json=true"], [SESSION, "-x"]])(
    "rejects invalid argv %j with usage exit 2", async (...args: string[]) => {
      // Vitest spreads table rows into callback arguments.
      const result = await run(args); expect(result.code).toBe(2); expect(JSON.parse(result.stderr).error).toBe("usage");
      expect(result.stdout).toBe("");
    },
  );

  it("provides --help without needing mesh state", async () => {
    const result = await run(["--help"]); expect(result.code).toBe(0); expect(result.stdout).toContain("Usage:"); expect(result.stderr).toBe("");
  });

  it("is reachable as `pi-fabric session pending` through the standalone CLI entry", async () => {
    const mesh = fixture(); followUps(mesh, 2); const before = snapshot(mesh);
    const writes: string[] = [];
    const stdout = vi.spyOn(process.stdout, "write").mockImplementation(chunk => { writes.push(String(chunk)); return true; });
    const stderr = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    try {
      const { runFabricCli } = await import("../src/cli/index.js");
      expect(await runFabricCli(["session", "pending", SESSION, "--mesh", mesh])).toBe(0);
      expect(JSON.parse(writes.join(""))).toMatchObject({ rootId: ROOT, complete: true, followUps: { count: 2 } });
      expect(await runFabricCli(["session", "unknown"])).toBe(2);
      expect(stderr).toHaveBeenCalled();
    } finally { stdout.mockRestore(); stderr.mockRestore(); }
    expect(snapshot(mesh)).toEqual(before);
  });
});
