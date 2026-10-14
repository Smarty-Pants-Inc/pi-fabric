import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { ActorLogStore } from "../src/actors/log-store.js";
import { privateDirTestHooks } from "../src/storage/private-dir.js";
import { createHostTaskRun, MAX_TASK_CONTROL_BYTES, routeControlFile, TASK_CONTROL_LAYOUT, taskControlDirectory, withRouteControlDirectory } from "../src/worker/terminal-answer.js";

// smarty-dev#12444 archive lifecycle of the central route/ namespace. Archive-only:
// this suite reaches no product-deletion entrypoint (retention judgments of the
// same trees are in task-control-archive.test.ts / task-control-legacy-retention.test.ts).
// The parent supplies an isolated XDG_STATE_HOME outside temp (the central root refuses temp).
const isolatedState = process.env.XDG_STATE_HOME;
const insideTemp = (value: string) => [os.tmpdir(), process.env.TMPDIR].filter(Boolean)
  .some(temp => path.resolve(value) === path.resolve(temp!) || path.resolve(value).startsWith(path.resolve(temp!) + path.sep));
const usable = !!isolatedState && path.isAbsolute(isolatedState) && !insideTemp(isolatedState);
const roots: string[] = [];
afterEach(() => {
  delete privateDirTestHooks.platform;
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const day = 24 * 60 * 60 * 1000;
const RECEIPT = "route-dispatch-receipt.json", PENDING = "pending-route-outcome.json", SESSION = "route-session.jsonl";
const record = (id: string) => ({ id, runner: "pi", status: "completed", transport: "process", sessionId: "2147483647", finishedAt: day, taskControlLayout: TASK_CONTROL_LAYOUT });
const dispatch = (runId: string) => ({ runId, ledger: "/ledger.jsonl", decision: { decisionId: "dispatch", routeClass: "bounded-lookup", mode: "live" } });
/** Host-allocated current run (createHostTaskRun), route controls through the central writer helper. */
const hostRun = (run: string, id: string, route: Partial<Record<string, string | Buffer>> = {}) => {
  fs.mkdirSync(path.dirname(run), { recursive: true, mode: 0o700 });
  createHostTaskRun(run);
  fs.writeFileSync(path.join(run, "status.json"), JSON.stringify(record(id)), { mode: 0o600 });
  if (Object.keys(route).length) withRouteControlDirectory(run, directory => {
    for (const [name, bytes] of Object.entries(route)) fs.writeFileSync(path.join(directory, name), bytes!, { mode: 0o600 });
  });
};
const setup = (route: Partial<Record<string, string | Buffer>> = { [RECEIPT]: JSON.stringify(dispatch("archive")) }) => {
  const root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), "task-control-archive-route-")); roots.push(root);
  const source = path.join(root, "source-run");
  hostRun(source, "source-run", route);
  const actor = { sessionFile: path.join(root, "actor", "session.jsonl"), lastRunId: "archive" };
  const store = new ActorLogStore({ maxEventBytes: 8192 }, { eventContextChars: 1000 }, { actorRunArchiveMs: day });
  return { root, source, actor, store, archive: path.join(root, "actor", "runs", "archive"), route: path.dirname(routeControlFile(source, RECEIPT)) };
};
const tree = (directory: string): string[] => fs.readdirSync(directory, { recursive: true }).map(String).sort();
const noRouteNames = (directory: string) => {
  for (const name of [RECEIPT, PENDING, SESSION, "route"]) expect(fs.existsSync(path.join(directory, name)), name).toBe(false);
};

describe.skipIf(!usable)("archive of central route/ controls (smarty-dev#12444)", () => {
  it("copies the bounded receipt and pending outcome into the archive's own central route/ and never reads the session", async () => {
    const session = Buffer.alloc(MAX_TASK_CONTROL_BYTES * 16, "s"); // far beyond the 64 KiB control bound
    const pending = JSON.stringify({ ledger: "/ledger.jsonl", record: { type: "outcome", decisionId: "dispatch" } });
    const f = setup({ [RECEIPT]: JSON.stringify(dispatch("archive")), [PENDING]: pending, [SESSION]: session });
    const before = tree(f.route);
    await f.store.retainRun(f.actor, "archive", f.source);
    expect(JSON.parse(fs.readFileSync(routeControlFile(f.archive, RECEIPT), "utf8"))).toEqual(dispatch("archive"));
    expect(fs.readFileSync(routeControlFile(f.archive, PENDING), "utf8")).toBe(pending);
    expect(fs.existsSync(routeControlFile(f.archive, SESSION))).toBe(false);
    if (process.platform !== "win32") expect(fs.statSync(path.dirname(routeControlFile(f.archive, RECEIPT))).mode & 0o777).toBe(0o700);
    // Destination-own binding, never a name in the archive tree (C).
    const binding = JSON.parse(fs.readFileSync(path.join(taskControlDirectory(f.archive), ".legacy-read.json"), "utf8"));
    expect(binding).toMatchObject({ version: 2, runId: "archive", ino: String(fs.lstatSync(f.archive, { bigint: true }).ino), snapshot: null });
    noRouteNames(f.archive);
    // Source custody untouched.
    expect(tree(f.route)).toEqual(before);
    expect(fs.readFileSync(path.join(f.route, SESSION))).toEqual(session);
  });

  it.each(["oversized receipt", "oversized pending", "unknown entry", "publication temp", "linked receipt", "hardlinked pending", "linked route", "mismatched receipt", "malformed receipt", ...(process.platform === "win32" ? [] : ["open route"])])("refuses %s without publishing archive controls", async bad => {
    const f = setup({ [RECEIPT]: JSON.stringify(dispatch("archive")), [PENDING]: "{}" });
    const receipt = path.join(f.route, RECEIPT);
    if (bad === "oversized receipt") fs.writeFileSync(receipt, JSON.stringify({ ...dispatch("archive"), pad: "x".repeat(MAX_TASK_CONTROL_BYTES) }));
    if (bad === "oversized pending") fs.writeFileSync(path.join(f.route, PENDING), "x".repeat(MAX_TASK_CONTROL_BYTES + 1));
    if (bad === "unknown entry") fs.writeFileSync(path.join(f.route, "other.json"), "{}", { mode: 0o600 });
    if (bad === "publication temp") fs.writeFileSync(path.join(f.route, `.${RECEIPT}.tmp`), "{}", { mode: 0o600 });
    if (bad === "linked receipt") { fs.unlinkSync(receipt); fs.symlinkSync(path.join(f.source, "status.json"), receipt); }
    if (bad === "hardlinked pending") fs.linkSync(path.join(f.route, PENDING), path.join(f.root, "outside"));
    if (bad === "linked route") { const moved = path.join(f.root, "moved"); fs.renameSync(f.route, moved); fs.symlinkSync(moved, f.route, "junction"); }
    if (bad === "open route") fs.chmodSync(f.route, 0o755);
    if (bad === "mismatched receipt") fs.writeFileSync(receipt, JSON.stringify(dispatch("other-run")));
    if (bad === "malformed receipt") fs.writeFileSync(receipt, "{broken");
    await expect(f.store.retainRun(f.actor, "archive", f.source), bad).rejects.toThrow();
    expect(fs.existsSync(taskControlDirectory(f.archive)), bad).toBe(false);
    noRouteNames(f.archive);
  });

  it("archives a deliberately legacy in-run receipt and pending outcome read-only, into central route/ only", async () => {
    const f = setup({});
    const legacyReceipt = JSON.stringify(dispatch("archive")) + "\n", legacyPending = "{\"record\":{}}";
    fs.writeFileSync(path.join(f.source, RECEIPT), legacyReceipt, { mode: 0o600 });
    fs.writeFileSync(path.join(f.source, PENDING), legacyPending, { mode: 0o600 });
    expect(routeControlFile(f.source, RECEIPT, { legacyReadOnly: true })).toBe(path.join(f.source, RECEIPT));
    await f.store.retainRun(f.actor, "archive", f.source);
    expect(JSON.parse(fs.readFileSync(routeControlFile(f.archive, RECEIPT), "utf8"))).toEqual(dispatch("archive"));
    expect(fs.readFileSync(routeControlFile(f.archive, PENDING), "utf8")).toBe(legacyPending);
    noRouteNames(f.archive);
    // Read-only source: bytes in place, no central route/ minted for the older run.
    expect(fs.readFileSync(path.join(f.source, RECEIPT), "utf8")).toBe(legacyReceipt);
    expect(fs.existsSync(f.route)).toBe(false);
  });

  it("a present central route/ wins: a stray in-run receipt is never the archive source", async () => {
    const f = setup();
    fs.writeFileSync(path.join(f.source, RECEIPT), JSON.stringify(dispatch("forged")), { mode: 0o600 });
    await f.store.retainRun(f.actor, "archive", f.source);
    expect(JSON.parse(fs.readFileSync(routeControlFile(f.archive, RECEIPT), "utf8"))).toEqual(dispatch("archive"));
    noRouteNames(f.archive);
  });

  it("preserves nested route custody under each nested archive's own binding", async () => {
    const f = setup();
    const child = path.join(f.source, "nested", "child");
    fs.mkdirSync(path.join(f.source, "nested"), { mode: 0o700 });
    hostRun(child, "child", { [RECEIPT]: JSON.stringify(dispatch("child")), [PENDING]: "{}", [SESSION]: "{}\n" });
    await f.store.retainRun(f.actor, "archive", f.source);
    const archived = path.join(f.archive, "nested", "child");
    expect(JSON.parse(fs.readFileSync(routeControlFile(archived, RECEIPT), "utf8"))).toEqual(dispatch("child"));
    expect(fs.readFileSync(routeControlFile(archived, PENDING), "utf8")).toBe("{}");
    expect(fs.existsSync(routeControlFile(archived, SESSION))).toBe(false);
    expect(JSON.parse(fs.readFileSync(path.join(taskControlDirectory(archived), ".legacy-read.json"), "utf8")))
      .toMatchObject({ version: 2, runId: "child", ino: String(fs.lstatSync(archived, { bigint: true }).ino), snapshot: null });
    noRouteNames(archived);
  });

  it("moves nested legacy route custody to central without copying route names into the archive tree", async () => {
    const f = setup();
    const child = path.join(f.source, "nested", "child");
    fs.mkdirSync(path.join(f.source, "nested"), { mode: 0o700 });
    hostRun(child, "child");
    const bytes = JSON.stringify(dispatch("child"));
    fs.writeFileSync(path.join(child, RECEIPT), bytes, { mode: 0o600 });
    fs.writeFileSync(path.join(child, PENDING), "{}", { mode: 0o600 });
    fs.writeFileSync(path.join(child, SESSION), "legacy session", { mode: 0o600 });
    await f.store.retainRun(f.actor, "archive", f.source);
    const archived = path.join(f.archive, "nested", "child");
    noRouteNames(archived);
    expect(JSON.parse(fs.readFileSync(routeControlFile(archived, RECEIPT), "utf8"))).toEqual(dispatch("child"));
    expect(fs.readFileSync(routeControlFile(archived, PENDING), "utf8")).toBe("{}");
    expect(fs.existsSync(routeControlFile(archived, SESSION))).toBe(false);
    expect(fs.readFileSync(path.join(child, RECEIPT), "utf8")).toBe(bytes);
    expect(fs.existsSync(path.dirname(routeControlFile(child, RECEIPT)))).toBe(false);
  });

  it("refuses a nested central receipt attributed to a different child", async () => {
    const f = setup();
    const child = path.join(f.source, "nested", "child");
    fs.mkdirSync(path.join(f.source, "nested"), { mode: 0o700 });
    hostRun(child, "child", { [RECEIPT]: JSON.stringify(dispatch("another-child")) });
    await expect(f.store.retainRun(f.actor, "archive", f.source)).rejects.toThrow("Mismatched route dispatch receipt");
    expect(fs.existsSync(taskControlDirectory(f.archive))).toBe(false);
  });

  it("refuses an unsafe nested route/ rather than skipping it", async () => {
    const f = setup();
    const child = path.join(f.source, "nested", "child");
    fs.mkdirSync(path.join(f.source, "nested"), { mode: 0o700 });
    hostRun(child, "child", { [RECEIPT]: JSON.stringify(dispatch("child")) });
    fs.writeFileSync(path.join(path.dirname(routeControlFile(child, RECEIPT)), "unknown"), "{}", { mode: 0o600 });
    await expect(f.store.retainRun(f.actor, "archive", f.source)).rejects.toThrow("Unknown archive route control artifact");
    expect(fs.existsSync(taskControlDirectory(f.archive))).toBe(false);
  });

  // Windows has no directory fd: the archive uses the checked no-follow path (lstat +
  // realpath equality), an alias/identity check only, never OS confinement.
  it("uses the checked no-fd run path on win32 and still refuses an aliased run", async () => {
    const f = setup();
    privateDirTestHooks.platform = "win32";
    const open = fs.openSync;
    const opened: string[] = [];
    const spy = (fs as { openSync: typeof fs.openSync }).openSync = ((file: fs.PathLike, ...rest: unknown[]) => {
      opened.push(String(file));
      return (open as (...args: unknown[]) => number)(file, ...rest);
    }) as typeof fs.openSync;
    try {
      await f.store.retainRun(f.actor, "archive", f.source);
      expect(opened).not.toContain(f.source);
      expect(JSON.parse(fs.readFileSync(routeControlFile(f.archive, RECEIPT), "utf8"))).toEqual(dispatch("archive"));
      const alias = path.join(f.root, "alias");
      fs.symlinkSync(f.source, alias, "junction");
      await expect(f.store.retainRun({ ...f.actor, lastRunId: "aliased" }, "aliased", alias)).rejects.toThrow();
      expect(fs.existsSync(taskControlDirectory(path.join(f.root, "actor", "runs", "aliased")))).toBe(false);
    } finally { if (fs.openSync === spy) (fs as { openSync: typeof fs.openSync }).openSync = open; }
  });
});
