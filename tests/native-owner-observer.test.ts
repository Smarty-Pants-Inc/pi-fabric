// Actual Node recursive-FSWatcher observer unit coverage only: no Pi SDK/Main or kernel OWNER proof.
// Skip only platforms/runtime versions without Node recursive fs.watch support; Windows execution is
// watcher-unit behavior and must not be described as native proof.
import assert from "node:assert/strict";
import fs, { type FSWatcher, type PathLike } from "node:fs";
import path from "node:path";
import { describe, it } from "vitest";
import { isolatedTestTemp } from "../scripts/test-temp.js";
import { observeNativeOwner } from "./helpers/native-actor-owner.js";

type DeletionGuard = { root?: string; count: number };
type WatchOptions = { recursive?: boolean; persistent?: boolean; encoding?: BufferEncoding };
type WatchListener = (event: string, filename: string | Buffer | null) => void;
type WatchHook = (value: { attempts: number }) => void;
type WatchRecord = {
  watcher: FSWatcher;
  id: number;
  events: { event: string; file: string | null }[];
  futureEvents: number;
  closeCalls: number;
  closed: boolean;
};
type Settled = { ok: true } | { ok: false; error: unknown };
type UnitContext = {
  root: string;
  records: WatchRecord[];
  observer: ReturnType<typeof observeNativeOwner>;
  churn: () => void;
  inject: (record: WatchRecord, code: string) => Error;
  attached: Promise<WatchRecord>;
  setHook: (value: WatchHook | undefined) => void;
  attempts: () => number;
};

const nodeVersion = process.versions.node.split(".").map(Number);
const recursiveWatchSkipReason = !["linux", "darwin", "win32", "aix"].includes(process.platform)
  ? `Node recursive fs.watch is unsupported on ${process.platform}`
  : (["linux", "aix"].includes(process.platform) &&
      (nodeVersion[0]! < 19 || (nodeVersion[0] === 19 && nodeVersion[1]! < 1)))
    ? `Node ${process.version} lacks recursive fs.watch support on ${process.platform} (requires 19.1+)`
    : undefined;

const inside = (root: string, target: string): boolean => {
  const relative = path.relative(root, target);
  return relative === "" || (!path.isAbsolute(relative) && relative !== ".." && !relative.startsWith(`..${path.sep}`));
};
const guard = (process as unknown as Record<symbol, DeletionGuard>)[Symbol.for("pi-fabric.test-deletion-guard")];
const guardRoot = guard?.root;
const privateTemp = fs.realpathSync(isolatedTestTemp("pi-fabric-vitest-").TMPDIR);
if (!guard || !guardRoot || !inside(fs.realpathSync(guardRoot), privateTemp)) {
  throw new Error("Native owner observer tests require the existing Vitest private-temp deletion guard");
}
const initialOutsideCount = guard.count;
const packetRoot = fs.realpathSync(fs.mkdtempSync(path.join(privateTemp, "native-owner-observer-")));
assert(inside(fs.realpathSync(guardRoot), packetRoot), "observer traces must stay under the guarded Vitest temp root");
const traceFile = path.join(packetRoot, "trace.jsonl");
const trace = (value: Record<string, unknown>): void => {
  const line = JSON.stringify({ ns: String(process.hrtime.bigint()), ...value });
  fs.appendFileSync(traceFile, `${line}\n`, { mode: 0o600 });
  console.log(line);
};
trace({ event: "input", observer: "tests/helpers/native-actor-owner.ts#observeNativeOwner", node: process.execPath,
  nodeVersion: process.version, packetRoot, traceFile, runtimeScope: "actual-observer-unit-only" });

const deferred = <T = void>() => {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
};
const immediate = (): Promise<void> => new Promise(resolve => setImmediate(resolve));
// One-shot unit deadlines from the private regression; these are not polling or observer wakes.
const bounded = async <T>(promise: Promise<T>, label: string): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`private unit deadline: ${label}`)), 4_000);
    })]);
  } finally { clearTimeout(timer); }
};
const errno = (code: string): Error & { code: string } => Object.assign(new Error(`controlled actual-FSWatcher ${code}`), { code });
const settle = (promise: Promise<void>): Promise<Settled> => promise.then(() => ({ ok: true }), error => ({ ok: false, error }));
const requireOk = (result: Settled): void => { if (!result.ok) throw result.error; };

async function unit(name: string, body: (context: UnitContext) => Promise<void>): Promise<void> {
  const root = fs.mkdtempSync(path.join(packetRoot, "case-"));
  const originalWatch = fs.watch;
  const records: WatchRecord[] = [];
  let attempts = 0, hook: WatchHook | undefined;
  let observer: ReturnType<typeof observeNativeOwner> | undefined;
  const attached = deferred<WatchRecord>();
  const delegatedWatch = function(this: typeof fs, target: PathLike, options?: WatchOptions, listener?: WatchListener): FSWatcher {
    // Match only the observer's exact root + recursive=true call; every other fs.watch call delegates untouched.
    if (path.resolve(String(target)) !== root || options?.recursive !== true) {
      return Reflect.apply(originalWatch, this, arguments as unknown as Parameters<typeof fs.watch>);
    }
    attempts++;
    trace({ event: "watch-attempt", name, attempts });
    hook?.({ attempts }); // A controlled synchronous attachment error may be thrown here.
    let record: WatchRecord | undefined;
    const watcher = Reflect.apply(originalWatch, this, [target, options, (event: string, filename: string | Buffer | null) => {
      const file = filename == null ? null : String(filename);
      if (record) {
        record.events.push({ event, file });
        if (file === "future.json") record.futureEvents++;
      }
      trace({ event: "ACTUAL-file-event", name, watcher: record?.id, kind: event, file });
      listener?.(event, filename);
    }]) as FSWatcher;
    record = { watcher, id: records.length + 1, events: [], futureEvents: 0, closeCalls: 0, closed: false };
    records.push(record);
    const originalClose = watcher.close;
    watcher.close = function(this: FSWatcher) {
      record!.closeCalls++;
      trace({ event: "actual-close-call", name, watcher: record!.id });
      return Reflect.apply(originalClose, this, []);
    };
    watcher.on("close", () => {
      record!.closed = true;
      trace({ event: "actual-close-event", name, watcher: record!.id });
    });
    // No error listener here: the actual helper must own and handle watcher errors.
    if (records.length > 1) attached.resolve(record);
    return watcher;
  };
  fs.watch = delegatedWatch as typeof fs.watch;

  const churn = (): void => {
    const nested = path.join(root, "fleeting", "nested");
    fs.mkdirSync(nested, { recursive: true, mode: 0o700 });
    fs.rmdirSync(nested);
    fs.rmdirSync(path.dirname(nested));
    trace({ event: "actual-nested-mkdir-rmdir-DURING-until", name });
  };
  const inject = (record: WatchRecord, code: string): Error => {
    trace({ event: "INJECTED-error-not-file-event", name, watcher: record.id, code, closeCalls: record.closeCalls });
    const error = errno(code);
    record.watcher.emit("error", error);
    return error;
  };

  let bodyFailed = false, bodyFailure: unknown;
  try {
    observer = observeNativeOwner(root);
    assert.equal(records.length, 1, "capture the actual exported observer's watcher");
    await body({ root, records, observer, churn, inject, attached: attached.promise,
      setHook: value => { hook = value; }, attempts: () => attempts });
    trace({ event: "UNIT-PASS", name });
  } catch (error) {
    bodyFailed = true;
    bodyFailure = error;
    trace({ event: "UNIT-FAIL", name, error: error instanceof Error ? error.stack ?? String(error) : String(error) });
  } finally {
    let attemptsBeforeFinalTurn = attempts, attemptsAfterFinalTurn = attempts, everyWatcherClosed = false;
    let outsideCount = guard.count;
    try {
      observer?.close();
      // Let close/rearm callbacks run; no reopened watcher may survive shutdown.
      await immediate();
      await immediate();
      attemptsBeforeFinalTurn = attempts;
      await immediate();
      attemptsAfterFinalTurn = attempts;
      everyWatcherClosed = records.every(record => record.closeCalls > 0 && record.closed);
      outsideCount = guard.count;
    } finally {
      fs.watch = originalWatch;
      // Emergency test-only close. Fixture roots and traces are retained for the runner; no root is deleted here.
      for (const record of records) if (!record.closed) record.watcher.close();
    }
    assert.equal(attemptsAfterFinalTurn, attemptsBeforeFinalTurn, "shutdown must not re-open a watcher");
    assert(everyWatcherClosed, "every real watcher must close");
    assert.equal(outsideCount, initialOutsideCount, "no deletion outside inherited private temp");
  }
  if (bodyFailed) throw bodyFailure;
}

const transientCases = (["ENOENT", "ENOTDIR"] as const).flatMap(code =>
  [false, true].flatMap(nodeClosed => [false, true].map(synchronousRetry => ({
    code, nodeClosed, synchronousRetry,
    name: `${code}/${nodeClosed ? "closed" : "open"}/${synchronousRetry ? "sync-race" : "direct"}`,
  }))));

describe.skipIf(!!recursiveWatchSkipReason)(
  `actual native-owner observer real-FSWatcher unit coverage${recursiveWatchSkipReason ? ` [SKIP: ${recursiveWatchSkipReason}]` : ""}`,
  () => {
    // Each row changes real nested directories during until(). A separately counted genuine future.json
    // watcher event is required for success; neither churn nor injected errors stand in for file events.
    it.each(transientCases)("$name", async ({ code, nodeClosed, synchronousRetry, name }) => {
      await unit(name, async t => {
        const firstCheck = deferred(), transientCheck = deferred();
        let calls = 0;
        const outcome = settle(t.observer.until(() => {
          calls++;
          if (calls === 1) firstCheck.resolve(); else transientCheck.resolve();
          // A recorded actual fs.watch future.json event is required in addition to real file presence.
          return t.records.some(record => record.futureEvents > 0) && fs.existsSync(path.join(t.root, "future.json"));
        }, "future real file event after transient", 2_000));
        await firstCheck.promise;
        t.churn();
        if (synchronousRetry) t.setHook(({ attempts }) => {
          if (attempts === 2) { trace({ event: "INJECTED-sync-watch-throw", code }); throw errno(code); }
        });
        const first = t.records[0]!;
        if (nodeClosed) first.watcher.close(); // Real closure before the injected error, not a fabricated watcher.
        t.inject(first, code);
        await bounded(Promise.race([transientCheck.promise, outcome.then(result => {
          requireOk(result);
          throw new Error("until resolved without future actual file event");
        })]), "transient must wake predicate");
        await bounded(Promise.race([t.attached, outcome.then(result => {
          requireOk(result);
          throw new Error("unexpected early completion");
        })]), "real replacement watcher attached");
        await immediate();
        assert(calls >= 2, "transient must wake/check real current state");
        assert(t.records.every(record => record.futureEvents === 0));
        assert(first.closeCalls > 0, "old watcher must not leak");
        fs.writeFileSync(path.join(t.root, "future.json"), "actual future event\n", { mode: 0o600 });
        requireOk(await bounded(outcome, "actual future-file event resolves until without wake/poll"));
        assert(t.records.some(record => record.futureEvents > 0));
        assert(t.attempts() <= 3, "single controlled sync race must recover without retry loop");
      });
    });

    for (const code of ["EIO", "EACCES"] as const) {
      it(`fatal-${code}`, async () => unit(`fatal-${code}`, async t => {
        const outcome = settle(t.observer.until(() => false, "fatal error remains fatal", 2_000));
        t.churn();
        const error = t.inject(t.records[0]!, code);
        const result = await bounded(outcome, code);
        assert.equal(result.ok, false);
        if (result.ok) throw new Error("fatal observer outcome unexpectedly succeeded");
        assert.equal(result.error, error, "preserve fatal error identity");
      }));

      it(`fatal-${code}-after-transient-on-retired-watcher`, async () => unit(`fatal-${code}-after-transient-on-retired-watcher`, async t => {
        const release = deferred<boolean>();
        const outcome = settle(t.observer.until(() => release.promise, "fatal cannot be hidden by in-flight success", 2_000));
        t.churn();
        t.inject(t.records[0]!, "ENOENT");
        const error = t.inject(t.records[0]!, code);
        release.resolve(true);
        const result = await bounded(outcome, "sticky fatal after transient");
        assert.equal(result.ok, false);
        if (result.ok) throw new Error("retired watcher fatal outcome unexpectedly succeeded");
        assert.equal(result.error, error);
      }));

      it(`fatal-sync-rearm-${code}`, async () => unit(`fatal-sync-rearm-${code}`, async t => {
        const outcome = settle(t.observer.until(() => false, "fatal synchronous rearm", 2_000));
        t.churn();
        const error = errno(code);
        t.setHook(({ attempts }) => { if (attempts === 2) throw error; });
        t.inject(t.records[0]!, "ENOTDIR");
        const result = await bounded(outcome, "fatal synchronous rearm");
        assert.equal(result.ok, false);
        if (result.ok) throw new Error("synchronous rearm fatal outcome unexpectedly succeeded");
        assert.equal(result.error, error);
      }));
    }

    it("unexpected-close-without-error-is-fatal", async () => unit("unexpected-close-without-error-is-fatal", async t => {
      const outcome = settle(t.observer.until(() => false, "unexpected close", 2_000));
      t.records[0]!.watcher.close();
      const result = await bounded(outcome, "unexpected close");
      assert.equal(result.ok, false);
      if (result.ok) throw new Error("unexpected watcher close outcome succeeded");
      assert.match(String(result.error), /watcher closed unexpectedly/);
    }));

    it("close-cancels-queued-rearm", async () => unit("close-cancels-queued-rearm", async t => {
      const outcome = settle(t.observer.until(() => false, "close while rearm queued", 2_000));
      t.churn();
      t.inject(t.records[0]!, "ENOENT");
      t.observer.close();
      const result = await bounded(outcome, "close cancels pending observation");
      assert.equal(result.ok, false);
      if (result.ok) throw new Error("closed observation unexpectedly succeeded");
      assert.match(String(result.error), /observation closed/);
      await immediate();
      assert.equal(t.attempts(), 1);
      const afterClose = await settle(t.observer.until(() => true, "closed observer", 2_000));
      assert.equal(afterClose.ok, false);
    }));

    it("close-DURING-actual-rearm-closes-new-handle", async () => unit("close-DURING-actual-rearm-closes-new-handle", async t => {
      const outcome = settle(t.observer.until(() => false, "close during attachment", 2_000));
      t.setHook(({ attempts }) => { if (attempts === 2) t.observer.close(); });
      t.churn();
      t.inject(t.records[0]!, "ENOTDIR");
      const result = await bounded(outcome, "close during actual fs.watch attachment");
      assert.equal(result.ok, false);
      if (result.ok) throw new Error("closed rearm observation unexpectedly succeeded");
      assert.match(String(result.error), /observation closed/);
      await immediate();
      assert.equal(t.attempts(), 2);
      assert.equal(t.records.length, 2);
      assert(t.records.every(record => record.closeCalls > 0));
    }));

    it("repeated-sync-transients-have-finite-recovery", async () => unit("repeated-sync-transients-have-finite-recovery", async t => {
      const outcome = settle(t.observer.until(() => false, "bounded attachment recovery", 2_000));
      t.setHook(({ attempts }) => { if (attempts > 1) throw errno("ENOENT"); });
      t.churn();
      t.inject(t.records[0]!, "ENOENT");
      const result = await bounded(outcome, "bounded consecutive attachment failures");
      assert.equal(result.ok, false);
      if (result.ok) throw new Error("repeated transient rearm unexpectedly succeeded");
      assert.match(String(result.error), /could not re-arm/);
      assert(t.attempts() <= 8);
    }));
  },
);
