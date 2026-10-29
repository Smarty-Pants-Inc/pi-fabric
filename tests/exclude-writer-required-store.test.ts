// Keep: REQUIRED data-safety/trust boundary. Real files and committed B6, but
// fixture B5 bytes and B1/native/CAS ports are synthetic, NOT authenticity proof.
import fs from "node:fs";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as module from "../src/recovery/exclude-writer.js";
import { openRecoveryJournal } from "../src/recovery/recovery-journal.js";
import { fixture, treeBytes, watchRetainedIO, type Fixture } from "./helpers/portable-exclude-writer-fixture.js";

const fixtures: Fixture[] = [];
afterEach(async () => { vi.restoreAllMocks(); for (const s of fixtures.splice(0)) await s.dispose(); });
async function setup() {
  // A broken module/export cannot count as a successful refusal.
  expect(typeof module.createPortablePreOpenBinding).toBe("function");
  const s = await fixture(); fixtures.push(s);
  return { s, binding: () => s.bind(module.createPortablePreOpenBinding) };
}
const identity = (s: Fixture) => path.join(s.b6, "journal.identity");
const noHistory = (s: Fixture) => expect(s.historyOpens).toEqual({ native: 0, b5: 0, b6: 0 });
function historyError(error: unknown): Error {
  expect(error).toBeInstanceOf(Error);
  if (!(error instanceof Error)) throw new Error("required refusal must be an actual Error");
  expect(error).toHaveProperty("code", "RECOVERY_HISTORY_INCOMPLETE");
  expect(error.cause).toBeInstanceOf(Error);
  return error;
}
async function requiredFailure(attempt: Promise<unknown>): Promise<Error> {
  return historyError(await attempt.then(() => undefined, (error: unknown) => error));
}
function thrownFailure(action: () => void): Error {
  try { action(); } catch (error) {
    if (error instanceof Error) return error;
    throw error;
  }
  throw new Error("current assertion unexpectedly succeeded");
}
function noEffects(s: Fixture) {
  expect(fs.readFileSync(s.generationFile, "utf8")).toBe("7\n");
  expect(s.record!.highWater).toBe(7); expect(s.cas).toEqual([]);
  expect(s.acquired).toBe(0); expect(s.kv.writes).toEqual([]); noHistory(s);
}
const initialFaults = ["native-missing", "native-directory", "native-symlink", "native-hardlink",
  "B5-missing", "B5-directory", "B5-symlink", "B5-hardlink", "B5-parent-symlink",
  "B6-root-missing", "B6-root-symlink", "B6-identity-missing", "B6-identity-directory", "B6-identity-symlink", "B6-identity-hardlink"] as const;
function fault(s: Fixture, kind: typeof initialFaults[number]) {
  if (kind === "B5-parent-symlink") {
    const actual = path.join(s.root, "ledger-parent"); fs.mkdirSync(actual);
    fs.renameSync(s.b5File, path.join(actual, "ledger"));
    const alias = path.join(s.root, "ledger-alias"); fs.symlinkSync(actual, alias);
    s.record!.b5File = path.join(alias, "ledger"); return;
  }
  const file = kind.startsWith("native-") ? s.sessionFile : kind.startsWith("B5-") ? s.b5File : kind.startsWith("B6-root-") ? s.b6 : identity(s);
  const saved = path.join(s.root, "saved-required-store");
  if (kind.endsWith("hardlink")) { fs.linkSync(file, saved); return; }
  fs.renameSync(file, saved);
  if (kind.endsWith("directory")) fs.mkdirSync(file);
  if (kind.endsWith("symlink")) fs.symlinkSync(saved, file);
}
function replaceB5(s: Fixture, label: string) {
  const next = path.join(s.root, `next-${label}`);
  fs.writeFileSync(next, `${JSON.stringify({ fixture: `synthetic B5 ${label}, NOT validation` })}\n`);
  fs.renameSync(next, s.b5File);
}

describe.skipIf(process.platform !== "linux")("M2(a) REQUIRED-store real-FS preflight decisions", () => {
  it.each(initialFaults)("%s refuses without history, generation, CAS, reclaim or lease", async kind => {
    const { s, binding } = await setup(); fault(s, kind);
    const before = treeBytes(s.root), watch = watchRetainedIO(s);
    try {
      await requiredFailure(binding().preOpen.excludeWriter({ sessionFile: s.sessionFile }));
      expect(watch.writes).toEqual([]);
      // Metadata-only initial refusal must not read any retained file bytes.
      expect(watch.reads).toEqual([]);
    } finally { watch.stop(); }
    noEffects(s); expect(treeBytes(s.root)).toEqual(before);
  });

  it.each(["wrong-store", "malformed", "empty"])("actual B6 %s identity refuses under guard before effects", async kind => {
    const { s, binding } = await setup();
    fs.writeFileSync(identity(s), kind === "wrong-store" ? '{"version":1,"storeId":"other"}\n' : kind === "empty" ? "" : "not identity\n");
    const before = treeBytes(s.root);
    await requiredFailure(binding().preOpen.excludeWriter({ sessionFile: s.sessionFile }));
    noEffects(s); expect(treeBytes(s.root)).toEqual(before);
  });

  it("positive committed store opens through the real required journal without changing retained data", async () => {
    const { s, binding } = await setup(); const kept = fs.readFileSync(identity(s));
    const result = await s.consume(binding());
    expect(Buffer.from(result.b6).toString()).toBe("B6 exact retained value");
    expect(fs.readFileSync(identity(s))).toEqual(kept); expect(s.acquired).toBe(1);
    expect(s.historyOpens).toEqual({ native: 1, b5: 1, b6: 1 });
  });

  it("positive NEW-provisioned empty store uses the real journal identity, not a fabricated frame/seal", async () => {
    const { s, binding } = await setup(); const empty = path.join(s.root, "empty-b6");
    const journal = await openRecoveryJournal({ directory: empty, storeId: s.record!.storeId });
    expect(journal.entries()).toEqual([]); await journal.close();
    // Replace only the fixture's provisioned root, keeping all old evidence.
    fs.renameSync(s.b6, path.join(s.root, "old-b6")); fs.renameSync(empty, s.b6);
    const current = await binding().preOpen.excludeWriter({ sessionFile: s.sessionFile }); current.assertCurrent();
    const reopened = await openRecoveryJournal({ directory: s.b6, storeId: s.record!.storeId, required: true });
    try { expect(reopened.entries()).toEqual([]); } finally { await reopened.close(); }
  });

  it("predecessor atomic B5 replacement before B1 completion and receiver atomic writes remain valid", async () => {
    const { s, binding } = await setup(); const read = s.input.b1.readCompletedForSelf;
    const original = fs.statSync(s.b5File).ino;
    s.input.b1.readCompletedForSelf = async (...args) => {
      replaceB5(s, "predecessor"); await Promise.resolve(); return read(...args);
    };
    const b = binding(), current = await b.preOpen.excludeWriter({ sessionFile: s.sessionFile });
    expect(fs.statSync(s.b5File).ino).not.toBe(original); current.assertCurrent();
    replaceB5(s, "receiver"); expect(() => current.assertCurrent()).not.toThrow();
    await b.preOpen.excludeWriter({ sessionFile: s.sessionFile }); expect(s.acquired).toBe(1);
  });

  it.each(["native", "B5", "B6-root", "B6-identity"] as const)("%s disappears while B1 awaits: guarded recheck refuses before effects", async kind => {
    const { s, binding } = await setup(); const read = s.input.b1.readCompletedForSelf;
    let injected = false;
    s.input.b1.readCompletedForSelf = async (...args) => {
      const result = await read(...args); injected = true;
      fs.renameSync(kind === "native" ? s.sessionFile : kind === "B5" ? s.b5File : kind === "B6-root" ? s.b6 : identity(s), path.join(s.root, "removed-during-b1"));
      return result;
    };
    await requiredFailure(binding().preOpen.excludeWriter({ sessionFile: s.sessionFile }));
    expect(injected).toBe(true); noEffects(s);
  });

  it.each(["native", "B5", "B6-root", "B6-identity"] as const)("%s misroutes after native hold: refuse before generation/CAS/reclaim", async kind => {
    const { s, binding } = await setup(); const hold = s.input.nativeCustody.hold;
    let injected = false;
    s.input.nativeCustody.hold = async (...args) => {
      const held = await hold(...args); injected = true;
      const file = kind === "native" ? s.sessionFile : kind === "B5" ? s.b5File : kind === "B6-root" ? s.b6 : identity(s);
      const saved = path.join(s.root, "misrouted-after-hold"); fs.renameSync(file, saved); fs.symlinkSync(saved, file);
      return held;
    };
    await requiredFailure(binding().preOpen.excludeWriter({ sessionFile: s.sessionFile }));
    expect(injected).toBe(true); noEffects(s);
  });

  it.each(["native", "B5", "B6-root", "B6-identity"] as const)("missing %s at the actual held-OFD boundary refuses before effects", async kind => {
    const { s, binding } = await setup(); const mounted = s.input.nativeCustody.readMountedAtOpen;
    let reads = 0, sawHeldOfd = false, injected = false;
    s.input.nativeCustody.readMountedAtOpen = async (...args) => {
      const result = await mounted(...args);
      if (++reads === 2) {
        sawHeldOfd = fs.readdirSync("/proc/self/fd").some(fd => {
          try { return fs.readlinkSync(`/proc/self/fd/${fd}`) === s.lockFile &&
            /OFDLCK.*\s0\sEOF/.test(fs.readFileSync(`/proc/self/fdinfo/${fd}`, "utf8")); } catch { return false; }
        });
        const file = kind === "native" ? s.sessionFile : kind === "B5" ? s.b5File : kind === "B6-root" ? s.b6 : identity(s);
        fs.renameSync(file, path.join(s.root, "lost-under-ofd")); injected = true;
      }
      return result;
    };
    await requiredFailure(binding().preOpen.excludeWriter({ sessionFile: s.sessionFile }));
    expect(injected).toBe(true); expect(sawHeldOfd).toBe(true); noEffects(s);
  });

  it.each((["native", "B5", "B6-root", "B6-identity"] as const).flatMap(kind =>
    (["CAS", "lease", "final-mount"] as const).map(phase => ({ kind, phase }))))(
    "missing $kind after asynchronous $phase boundary cannot return current", async ({ kind, phase }) => {
    const { s, binding } = await setup(); let injected = false;
    const remove = () => {
      injected = true;
      const file = kind === "native" ? s.sessionFile : kind === "B5" ? s.b5File : kind === "B6-root" ? s.b6 : identity(s);
      fs.renameSync(file, path.join(s.root, "late-missing-store"));
    };
    if (phase === "CAS") {
      const cas = s.input.identityRecord.casHighWater;
      s.input.identityRecord.casHighWater = async (...args) => { const result = await cas(...args); remove(); return result; };
    } else if (phase === "lease") {
      const acquire = s.input.leaseOwner.acquireOnce;
      s.input.leaseOwner.acquireOnce = async (...args) => { const result = await acquire(...args); remove(); return result; };
    } else {
      const mounted = s.input.nativeCustody.readMountedAtOpen;
      s.input.nativeCustody.readMountedAtOpen = async (...args) => { const result = await mounted(...args); if (s.acquired > 0) remove(); return result; };
    }
    await requiredFailure(binding().preOpen.excludeWriter({ sessionFile: s.sessionFile }));
    expect(injected).toBe(true); noHistory(s);
    if (phase === "CAS") { expect(s.acquired).toBe(0); expect(s.kv.writes).toEqual([]); }
  });

  it.each(["native", "B5", "B6-root", "B6-identity"] as const)("post-open missing %s latches refusal, never recreates it", async kind => {
    const { s, binding } = await setup(); const b = binding();
    const current = await b.preOpen.excludeWriter({ sessionFile: s.sessionFile });
    const file = kind === "native" ? s.sessionFile : kind === "B5" ? s.b5File : kind === "B6-root" ? s.b6 : identity(s), saved = path.join(s.root, "lost-after-open");
    fs.renameSync(file, saved); const first = historyError(thrownFailure(() => current.assertCurrent()));
    fs.renameSync(saved, file); expect(thrownFailure(() => current.assertCurrent())).toBe(first);
    await expect(b.preOpen.excludeWriter({ sessionFile: s.sessionFile })).rejects.toBe(first); noHistory(s);
  });

  it("initial required-store refusal is one-shot even after restoration, preserving raw cause", async () => {
    const { s, binding } = await setup(); const b = binding(), saved = path.join(s.root, "saved-b5");
    fs.renameSync(s.b5File, saved);
    const first = await requiredFailure(b.preOpen.excludeWriter({ sessionFile: s.sessionFile }));
    if (!(first.cause instanceof Error)) throw new Error("missing preflight context");
    expect(first.cause.cause).toHaveProperty("code", "ENOENT");
    fs.renameSync(saved, s.b5File);
    await expect(b.preOpen.excludeWriter({ sessionFile: s.sessionFile })).rejects.toBe(first); noEffects(s);
  });

  it.each(["B1", "current-mount"] as const)("fixture-invalid %s refuses without history-incomplete classification", async kind => {
    const { s, binding } = await setup();
    if (kind === "B1") s.fence = undefined;
    else s.mounted = { ...s.mounted, attachment: "fixture-invalid-attachment" };
    const b = binding(), before = treeBytes(s.root);
    const error = await b.preOpen.excludeWriter({ sessionFile: s.sessionFile }).then(() => undefined, (cause: unknown) => cause);
    expect(error).toBeInstanceOf(Error);
    expect(error).not.toHaveProperty("code", "RECOVERY_HISTORY_INCOMPLETE");
    if (!(error instanceof Error)) throw new Error("expected real boundary refusal");
    expect(error.cause).toBeInstanceOf(Error);
    await expect(b.preOpen.excludeWriter({ sessionFile: s.sessionFile })).rejects.toBe(error);
    noEffects(s); expect(treeBytes(s.root)).toEqual(before);
  });
});
