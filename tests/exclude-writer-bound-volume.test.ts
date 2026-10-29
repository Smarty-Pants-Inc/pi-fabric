// Keep: data safety and the REQUIRED volume/path trust boundary. Real FS/OFD;
// fixture B1/native/CAS decisions are NOT production authentication evidence.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createPortablePreOpenBinding } from "../src/recovery/exclude-writer.js";
import { fixture, treeBytes, watchRetainedIO, type Fixture } from "./helpers/portable-exclude-writer-fixture.js";

const fixtures: Fixture[] = [], externalTrees: string[] = [];
const custodyFields: ("custodyDirectory" | "generationFile" | "lockFile")[] = ["custodyDirectory", "generationFile", "lockFile"];
const fixedFields: ("generationFile" | "lockFile")[] = ["generationFile", "lockFile"];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const s of fixtures.splice(0)) await s.dispose();
  for (const directory of externalTrees.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});
async function setup() {
  expect(typeof createPortablePreOpenBinding).toBe("function");
  const s = await fixture(); fixtures.push(s); return s;
}
function outside() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "m2-bound-volume-external-")); externalTrees.push(directory);
  const generationFile = path.join(directory, "custody.gen"), lockFile = path.join(directory, "custody.ofd");
  fs.writeFileSync(generationFile, "7\n", { flag: "wx" });
  return { directory, generationFile, lockFile };
}
function record(s: Fixture) {
  if (!s.record) throw new Error("fixture REQUIRED record absent");
  return s.record;
}
async function failure(attempt: Promise<unknown>): Promise<Error> {
  const error: unknown = await attempt.then(() => undefined, (cause: unknown) => cause);
  expect(error).toBeInstanceOf(Error);
  if (!(error instanceof Error)) throw new Error("pre-open admitted instead of refusing custody path");
  expect(error).toHaveProperty("code", "RECOVERY_HISTORY_INCOMPLETE");
  expect(error.cause).toBeInstanceOf(Error); return error;
}
function noEffects(s: Fixture) {
  expect(s.events).toEqual(["identity:read"]);
  expect(s.cas).toEqual([]); expect(record(s).highWater).toBe(7);
  expect(s.acquired).toBe(0); expect(s.kv.writes).toEqual([]);
  expect(s.historyOpens).toEqual({ native: 0, b5: 0, b6: 0 });
}
// Additional actual external-path attempts, including descriptor-relative IO.
// The existing complete 90-entry retained-root observer is used alongside this.
function watchExternal(directory: string) {
  const reads: string[] = [], writes: string[] = [];
  function resolved(value: unknown): string {
    try {
      if (typeof value === "number") return fs.readlinkSync(`/proc/self/fd/${value}`);
      const file = value instanceof URL ? value.pathname : Buffer.isBuffer(value) ? value.toString() : typeof value === "string" ? value : "";
      if (file.startsWith("/proc/self/fd/")) return fs.realpathSync(file);
      return file;
    } catch { return ""; }
  }
  function observe(value: unknown, events: string[]) {
    const file = resolved(value);
    if (file === directory || file.startsWith(`${directory}${path.sep}`)) events.push(file);
  }
  // Vitest reuses an existing spy: capture its implementation, not the spy itself,
  // so forwarding preserves the full retained observer without recursive reentry.
  const open = vi.mocked(fs.openSync).getMockImplementation() ?? fs.openSync;
  const readFile = vi.mocked(fs.readFileSync).getMockImplementation() ?? fs.readFileSync;
  const read = vi.mocked(fs.readSync).getMockImplementation() ?? fs.readSync;
  const write = vi.mocked(fs.writeSync).getMockImplementation() ?? fs.writeSync;
  const spies = [
    vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
      if (typeof flags === "string" ? /[wa+]/.test(flags) : (flags & (fs.constants.O_WRONLY | fs.constants.O_RDWR | fs.constants.O_CREAT | fs.constants.O_TRUNC | fs.constants.O_APPEND)) !== 0) observe(file, writes);
      return open(file, flags, mode);
    }),
    vi.spyOn(fs, "readFileSync").mockImplementation((...args: Parameters<typeof fs.readFileSync>) => {
      observe(args[0], reads); return Reflect.apply(readFile, fs, args);
    }),
    vi.spyOn(fs, "readSync").mockImplementation((...args: Parameters<typeof fs.readSync>) => {
      observe(args[0], reads); return Reflect.apply(read, fs, args);
    }),
    vi.spyOn(fs, "writeSync").mockImplementation((...args: Parameters<typeof fs.writeSync>) => {
      observe(args[0], writes); return Reflect.apply(write, fs, args);
    }),
  ];
  return { reads, writes, stop() { for (const spy of spies.reverse()) spy.mockRestore(); } };
}

function kernelMetadata(file: string) {
  const fd = fs.openSync(file, 0x200000 | fs.constants.O_NOFOLLOW); // Actual Linux O_PATH, no content access.
  try {
    const stat = fs.fstatSync(fd, { bigint: true });
    const mount = /^mnt_id:\s+([1-9][0-9]*)$/m.exec(fs.readFileSync(`/proc/self/fdinfo/${fd}`, "utf8"))?.[1];
    if (!mount) throw new Error("kernel mount identity unavailable");
    return { file, dev: String(stat.dev), ino: String(stat.ino), mount };
  } finally { fs.closeSync(fd); }
}
function causes(error: unknown): Error[] {
  const result: Error[] = [];
  for (let cause = error; cause instanceof Error; cause = cause.cause) result.push(cause);
  return result;
}

describe.skipIf(process.platform !== "linux")("REQUIRED custody bound-volume real-FS decisions", () => {
  it("valid original fixed paths admit, hold current, increment 7 to 8 and CAS", async () => {
    const s = await setup(), binding = s.bind(createPortablePreOpenBinding);
    const current = await binding.preOpen.excludeWriter({ sessionFile: s.sessionFile }); current.assertCurrent();
    expect(fs.readFileSync(s.generationFile, "utf8")).toBe("8\n");
    expect(s.cas).toEqual([{ before: 7, next: 8, disk: "8\n" }]); expect(s.acquired).toBe(1);
  });

  it("post-open generation-byte currentness loss is typed, sticky and preserves its first cause", async () => {
    const s = await setup(), binding = s.bind(createPortablePreOpenBinding);
    const current = await binding.preOpen.excludeWriter({ sessionFile: s.sessionFile });
    fs.writeFileSync(s.generationFile, "9\n");
    let observed: unknown;
    try { current.assertCurrent(); } catch (cause) { observed = cause; }
    const first = await failure(Promise.reject(observed));
    if (!(first.cause instanceof Error) || !(first.cause.cause instanceof Error)) throw new Error("missing original custody currentness context");
    const originalCause = first.cause.cause; expect(originalCause.message).toContain("metadata inode/raw bytes changed");
    fs.writeFileSync(s.generationFile, "8\n");
    let repeated: unknown; try { current.assertCurrent(); } catch (cause) { repeated = cause; }
    expect(repeated).toBe(first); expect(first.cause.cause).toBe(originalCause);
    await expect(binding.preOpen.excludeWriter({ sessionFile: s.sessionFile })).rejects.toBe(first);
    expect(s.cas).toHaveLength(1); expect(s.acquired).toBe(1);
  });

  it.each(custodyFields)("missing or malformed REQUIRED %s refuses before effects", async field => {
    for (const value of [undefined, "", "relative", `${os.tmpdir()}/a/../b`, `${os.tmpdir()}/tail/`]) {
      const s = await setup(); Object.defineProperty(record(s), field, { value, enumerable: true, configurable: true });
      const binding = s.bind(createPortablePreOpenBinding), before = treeBytes(s.root), watch = watchRetainedIO(s);
      try {
        await failure(binding.preOpen.excludeWriter({ sessionFile: s.sessionFile }));
        expect(watch.reads).toEqual([]); expect(watch.writes).toEqual([]);
      } finally { watch.stop(); }
      noEffects(s); expect(treeBytes(s.root)).toEqual(before);
    }
  });

  it("REQUIRED custody on an actual different mount refuses metadata-only before B1", async context => {
    const s = await setup(), sessionFile = fs.realpathSync(path.resolve("tsconfig.json"));
    if (fs.statSync(sessionFile).dev === fs.statSync(s.root).dev) {
      context.skip(); return; // No privileged mounts or alternate temp allocation to manufacture coverage.
    }
    record(s).sessionFile = sessionFile; s.input.bootstrap.sessionFile = sessionFile;
    const before = treeBytes(s.root), watch = watchRetainedIO(s);
    try {
      await failure(s.bind(createPortablePreOpenBinding).preOpen.excludeWriter({ sessionFile }));
      expect(watch.reads).toEqual([]); expect(watch.writes).toEqual([]);
    } finally { watch.stop(); }
    noEffects(s); expect(treeBytes(s.root)).toEqual(before);
  });

  it("read-only existing same-device wrong-mount custody is refused by actual kernel mnt_id", async context => {
    const routes = ["/run", "/run/netns"];
    const metadata: { directory: string; dev: string; mount: string | undefined }[] = [];
    try {
      for (const directory of routes) {
        const fd = fs.openSync(directory, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY | fs.constants.O_NOFOLLOW);
        try { metadata.push({ directory, dev: String(fs.fstatSync(fd, { bigint: true }).dev),
          mount: /^mnt_id:\s+([1-9][0-9]*)$/m.exec(fs.readFileSync(`/proc/self/fdinfo/${fd}`, "utf8"))?.[1] }); }
        finally { fs.closeSync(fd); }
      }
    } catch (cause) {
      process.stdout.write(`M2_SAME_DEVICE_MOUNT_UNAVAILABLE ${String(cause)}\n`); context.skip(); return;
    }
    const [session, custody] = metadata;
    if (!session || !custody || !session.mount || !custody.mount || session.dev !== custody.dev || session.mount === custody.mount) {
      process.stdout.write(`M2_SAME_DEVICE_MOUNT_UNAVAILABLE ${JSON.stringify(metadata)}\n`); context.skip(); return;
    }
    process.stdout.write(`M2_SAME_DEVICE_MOUNT_ACTUAL ${JSON.stringify(metadata)}\n`);
    const s = await setup(), required = record(s);
    required.sessionFile = path.join(session.directory, "m2-never-created-session");
    required.custodyDirectory = custody.directory; required.generationFile = path.join(custody.directory, "custody.gen"); required.lockFile = path.join(custody.directory, "custody.ofd");
    s.input.bootstrap.sessionFile = required.sessionFile; s.input.bootstrap.generationFile = required.generationFile; s.input.bootstrap.lockFile = required.lockFile;
    const error = await failure(s.bind(createPortablePreOpenBinding).preOpen.excludeWriter({ sessionFile: required.sessionFile }));
    const causes: string[] = []; for (let cause: unknown = error; cause instanceof Error; cause = cause.cause) causes.push(cause.message);
    expect(causes.some(message => message.includes("custody directory is not on the bound session mount"))).toBe(true);
    noEffects(s); // No leaf contents inspected or shared mount modified; mismatch precedes leaf metadata.
  });

  it.each(fixedFields)("REQUIRED %s must be its exact fixed basename", async field => {
    const s = await setup(); record(s)[field] = path.join(s.root, "not-the-fixed-file");
    const before = treeBytes(s.root);
    await failure(s.bind(createPortablePreOpenBinding).preOpen.excludeWriter({ sessionFile: s.sessionFile }));
    noEffects(s); expect(treeBytes(s.root)).toEqual(before);
  });

  it.each(["generation", "lock"])("existing %s missing/symlink/hardlink/wrong-type metadata refuses before B1/content/effects", async kind => {
    for (const fault of ["missing", "symlink", "hardlink", "directory"]) {
      const s = await setup(), file = kind === "generation" ? s.generationFile : s.lockFile, saved = path.join(s.root, "saved-custody-file");
      if (fault === "hardlink") fs.linkSync(file, saved);
      else {
        fs.renameSync(file, saved);
        if (fault === "symlink") fs.symlinkSync(saved, file);
        if (fault === "directory") fs.mkdirSync(file);
      }
      const before = treeBytes(s.root), watch = watchRetainedIO(s);
      try {
        await failure(s.bind(createPortablePreOpenBinding).preOpen.excludeWriter({ sessionFile: s.sessionFile }));
        expect(watch.reads).toEqual([]); expect(watch.writes).toEqual([]);
      } finally { watch.stop(); }
      noEffects(s); expect(treeBytes(s.root)).toEqual(before);
    }
  });

  it("a canonical custody directory symlink is not a valid bound route", async () => {
    const s = await setup(), actual = path.join(s.root, "actual-custody"), alias = path.join(s.root, "alias-custody");
    fs.mkdirSync(actual); fs.renameSync(s.generationFile, path.join(actual, "custody.gen")); fs.renameSync(s.lockFile, path.join(actual, "custody.ofd"));
    fs.symlinkSync(actual, alias);
    record(s).custodyDirectory = alias; record(s).generationFile = path.join(alias, "custody.gen"); record(s).lockFile = path.join(alias, "custody.ofd");
    s.input.bootstrap.generationFile = record(s).generationFile; s.input.bootstrap.lockFile = record(s).lockFile;
    const before = treeBytes(s.root);
    await failure(s.bind(createPortablePreOpenBinding).preOpen.excludeWriter({ sessionFile: s.sessionFile }));
    noEffects(s); expect(treeBytes(s.root)).toEqual(before);
  });

  it.each(["B1", "native-hold", "held-OFD", "CAS", "lease", "final-mount", "lifetime"])("custody inode replacement across %s stays refused after restoration", async phase => {
    for (const kind of ["generation", "lock", "directory"]) {
      const s = await setup();
      // A separately provisioned existing custody directory also exercises its pinned route.
      const directory = path.join(s.root, "existing-custody"); fs.mkdirSync(directory);
      fs.renameSync(s.generationFile, path.join(directory, "custody.gen")); fs.renameSync(s.lockFile, path.join(directory, "custody.ofd"));
      record(s).custodyDirectory = directory; record(s).generationFile = path.join(directory, "custody.gen"); record(s).lockFile = path.join(directory, "custody.ofd");
      s.input.bootstrap.generationFile = record(s).generationFile; s.input.bootstrap.lockFile = record(s).lockFile;
      // CAS fixture's audit reads its original gen name; forward that read from the provisioned route.
      s.input.identityRecord.casHighWater = async (snapshot, next, signal) => {
        signal.throwIfAborted(); expect(snapshot.revision).toBe(record(s).revision);
        record(s).highWater = next; record(s).revision = Symbol("next-native-revision");
        return { status: "confirmed", snapshot: record(s) };
      };
      const file = kind === "generation" ? record(s).generationFile : kind === "lock" ? record(s).lockFile : directory;
      const saved = path.join(s.root, "retained-replaced-custody"); let injected = false;
      const replace = () => {
        if (injected) return; injected = true; fs.renameSync(file, saved);
        if (kind === "directory") {
          fs.mkdirSync(file); fs.writeFileSync(path.join(file, "custody.gen"), "8\n"); fs.writeFileSync(path.join(file, "custody.ofd"), "", { mode: 0o600 });
        } else fs.writeFileSync(file, kind === "generation" ? "8\n" : "", { mode: 0o600 });
      };
      if (phase === "B1") {
        const read = s.input.b1.readCompletedForSelf;
        s.input.b1.readCompletedForSelf = async (...args) => { const result = await read(...args); replace(); return result; };
      } else if (phase === "native-hold") {
        const hold = s.input.nativeCustody.hold;
        s.input.nativeCustody.hold = async (...args) => { const result = await hold(...args); replace(); return result; };
      } else if (phase === "CAS") {
        const cas = s.input.identityRecord.casHighWater;
        s.input.identityRecord.casHighWater = async (...args) => { const result = await cas(...args); replace(); return result; };
      } else if (phase === "lease") {
        const acquire = s.input.leaseOwner.acquireOnce;
        s.input.leaseOwner.acquireOnce = async (...args) => { const result = await acquire(...args); replace(); return result; };
      } else if (phase !== "lifetime") {
        const mounted = s.input.nativeCustody.readMountedAtOpen; let reads = 0;
        s.input.nativeCustody.readMountedAtOpen = async (...args) => {
          const result = await mounted(...args); if (++reads === 2 && phase === "held-OFD" || s.acquired > 0 && phase === "final-mount") replace(); return result;
        };
      }
      const binding = s.bind(createPortablePreOpenBinding);
      let first: Error;
      if (phase === "lifetime") {
        const current = await binding.preOpen.excludeWriter({ sessionFile: s.sessionFile }); replace();
        let observed: unknown; try { current.assertCurrent(); } catch (cause) { observed = cause; }
        first = await failure(Promise.reject(observed));
      } else first = await failure(binding.preOpen.excludeWriter({ sessionFile: s.sessionFile }));
      expect(injected).toBe(true);
      fs.renameSync(file, path.join(s.root, "retained-replacement")); fs.renameSync(saved, file);
      await expect(binding.preOpen.excludeWriter({ sessionFile: s.sessionFile })).rejects.toBe(first);
      expect(s.historyOpens).toEqual({ native: 0, b5: 0, b6: 0 });
      if (["B1", "native-hold", "held-OFD"].includes(phase)) {
        expect(record(s).highWater).toBe(7); expect(fs.readFileSync(record(s).generationFile, "utf8")).toBe("7\n"); expect(s.acquired).toBe(0);
      }
      if (phase === "CAS") expect(s.acquired).toBe(0);
    }
  });

  it("REQUIRED custody projection given to B1 is closed/frozen while native CAS receives the actual original snapshot", async () => {
    const s = await setup(), read = s.input.identityRecord.readRequired, b1 = s.input.b1.readCompletedForSelf;
    let original: unknown;
    s.input.identityRecord.readRequired = async (...args) => { const result = await read(...args); original = result; return result; };
    s.input.b1.readCompletedForSelf = async (...args) => {
      const required = args[0]; expect(Object.isFrozen(required)).toBe(true);
      expect(required.custodyDirectory).toBe(s.root); expect(required.generationFile).toBe(s.generationFile); expect(required.lockFile).toBe(s.lockFile);
      expect(() => Object.defineProperty(required, "lockFile", { value: path.join(s.root, "other-lock") })).toThrow();
      return b1(...args);
    };
    const cas = s.input.identityRecord.casHighWater;
    s.input.identityRecord.casHighWater = async (...args) => { expect(args[0]).toBe(original); return cas(...args); };
    const binding = s.bind(createPortablePreOpenBinding), current = await binding.preOpen.excludeWriter({ sessionFile: s.sessionFile }); current.assertCurrent();
    expect(s.cas).toEqual([{ before: 7, next: 8, disk: "8\n" }]);
  });

  it("atomic native session replacement remains valid while custody inode/route is pinned", async () => {
    const s = await setup(), binding = s.bind(createPortablePreOpenBinding);
    const current = await binding.preOpen.excludeWriter({ sessionFile: s.sessionFile });
    const replacement = path.join(s.root, "native-replacement"); fs.writeFileSync(replacement, fs.readFileSync(s.sessionFile));
    const old = fs.statSync(s.sessionFile).ino; fs.renameSync(replacement, s.sessionFile);
    expect(fs.statSync(s.sessionFile).ino).not.toBe(old); expect(() => current.assertCurrent()).not.toThrow();
    expect(s.acquired).toBe(1); expect(s.cas).toHaveLength(1);
  });

  it.each(["bootstrap", "B1", "native", "CAS", "lease", "OFD"])("unrelated %s failure is not mislabeled as history incomplete", async kind => {
    const s = await setup(), original = new Error(`original ${kind} fault`);
    if (kind === "bootstrap") s.input.bootstrap.identityKey = "";
    if (kind === "B1") s.input.b1.readCompletedForSelf = async () => { throw original; };
    if (kind === "native") s.input.nativeCustody.hold = async () => { throw original; };
    if (kind === "CAS") s.input.identityRecord.casHighWater = async () => { throw original; };
    if (kind === "lease") s.input.leaseOwner.acquireOnce = async () => { throw original; };
    if (kind === "OFD") fs.chmodSync(s.lockFile, 0o644); // Same safe inode/mount, but actual OFD permissions refuse.
    const binding = s.bind(createPortablePreOpenBinding);
    const error: unknown = await binding.preOpen.excludeWriter({ sessionFile: s.sessionFile }).then(() => undefined, (cause: unknown) => cause);
    expect(error).toBeInstanceOf(Error); expect(error).not.toHaveProperty("code", "RECOVERY_HISTORY_INCOMPLETE");
    if (kind !== "bootstrap" && kind !== "OFD") {
      if (!(error instanceof Error)) throw new Error("expected unrelated original failure");
      expect(error.cause).toBe(original);
    }
    await expect(binding.preOpen.excludeWriter({ sessionFile: s.sessionFile })).rejects.toBe(error);
  });

  it("REQUIRED real OFDLCK survives TTL and pending native join; duplicate startup contends until final close", async () => {
    const s = await setup(), binding = s.bind(createPortablePreOpenBinding);
    const current = await binding.preOpen.excludeWriter({ sessionFile: s.sessionFile });
    const descriptions = () => fs.readdirSync("/proc/self/fd").filter(name => {
      try { return fs.readlinkSync(`/proc/self/fd/${name}`) === s.lockFile &&
        /OFDLCK\s+ADVISORY\s+WRITE\s+-1\s+/.test(fs.readFileSync(`/proc/self/fdinfo/${name}`, "utf8")); }
      catch { return false; }
    });
    expect(descriptions()).toHaveLength(1);
    s.clock = 13_001; expect(binding.borrowLease().lease.live()).toBe(false);
    current.assertCurrent(); expect(descriptions()).toHaveLength(1);
    const duplicate = s.bind(createPortablePreOpenBinding);
    const rejected: unknown = await duplicate.preOpen.excludeWriter({ sessionFile: s.sessionFile }).then(() => undefined, (cause: unknown) => cause);
    expect(rejected).toBeInstanceOf(Error); expect(rejected).not.toHaveProperty("code", "RECOVERY_HISTORY_INCOMPLETE");
    expect(s.cas).toHaveLength(1); expect(s.acquired).toBe(1); expect(descriptions()).toHaveLength(1);
    await duplicate.closeAfterNativeJoin(Promise.resolve());
    const joined = s.gate(), closing = binding.closeAfterNativeJoin(joined.promise);
    await Promise.resolve(); expect(descriptions()).toHaveLength(1);
    joined.resolve(); await closing; expect(descriptions()).toEqual([]);
    expect(() => current.assertCurrent()).toThrow();
  });

  it("loss immediately before the real OFD open cannot provision missing REQUIRED custody", async () => {
    const s = await setup(), open = fs.openSync, saved = path.join(s.root, "retained-lock-before-ofd-open");
    let injected = false;
    vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
      if (!injected && typeof flags === "number" && (flags & fs.constants.O_RDWR) !== 0) {
        let target = "";
        try { target = fs.realpathSync(file); } catch { /* Unrelated create target. */ }
        if (target === s.lockFile) { injected = true; fs.renameSync(s.lockFile, saved); }
      }
      return open(file, flags, mode);
    });
    const binding = s.bind(createPortablePreOpenBinding);
    await failure(binding.preOpen.excludeWriter({ sessionFile: s.sessionFile }));
    expect(injected).toBe(true);
    expect(fs.existsSync(s.lockFile)).toBe(false);
    expect(fs.statSync(saved).isFile()).toBe(true); expect(fs.readFileSync(s.generationFile, "utf8")).toBe("7\n");
    expect(s.cas).toEqual([]); expect(s.acquired).toBe(0);
  });

  it.each(custodyFields)("original REQUIRED %s mutation during B1 wait cannot alter the pinned native CAS projection", async field => {
    const s = await setup(), read = s.input.identityRecord.readRequired, b1 = s.input.b1.readCompletedForSelf;
    let mutate: () => void = () => { throw new Error("original record was not read"); };
    s.input.identityRecord.readRequired = async (...args) => {
      const snapshot = await read(...args);
      if (!snapshot) throw new Error("missing test original record");
      mutate = () => { snapshot[field] = path.join(s.root, "mutated-original-custody"); };
      return snapshot;
    };
    s.input.b1.readCompletedForSelf = async (...args) => { const result = await b1(...args); mutate(); return result; };
    const binding = s.bind(createPortablePreOpenBinding), first = await failure(binding.preOpen.excludeWriter({ sessionFile: s.sessionFile }));
    await expect(binding.preOpen.excludeWriter({ sessionFile: s.sessionFile })).rejects.toBe(first);
    expect(fs.readFileSync(s.generationFile, "utf8")).toBe("8\n"); expect(s.cas).toEqual([]); expect(s.acquired).toBe(0);
  });

  it("generation currentness failure during native CAS is typed and does not proceed to reclaim/lease", async () => {
    const s = await setup(), cas = s.input.identityRecord.casHighWater;
    s.input.identityRecord.casHighWater = async (...args) => {
      const result = await cas(...args); fs.writeFileSync(s.generationFile, "9\n"); return result;
    };
    const binding = s.bind(createPortablePreOpenBinding), first = await failure(binding.preOpen.excludeWriter({ sessionFile: s.sessionFile }));
    fs.writeFileSync(s.generationFile, "8\n");
    await expect(binding.preOpen.excludeWriter({ sessionFile: s.sessionFile })).rejects.toBe(first);
    expect(s.cas).toHaveLength(1); expect(s.acquired).toBe(0); expect(s.kv.writes).toEqual([]);
    expect(s.historyOpens).toEqual({ native: 0, b5: 0, b6: 0 });
  });

  it.each(custodyFields)("confirmed and unknown CAS cannot change custody projection %s", async field => {
    for (const unknown of [false, true]) {
      const s = await setup(), cas = s.input.identityRecord.casHighWater;
      s.input.identityRecord.casHighWater = async (...args) => {
        const reply = await cas(...args);
        record(s)[field] = path.join(s.root, "changed-custody");
        return unknown ? { status: "unknown" } : reply;
      };
      const binding = s.bind(createPortablePreOpenBinding);
      const first = await failure(binding.preOpen.excludeWriter({ sessionFile: s.sessionFile }));
      await expect(binding.preOpen.excludeWriter({ sessionFile: s.sessionFile })).rejects.toBe(first);
      expect(fs.readFileSync(s.generationFile, "utf8")).toBe("8\n");
      expect(s.cas).toHaveLength(1); expect(s.acquired).toBe(0); expect(s.kv.writes).toEqual([]);
      expect(s.historyOpens).toEqual({ native: 0, b5: 0, b6: 0 });
    }
  });

  it("F001 actual wrong-mount B5 file refuses metadata-only before B1; original same-mount control remains valid", async context => {
    const s = await setup(), external = fs.realpathSync(path.resolve("tsconfig.json"));
    const metadata = [kernelMetadata(s.sessionFile), kernelMetadata(external)];
    if (metadata[0]?.mount === metadata[1]?.mount) {
      process.stdout.write(`M2_F001_B5_UNAVAILABLE ${JSON.stringify(metadata)}\n`); context.skip(); return;
    }
    record(s).b5File = external;
    const original = fs.readFileSync(external), before = treeBytes(s.root);
    const retained = watchRetainedIO(s), extra = watchExternal(path.dirname(external));
    let observed: unknown;
    try {
      observed = await s.bind(createPortablePreOpenBinding).preOpen.excludeWriter({ sessionFile: s.sessionFile })
        .then(() => undefined, (cause: unknown) => cause);
    } finally { extra.stop(); retained.stop(); }
    process.stdout.write(`M2_F001_B5_ACTUAL ${JSON.stringify({ metadata, refused: observed instanceof Error,
      causes: causes(observed).map(error => error.message), generation: fs.readFileSync(s.generationFile, "utf8"),
      events: s.events, cas: s.cas, acquired: s.acquired, reads: retained.reads, writes: retained.writes,
      externalReads: extra.reads, externalWrites: extra.writes })}\n`);
    await failure(Promise.reject(observed));
    expect(retained.reads).toEqual([]); expect(retained.writes).toEqual([]);
    expect(extra.reads).toEqual([]); expect(extra.writes).toEqual([]);
    noEffects(s); expect(treeBytes(s.root)).toEqual(before); expect(fs.readFileSync(external)).toEqual(original);
  });

  for (const kind of ["B6", "transport"]) {
    it(`F001 actual wrong-mount ${kind} root refuses before any leaf lookup or B1`, async context => {
    const s = await setup(), directory = "/run";
    let metadata: ReturnType<typeof kernelMetadata>[];
    try { metadata = [kernelMetadata(s.sessionFile), kernelMetadata(directory)]; }
    catch (cause) { process.stdout.write(`M2_F001_ROOT_UNAVAILABLE ${String(cause)}\n`); context.skip(); return; }
    if (metadata[0]?.mount === metadata[1]?.mount) { context.skip(); return; }
    const required = record(s);
    if (kind === "B6") {
      required.b6Directory = directory;
      required.locks = required.locks.map(lock => lock.kind === "b6" ? { ...lock, directory: path.join(directory, "custody.lock") } : lock);
    } else {
      required.stateDirectory = directory;
      required.locks = required.locks.map(lock => lock.kind === "transport-receipt"
        ? { ...lock, directory: path.join(`${path.join(directory, path.basename(s.receiptFile))}.custody`, "custody.lock") } : lock);
    }
    // This sentinel prevents a defective baseline from reaching any shared-root writer.
    s.input.b1.readCompletedForSelf = async () => { s.events.push("b1:unexpected"); throw new Error("test stop before shared-root effects"); };
    const before = treeBytes(s.root), watch = watchRetainedIO(s);
    let observed: unknown;
    try { observed = await s.bind(createPortablePreOpenBinding).preOpen.excludeWriter({ sessionFile: s.sessionFile }).then(() => undefined, (cause: unknown) => cause); }
    finally { watch.stop(); }
    process.stdout.write(`M2_F001_ROOT_ACTUAL ${JSON.stringify({ kind, metadata, causes: causes(observed).map(error => error.message), events: s.events })}\n`);
    await failure(Promise.reject(observed));
    expect(causes(observed).some(error => error.message.includes("required retained directory is not on the bound session mount"))).toBe(true);
    expect(watch.reads).toEqual([]); expect(watch.writes).toEqual([]);
    noEffects(s); expect(treeBytes(s.root)).toEqual(before);
    });
  }

  it("F001 same-mount B5 outside the session subtree remains valid (no path-prefix proof)", async () => {
    const s = await setup(), external = outside();
    const metadata = [kernelMetadata(s.sessionFile), kernelMetadata(external.generationFile)];
    expect(metadata[0]?.mount).toBe(metadata[1]?.mount);
    record(s).b5File = external.generationFile;
    const binding = s.bind(createPortablePreOpenBinding), current = await binding.preOpen.excludeWriter({ sessionFile: s.sessionFile });
    current.assertCurrent();
    expect(fs.readFileSync(s.generationFile, "utf8")).toBe("8\n");
    expect(fs.readFileSync(external.generationFile, "utf8")).toBe("7\n"); expect(fs.existsSync(external.lockFile)).toBe(false);
    expect(s.cas).toEqual([{ before: 7, next: 8, disk: "8\n" }]); expect(s.acquired).toBe(1);
  });

  it.each(["B5", "B6-identity", "B6-segment", "transport-receipt"])("F001 %s leaf mount is rechecked using real foreign descriptors at metadata opens", async kind => {
    const external = fs.realpathSync(path.resolve("tsconfig.json")), original = fs.readFileSync(external);
    for (const phase of ["preflight", "B1", "native", "CAS", "lease", "lifetime"]) {
      const s = await setup();
      const raw = fs.readdirSync(s.b6).find(name => name.startsWith("raw-"));
      if (!raw) throw new Error("retained B6 segment missing from fixture");
      const target = kind === "B5" ? s.b5File : kind === "B6-identity" ? path.join(s.b6, "journal.identity")
        : kind === "B6-segment" ? path.join(s.b6, raw) : s.receiptFile;
      const metadata = [kernelMetadata(target), kernelMetadata(external)];
      expect(metadata[0]?.mount).not.toBe(metadata[1]?.mount);
      let armed = phase === "preflight", injected = 0;
      const arm = () => { armed = true; };
      if (phase === "B1") {
        const read = s.input.b1.readCompletedForSelf;
        s.input.b1.readCompletedForSelf = async (...args) => { const value = await read(...args); arm(); return value; };
      } else if (phase === "native") {
        const hold = s.input.nativeCustody.hold;
        s.input.nativeCustody.hold = async (...args) => { const value = await hold(...args); arm(); return value; };
      } else if (phase === "CAS") {
        const cas = s.input.identityRecord.casHighWater;
        s.input.identityRecord.casHighWater = async (...args) => { const value = await cas(...args); arm(); return value; };
      } else if (phase === "lease") {
        const acquire = s.input.leaseOwner.acquireOnce;
        s.input.leaseOwner.acquireOnce = async (...args) => { const value = await acquire(...args); arm(); return value; };
      }
      const open = fs.openSync;
      const spy = vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
        if (armed && typeof flags === "number" && (flags & 0x200000) !== 0) {
          let resolved = ""; try { resolved = fs.realpathSync(file); } catch { /* Other metadata target. */ }
          if (resolved === target) { injected++; return open(external, flags, mode); }
        }
        return open(file, flags, mode);
      });
      // Adversarial real-descriptor substitution, NOT a claim that this test made
      // a live bind mount. No fstat/fdinfo/mount identity is fabricated or mocked.
      const binding = s.bind(createPortablePreOpenBinding);
      let observed: unknown;
      try {
        if (phase === "lifetime") {
          const current = await binding.preOpen.excludeWriter({ sessionFile: s.sessionFile }); arm();
          try { current.assertCurrent(); } catch (cause) { observed = cause; }
        } else observed = await binding.preOpen.excludeWriter({ sessionFile: s.sessionFile }).then(() => undefined, (cause: unknown) => cause);
      } finally { spy.mockRestore(); }
      process.stdout.write(`M2_F001_LEAF_DESCRIPTOR ${JSON.stringify({ kind, phase, metadata, injected, causes: causes(observed).map(error => error.message) })}\n`);
      const first = await failure(Promise.reject(observed)); expect(injected).toBeGreaterThan(0);
      expect(causes(first).some(error => error.message.includes("not on the bound session mount"))).toBe(true);
      await expect(binding.preOpen.excludeWriter({ sessionFile: s.sessionFile })).rejects.toBe(first);
      expect(s.historyOpens).toEqual({ native: 0, b5: 0, b6: 0 });
      if (phase === "preflight") noEffects(s);
      if (["preflight", "B1", "native"].includes(phase)) {
        expect(fs.readFileSync(s.generationFile, "utf8")).toBe("7\n"); expect(s.cas).toEqual([]); expect(s.acquired).toBe(0);
      }
      if (phase === "CAS") expect(s.acquired).toBe(0);
    }
    expect(fs.readFileSync(external)).toEqual(original);
  });

  it.each(["initial-read", "write-open", "readback-pin"])("F002 generation loss at real %s open is coded, preserves raw ENOENT and stays sticky", async phase => {
    const s = await setup(), open = fs.openSync, saved = path.join(s.root, `generation-before-${phase}`);
    let injected = false, readOpens = 0, raw: unknown;
    vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
      let target = "";
      if (typeof flags === "number" && (flags & (0x200000 | fs.constants.O_DIRECTORY)) === 0) {
        try { target = fs.realpathSync(file); } catch { /* Other target. */ }
        if (target === s.generationFile) {
          const write = (flags & fs.constants.O_RDWR) !== 0;
          if (!write) readOpens++;
          if (!injected && (phase === "write-open" ? write : !write && readOpens === (phase === "initial-read" ? 1 : 2))) {
            injected = true; fs.renameSync(s.generationFile, saved);
            try { return open(file, flags, mode); } catch (cause) { raw = cause; throw cause; }
          }
        }
      }
      return open(file, flags, mode);
    });
    const binding = s.bind(createPortablePreOpenBinding);
    const observed: unknown = await binding.preOpen.excludeWriter({ sessionFile: s.sessionFile }).then(() => undefined, (cause: unknown) => cause);
    process.stdout.write(`M2_F002_ACTUAL ${JSON.stringify({ phase, injected, causes: causes(observed).map(error => error.message),
      rawCode: raw instanceof Error && "code" in raw ? raw.code : undefined,
      coded: observed instanceof Error && "code" in observed ? observed.code : undefined, events: s.events, cas: s.cas })}\n`);
    expect(injected).toBe(true); expect(raw).toBeInstanceOf(Error); expect(raw).toHaveProperty("code", "ENOENT");
    const first = await failure(Promise.reject(observed));
    expect(causes(first)).toContain(raw); expect(causes(first).some(error => error.message.includes("required retained stores are not current"))).toBe(true);
    expect(fs.existsSync(s.generationFile)).toBe(false);
    expect(fs.readFileSync(saved, "utf8")).toBe(phase === "readback-pin" ? "8\n" : "7\n");
    expect(s.cas).toEqual([]); expect(record(s).highWater).toBe(7); expect(s.acquired).toBe(0); expect(s.kv.writes).toEqual([]);
    expect(s.historyOpens).toEqual({ native: 0, b5: 0, b6: 0 });
    fs.renameSync(saved, s.generationFile);
    await expect(binding.preOpen.excludeWriter({ sessionFile: s.sessionFile })).rejects.toBe(first);
    expect(causes(first)).toContain(raw);
  });

  it.each(["generation-only", "lock-only", "both"])("bootstrap external %s refuses before any external or retained effects", async kind => {
    const s = await setup(), external = outside();
    const before = treeBytes(s.root), externalBefore = treeBytes(external.directory);
    const binding = s.bind(input => createPortablePreOpenBinding({ ...input, bootstrap: { ...input.bootstrap,
      generationFile: kind === "lock-only" ? input.bootstrap.generationFile : external.generationFile,
      lockFile: kind === "generation-only" ? input.bootstrap.lockFile : external.lockFile,
    } }));
    const retained = watchRetainedIO(s), extra = watchExternal(external.directory);
    let observed: unknown;
    try {
      observed = await binding.preOpen.excludeWriter({ sessionFile: s.sessionFile }).then(current => {
        current.assertCurrent(); return undefined;
      }, (cause: unknown) => cause);
      // Persist observed counterexample effects even when the desired refusal is RED.
      const read = fs.readFileSync;
      extra.stop(); retained.stop();
      const causes: string[] = [];
      for (let cause = observed; cause instanceof Error; cause = cause.cause) causes.push(cause.stack ?? cause.message);
      process.stdout.write(`M2_EXTERNAL_EFFECTS ${JSON.stringify({ kind, refused: observed instanceof Error, causes,
        externalGeneration: read(external.generationFile, "utf8"), originalGeneration: read(s.generationFile, "utf8"),
        externalLockExists: fs.existsSync(external.lockFile), externalReads: extra.reads, externalWrites: extra.writes,
        retainedReads: retained.reads, retainedWrites: retained.writes, events: s.events, cas: s.cas })}\n`);
      await failure(Promise.reject(observed));
      expect(extra.reads).toEqual([]); expect(extra.writes).toEqual([]);
      expect(retained.reads).toEqual([]); expect(retained.writes).toEqual([]);
    } finally { extra.stop(); retained.stop(); }
    noEffects(s); expect(treeBytes(s.root)).toEqual(before); expect(treeBytes(external.directory)).toEqual(externalBefore);
    expect(fs.existsSync(external.lockFile)).toBe(false);
  });
});
