// Keep reasons: ownership/data-safety boundary; exact encoded-budget arithmetic;
// whole-commit trimming can only be checked meaningfully through the ordinary CLI/store path.
import { describe, expect, it } from "vitest";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { pathToFileURL } from "node:url";
import { createHash } from "node:crypto";

const load = () => import(`../scripts/${"populated-mesh-state-fixture"}.mjs`);
const bytes = (value: unknown) => Buffer.byteLength(JSON.stringify(value));
const group = (prefix: string, rows: number, valueBytes: number) => ({ prefix, rows, valueBytes });
function small() {
  return { version: 1, tables: {
    kv: { rows: 7, prefixes: [group("group-a", 3, 41), group("group-b", 2, 17)], unlistedRows: 2 },
    changes: { rows: 4, prefixes: [group("group-a", 4, 0)] },
    tombstones: { rows: 3, prefixes: [group("group-b", 3, 0)] },
    meta: { rows: 17, prefixes: [group("fixture-meta", 17, 0)] },
  } };
}
const freshParent = () => {
  const parent = fs.mkdtempSync(path.join(os.tmpdir(), "populated-fixture-test-"));
  fs.chmodSync(parent, 0o700); return parent;
};
function large() {
  const profile = small();
  profile.tables.kv = { rows: 6270, prefixes: [group("group-a", 4000, 400000), group("group-b", 2240, 224000)], unlistedRows: 30 };
  profile.tables.changes = { rows: 4119, prefixes: [group("group-a", 2777, 0), group("group-b", 1342, 0)] };
  profile.tables.tombstones = { rows: 991, prefixes: [group("group-a", 600, 0), group("group-b", 391, 0)] };
  return profile;
}
const defaultIdentity = { id: "fixture-seeder", name: "Synthetic fixture", kind: "agent" };
const historicalOverlays = (prefix: string) => [1, 2].map(index => {
  const identity = { id: `fixture-history-${index}`, name: `Synthetic historical ${index}`, kind: "main", sessionId: `fixture-session-${index}` };
  return { key: `${prefix}/archive/history-${index}`, identity,
    value: { id: identity.id, name: identity.name, sessionId: identity.sessionId, kind: "root", remoteHost: "fixture-archive" } };
});
const receipt = (fixture: any, step: string) => {
  const found = fixture.receipts.find((row: any) => row.step === step);
  expect(found, step).toBeTruthy();
  return JSON.parse(fs.readFileSync(found.file, "utf8")).result;
};

describe("populated state allocator", () => {
  it("matches every prefix's JSON VALUE budget and explicitly allocates the unlisted remainder", async () => {
    const { buildStateSeed } = await load();
    const profile = small(), { state, manifest } = buildStateSeed(profile, { now: 123 });
    expect(state.format).toBe(1); expect(state.revisionFormat).toBe(2);
    expect(state).not.toHaveProperty("tombstones");
    const entries: any[] = Object.values(state.entries);
    expect(entries).toHaveLength(7);
    for (const expected of profile.tables.kv.prefixes) {
      const rows = entries.filter(row => row.key.startsWith(expected.prefix + "/"));
      expect(rows).toHaveLength(expected.rows);
      expect(rows.reduce((sum, row) => sum + bytes(row.value), 0)).toBe(expected.valueBytes);
      expect(Math.max(...rows.map(row => bytes(row.value))) - Math.min(...rows.map(row => bytes(row.value)))).toBeLessThanOrEqual(1);
      expect(rows.every(row => row.key.includes("/fixture/") && typeof row.value === "string")).toBe(true);
    }
    expect(manifest.expected.unlisted).toEqual({ namespace: "fixture-unlisted", rows: 2, valueBytes: 4 });
    expect(entries.filter(row => row.key.startsWith("fixture-unlisted/")).map(row => row.value)).toEqual(["", ""]);
    expect(manifest.inputBytes).toBe(bytes(state));
    expect(manifest.stateBytes + 256).toBeLessThanOrEqual(32 * 1024 * 1024);
    expect(state.highWater).toBe(10);
    expect(new Set(Object.values(state.versions)).size).toBe(10);
    expect(state.tombstoneOrder).toHaveLength(3);
    expect(new Set(state.tombstoneOrder).size).toBe(3);
    for (const row of entries) {
      expect(row.version).toBe(state.versions[row.key]);
      expect(row.version).toBeGreaterThan(0); expect(Number.isSafeInteger(row.version)).toBe(true);
      expect(row.updatedAt).toBe(123); expect(row.updatedBy.kind).toBe("agent");
    }
    for (const key of state.tombstoneOrder) {
      expect(state.entries[key]).toBeUndefined();
      expect(state.versions[key]).toBeGreaterThan(7);
      expect(state.versions[key]).toBeLessThanOrEqual(state.highWater);
    }
  });

  it("keeps omitted identities byte-for-byte compatible with the frozen baseline", async () => {
    const { buildStateSeed } = await load();
    // SHA-256 of the complete deterministic result from the frozen pre-change helper.
    const hash = (value: unknown) => createHash("sha256").update(JSON.stringify(value)).digest("hex");
    expect(hash(buildStateSeed(small(), { now: 123 }))).toBe("93bbc58f2fe24295a30a16ecd87524687a8561214f8a95cb37954eb8003c7d63");
    const overlay = { key: "group-a/archive/one", value: "" };
    const omitted = buildStateSeed(small(), { now: 123, overlays: [overlay] });
    expect(hash(omitted)).toBe("0299774c81072d2a27c732b515ea3db58b4558c724a4114abe7a9d8b87c9d78c");
    expect(buildStateSeed(small(), { now: 123, overlays: [{ ...overlay, identity: defaultIdentity }] })).toEqual(omitted);
    expect(buildStateSeed(small(), { now: 123, overlays: [{ ...overlay, identity: undefined }] })).toEqual(omitted);
  });

  it("captures distinct synthetic main identities and accounts for their actual UTF-8 bytes", async () => {
    const { buildStateSeed } = await load(), profile = small(), overlays = historicalOverlays("group-a");
    overlays[0]!.identity.name += " é";
    profile.tables.kv.prefixes[0]!.valueBytes = overlays.reduce((sum, row) => sum + bytes(row.value), 0) + 2;
    const defaults = buildStateSeed(profile, { overlays: overlays.map(({ key, value }) => ({ key, value })) });
    const { state, manifest } = buildStateSeed(profile, { now: 123, overlays });
    const expected = JSON.parse(JSON.stringify(overlays));
    for (const row of overlays) {
      expect(state.entries[row.key].updatedBy).toEqual(row.identity);
      expect(state.entries[row.key].updatedBy).not.toBe(row.identity);
    }
    expect(manifest.stateBytes - defaults.manifest.stateBytes).toBe(overlays.reduce((sum, row) => sum + bytes(row.identity) - bytes(defaultIdentity), 0));
    expect(manifest.inputBytes).toBe(bytes(state));
    for (const entry of Object.values(state.entries) as any[]) {
      if (!overlays.some(row => row.key === entry.key)) expect(entry.updatedBy).toEqual(defaultIdentity);
    }
    overlays[0]!.identity.id = "caller-mutated";
    overlays[0]!.identity.sessionId = "caller-mutated-session";
    expect(state.entries[expected[0].key].updatedBy).toEqual(expected[0].identity);
    state.entries[expected[1].key].updatedBy.name = "seed-mutated";
    expect(overlays[1]!.identity).toEqual(expected[1].identity);
    const shared = { id: "shared", name: "", kind: "actor" };
    const captured = buildStateSeed(profile, { overlays: overlays.map(row => ({ ...row, identity: shared })) });
    expect(captured.state.entries[overlays[0]!.key].updatedBy).not.toBe(captured.state.entries[overlays[1]!.key].updatedBy);
    expect(captured.state.entries[overlays[0]!.key].updatedBy).not.toHaveProperty("sessionId");
  });

  it("rejects malformed, non-JSON, bridge-verified and over-budget identities before database work", async () => {
    const { buildStateSeed } = await load();
    const cyclic: any = { ...defaultIdentity }; cyclic.self = cyclic;
    for (const identity of [null, [], "agent", 1, {}, { ...defaultIdentity, id: "" },
      { ...defaultIdentity, id: 1 }, { ...defaultIdentity, name: undefined }, { ...defaultIdentity, kind: "root" },
      { ...defaultIdentity, sessionId: null }, { ...defaultIdentity, sessionId: 1 },
      { ...defaultIdentity, verified: "bridge" }, { ...defaultIdentity, extra: 1n }, cyclic,
      { toJSON: () => null }]) {
      expect(() => buildStateSeed(small(), { overlays: [{ key: "group-a/archive/one", value: "", identity }] })).toThrow();
    }
    expect(() => buildStateSeed(small(), { overlays: [{ key: "group-a/archive/one", value: "",
      identity: { ...defaultIdentity, name: "x".repeat(32 * 1024 * 1024) } }] })).toThrow(/state budget/);
  });

  it("aliases identifier-like namespaces by ordinal, including deletion/change mappings", async () => {
    const { buildStateSeed } = await load(), profile = small();
    const label = "archive." + "abcdef012345";
    profile.tables.kv.prefixes[0]!.prefix = label;
    profile.tables.changes.prefixes[0]!.prefix = label;
    const { state, manifest } = buildStateSeed(profile);
    const allocation = manifest.allocations[0];
    expect(allocation).toMatchObject({ ordinal: 0, aliased: true, rows: 3, valueBytes: 41 });
    expect(Object.keys(state.entries).some(key => key.startsWith(label))).toBe(false);
    expect(manifest.expected.changesPrefixes[0].namespace).toBe(allocation.namespace);
    expect(JSON.stringify(manifest)).not.toContain(label);
  });

  it("subtracts exact overlay bytes (including Unicode), without mutating caller values", async () => {
    const { buildStateSeed } = await load(), profile = small();
    const value = { archived: true, note: "é" }, original = JSON.stringify(value);
    profile.tables.kv.prefixes[0]!.valueBytes = bytes(value) + 9;
    const { state, manifest } = buildStateSeed(profile, { overlays: [{ key: "group-a/archive/one", value }] });
    expect(state.entries["group-a/archive/one"].value).toEqual(value);
    expect(JSON.stringify(value)).toBe(original);
    expect(manifest.allocations[0]).toMatchObject({ overlayRows: 1, overlayBytes: bytes(value), fillerRows: 2 });
    expect(Object.values(state.entries).filter((row: any) => row.key.startsWith("group-a/"))
      .reduce((sum: number, row: any) => sum + bytes(row.value), 0)).toBe(profile.tables.kv.prefixes[0]!.valueBytes);
  });

  it("supports zero-filler archival padding only on explicit permission, preserving semantic fields", async () => {
    const { buildStateSeed } = await load(), profile = small();
    const overlays = Array.from({ length: 3 }, (_, i) => ({ key: `group-a/archive/${i}`, value: { archived: true, note: i }, pad: i === 2 }));
    const budget = profile.tables.kv.prefixes[0]!, padded = overlays[2]!;
    budget.valueBytes = overlays.reduce((sum, row) => sum + bytes(row.value), 0) + 57;
    const { state, manifest } = buildStateSeed(profile, { overlays });
    expect(manifest.allocations[0]).toMatchObject({ fillerRows: 0, paddingBytes: 57 });
    expect(state.entries[padded.key].value).toMatchObject(padded.value);
    expect(typeof state.entries[padded.key].value.fixturePadding).toBe("string");
    expect(padded.value).not.toHaveProperty("fixturePadding");
    expect(() => buildStateSeed(profile, { overlays: overlays.map(row => ({ ...row, pad: false })) })).toThrow(/padding/);
    budget.valueBytes -= 57;
    expect(buildStateSeed(profile, { overlays: overlays.map(row => ({ ...row, pad: false })) }).manifest.allocations[0].paddingBytes).toBe(0);
    budget.valueBytes += 1;
    expect(() => buildStateSeed(profile, { overlays })).toThrow(/unspent/);
  });

  it("allows a fully overlaid dense archival group with no remaining ballast rows", async () => {
    const { buildStateSeed } = await load(), profile = small();
    const overlays = Array.from({ length: 851 }, (_, index) => ({ key: `group-a/archive/${index}`,
      value: { archived: true, ordinal: index }, pad: true }));
    profile.tables.kv.rows = 855;
    profile.tables.kv.prefixes[0]!.rows = overlays.length;
    profile.tables.kv.prefixes[0]!.valueBytes = overlays.reduce((sum, item) => sum + bytes(item.value), 0) + 12345;
    const { state, manifest } = buildStateSeed(profile, { overlays });
    expect(manifest.allocations[0]).toMatchObject({ overlayRows: 851, fillerRows: 0, paddingBytes: 12345 });
    for (const item of overlays) expect(state.entries[item.key].value).toMatchObject(item.value);
    expect(Object.keys(state.entries).filter(key => key.startsWith("group-a/fixture/"))).toHaveLength(0);
  });

  it.each([
    ["profile version", (p: any) => { p.version = 2; }],
    ["row undercount", (p: any) => { p.tables.kv.rows = 4; }],
    ["fractional count", (p: any) => { p.tables.kv.rows = 7.5; }],
    ["unsafe count", (p: any) => { p.tables.kv.rows = Number.MAX_SAFE_INTEGER + 1; }],
    ["negative budget", (p: any) => { p.tables.kv.prefixes[0].valueBytes = -1; }],
    ["too-small budget", (p: any) => { p.tables.kv.prefixes[0].valueBytes = 5; }],
    ["duplicate prefix", (p: any) => { p.tables.kv.prefixes[1].prefix = "group-a"; }],
    ["incorrect unlisted assumption", (p: any) => { p.tables.kv.unlistedRows = 30; }],
    ["incorrect listed total", (p: any) => { p.tables.kv.listedPrefixRows = 7; }],
    ["unmapped changes", (p: any) => { p.tables.changes.prefixes[0].prefix = "absent"; }],
    ["unlisted tombstone shape", (p: any) => { p.tables.tombstones.rows = 4; }],
    ["tombstone cap", (p: any) => { p.tables.tombstones.rows = 1001; p.tables.tombstones.prefixes[0].rows = 1001; }],
    ["missing tombstone schema", (p: any) => { p.tables.tombstones = undefined; }],
    ["64 KiB encoded value", (p: any) => { p.tables.kv.prefixes[0].valueBytes = 3 * 65537; }],
    ["32 MiB aggregate budget", (p: any) => {
      p.tables.kv.rows = 602;
      for (const item of p.tables.kv.prefixes) { item.rows = 300; item.valueBytes = 18000000; }
    }],
  ])("rejects malformed %s before any database exists", async (_label, mutate) => {
    const { buildStateSeed } = await load(), profile = small(); mutate(profile);
    expect(() => buildStateSeed(profile)).toThrow();
  });

  it("rejects unsafe clocks, duplicate/oversized/unmapped overlays and live/deleted overlap", async () => {
    const { buildStateSeed } = await load(), profile = small();
    for (const now of [-1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) expect(() => buildStateSeed(profile, { now })).toThrow();
    for (const overlays of [
      [{ key: "missing/item", value: "" }],
      [{ key: "group-a/item", value: "" }, { key: "group-a/item", value: "" }],
      [{ key: "group-a/item", value: "x".repeat(65536) }],
      [{ key: "group-a/item", value: "x".repeat(100) }],
      [{ key: "group-b/fixture-deleted/000000", value: "" }],
      [{ key: "group-a/item", value: undefined }],
    ]) expect(() => buildStateSeed(profile, { overlays })).toThrow();
    const many = Array.from({ length: 4 }, (_, i) => ({ key: `group-a/${i}`, value: "" }));
    expect(() => buildStateSeed(profile, { overlays: many })).toThrow(/count/);
  });
});

describe("owned ordinary CLI/store preparation", () => {
  it("refuses external roots, reused parents and forged inspection fixtures", async () => {
    const { createPopulatedMesh, inspectPopulatedMesh } = await load();
    expect(() => inspectPopulatedMesh({ root: os.tmpdir() })).toThrow(/helper-returned/);
    const parent = freshParent(), candidate = path.resolve(import.meta.dirname, "..");
    fs.writeFileSync(path.join(parent, "not-empty"), "sentinel", { mode: 0o600 });
    await expect(createPopulatedMesh({ candidate, parent, profile: large(), eventBytes: 0 })).rejects.toThrow(/fresh and empty/);
    await expect(createPopulatedMesh({ candidate, parent, profile: large(), root: parent })).rejects.toThrow(/no existing root/);
    expect(fs.readdirSync(parent)).toEqual(["not-empty"]);
  });

  it("imports, genuinely exports/reimports, aligns single puts, publishes events, and reopens exact shape", async () => {
    const { createPopulatedMesh, inspectPopulatedMesh } = await load();
    // Optional private aggregate metadata is supplied at invocation, never committed here.
    const profile = process.env.POPULATED_FIXTURE_PROFILE
      ? JSON.parse(fs.readFileSync(process.env.POPULATED_FIXTURE_PROFILE, "utf8")) : large();
    // Keep the first group's ballast first: the two feed-alignment sacrificial puts retain
    // the default identity; both historical rows also receive ordinary target reseed puts.
    const prefix = profile.tables.kv.prefixes.find((row: any) => row.prefix === "topology")?.prefix
      ?? profile.tables.kv.prefixes[1].prefix;
    const overlays = historicalOverlays(prefix);
    const expectedOverlays = JSON.parse(JSON.stringify(overlays));
    const parent = freshParent(), candidate = path.resolve(import.meta.dirname, "..");
    const originalPrepare = DatabaseSync.prototype.prepare;
    const connections = new Set<DatabaseSync>();
    DatabaseSync.prototype.prepare = function (...args: Parameters<DatabaseSync["prepare"]>) {
      connections.add(this); return originalPrepare.apply(this, args);
    };
    const originalEnv = { ...process.env };
    process.env.PI_FABRIC_TEST_PAUSE_AFTER_RESERVE = path.join(parent, "must-not-read-inherited-pause");
    process.env.PI_FABRIC_MESH_ROOT = path.join(parent, "must-not-select-inherited-mesh");
    const beforeEnv = { ...process.env };
    let fixture: any;
    try {
      fixture = await createPopulatedMesh({ candidate, parent, profile, overlays, eventBytes: 64000 });
      DatabaseSync.prototype.prepare = originalPrepare;
      expect(connections.size).toBeGreaterThan(0);
      for (const db of connections) expect(() => originalPrepare.call(db, "SELECT 1")).toThrow(/closed|not open/i);
      expect({ ...process.env }).toEqual(beforeEnv);
      const seed = JSON.parse(fs.readFileSync(path.join(parent, "seed.json"), "utf8"));
      for (const row of expectedOverlays) expect(seed.entries[row.key].updatedBy).toEqual(row.identity);
      overlays[0]!.identity.name = "caller changed after preparation";
      const initial = receipt(fixture, "import-shape"), reimport = receipt(fixture, "reimport-shape");
      expect(initial.logical.counts).toEqual({ kv: 6270, changes: 6270, tombstones: 991, meta: 13 });
      expect(reimport.logical.counts.meta).toBe(17);
      expect(reimport.logical.valueDigest).toBe(initial.logical.valueDigest);
      expect(reimport.logical.meta.store_id).toBe(initial.logical.meta.store_id);
      expect(reimport.logical.meta.epoch).toBe(3);
      for (const name of ["rollback_from_epoch", "export_digest", "export_generation", "exported_at"]) expect(reimport.logical.meta[name]).toBeDefined();
      expect(receipt(fixture, "rollback").output.digest).toBe(reimport.logical.meta.export_digest);
      expect(receipt(fixture, "reimport").output.digest).toBe(reimport.logical.meta.import_digest);
      expect(fixture.manifest.feedPreparation.sacrificialSinglePuts).toBe(2);
      // This is a real intermediate observation after 4119 ordinary production puts, not a trim mock.
      expect(receipt(fixture, "only-target-count-puts").logical.counts.changes).toBe(4117);
      const baseline = inspectPopulatedMesh(fixture);
      expect(baseline.logical.counts).toEqual({ kv: 6270, changes: 4119, tombstones: 991, meta: 17 });
      expect(baseline.logical.feed).toEqual({ firstSeq: 6273, lastSeq: 10391, firstCommit: 5, lastCommit: 4123, commits: 4119, deleted: 0 });
      expect(baseline.logical.meta.high_water).toBe(11382);
      expect(baseline.logical.tombstones).toEqual({ ords: 991, firstOrd: 1, lastOrd: 991 });
      expect(baseline.logical.tombstoneDigest).toBe(initial.logical.tombstoneDigest);
      expect(baseline.logical.valueDigest).toBe(initial.logical.valueDigest);
      expect(Object.values(baseline.logical.invalid)).toEqual([0, 0, 0, 0]);
      const actualKv = new Map(baseline.logical.prefixes.kv.map((row: any) => [row.namespace, row]));
      for (const allocation of fixture.manifest.allocations) {
        const source = profile.tables.kv.prefixes[allocation.ordinal];
        expect(actualKv.get(allocation.namespace)).toEqual({ namespace: allocation.namespace, rows: source.rows, valueBytes: source.valueBytes });
      }
      expect(actualKv.get(fixture.manifest.expected.unlisted.namespace)).toEqual(fixture.manifest.expected.unlisted);
      for (const policy of baseline.productionConnectionPolicies) expect(policy).toMatchObject({ journal_mode: "wal", synchronous: 1,
        wal_autocheckpoint: 1000, journal_size_limit: 16777216, busy_timeout: 2 });
      expect(baseline.productionConnectionPolicies.length).toBeGreaterThanOrEqual(2);
      expect(baseline.pages.page_size).toBeGreaterThan(0);
      expect(baseline.physical["state.db"].bytes).toBeGreaterThan(0);
      expect(baseline.physical["state.db"].allocatedBytes).toBeGreaterThan(0);
      const originalExec = DatabaseSync.prototype.exec, verbs: string[] = [];
      DatabaseSync.prototype.exec = function (sql: string) { verbs.push(sql); return originalExec.call(this, sql); };
      let again: any;
      try { again = inspectPopulatedMesh(fixture); } finally { DatabaseSync.prototype.exec = originalExec; }
      expect(verbs).toEqual(["BEGIN", "COMMIT"]);
      expect(again.observerTotalChanges).toBe(0);
      expect(again.logicalDigest).toBe(baseline.logicalDigest);
      expect(receipt(fixture, "before-events").logicalDigest).toBe(receipt(fixture, "after-events").logicalDigest);
      const text = fs.readFileSync(path.join(fixture.root, "events.jsonl"), "utf8"), lines = text.trimEnd().split("\n");
      const events = lines.map(line => JSON.parse(line));
      expect(Buffer.byteLength(text)).toBe(fixture.manifest.events.actualBytes);
      expect(Buffer.byteLength(text)).toBeGreaterThanOrEqual(64000);
      expect(Buffer.byteLength(text) - 64000).toBeLessThanOrEqual(2048);
      expect(events).toHaveLength(fixture.manifest.events.count);
      expect(events.length).toBeGreaterThan(20);
      expect(new Set(events.map(event => event.id)).size).toBe(events.length);
      events.forEach((event, index) => {
        expect(event.sequence).toBe(index + 1); expect(event.topic).toBe("fixture.seed");
        expect(event.kind).toBe("fixture"); expect(event.from.id).toBe("fixture-seeder"); expect(event.to).toBeUndefined();
        expect(Buffer.byteLength(lines[index]!) + 1).toBeLessThanOrEqual(2048);
      });
      expect(() => inspectPopulatedMesh({ ...fixture })).toThrow(/helper-returned/);
      expect(Object.isFrozen(fixture.databaseIdentity)).toBe(true);
      // After the pre-worker baseline, legitimate runtime deletions must remain inspectable.
      const { MeshStore } = await import(pathToFileURL(path.join(candidate, "dist/mesh.js")).href);
      const runtime = new MeshStore(fixture.root, 65536, 1000, { stateBackend: "sqlite", lockTimeoutMs: 2000 });
      try {
        const identityProof = expectedOverlays.map((row: any) => {
          const actual = runtime.get(row.key, { fresh: true });
          expect(actual.updatedBy).toEqual(row.identity);
          expect(actual.value).toEqual(row.value);
          expect(actual.version).toBeGreaterThan(seed.entries[row.key].version);
          expect(actual.updatedAt).toBeGreaterThan(0); // Genuine put time, not fabricated commit age.
          for (const field of ["id", "name", "sessionId"]) expect(actual.updatedBy[field]).toBe(actual.value[field]);
          return actual;
        });
        expect(new Set(identityProof.map((row: any) => row.updatedBy.id)).size).toBe(2);
        const filler = runtime.listAll("", { fresh: true }).filter((row: any) => !expectedOverlays.some((overlay: any) => overlay.key === row.key));
        expect(filler.every((row: any) => JSON.stringify(row.updatedBy) === JSON.stringify(defaultIdentity))).toBe(true);
        fs.writeFileSync(path.join(parent, "evidence", "identity-roundtrip.json"), JSON.stringify({ identityProof,
          inProcessConnectionsClosedAfterCreation: connections.size, sacrificialIdentity: filler[0].updatedBy }), { flag: "wx", mode: 0o600 });
        const key = runtime.listAll("", { fresh: true })[0].key;
        await runtime.delete({ key, identity: { id: "fixture-window", name: "Synthetic window", kind: "agent" } });
      } finally { runtime.closeState(); }
      const postWindow = inspectPopulatedMesh(fixture);
      expect(postWindow.logical.counts.kv).toBe(6269);
      expect(postWindow.logical.counts.tombstones).toBe(992);
      expect(postWindow.logical.feed.deleted).toBe(1);
      expect(Object.values(postWindow.logical.invalid)).toEqual([0, 0, 0, 0]);
      expect(inspectPopulatedMesh(fixture).logicalDigest).toBe(postWindow.logicalDigest);
      fs.writeFileSync(path.join(parent, "evidence", "post-window-shape.json"), JSON.stringify(postWindow), { flag: "wx", mode: 0o600 });
      const realRoot = fixture.root;
      fixture.root = parent;
      expect(() => inspectPopulatedMesh(fixture)).toThrow(/root changed/);
      fixture.root = realRoot;
    } finally {
      DatabaseSync.prototype.prepare = originalPrepare;
      fixture?.close();
      process.env = originalEnv;
      // The prescribed test wrapper removes its disposable temp tree. Preserve raw evidence outside
      // that tree when the caller supplies a private evidence parent; never reopen copied databases.
      if (process.env.POPULATED_FIXTURE_EVIDENCE && fs.existsSync(path.join(parent, "evidence"))) {
        const retained = fs.mkdtempSync(path.join(process.env.POPULATED_FIXTURE_EVIDENCE, "fixture-evidence-"));
        fs.chmodSync(retained, 0o700);
        fs.mkdirSync(path.join(retained, "evidence"), { mode: 0o700 });
        const preserve = (from: string, to: string) => fs.writeFileSync(to, fs.readFileSync(from), { flag: "wx", mode: 0o600 });
        for (const name of fs.readdirSync(path.join(parent, "evidence"))) {
          preserve(path.join(parent, "evidence", name), path.join(retained, "evidence", name));
        }
        preserve(path.join(parent, "seed.json"), path.join(retained, "seed.json"));
        if (fixture) preserve(path.join(fixture.root, "events.jsonl"), path.join(retained, "events.jsonl"));
        console.log(JSON.stringify({ retainedEvidence: retained, ownership: fixture?.ownership, receipts: fixture?.receipts }));
      }
    }
    expect(() => inspectPopulatedMesh(fixture)).toThrow(/helper-returned/);
  }, 180000);
});
