// Keep reason: data safety and the authority/privacy trust boundary, exercised through the real CLI.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import crypto, { createHash } from "node:crypto";
import { syncBuiltinESMExports } from "node:module";
import { spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";
import { DatabaseSync } from "node:sqlite";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";

const libraryPath = path.resolve("scripts/lib/fleet-authority-snapshot.mjs");
let collect: (options: Record<string, unknown>) => Promise<any>;
let tokenHash: (raw: string) => string;
beforeAll(async () => { const module = await import(pathToFileURL(libraryPath).href); collect = module.collectAuthoritySnapshot; tokenHash = module.tokenHash; });
const roots: string[] = [];
afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const hash = (text: string) => createHash("sha256").update(text).digest("hex");
const ID = "session:11111111-2222-3333-4444-555555555555", ROOT = "session:66666666-7777-8888-9999-aaaaaaaaaaaa";
const SECRET = "UNRELATED-PRIVATE-BODY-DO-NOT-PARSE-9fcd";
const TOKEN = "secret-native-token-aaaaaaaa", OLD = "secret-superseded-token-bbbbbbbb";
const key = (prefix: string, id = ID) => `topology/${prefix}/${hash(id)}`;
const by = (id = ID) => ({ id, name: "Private Human Name", kind: "main", sessionId: ID.slice(8), verified: "bridge" });
const host = () => ({ format: 1, id: ID, rootId: ROOT, identity: by(), startedAt: 100, updatedAt: 200, expiresAt: 1000,
  incarnationToken: TOKEN, bridgeSupersededTokens: [OLD, TOKEN], bridgeSupersededTokenSeen: { [OLD]: 0, [TOKEN]: 1 }, remoteHost: "dev1", mirrorLease: 1 });
const participant = () => ({ format: 1, id: ID, rootId: ROOT, ownerHostId: ID, ownerIdentityId: ID, ownerIncarnation: TOKEN,
  kind: "root", name: "Private Participant Name", status: "idle", startedAt: 100, updatedAt: 200, capabilities: ["steer", "fabric"], transport: "host", controlProtocol: "v1",
  runner: "pi-durable", cwd: "/private/customer/path", model: "private-model", task: { body: SECRET }, usage: { private: SECRET }, mainProcess: { pid: 100, host: "dev1", startTime: TOKEN } });
const envelope = (prefix: string, value: unknown) => ({ key: key(prefix), value, updatedAt: 200, version: 7, updatedBy: by() });
const lease = () => ({ format: 1, id: ID, rootId: ROOT, identityId: ID, startedAt: 100, updatedAt: 200, expiresAt: 1000, incarnationToken: TOKEN,
  bridgeSupersededTokens: [OLD, TOKEN], bridgeSupersededTokenSeen: { [OLD]: 0, [TOKEN]: 1 }, session: { id: ID.slice(8), startedAt: 100, updatedAt: 200, expiresAt: 900 },
  writer: { pid: 100, host: "dev1", releaseSha: "a".repeat(40), lockProtocol: 3, stateBackend: "file", startedAt: 100 },
  mirror: { remoteHost: "dev1", observedAt: 300, sourceObservedAt: 290, sourceUpdatedAt: 200, sourceExpiresAt: 1000,
    bridgeProcess: { pid: 101, link: "dev1", machineId: OLD, bootId: OLD, incarnation: TOKEN } } });
function fixture() {
  const top = fs.mkdtempSync(path.join(os.tmpdir(), "authority-only-")); roots.push(top);
  const mesh = path.join(top, "mesh"), out = path.join(top, "out"); fs.mkdirSync(mesh, { mode: 0o700 }); fs.mkdirSync(out, { mode: 0o700 });
  fs.mkdirSync(path.join(mesh, "participants")); fs.mkdirSync(path.join(mesh, "host-leases"));
  const entries = { [key("hosts")]: envelope("hosts", host()), [key("participants")]: envelope("participants", participant()),
    "topology/liveness": { key: "topology/liveness", value: { version: 1, participants: "files", hostLeases: "files" }, version: 2, updatedAt: 200, updatedBy: by() },
    "actors/private-name": { private: { body: SECRET, tokens: [TOKEN], nested: [SECRET] } } };
  fs.writeFileSync(path.join(mesh, "state.json"), JSON.stringify({ format: 1, revisionFormat: 2, highWater: 9, entries, versions: { "private-key": 1 }, tombstoneOrder: ["private-name"] }));
  fs.writeFileSync(path.join(mesh, "participants", `${hash(ID)}.json`), JSON.stringify({ format: 1, ...envelope("participants", participant()) }));
  fs.writeFileSync(path.join(mesh, "host-leases", `${hash(ID).slice(0, 32)}.json`), JSON.stringify(lease()));
  const cursor = path.join(top, "private-cursor-location.json");
  fs.writeFileSync(cursor, JSON.stringify({ format: 1, local: "dev1", remote: "ryzen2", toRemote: { after: 12, mark: 6, offset: 4294967300, relayGrace: { sequence: 13, startedAt: 100, id: TOKEN } }, toLocal: { after: 6, mark: 12 } }));
  return { top, mesh, out, cursor, entries };
}
function options(f: ReturnType<typeof fixture>, extra = {}) { return { mesh: f.mesh, site: "work", name: "ryzen2", peer: "dev1", cursors: [f.cursor], ...extra }; }
const cli = path.resolve("scripts/fleet-authority-snapshot.mjs");
function args(f: ReturnType<typeof fixture>, site = "work", output = path.join(f.out, "snapshot.json"), census = false) {
  return [cli, ...(census ? ["--census"] : []), "--mesh", f.mesh, "--site", site, "--name", site === "hub" ? "dev1" : "ryzen2", "--peer", "dev1", "--cursor", f.cursor, "--output", output];
}
function run(f: ReturnType<typeof fixture>, site = "work", output?: string, census = false) { return spawnSync(process.execPath, args(f, site, output, census), { encoding: "utf8", timeout: 5000 }); }
function sqlite(f: ReturnType<typeof fixture>, wal = false) {
  fs.unlinkSync(path.join(f.mesh, "state.json"));
  const db = new DatabaseSync(path.join(f.mesh, "state.db"));
  if (wal) db.exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;");
  db.exec("CREATE TABLE meta(name TEXT PRIMARY KEY NOT NULL,value) WITHOUT ROWID; CREATE TABLE kv(key TEXT PRIMARY KEY NOT NULL,value TEXT NOT NULL,version INTEGER NOT NULL,updated_at INTEGER NOT NULL,updated_by TEXT NOT NULL,bytes INTEGER NOT NULL) WITHOUT ROWID;");
  const meta = db.prepare("INSERT INTO meta VALUES (?,?)"); for (const [name, value] of [["schema", 1], ["backend", "sqlite"], ["epoch", 1], ["commit_no", 3]] as const) meta.run(name, value);
  const put = db.prepare("INSERT INTO kv VALUES (?,?,?,?,?,?)");
  for (const entry of Object.values(f.entries)) {
    if (!("key" in entry)) { put.run("actors/private", JSON.stringify(entry), 1, 100, JSON.stringify(by()), 100); continue; }
    put.run(entry.key, JSON.stringify(entry.value), entry.version, entry.updatedAt, JSON.stringify(entry.updatedBy), 100);
  }
  return db;
}
function treeBytes(root: string): Record<string, string> {
  const output: Record<string, string> = {};
  for (const name of fs.readdirSync(root).sort()) { const file = path.join(root, name), stat = fs.lstatSync(file); if (stat.isDirectory()) for (const [sub, value] of Object.entries(treeBytes(file))) output[`${name}/${sub}`] = value; else if (stat.isFile()) output[name] = hash(fs.readFileSync(file).toString("base64")); }
  return output;
}
function censusInputs(f: ReturnType<typeof fixture>) {
  const badBridge = "bridge:bad-census", badTool = "spawn-router-extra", badPath = "/private/census/path";
  const first = host(); first.id = badBridge; first.rootId = badBridge;
  const second = { ...host(), id: ROOT, rootId: badBridge };
  const stateParticipant: any = participant(); stateParticipant.ownerHostId = badTool; stateParticipant.parentId = badPath;
  const policy = { key: "topology/liveness", value: { version: 1, participants: "files", hostLeases: "files" }, version: 2, updatedAt: 200, updatedBy: by("/private/policy-owner") };
  const entries: Record<string, any> = {
    [key("hosts", badBridge)]: { ...envelope("hosts", first), key: key("hosts", badBridge) },
    [key("hosts", ROOT)]: { ...envelope("hosts", second), key: key("hosts", ROOT) },
    [key("participants")]: { ...envelope("participants", stateParticipant), key: key("participants") },
    "topology/liveness": policy,
  };
  for (const name of Object.keys(f.entries)) delete f.entries[name];
  Object.assign(f.entries, entries);
  fs.writeFileSync(path.join(f.mesh, "state.json"), JSON.stringify({ format: 1, entries }));
  const participantFile: any = participant(); participantFile.ownerIdentityId = badTool;
  fs.writeFileSync(path.join(f.mesh, "participants", `${hash(ID)}.json`), JSON.stringify({ format: 1, ...envelope("participants", participantFile) }));
  const leaseValue: any = lease(); leaseValue.rootId = badBridge; leaseValue.identityId = badPath;
  fs.writeFileSync(path.join(f.mesh, "host-leases", `${hash(ID).slice(0, 32)}.json`), JSON.stringify(leaseValue));
}

describe("fleet authority snapshot privacy and real backend boundaries", () => {
  it("projects canonical FILE plus separate files/cursor without parsing private subtrees", async () => {
    const f = fixture(), before = treeBytes(f.mesh), parse = JSON.parse;
    vi.spyOn(JSON, "parse").mockImplementation((text: string) => { if (text.includes(SECRET)) throw new Error("private body materialized"); return parse(text); });
    const result = await collect(options(f)), text = JSON.stringify(result);
    expect(result.stateEntries).toHaveLength(3); expect(result.participantFiles).toHaveLength(1); expect(result.leaseFiles).toHaveLength(1);
    expect(result.stateEntries.find((row: any) => row.value.incarnationToken)?.value.id).toBe(ID);
    expect(result.stateEntries.find((row: any) => row.value.incarnationToken)?.value.rootId).toBe(ROOT);
    expect(text).not.toContain(TOKEN); expect(text).not.toContain(OLD); expect(text).not.toContain(SECRET); expect(text).not.toContain("Private Human"); expect(text).not.toContain("/private");
    expect(result.participantFiles[0].value.runner).toBeUndefined(); expect(result.participantFiles[0].value.name).toMatch(/^n:[a-f0-9]{32}$/);
    expect(result.leaseFiles[0].writer.pid).toBe(100); expect(result.leaseFiles[0].session.expiresAt).toBe(900);
    expect(result.cursors[0].toRemote).toEqual({ after: 12, mark: 6, offset: 4294967300, relayGrace: { sequence: 13, startedAt: 100, id: tokenHash(TOKEN) } });
    expect(result.counts.fields.every((row: any) => !row.field.includes("private") && !row.field.includes(ID))).toBe(true);
    expect(result.counts.dropped).toBeGreaterThan(0); expect(treeBytes(f.mesh)).toEqual(before);
  });
  it("counts unknown scalar exclusions using only fixed labels and retains lease-only history", async () => {
    const f = fixture(), value: any = host(); value[SECRET] = "excluded-scalar";
    fs.writeFileSync(path.join(f.mesh, "state.json"), JSON.stringify({ format: 1, entries: { [key("hosts")]: envelope("hosts", value) } }));
    const orphan = { ...lease(), id: ROOT, rootId: ROOT, identityId: ROOT };
    fs.writeFileSync(path.join(f.mesh, "host-leases", `${hash(ROOT).slice(0, 32)}.json`), JSON.stringify(orphan));
    const snapshot = await collect(options(f));
    expect(snapshot.leaseFiles).toHaveLength(2); expect(snapshot.leaseFiles.find((row: any) => row.id === ROOT).bridgeSupersededTokenSeen[tokenHash(OLD)]).toBe(0);
    expect(snapshot.counts.fields.find((row: any) => row.field === "unknown.scalar").dropped).toBe(1);
    expect(JSON.stringify(snapshot)).not.toContain(SECRET);
  });
  it("rejects an unknown cursor payload container without materializing or exporting it", async () => {
    const f = fixture(); const value = JSON.parse(fs.readFileSync(f.cursor, "utf8")); value.pending = { event: { text: SECRET, token: TOKEN } }; fs.writeFileSync(f.cursor, JSON.stringify(value));
    await expect(collect(options(f))).rejects.toThrow("UNSAFE_SCHEMA"); const result = run(f); expect(result.status).toBe(1); expect(result.stdout).toBe(""); expect(result.stderr).toBe('{"error":"UNSAFE_SCHEMA"}\n');
  });
  it("keeps token equality across scalar, owner, process, array and history keys", async () => {
    const f = fixture(), result = await collect(options(f)); const projected = result.leaseFiles[0];
    expect(projected.incarnationToken).toBe(tokenHash(TOKEN)); expect(projected.bridgeSupersededTokens).toEqual([tokenHash(OLD), tokenHash(TOKEN)]);
    expect(projected.bridgeSupersededTokenSeen).toEqual({ [tokenHash(OLD)]: 0, [tokenHash(TOKEN)]: 1 });
    expect(result.participantFiles[0].value.ownerIncarnation).toBe(tokenHash(TOKEN));
    expect(projected.mirror.bridgeProcess.incarnation).toBe(tokenHash(TOKEN));
  });
  it("preserves canonical aliases and token absence without inventing an incarnation", async () => {
    const f = fixture(), value = host(); value.identity.name = ROOT; delete (value as Partial<typeof value>).incarnationToken;
    fs.writeFileSync(path.join(f.mesh, "state.json"), JSON.stringify({ format: 2, entries: { [key("hosts")]: envelope("hosts", value) } }));
    const result = await collect(options(f)); expect(result.stateEntries[0].value.identity.name).toBe(ROOT); expect(result.stateEntries[0].value.incarnationToken).toBeUndefined();
    value.identity.name = "session:unseen-canonical-looking-name"; fs.writeFileSync(path.join(f.mesh, "state.json"), JSON.stringify({ format: 2, entries: { [key("hosts")]: envelope("hosts", value) } }));
    const second = await collect(options(f)); expect(second.stateEntries[0].value.identity.name).toMatch(/^n:[0-9a-f]{32}$/);
  });
  it.each(["unknown-container", "duplicate-key", "overflow", "bad-id", "key-mismatch"])("fails closed for %s with no raw diagnostic", async mode => {
    const f = fixture(); let text: string;
    if (mode === "duplicate-key") text = '{"format":1,"format":1,"entries":{}}';
    else {
      const value: any = host(); if (mode === "unknown-container") value[SECRET] = { token: TOKEN };
      if (mode === "overflow") value.expiresAt = Number.MAX_SAFE_INTEGER + 1;
      if (mode === "bad-id") value.rootId = "/private/path";
      if (mode === "key-mismatch") value.id = ROOT;
      text = JSON.stringify({ format: 1, entries: { [key("hosts")]: envelope("hosts", value) } });
    }
    fs.writeFileSync(path.join(f.mesh, "state.json"), text);
    await expect(collect(options(f))).rejects.toThrow(/^(UNSAFE_SCHEMA|INVALID_SCALAR|UNSUPPORTED_ID|KEY_ID_MISMATCH)$/);
    const result = run(f); expect(result.status).toBe(1); expect(result.stdout).toBe(""); expect(result.stderr).not.toContain(TOKEN); expect(result.stderr).not.toContain(SECRET); expect(fs.existsSync(path.join(f.out, "snapshot.json"))).toBe(false);
  });
  it.skipIf(process.platform === "win32")("rejects symlinked input files before following them (Windows skips: symlink creation may require privileges not guaranteed)", async () => {
    const f = fixture(), target = path.join(f.top, "hidden.json"); fs.renameSync(path.join(f.mesh, "state.json"), target); fs.symlinkSync(target, path.join(f.mesh, "state.json"));
    await expect(collect(options(f))).rejects.toThrow("UNSAFE_INPUT");
  });
  it("writes only a new private output, refuses overwrite and mesh-local outputs", () => {
    const f = fixture(), result = run(f); expect(result.status).toBe(0); const output = path.join(f.out, "snapshot.json");
    expect(JSON.parse(result.stdout).schemaVersion).toBe(1); expect(fs.readFileSync(output, "utf8")).toBe(result.stdout); if (process.platform !== "win32") expect(fs.statSync(output).mode & 0o777).toBe(0o600);
    expect(run(f).status).toBe(1); expect(run(f, "work", path.join(f.mesh, "output.json")).status).toBe(1);
  });
  it("retains source-generated resident/runtime sibling IDs and common root joins", async () => {
    const f = fixture(), ids = [ID, `resident:${hash(ROOT).slice(0, 24)}`, `runtime:${ID.slice(8)}`];
    const entries = Object.fromEntries(ids.map(id => { const value = { ...host(), id }; const k = key("hosts", id); return [k, { ...envelope("hosts", value), key: k }]; }));
    fs.writeFileSync(path.join(f.mesh, "state.json"), JSON.stringify({ format: 1, entries }));
    const result = await collect(options(f)); expect(result.stateEntries.map((row: any) => row.value.id).sort()).toEqual(ids.sort()); expect(result.stateEntries.every((row: any) => row.value.rootId === ROOT)).toBe(true);
  });
  it.each([
    ID, `session:018f1234-5678-7abc-8def-0123456789ab`, "a".repeat(32),
    `resident:${"a".repeat(24)}`, `runtime:${ID.slice(8)}`,
    "session:actor-outbox-exact-owner:abcdef1234567", "session:actor-mailbox-exact-owner:a",
  ])("retains only the explicit source-traced ID shape (%s)", async id => {
    const f = fixture(), k = key("hosts", id), value = { ...host(), id };
    fs.writeFileSync(path.join(f.mesh, "state.json"), JSON.stringify({ format: 1, entries: { [k]: { ...envelope("hosts", value), key: k } } }));
    const snapshot = await collect(options(f)); expect(snapshot.stateEntries[0].value.id).toBe(id);
    expect(snapshot.stateEntries[0].key).toBe(k);
  });
  it.each(["actor-outbox-exact-owner", "actor-mailbox-exact-owner", "018f1234-5678-7abc-8def-0123456789ab"])("accepts the explicit sessionId shape (%s)", async sessionId => {
    const f = fixture(), value = host(); value.identity.sessionId = sessionId;
    fs.writeFileSync(path.join(f.mesh, "state.json"), JSON.stringify({ format: 1, entries: { [key("hosts")]: envelope("hosts", value) } }));
    expect((await collect(options(f))).stateEntries[0].value.identity.sessionId).toBe(sessionId);
  });
  it.each([
    ["freeform-not-an-id", "length:1-32"], ["/private/path/secret", "length:1-32"],
    [SECRET, "length:33-64"], [`private-prefix:${TOKEN}`, "length:33-64"],
    ["session:arbitrary-ci-name:abcdef", "session:other"], ["session:actor-outbox-exact-owner:abcdef12345678", "session:other"],
    ["session:actor-mailbox-exact-owner:../../private", "session:other"],
    ["resident:private-body", "resident:other"], ["participant:private-body", "participant:other"],
    ["runtime:private-body", "runtime:other"], ["actor:private-body", "actor:other"], ["agent:private-body", "agent:other"],
    ["a".repeat(8), "length:1-32"], ["a".repeat(64), "length:33-64"],
    ["a".repeat(129), "length:129-256"], ["a".repeat(257), "length:over-256"],
    ["", "length:0"], [123, "non-string"], [null, "non-string"],
  ])("rejects unsupported IDs with fixed redacted diagnostics (%s)", async (id, shape) => {
    const f = fixture(), value: any = host(); value.rootId = id; value[`secret-input-key-${TOKEN}`] = SECRET;
    fs.writeFileSync(path.join(f.mesh, "state.json"), JSON.stringify({ format: 1, entries: { [key("hosts")]: envelope("hosts", value) } }));
    await expect(collect(options(f))).rejects.toMatchObject({ code: "UNSUPPORTED_ID", field: "entry.value.rootId", recordKind: "host", shape });
    const result = run(f); expect(result.status).toBe(1); expect(result.stdout).toBe("");
    expect(JSON.parse(result.stderr)).toEqual({ error: "UNSUPPORTED_ID", field: "entry.value.rootId", recordKind: "host", shape });
    expect(result.stderr).toBe(`${JSON.stringify(JSON.parse(result.stderr))}\n`);
    expect(result.stderr).not.toContain(TOKEN); expect(result.stderr).not.toContain(SECRET);
    expect(fs.existsSync(path.join(f.out, "snapshot.json"))).toBe(false);
  });
  it.each(["file", "sqlite"])("reports full nested fixed schema labels for %s", async backend => {
    const f = fixture(), value: any = host(); value.identity.sessionId = `unknown-prefix:${TOKEN}`;
    f.entries[key("hosts")] = envelope("hosts", value);
    let db: DatabaseSync | undefined;
    if (backend === "sqlite") db = sqlite(f); else fs.writeFileSync(path.join(f.mesh, "state.json"), JSON.stringify({ format: 1, entries: f.entries }));
    try {
      const result = run(f, backend === "sqlite" ? "hub" : "work");
      expect(result.status).toBe(1); expect(result.stdout).toBe("");
      expect(JSON.parse(result.stderr)).toEqual({ error: "UNSUPPORTED_ID", field: "entry.value.identity.sessionId", recordKind: "host", shape: "length:33-64" });
      expect(fs.existsSync(path.join(f.out, "snapshot.json"))).toBe(false);
    } finally { db?.close(); }
  });
  it.each(["bridge:dev1", "spawn-router", "maintenance:nightly-job"])("accepts the pinned updatedBy.id shape in FILE, SQLite, and participant-file writers (%s)", async id => {
    for (const backend of ["file", "sqlite", "participant-file"] as const) {
      const f = fixture(); let db: DatabaseSync | undefined;
      if (backend === "file") {
        const entry = envelope("hosts", host()); entry.updatedBy = by(id);
        fs.writeFileSync(path.join(f.mesh, "state.json"), JSON.stringify({ format: 1, entries: { [key("hosts")]: entry } }));
      } else if (backend === "sqlite") {
        (f.entries[key("hosts")] as any).updatedBy = by(id); db = sqlite(f);
      } else {
        const entry = envelope("participants", participant()); entry.updatedBy = by(id);
        fs.writeFileSync(path.join(f.mesh, "participants", `${hash(ID)}.json`), JSON.stringify({ format: 1, ...entry }));
      }
      try {
        const snapshot = await collect(options(f, backend === "sqlite" ? { site: "hub", name: "dev1" } : {}));
        const projected = backend === "participant-file" ? snapshot.participantFiles[0] : snapshot.stateEntries.find((row: any) => row.value.id === ID);
        expect(projected.updatedBy.id).toBe(id);
      } finally { db?.close(); }
    }
  });
  it.each(["bridge:dev1", "spawn-router", "maintenance:nightly-job"])("does not accept the pinned updatedBy.id shape in authority identity slots (%s)", async id => {
    const cases = [
      ["entry.value.id", "host"], ["entry.value.rootId", "root"], ["entry.value.ownerHostId", "owner"], ["lease.identityId", "lease"],
    ] as const;
    for (const [field, target] of cases) {
      const f = fixture();
      if (target === "host" || target === "root") {
        const value: any = host(); value[target === "host" ? "id" : "rootId"] = id;
        fs.writeFileSync(path.join(f.mesh, "state.json"), JSON.stringify({ format: 1, entries: { [key("hosts")]: { ...envelope("hosts", value), key: key("hosts") } } }));
      } else if (target === "owner") {
        const value: any = participant(); value.ownerHostId = id;
        fs.writeFileSync(path.join(f.mesh, "state.json"), JSON.stringify({ format: 1, entries: { [key("participants")]: { ...envelope("participants", value), key: key("participants") } } }));
      } else {
        const value: any = lease(); value.identityId = id;
        fs.writeFileSync(path.join(f.mesh, "host-leases", `${hash(ID).slice(0, 32)}.json`), JSON.stringify(value));
      }
      await expect(collect(options(f))).rejects.toMatchObject({ code: "UNSUPPORTED_ID", field, recordKind: target === "lease" ? "lease" : target === "owner" ? "participant" : "host" });
    }
  });
  it.each(["bridge:UPPER", "bridge:", `maintenance:${"x".repeat(41)}`, "spawn-router-extra", "bridge:\u0001", "/private/census/path"])("rejects non-pinned updatedBy.id shapes with fixed diagnostics (%s)", async id => {
    const f = fixture(), entry = envelope("hosts", host()); entry.updatedBy = by(id);
    fs.writeFileSync(path.join(f.mesh, "state.json"), JSON.stringify({ format: 1, entries: { [key("hosts")]: entry } }));
    const result = run(f); expect(result.status).toBe(1); expect(result.stdout).toBe("");
    const diagnostic = JSON.parse(result.stderr); expect(diagnostic).toMatchObject({ error: "UNSUPPORTED_ID", field: "entry.updatedBy.id", recordKind: "host" });
    expect(result.stderr).not.toContain(id);
  });
  it.each(["participant", "lease", "updatedBy"])("keeps the record kind and fixed field for %s ID failures", async target => {
    const f = fixture(); let field: string, recordKind: string;
    if (target === "participant") {
      const file = path.join(f.mesh, "participants", `${hash(ID)}.json`), value: any = participant(); value.ownerHostId = SECRET;
      fs.writeFileSync(file, JSON.stringify({ format: 1, ...envelope("participants", value) })); field = "entry.value.ownerHostId"; recordKind = "participant";
    } else if (target === "lease") {
      const value: any = lease(); value.session.id = SECRET;
      fs.writeFileSync(path.join(f.mesh, "host-leases", `${hash(ID).slice(0, 32)}.json`), JSON.stringify(value)); field = "lease.session.id"; recordKind = "lease";
    } else {
      const entry = envelope("hosts", host()); entry.updatedBy.id = SECRET;
      fs.writeFileSync(path.join(f.mesh, "state.json"), JSON.stringify({ format: 1, entries: { [key("hosts")]: entry } })); field = "entry.updatedBy.id"; recordKind = "host";
    }
    const result = run(f); expect(result.status).toBe(1); expect(result.stdout).toBe("");
    expect(JSON.parse(result.stderr)).toEqual({ error: "UNSUPPORTED_ID", field, recordKind, shape: "length:33-64" });
    expect(fs.existsSync(path.join(f.out, "snapshot.json"))).toBe(false);
  });
  it("emits a complete aggregate-only census without authority payloads", () => {
    const f = fixture(), result = run(f, "work", undefined, true), report = JSON.parse(result.stdout);
    expect(result.status).toBe(0); expect(result.stderr).toBe(""); expect(report).toMatchObject({ schemaVersion: 1, mode: "census", complete: true, backend: "file", unsupported: [] });
    expect(report.stateEntries).toBeUndefined(); expect(report.participantFiles).toBeUndefined(); expect(report.leaseFiles).toBeUndefined(); expect(report.cursors).toBeUndefined();
    expect(result.stdout).not.toContain(TOKEN); expect(result.stdout).not.toContain(SECRET); expect(fs.readFileSync(path.join(f.out, "snapshot.json"), "utf8")).toBe(result.stdout);
  });
  it.each(["file", "sqlite"])("aggregates every unsupported fixed authority ID for %s without partial output", backend => {
    const normal = fixture(); censusInputs(normal); const normalResult = run(normal);
    expect(normalResult.status).toBe(1); expect(normalResult.stdout).toBe(""); expect(fs.existsSync(path.join(normal.out, "snapshot.json"))).toBe(false);
    const f = fixture(); censusInputs(f); let db: DatabaseSync | undefined;
    if (backend === "sqlite") db = sqlite(f, true);
    try {
      const result = run(f, backend === "sqlite" ? "hub" : "work", undefined, true), report = JSON.parse(result.stdout);
      expect(result.status).toBe(1); expect(result.stderr).toBe(""); expect(report).toMatchObject({ schemaVersion: 1, mode: "census", complete: false, backend });
      expect(report.stateEntries).toBeUndefined(); expect(report.participantFiles).toBeUndefined(); expect(report.leaseFiles).toBeUndefined(); expect(report.cursors).toBeUndefined();
      const counts = new Map(report.unsupported.map((row: any) => [`${row.field}|${row.recordKind}|${row.shape}`, row.count]));
      expect(counts.get("entry.value.id|host|bridge")).toBe(1);
      expect(counts.get("entry.value.rootId|host|bridge")).toBe(2);
      expect(counts.get("lease.rootId|lease|bridge")).toBe(1);
      expect(counts.get("entry.value.ownerHostId|participant|literal-tool")).toBe(1);
      expect(counts.get("entry.value.ownerIdentityId|participant|literal-tool")).toBe(1);
      expect(counts.get("entry.value.parentId|participant|length:1-32")).toBe(1);
      expect(counts.get("lease.identityId|lease|length:1-32")).toBe(1);
      expect(counts.get("entry.updatedBy.id|policy|length:1-32")).toBe(1);
      expect(report.counts.included).toBeGreaterThan(0); expect(report.counts.dropped).toBeGreaterThan(0);
      const text = result.stdout; expect(text).not.toContain("bridge:bad-census"); expect(text).not.toContain("spawn-router-extra"); expect(text).not.toContain("/private/census/path"); expect(text).not.toContain("/private/policy-owner"); expect(text).not.toContain(TOKEN); expect(text).not.toContain(SECRET);
      expect(fs.readFileSync(path.join(f.out, "snapshot.json"), "utf8")).toBe(result.stdout);
    } finally { db?.close(); }
  });
  it("suppresses native error paths and refuses untrusted diagnostic metadata", async () => {
    const f = fixture(); fs.unlinkSync(path.join(f.mesh, "state.json"));
    const result = run(f); expect(result.status).toBe(1); expect(result.stdout).toBe(""); expect(result.stderr).toBe('{"error":"COLLECTION_FAILED"}\n');
    expect(fs.existsSync(path.join(f.out, "snapshot.json"))).toBe(false);
    const { SnapshotError } = await import(pathToFileURL(libraryPath).href);
    for (const details of [
      { field: SECRET, recordKind: "host", shape: "length:1-32" },
      { field: "entry.value.id", recordKind: SECRET, shape: "length:1-32" },
      { field: "entry.value.id", recordKind: "host", shape: SECRET },
    ]) {
      const error = new SnapshotError("UNSUPPORTED_ID", details);
      expect(error.field).toBeUndefined(); expect(error.recordKind).toBeUndefined(); expect(error.shape).toBeUndefined(); expect(JSON.stringify(error)).not.toContain(SECRET);
    }
  });
  it("fails closed on a forced truncated digest collision without emitting either raw value", async () => {
    const f = fixture(), original = crypto.createHash;
    try {
      (crypto as any).createHash = () => ({ update() { return this; }, digest() { return "0".repeat(64); } }); syncBuiltinESMExports();
      await expect(collect(options(f))).rejects.toThrow("HASH_COLLISION");
    } finally { crypto.createHash = original; syncBuiltinESMExports(); }
  });
  it("reads live SQLite WAL on a writable source under accepted reader-index semantics", async () => {
    const f = fixture(), db = sqlite(f, true), before = treeBytes(f.mesh);
    try {
      const snapshot = await collect(options(f, { site: "hub", name: "dev1" }));
      expect(snapshot.counts.sqlite.pageCountBefore).toBeGreaterThan(0); expect(snapshot.counts.sqlite.pageCountAfter).toBe(snapshot.counts.sqlite.pageCountBefore);
      expect(snapshot.counts.sqlite.walFramesBefore).toBeGreaterThan(0); expect(snapshot.counts.sqlite.walFramesAfter).toBe(snapshot.counts.sqlite.walFramesBefore);
      const after = treeBytes(f.mesh); delete before["state.db-shm"]; delete after["state.db-shm"]; expect(after).toEqual(before);
    } finally { db.close(); }
  });
  it("runs the actual Node readonly SQLite CLI with authority DB/WAL unchanged and sanitized output", () => {
    const f = fixture(), db = sqlite(f, true), before = treeBytes(f.mesh);
    try {
      const result = run(f, "hub"); expect(result.error, result.stderr).toBeUndefined(); expect(result.status, result.stderr).toBe(0);
      const snapshot = JSON.parse(result.stdout); expect(snapshot.backend).toBe("sqlite"); expect(snapshot.stateMetadata).toEqual({ schema: 1, epoch: 1, commit: 3 });
      expect(snapshot.stateEntries).toHaveLength(3); expect(snapshot.leaseFiles[0].incarnationToken).toBe(tokenHash(TOKEN)); expect(result.stdout).not.toContain(SECRET); expect(result.stdout).not.toContain(TOKEN);
      const after = treeBytes(f.mesh); delete before["state.db-shm"]; delete after["state.db-shm"]; expect(after).toEqual(before);
    } finally { db.close(); }
  });
});
