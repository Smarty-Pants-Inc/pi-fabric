import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { importMeshStateToNats, type MeshNatsCommitCheck } from "../src/mesh/backend-migration-nats.js";
import { main } from "../src/mesh/mesh-backend-cli.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { fakeJetStream } from "./helpers/nats-jetstream-fake.js";
import { FABRIC_IMPLEMENTATION, natsProofChallenge, parseProof, proveFabricReader, readerReadiness, requiredReaders, type NatsGateBind,
  type NatsProofBind } from "../src/mesh/reader-proof.js";

// smarty-dev#9690 item 1: a `nats` reader proof is bound to one switch's staged stream on one target
// (.local/design-9690-reader-proof.md, "The nats proof"). These cases cover the proof schema and the gate's binding.
const dirs: string[] = [];
afterEach(() => { vi.unstubAllEnvs(); for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true }); });
const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const tempDir = (label: string): string => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), `pi-fabric-nats-readiness-${label}-`));
  dirs.push(dir);
  return dir;
};
const installRoot = (release: string): string => {
  const root = tempDir("install");
  fs.mkdirSync(path.join(root, "releases", release), { recursive: true });
  fs.symlinkSync(path.join("releases", release), path.join(root, "current"));
  return root;
};

const proofBind: NatsProofBind = {
  switchId: "0123456789abcdef0123456789abcdef", stream: "FABRIC_MESH_abc_0123456789abcdef0123456789abcdef",
  streamCreated: "2026-10-11T20:00:00.000Z", head: 7, digest: `sha256:${"a".repeat(64)}`, target: "probe-a/probedomain/FLEET",
};
/** The gate's side: the bound fields and the switch's nonce (from the stream). */
const nonce = "5".repeat(32);
const bind: NatsGateBind = { ...proofBind, nonce };
/** A proof that answers the challenge for its own name and release (unless `challenge` is given). */
const natsProof = (fields: Record<string, unknown> = {}): Record<string, unknown> => {
  const proof: Record<string, unknown> = { name: "factory", implementation: "python-factory", version: "factory-1", release: "r1",
    backends: ["file", "nats"], provedAt: new Date().toISOString(), provedEpoch: 0, ...proofBind, ...fields };
  if (!("challenge" in fields)) proof.challenge = natsProofChallenge(nonce, String(proof.switchId), String(proof.name), String(proof.release));
  return proof;
};
const place = (root: string, proof: Record<string, unknown>): void => {
  fs.mkdirSync(path.join(root, "readers"), { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(root, "readers", `${String(proof.name)}.json`), JSON.stringify(proof), { mode: 0o600 });
};

describe("nats reader proofs (smarty-dev#9690)", () => {
  it("parseProof: a nats proof needs all six bound fields; a file/sqlite proof may carry none", () => {
    expect(parseProof(JSON.stringify(natsProof()), "factory")).toMatchObject({ backends: ["file", "nats"], ...proofBind });
    expect(parseProof(JSON.stringify(natsProof({ challenge: undefined })), "factory")).toMatch(/^invalid proof: challenge/);
    expect(parseProof(JSON.stringify(natsProof({ challenge: "x" })), "factory")).toMatch(/^invalid proof: challenge/);
    for (const field of ["switchId", "stream", "streamCreated", "head", "digest", "target"]) {
      expect(parseProof(JSON.stringify(natsProof({ [field]: undefined })), "factory")).toMatch(new RegExp(`^invalid proof: ${field}`));
    }
    expect(parseProof(JSON.stringify(natsProof({ switchId: "XYZ" })), "factory")).toMatch(/switchId/);
    expect(parseProof(JSON.stringify(natsProof({ stream: "a.b" })), "factory")).toMatch(/stream/);
    expect(parseProof(JSON.stringify(natsProof({ head: 0 })), "factory")).toMatch(/head/);
    expect(parseProof(JSON.stringify(natsProof({ digest: "md5:x" })), "factory")).toMatch(/digest/);
    expect(parseProof(JSON.stringify(natsProof({ target: "only-one" })), "factory")).toMatch(/target/);
    expect(parseProof(JSON.stringify(natsProof({ backends: ["sqlite"] })), "factory"))
      .toBe("invalid proof: switchId, stream, streamCreated, head, digest, target, challenge only with backends nats");
    expect(parseProof(JSON.stringify(natsProof({ backends: ["bogus"] })), "factory")).toMatch(/file, sqlite, nats/);
  });

  it("the gate: ready only when every bound field is the switch's; without a binding a nats proof is unready", () => {
    const root = tempDir("mesh");
    const required = requiredReaders([], [], [{ name: "factory", installRoot: installRoot("r1") }], []);
    place(root, natsProof());
    const window = { fromEpoch: 0, toEpoch: 1 };
    expect(readerReadiness(root, "nats", required, window, Date.now(), process.geteuid?.(), bind).ready).toEqual(["factory"]);
    expect(readerReadiness(root, "nats", required, window).unready[0]!.reason).toBe("a nats proof needs the switch binding (the gate has none)");
    for (const [field, other] of [["switchId", "f".repeat(32)], ["stream", "FABRIC_MESH_other"], ["streamCreated", "2026-10-11T21:00:00.000Z"],
      ["head", 8], ["digest", `sha256:${"b".repeat(64)}`], ["target", "probe-b/probedomain/FLEET"]] as const) {
      const reason = readerReadiness(root, "nats", required, window, Date.now(), process.geteuid?.(), { ...bind, [field]: other }).unready[0]!.reason;
      expect(reason).toMatch(new RegExp(`^bound to another ${field}: `));
    }
    // A file proof never proves nats.
    place(root, natsProof({ backends: ["file"], switchId: undefined, stream: undefined, streamCreated: undefined, head: undefined, digest: undefined, target: undefined, challenge: undefined }));
    expect(readerReadiness(root, "nats", required, window, Date.now(), process.geteuid?.(), bind).unready[0]!.reason).toBe("missing nats (has file)");
  });

  it("the challenge (pi-fabric#927): a proof without the stream's nonce, with a stale nonce, or copied from another reader is unready", () => {
    const root = tempDir("mesh-challenge");
    const required = requiredReaders([], [], [{ name: "factory", installRoot: installRoot("r1") }], []);
    const window = { fromEpoch: 0, toEpoch: 1 };
    const reason = (): string | undefined => readerReadiness(root, "nats", required, window, Date.now(), process.geteuid?.(), bind).unready[0]?.reason;
    // A forged proof: every bound field right (none of them is secret), but no knowledge of the nonce.
    place(root, natsProof({ challenge: "0".repeat(64) }));
    expect(reason()).toMatch(/the challenge does not answer this switch's nonce/);
    // A stale nonce (another switch's read).
    place(root, natsProof({ challenge: natsProofChallenge("6".repeat(32), proofBind.switchId, "factory", "r1") }));
    expect(reason()).toMatch(/the challenge does not answer/);
    // Another reader's answer copied into this reader's proof (the answer covers the reader's name and release).
    place(root, natsProof({ challenge: natsProofChallenge(nonce, proofBind.switchId, "fabric-relA", "relA") }));
    expect(reason()).toMatch(/the challenge does not answer/);
    // The real answer passes; a switch whose stream has no nonce refuses every proof.
    place(root, natsProof());
    expect(reason()).toBeUndefined();
    expect(readerReadiness(root, "nats", required, window, Date.now(), process.geteuid?.(), { ...bind, nonce: undefined }).unready[0]!.reason)
      .toMatch(/has no challenge nonce/);
  });

  it("a live Fabric release is matched by its proof's release and binding", () => {
    const root = tempDir("mesh-fabric");
    const required = requiredReaders([{ pid: 4242, release: "relA", mode: "file" }], [], [], []);
    place(root, natsProof({ name: "fabric-relA", implementation: FABRIC_IMPLEMENTATION, release: "relA" }));
    const window = { fromEpoch: 0, toEpoch: 1 };
    expect(readerReadiness(root, "nats", required, window, Date.now(), process.geteuid?.(), bind).ready).toHaveLength(1);
    expect(readerReadiness(root, "nats", required, window, Date.now(), process.geteuid?.(), { ...bind, head: 9 }).ready).toHaveLength(0);
  });

  it("the CLI: import --to nats is gated before the marker; a proof of the staged stream lets the rerun switch; rollback is recorded", async () => {
    const root = tempDir("cli");
    const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
    await store.put({ key: "a", value: { n: 1 }, identity });
    store.closeState();
    const original = fs.readFileSync(path.join(root, "state.json"));
    const server = fakeJetStream();
    vi.stubEnv("PI_FABRIC_RELEASE_SHA", "relA");
    const run = async (argv: string[]): Promise<{ code: number; out: string; err: string }> => {
      let out = "";
      let err = "";
      const code = await main(argv, { natsPort: () => server.port(), census: async () => ({ writers: [] }),
        gateCensus: () => ({ writers: [{ pid: 4242, release: "relA", mode: "file" }], unknown: [] }), builtinReaders: [],
        procScan: { listPids: () => [] }, stdout: (text) => { out += text; }, stderr: (text) => { err += text; } });
      return { code, out, err };
    };
    const nats = ["--nats-server", "nats://127.0.0.1:1"];
    expect((await run(["import", "--to", "nats", "--root", root, ...nats])).err).toMatch(/needs --accept-old-release-gap/);
    // pi-fabric#927: no --accept-unready override for a NATS switch (usage, exit 2, nothing written).
    const overridden = await run(["import", "--to", "nats", "--root", root, ...nats, "--accept-old-release-gap", "--accept-unready", "fabric@relA"]);
    expect(overridden.code).toBe(2);
    expect(overridden.err).toContain("--accept-unready is refused with --to nats");
    expect(fs.existsSync(path.join(root, "state.json.nats-intent"))).toBe(false);
    const refused = await run(["import", "--to", "nats", "--root", root, ...nats, "--accept-old-release-gap"]);
    expect(refused.code).toBe(3);
    const intent = JSON.parse(fs.readFileSync(path.join(root, "state.json.nats-intent"), "utf8")) as { stream: string; switchId: string };
    expect(refused.err).toContain(`reader-proof --backend nats --stream ${intent.stream}`);
    expect(refused.err).toMatch(/fabric@relA|relA/);
    expect(fs.readFileSync(path.join(root, "state.json"))).toEqual(original);
    expect((await run(["reader-proof", "--root", root, "--backend", "nats", "--stream", intent.stream, ...nats])).code).toBe(0);
    const done = await run(["import", "--to", "nats", "--root", root, ...nats, "--accept-old-release-gap", "--json"]);
    expect(done.code).toBe(0);
    const result = JSON.parse(done.out) as { stream: string; readers: { checkedUnderFence: string[] } };
    expect(result.stream).toBe(intent.stream);
    expect(result.readers.checkedUnderFence).toEqual(["nats-marker", "nats-import-commit"]);
    expect(fs.existsSync(path.join(root, "state.json.nats-intent"))).toBe(false);
    const records = fs.readFileSync(path.join(root, "backend-switches.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line) as Record<string, unknown>);
    expect(records.filter(record => record.phase === "intent" && record.backend === "nats").every(record => typeof (record.acceptOldReleaseGap as { user?: unknown })?.user === "string")).toBe(true);
    expect(records.at(-1)).toMatchObject({ phase: "outcome", ok: true, stream: intent.stream });
    // Human status names the stream; the rollback is ungated and recorded, and gives back the same state.
    const status = await run(["status", "--root", root]);
    expect(status.code).toBe(0);
    expect(status.out).toContain(`moved marker (backend=nats, stream ${intent.stream}`);
    const back = await run(["rollback", "--root", root, ...nats, "--json"]);
    expect(back.code).toBe(0);
    expect(JSON.parse(back.out)).toMatchObject({ from: "nats", ok: true, stream: intent.stream, epoch: 2 });
    const file = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
    try { expect(file.get("a")?.value).toEqual({ n: 1 }); } finally { file.closeState(); }
  });

  it("--abandon deletes exactly the intent's unfinished stream and the intent", async () => {
    const root = tempDir("abandon");
    const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
    await store.put({ key: "a", value: 1, identity });
    store.closeState();
    const server = fakeJetStream();
    await expect(importMeshStateToNats(root, { port: server.port(), beforeCommit: () => { throw new Error("unready"); } })).rejects.toThrow("unready");
    expect(server.messages.length).toBeGreaterThan(0);
    let out = "";
    const code = await main(["import", "--to", "nats", "--root", root, "--nats-server", "nats://127.0.0.1:1", "--abandon", "--json"],
      { natsPort: () => server.port(), stdout: (text) => { out += text; }, stderr: () => undefined });
    expect(code).toBe(0);
    expect(JSON.parse(out)).toMatchObject({ abandon: true, deleted: true });
    expect(server.streamExists).toBe(false);
    expect(fs.existsSync(path.join(root, "state.json.nats-intent"))).toBe(false);
    // Abandoned: a new import starts a new switch on a new stream.
    server.streamExists = false;
    const again = await importMeshStateToNats(root, { port: server.port() });
    expect(again.converged).toBe(false);
  });

  it("reader-proof --backend nats reads the staged stream and binds exactly what the switch commits", async () => {
    const root = tempDir("mesh-prove");
    const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
    await store.put({ key: "a", value: 1, identity });
    await store.put({ key: "b", value: 2, identity });
    await store.delete({ key: "b" });
    store.closeState();
    const server = fakeJetStream();
    const checks: MeshNatsCommitCheck[] = [];
    await expect(importMeshStateToNats(root, { port: server.port(), beforeCommit: (check) => { checks.push(check); throw new Error("gate: unready"); } }))
      .rejects.toThrow("gate: unready");
    const check = checks[0]!;
    // Without the stream, or on a missing stream, or without the account, no proof is written.
    await expect(proveFabricReader(root, { backend: "nats" })).rejects.toThrow(/needs the staged stream/);
    await expect(proveFabricReader(root, { backend: "nats", nats: { stream: "FABRIC_MESH_missing", port: fakeJetStream().port() } }))
      .rejects.toThrow();
    const anonymous = fakeJetStream();
    anonymous.identity = { name: "fake-server" };
    await expect(proveFabricReader(root, { backend: "nats", nats: { stream: check.stream, port: anonymous.port() } })).rejects.toThrow(/account/);
    expect(fs.existsSync(path.join(root, "readers"))).toBe(false);

    vi.stubEnv("PI_FABRIC_RELEASE_SHA", "relA");
    const proved = await proveFabricReader(root, { backend: "nats", nats: { stream: check.stream, port: server.port() } });
    expect(proved.entries).toBe(1);
    expect(proved.proof).toMatchObject({ backends: ["nats"], release: "relA", switchId: check.switchId, stream: check.stream,
      streamCreated: check.streamCreated, head: check.head, digest: check.digest, target: check.target });
    expect(check.target).toBe("fake-server//FLEET");
    const required = requiredReaders([{ pid: 4242, release: "relA", mode: "file" }], [], [], []);
    const bind = { switchId: check.switchId, stream: check.stream, streamCreated: check.streamCreated, head: check.head, digest: check.digest,
      target: check.target!, nonce: check.nonce };
    // The nonce is in the stream only: never in the proof, the mesh directory or the switch records.
    expect(check.nonce).toMatch(/^[0-9a-f]{32}$/);
    expect(JSON.stringify(proved.proof)).not.toContain(check.nonce!);
    for (const name of fs.readdirSync(root)) {
      if (fs.statSync(path.join(root, name)).isFile()) expect(fs.readFileSync(path.join(root, name), "utf8")).not.toContain(check.nonce!);
    }
    expect(readerReadiness(root, "nats", required, { fromEpoch: 0, toEpoch: 1 }, Date.now(), process.geteuid?.(), bind).unready).toEqual([]);
    // The stream is still importing: an ordinary read of it refuses; only the proof path read it.
    expect(server.messages.some(message => JSON.parse(Buffer.from(message.data).toString("utf8")).type === "import-commit")).toBe(false);
  });
});
