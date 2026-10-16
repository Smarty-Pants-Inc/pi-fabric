import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { jetstreamManager } from "@nats-io/jetstream";
import { connect } from "@nats-io/transport-node";
import { afterEach, describe, expect, it } from "vitest";
import { readMeshStateNatsMarker } from "../src/mesh/backend-fence.js";
import { BUILTIN_READERS, installedRelease, natsProofChallenge } from "../src/mesh/reader-proof.js";
import { closeNatsStateBackend, openNatsStateBackend } from "../src/mesh/state-backend-nats.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { itFor, natsMonitorUrl, natsServerUrl, reportNatsSkip } from "./helpers/nats-state-server.js";

// smarty-dev#9690 acceptance: the operator's real bin (bin/fabric-mesh-backend -> dist) on a real nats-server moves a
// file root to NATS and back. A required reader without a proof refuses the switch before the marker; after it proves
// a read of the staged stream, the rerun switches; the rollback restores the state, with the NATS-era write.
const identity: MeshIdentity = { id: "e2e", name: "e2e", kind: "agent" };
const BIN = path.resolve("bin/fabric-mesh-backend");
const built = fs.existsSync(path.resolve("dist/mesh/mesh-backend-cli.js"));
const dirs: string[] = [];
afterEach(() => { for (const dir of dirs.splice(0)) { closeNatsStateBackend(dir); fs.rmSync(dir, { recursive: true, force: true }); } });
const tempDir = (label: string): string => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), `pi-fabric-nats-e2e-${label}-`));
  dirs.push(dir);
  return dir;
};
/** The challenge nonce of a staged stream: its `import-begin` record, read on NATS (the only place it is). */
const streamNonce = async (stream: string): Promise<string> => {
  const nc = await connect({ servers: [natsServerUrl!] });
  try {
    const jsm = await jetstreamManager(nc);
    const begin = await jsm.streams.getMessage(stream, { seq: 1 });
    const nonce = (JSON.parse(new TextDecoder().decode(begin!.data)) as { nonce?: unknown }).nonce;
    if (typeof nonce !== "string") throw new Error("no nonce in import-begin");
    return nonce;
  } finally { await nc.close(); }
};
/** The consumers the server holds on a stream (a direct JetStream API read). */
const consumerCount = async (stream: string): Promise<number> => {
  const nc = await connect({ servers: [natsServerUrl!] });
  try {
    const jsm = await jetstreamManager(nc);
    let count = 0;
    for await (const _consumer of jsm.consumers.list(stream)) count++;
    return count;
  } finally { await nc.close(); }
};
const cli = (...argv: string[]) => spawnSync(process.execPath, [BIN, ...argv], { encoding: "utf8", timeout: 120_000,
  env: { ...process.env, PI_FABRIC_MESH_STATE_BACKEND: "" } });

describe("fabric-mesh-backend file -> nats -> file, real bin, real nats-server (smarty-dev#9690)", () => {
  reportNatsSkip();
  if (process.env.NATS_SERVER_REQUIRED === "1" && !built) throw new Error("NATS_SERVER_REQUIRED=1 but dist/ is not built (bun run build)");
  itFor("nats")("a reader present refuses before the marker; after its proof of the staged stream the switch runs; rollback restores", async () => {
    if (!built) throw new Error("dist/ is not built (bun run build)");
    const root = tempDir("mesh");
    const seed = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
    await seed.put({ key: "e2e/a", value: { n: 1 }, identity });
    await seed.put({ key: "e2e/gone", value: 0, identity });
    await seed.delete({ key: "e2e/gone" });
    seed.closeState();
    const original = fs.readFileSync(path.join(root, "state.json"));
    // A required non-Fabric reader, installed at release r1 (its trusted install root).
    const install = tempDir("install");
    fs.mkdirSync(path.join(install, "releases", "r1"), { recursive: true });
    fs.symlinkSync(path.join("releases", "r1"), path.join(install, "current"));
    const nats = ["--nats-server", natsServerUrl!, "--nats-monitor", natsMonitorUrl!];
    const switchArgs = ["import", "--to", "nats", "--root", root, ...nats, "--accept-old-release-gap", "--require-reader", `e2ereader=${install}`, "--json"];
    // pi-fabric#927: a NATS switch has no --accept-unready override.
    const overridden = cli(...switchArgs, "--accept-unready", "e2ereader");
    expect(overridden.status).toBe(2);
    expect(overridden.stderr).toContain("--accept-unready is refused with --to nats");

    const refused = cli(...switchArgs);
    expect(refused.status, refused.stderr).toBe(3);
    expect(refused.stderr).toContain("e2ereader");
    expect(fs.readFileSync(path.join(root, "state.json"))).toEqual(original);
    const intent = JSON.parse(fs.readFileSync(path.join(root, "state.json.nats-intent"), "utf8")) as { stream: string };
    expect(refused.stderr).toContain(`reader-proof --backend nats --stream ${intent.stream}`);
    // Nothing can open the root on NATS before the marker.
    await expect(openNatsStateBackend({ root, servers: [natsServerUrl!], monitoringUrl: natsMonitorUrl! })).rejects.toThrow(/no NATS moved marker/);

    // The reader proves a real read of the staged stream (here with Fabric's read code; its proof carries the binding).
    const proved = cli("reader-proof", "--root", root, "--backend", "nats", "--stream", intent.stream, ...nats, "--json");
    expect(proved.status, proved.stderr).toBe(0);
    const bind = (JSON.parse(proved.stdout) as { proof: Record<string, unknown> }).proof;
    // The proof's consumer is gone at once (its stop deletes it), not left for the server's inactive threshold.
    expect(await consumerCount(intent.stream)).toBe(0);
    expect(bind).toMatchObject({ backends: ["nats"], stream: intent.stream, head: expect.any(Number), digest: expect.stringMatching(/^sha256:/) });
    expect(JSON.stringify(bind)).not.toMatch(/nonce/);
    // The installed readers answer the challenge themselves: each reads the nonce from the staged stream on NATS (it
    // is in no file and no output) and binds it to its own name and release.
    // A forged proof: every bound field copied from the Fabric proof, but no knowledge of the nonce. Refused.
    const forgedFile = path.join(tempDir("forged"), "e2ereader.json");
    fs.writeFileSync(forgedFile, JSON.stringify({ name: "e2ereader", implementation: "e2e-reader", version: "1", release: "r1", backends: ["nats"],
      provedAt: new Date().toISOString(), provedEpoch: 0, switchId: bind.switchId, stream: bind.stream, streamCreated: bind.streamCreated,
      head: bind.head, digest: bind.digest, target: bind.target, challenge: bind.challenge }));
    expect(cli("reader-proof", "--root", root, "--name", "e2ereader", "--proof-file", forgedFile).status).toBe(0);
    const forged = cli(...switchArgs);
    expect(forged.status).toBe(3);
    expect(forged.stderr).toContain("e2ereader (the challenge does not answer this switch's nonce for this reader");
    expect(readMeshStateNatsMarker(root)).toBeUndefined();
    const nonce = await streamNonce(intent.stream);
    const readers = [{ name: "e2ereader", release: "r1" }];
    for (const reader of BUILTIN_READERS) {
      try { readers.push({ name: reader.name, release: installedRelease(reader.installRoot) }); } catch { /* not installed on this host */ }
    }
    for (const reader of readers) {
      const proofFile = path.join(tempDir("proof"), `${reader.name}.json`);
      fs.writeFileSync(proofFile, JSON.stringify({ name: reader.name, implementation: "e2e-reader", version: "1", release: reader.release, backends: ["nats"],
        provedAt: new Date().toISOString(), provedEpoch: 0, switchId: bind.switchId, stream: bind.stream, streamCreated: bind.streamCreated,
        head: bind.head, digest: bind.digest, target: bind.target,
        challenge: natsProofChallenge(nonce, String(bind.switchId), reader.name, reader.release) }));
      const installed = cli("reader-proof", "--root", root, "--name", reader.name, "--proof-file", proofFile);
      expect(installed.status, installed.stderr).toBe(0);
    }

    const switched = cli(...switchArgs);
    expect(switched.status, switched.stderr).toBe(0);
    const result = JSON.parse(switched.stdout) as { stream: string; readers: { ready: string[]; checkedUnderFence: string[] } };
    expect(result.stream).toBe(intent.stream);
    expect(result.readers.ready).toContain("e2ereader");
    expect(result.readers.checkedUnderFence).toEqual(["nats-marker", "nats-import-commit"]);
    expect(readMeshStateNatsMarker(root)?.natsStream).toBe(intent.stream);
    const status = cli("status", "--root", root);
    expect(status.stdout).toContain(`moved marker (backend=nats, stream ${intent.stream}`);

    // A nats-mode store on the root reads the imported state and writes.
    await openNatsStateBackend({ root, servers: [natsServerUrl!], monitoringUrl: natsMonitorUrl! });
    const live = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "nats" });
    expect(live.get("e2e/a")?.value).toEqual({ n: 1 });
    await live.put({ key: "e2e/on-nats", value: "yes", identity });
    live.closeState();
    closeNatsStateBackend(root);

    const back = cli("rollback", "--root", root, ...nats, "--json");
    expect(back.status, back.stderr).toBe(0);
    expect(JSON.parse(back.stdout)).toMatchObject({ from: "nats", ok: true, stream: intent.stream, epoch: 2 });
    const file = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
    try {
      expect(file.get("e2e/a")?.value).toEqual({ n: 1 });
      expect(file.get("e2e/on-nats")?.value).toBe("yes");
      expect(file.get("e2e/gone")).toBeUndefined();
    } finally { file.closeState(); }
    const records = fs.readFileSync(path.join(root, "backend-switches.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line) as Record<string, unknown>);
    expect(records.some(record => record.phase === "outcome" && record.ok === true && record.stream === intent.stream && record.epoch === 1)).toBe(true);
    expect(records.at(-1)).toMatchObject({ phase: "outcome", ok: true, stream: intent.stream, epoch: 2 });
  }, 180_000);
});
