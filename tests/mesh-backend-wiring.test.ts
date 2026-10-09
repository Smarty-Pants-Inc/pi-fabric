import { spawn, spawnSync, type ChildProcessWithoutNullStreams } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { main } from "../src/mesh/mesh-backend-cli.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#6477 W1: the operator path end to end through the CLI with the L4a writer census.
// A live file-mode writer of this release refuses cutover; once it stops, cutover, SQLite reads,
// rollback and file reads keep every write.

const WRITER = `
import { createJiti } from "jiti";
import { pathToFileURL } from "node:url";
import readline from "node:readline";
const [root] = process.argv.slice(1);
const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
const { MeshStore } = await jiti.import("./src/mesh/store.ts");
const store = new MeshStore(root, 64 * 1024, 1000, { stateBackend: "file" });
const identity = { id: "writer-" + process.pid, name: "writer", kind: "agent" };
const lines = readline.createInterface({ input: process.stdin });
process.stdout.write("ready\\n");
for await (const line of lines) {
  const [command, key, value] = line.split(" ");
  if (command === "exit") { store.closeState(); process.exit(0); }
  await store.put({ key, value: Number(value), identity });
  process.stdout.write("ok\\n");
}
`;

const identity: MeshIdentity = { id: "operator", name: "operator", kind: "agent" };
const roots: string[] = [];
const children: ChildProcessWithoutNullStreams[] = [];

afterEach(() => {
  for (const child of children.splice(0)) if (child.exitCode === null) child.kill("SIGKILL");
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const startWriter = (root: string): { child: ChildProcessWithoutNullStreams; send: (line: string) => Promise<void> } => {
  const child = spawn(process.execPath, ["--disable-warning=ExperimentalWarning", "--input-type=module", "-e", WRITER, root], {
    cwd: process.cwd(), env: { ...process.env, PI_FABRIC_MESH_STATE_BACKEND: "", PI_FABRIC_RELEASE_SHA: "w1-this-release" },
  });
  children.push(child);
  let buffer = "";
  let stderr = "";
  const waiters: Array<(line: string) => void> = [];
  child.stderr.on("data", (chunk) => { stderr += chunk; });
  child.stdout.on("data", (chunk) => {
    buffer += chunk;
    for (let index = buffer.indexOf("\n"); index >= 0; index = buffer.indexOf("\n")) {
      const line = buffer.slice(0, index);
      buffer = buffer.slice(index + 1);
      waiters.shift()?.(line);
    }
  });
  const next = (): Promise<string> => new Promise((resolve, reject) => {
    waiters.push(resolve);
    child.once("exit", (code) => reject(new Error(`writer exited ${String(code)}: ${stderr}`)));
  });
  const ready = next();
  return {
    child,
    send: async (line: string) => {
      await ready;
      const reply = next();
      child.stdin.write(`${line}\n`);
      expect(await reply).toBe("ok");
    },
  };
};

const run = async (...argv: string[]): Promise<{ code: number; out: string; err: string }> => {
  let out = "";
  let err = "";
  const code = await main(argv, { stdout: (text) => { out += text; }, stderr: (text) => { err += text; } });
  return { code, out, err };
};

// The operator runs the BUILT command (package.json bin -> bin/fabric-mesh-backend -> dist), not
// main() in-process. CI runs `bun run build` before the tests; locally, without dist, this skips.
const BIN = path.resolve("bin/fabric-mesh-backend");
const BUILT_CLI = path.resolve("dist/mesh/mesh-backend-cli.js");
const hasBuiltCli = fs.existsSync(BUILT_CLI);
if (!hasBuiltCli) console.warn(`skipping the built fabric-mesh-backend test: ${BUILT_CLI} is absent (run bun run build)`);

describe.skipIf(!hasBuiltCli)("the built fabric-mesh-backend command (W1, pi-fabric#671)", () => {
  const cli = (...argv: string[]): { code: number | null; out: string; err: string } => {
    const child = spawnSync(process.execPath, [BIN, ...argv], {
      encoding: "utf8", timeout: 30_000, env: { ...process.env, PI_FABRIC_MESH_STATE_BACKEND: "" },
    });
    return { code: child.status, out: child.stdout, err: child.stderr };
  };

  it("status, cutover and rollback as a subprocess: exit 0, the advisory census line, the result", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-w1-bin-"));
    roots.push(root);
    const seed = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
    await seed.put({ key: "keep/seed", value: 1, identity });
    seed.closeState();

    const before = cli("status", "--root", root);
    expect(before, before.err).toMatchObject({ code: 0 });
    expect(before.out).toContain("census        advisory: 0 writers, 0 unknown");
    // The built command scans this host's real /proc and /proc/locks (smarty-dev#7936): no holder list.
    const cutover = cli("cutover", "--root", root, "--accept-unready", "factory,fabric@unknown");
    expect(cutover, cutover.err).toMatchObject({ code: 0 });
    expect(cutover.err).toContain("fabric-mesh-backend: advisory: 0 writers, 0 unknown");
    expect(cutover.out).toMatch(/^cutover done: backend=sqlite epoch 1 \(from 0\), 1 entries/);
    const after = cli("status", "--root", root, "--json");
    expect(after.code).toBe(0);
    expect(JSON.parse(after.out)).toMatchObject({ backend: "sqlite", epoch: 1, reader: { source: "sqlite" }, fenceHolds: true });
    const rollback = cli("rollback", "--root", root);
    expect(rollback, rollback.err).toMatchObject({ code: 0 });
    // Advisory only: on a SQLite root the census may list unattributed state.db-wal/-shm evidence as unknown.
    expect(rollback.err).toMatch(/^fabric-mesh-backend: advisory: 0 writers, \d+ unknown$/m);
    expect(rollback.out).toMatch(/^rollback done: backend=file epoch 2/);
    // A usage error is the built command's exit 2, not a crash.
    expect(cli("cutover", "--root", root, "--assume-no-writers").code).toBe(2);
    const file = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
    expect(file.get("keep/seed", { fresh: true })?.value).toBe(1);
    file.closeState();
  }, 60_000);
});

describe("fabric-mesh-backend with the writer census (W1)", () => {
  it("advisory census -> cutover fenced on .lock and custody -> sqlite reads -> rollback -> file reads, no lost write", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-w1-"));
    roots.push(root);
    const operator = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
    await operator.put({ key: "keep/operator", value: 1, identity });
    const writer = startWriter(root);
    await writer.send("put keep/writer-1 11");

    // The census is advisory (smarty-dev#6982): it reports the live writer, exits 0, never says "safe".
    const census = await run("census", "--root", root);
    expect(census.code).toBe(0);
    expect(census.out).toContain("census        advisory: 1 writer, 0 unknown");
    expect(census.out).toContain(`pid ${writer.child.pid}  file  w1-this-release`);
    expect(census.out).not.toMatch(/safe|clean/i);
    // The removed attestation flag is a usage error: nothing gates on the census any more.
    expect((await run("cutover", "--root", root, "--assume-no-writers")).code).toBe(2);
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
    await writer.send("put keep/writer-2 12");

    // The operator stops the writers; the cutover is fenced on .lock and custody.lock only.
    writer.child.stdin.write("exit\n");
    await new Promise(resolve => writer.child.once("exit", resolve));
    expect(await run("census", "--root", root)).toMatchObject({ code: 0, out: "census        advisory: 0 writers, 0 unknown\n" });
    const cutover = await run("cutover", "--root", root, "--accept-unready", "factory,fabric@unknown", "--json");
    expect(cutover.code).toBe(0);
    expect(cutover.err).toContain("fabric-mesh-backend: advisory: 0 writers, 0 unknown");
    expect(JSON.parse(cutover.out)).toMatchObject({ command: "cutover", ok: true, backend: "sqlite", epoch: 1,
      census: { writers: [], unknown: [] } });
    expect(fs.existsSync(path.join(root, "custody.lock"))).toBe(false);

    const sqlite = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "sqlite" });
    expect(sqlite.stateBackend).toBe("sqlite");
    expect(["keep/operator", "keep/writer-1", "keep/writer-2"].map(key => sqlite.get(key, { fresh: true })?.value)).toEqual([1, 11, 12]);
    await sqlite.put({ key: "keep/sqlite", value: 21, identity });
    sqlite.closeState();

    const rollback = await run("rollback", "--root", root);
    expect(rollback.code).toBe(0);
    expect(rollback.out).toMatch(/rollback done: backend=file epoch 2/);
    const file = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
    expect(["keep/operator", "keep/writer-1", "keep/writer-2", "keep/sqlite"].map(key => file.get(key, { fresh: true })?.value))
      .toEqual([1, 11, 12, 21]);
    await file.put({ key: "keep/after-rollback", value: 31, identity });
    expect(file.get("keep/after-rollback", { fresh: true })?.value).toBe(31);
    file.closeState();
    operator.closeState();
  }, 60_000);
});
