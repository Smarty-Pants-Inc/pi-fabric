import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#9501 / #4664: the INSTALLED command path of a readiness refusal before the importing commit. The child is
// exactly what an operator runs: `node bin/fabric-mesh-backend import|cutover ...` against the freshly built
// dist/mesh/mesh-backend-cli.js (no main() call, no source import in the child). The readiness gate is real: an extra
// --require-reader with a custom install root and an installed proof makes the first check ready; a --import preload
// fixture (written into this test's own mkdtemp root) moves that proof away right after the CLI's exclusive create of
// state.db, so the fenced re-check before the importing commit refuses for real. The product's cleanup then really
// unlinks this run's state.db*: Linux GitHub-hosted runners only. Locally this file is collected and skipped.

const HOSTED = process.env.GITHUB_ACTIONS === "true" && process.platform === "linux";
const BIN = path.resolve("bin/fabric-mesh-backend");
const BUILT_CLI = path.resolve("dist/mesh/mesh-backend-cli.js");
const CHILD_TIMEOUT = 60_000;
const DB_NAME = /^state\.db(?:-wal|-shm|-journal)?$/;
const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };

const dirs: string[] = [];
afterEach(() => { for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true }); });

/**
 * The child's preload (node --import). Narrow: it acts only on `<root>/state.db` (by path or through a held
 * /proc/self/fd directory) and on `.import-quarantine-*` mkdtemp. Sequence:
 * 1. after the CLI's O_CREAT|O_EXCL open of state.db: record its inode, rename readers/forwarder.json out of the root;
 * 2. "swap" only: when an lstat of state.db returned that inode and the very next step is a quarantine mkdtemp, a
 *    rename or an unlink of state.db, first rename the owned file to moved-away.db and write a foreign state.db.
 * Everything is a real fs operation in this test's own root; the log is written at exit.
 */
const PRELOAD = String.raw`
import fs from "node:fs";
import path from "node:path";
import { syncBuiltinESMExports } from "node:module";
const cfg = JSON.parse(process.env.FABRIC_REFUSAL_CLI_FIXTURE ?? "null");
if (cfg) {
  const real = { openSync: fs.openSync, lstatSync: fs.lstatSync, renameSync: fs.renameSync, mkdtempSync: fs.mkdtempSync, unlinkSync: fs.unlinkSync };
  const realRoot = fs.realpathSync(cfg.root);
  const db = path.join(cfg.root, "state.db");
  const idOf = (stat) => stat ? stat.dev + ":" + stat.ino : undefined;
  const state = { created: false, owned: undefined, proofMoved: false, checked: false, fired: false, foreign: undefined, firedAt: undefined, events: [] };
  let busy = false;
  const isRootDb = (target) => {
    if (busy) return false;
    busy = true;
    try {
      const text = target instanceof URL ? target.pathname : String(target);
      return path.basename(text) === "state.db" && fs.realpathSync(path.dirname(path.resolve(text))) === realRoot;
    } catch { return false; } finally { busy = false; }
  };
  const fire = (at) => {
    state.fired = true;
    state.firedAt = at;
    real.renameSync(db, path.join(cfg.root, "moved-away.db"));
    fs.writeFileSync(db, "foreign", { flag: "wx", mode: 0o600 });
    state.foreign = idOf(real.lstatSync(db));
    state.events.push("swap before " + at);
  };
  const armedSwap = () => cfg.mode === "swap" && state.created && state.checked && !state.fired;
  fs.openSync = function (target, flags, mode) {
    const fd = real.openSync.apply(fs, arguments);
    const exclusive = flags === "wx" || (typeof flags === "number" && (flags & fs.constants.O_EXCL) !== 0 && (flags & fs.constants.O_CREAT) !== 0);
    if (!state.created && exclusive && isRootDb(target)) {
      state.created = true;
      state.owned = idOf(fs.fstatSync(fd));
      real.renameSync(cfg.proof, cfg.movedProof);
      state.proofMoved = true;
      state.events.push("created state.db " + state.owned + " via " + String(target) + "; moved the forwarder proof away");
    }
    return fd;
  };
  fs.lstatSync = function (target) {
    const result = real.lstatSync.apply(fs, arguments);
    const isDb = isRootDb(target);
    state.checked = isDb && state.created && !state.fired && idOf(result) === state.owned;
    if (state.checked) state.events.push("lstat state.db -> owned via " + String(target));
    return result;
  };
  fs.mkdtempSync = function (prefix) {
    if (armedSwap() && path.basename(String(prefix)).startsWith(".import-quarantine")) fire("quarantine mkdtemp");
    state.checked = false;
    return real.mkdtempSync.apply(fs, arguments);
  };
  fs.renameSync = function (from) {
    if (armedSwap() && isRootDb(from)) fire("rename of state.db");
    state.checked = false;
    return real.renameSync.apply(fs, arguments);
  };
  fs.unlinkSync = function (target) {
    if (armedSwap() && isRootDb(target)) fire("unlink of state.db");
    state.checked = false;
    return real.unlinkSync.apply(fs, arguments);
  };
  syncBuiltinESMExports();
  process.on("exit", () => { fs.writeFileSync(cfg.log, JSON.stringify(state)); });
}
`;

interface FixtureLog { created: boolean; owned?: string; proofMoved: boolean; fired: boolean; foreign?: string; firedAt?: string; events: string[] }
interface Refusal { ok: boolean; refused?: boolean; error?: string; cleanupWarnings?: string[]; retained?: string[]; reason?: string }

const idOf = (stat: fs.Stats | undefined): string | undefined => stat ? `${stat.dev}:${stat.ino}` : undefined;
const dbNames = (root: string): string[] => fs.readdirSync(root).filter(name => DB_NAME.test(name)).sort();

describe.skipIf(!HOSTED)("the built fabric-mesh-backend: readiness refusal before the importing commit (hosted Linux only)", () => {
  it("clean refusal removes this run's state.db*; a state.db swapped between the check and the move is kept", async () => {
    // Required on CI: a fresh `bun run build` (never skip when absent here).
    expect(fs.existsSync(BIN), BIN).toBe(true);
    expect(fs.existsSync(BUILT_CLI), `${BUILT_CLI}: run bun run build before the tests`).toBe(true);
    const newestSource = Math.max(...fs.readdirSync(path.resolve("src/mesh")).filter(name => name.endsWith(".ts"))
      .map(name => fs.statSync(path.resolve("src/mesh", name)).mtimeMs));
    expect(fs.statSync(BUILT_CLI).mtimeMs, "dist/mesh/mesh-backend-cli.js is older than src/mesh").toBeGreaterThanOrEqual(newestSource);

    const base = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-refusal-cli-"));
    dirs.push(base);
    const home = path.join(base, "home"); // no built-in factory install under this HOME: only the forwarder is required
    fs.mkdirSync(home, { mode: 0o700 });
    const install = path.join(base, "forwarder-install");
    fs.mkdirSync(path.join(install, "releases", "f1"), { recursive: true, mode: 0o700 });
    fs.symlinkSync(path.join("releases", "f1"), path.join(install, "current"));
    const preload = path.join(base, "refusal-preload.mjs");
    fs.writeFileSync(preload, PRELOAD, { mode: 0o600 });
    const env = { ...process.env, HOME: home, PI_FABRIC_MESH_STATE_BACKEND: "", FABRIC_REFUSAL_CLI_FIXTURE: "" };

    const cli = (argv: string[], fixture?: Record<string, string>): { code: number | null; out: string; err: string } => {
      const child = spawnSync(process.execPath, [...fixture ? ["--import", pathToFileURL(preload).href] : [], BIN, ...argv], {
        encoding: "utf8", timeout: CHILD_TIMEOUT, killSignal: "SIGKILL",
        env: { ...env, FABRIC_REFUSAL_CLI_FIXTURE: fixture ? JSON.stringify(fixture) : "" },
      });
      return { code: child.status, out: child.stdout, err: child.stderr };
    };

    const scenario = async (name: string, command: "import" | "cutover", mode: "clean" | "swap") => {
      const root = path.join(base, name);
      fs.mkdirSync(root, { mode: 0o700 });
      const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
      await store.put({ key: "seed/a", value: { n: 1 }, identity });
      await store.put({ key: "seed/b", value: "two", identity });
      store.closeState();
      // The custom reader's proof, installed by the real command: the first readiness check is ready.
      const proofFile = path.join(base, `${name}-forwarder.json`);
      fs.writeFileSync(proofFile, JSON.stringify({ name: "forwarder", implementation: "test-forwarder", version: "forwarder-1", release: "f1",
        backends: ["file", "sqlite"], provedAt: new Date().toISOString(), provedEpoch: 0 }), { mode: 0o600 });
      const proved = cli(["reader-proof", "--root", root, "--name", "forwarder", "--proof-file", proofFile, "--json"]);
      expect(proved, proved.err).toMatchObject({ code: 0 });
      const proof = path.join(root, "readers", "forwarder.json");
      expect(fs.existsSync(proof)).toBe(true);
      const stateJson = fs.readFileSync(path.join(root, "state.json"));
      expect(dbNames(root)).toEqual([]);

      const log = path.join(base, `${name}-fixture-log.json`);
      const movedProof = path.join(base, `${name}-moved-proof.json`);
      const result = cli([command, "--root", root, "--json", "--require-reader", `forwarder=${install}`,
        "--accept-unready", "factory,fabric@unknown,holder@lease-probe"], { root, proof, movedProof, log, mode });
      const trace = `${result.err}\n${fs.existsSync(log) ? fs.readFileSync(log, "utf8") : "(no fixture log)"}`;
      expect(result.code, trace).toBe(3);
      const fixture = JSON.parse(fs.readFileSync(log, "utf8")) as FixtureLog;
      const json = JSON.parse(result.out) as Refusal;
      // The initial (preview) readiness held the forwarder ready; the fenced re-check after the create refused.
      expect(fixture).toMatchObject({ created: true, proofMoved: true });
      expect(fs.existsSync(movedProof)).toBe(true);
      expect(result.err).not.toContain("reader forwarder not ready");
      expect(json, trace).toMatchObject({ ok: false, refused: true });
      expect(json.error).toContain("forwarder");
      expect(json.error).toContain("before the importing commit");
      expect(result.err).toContain(`fabric-mesh-backend: ${command} refused:`);
      // The disk state.json is byte-identical.
      expect(fs.readFileSync(path.join(root, "state.json")).equals(stateJson)).toBe(true);
      // The cleanup report is visible in both channels.
      for (const text of json.cleanupWarnings ?? []) expect(result.err).toContain(text);
      for (const entry of fs.readdirSync(root).filter(entry => entry.startsWith(".import-quarantine"))) {
        expect((json.retained ?? []).some(kept => kept === entry || kept.startsWith(`${entry}/`)), `unreported quarantine ${entry}`).toBe(true);
      }
      return { root, json, fixture, trace };
    };

    // 1. Clean refusal: every state.db* this run created is really removed, nothing retained or warned.
    const clean = await scenario("clean", "import", "clean");
    expect(clean.fixture.fired).toBe(false);
    expect(dbNames(clean.root), clean.trace).toEqual([]);
    expect(fs.readdirSync(clean.root).filter(entry => entry.startsWith(".import-quarantine"))).toEqual([]);
    expect(clean.json.retained ?? []).toEqual([]);
    expect(clean.json.cleanupWarnings ?? []).toEqual([]);

    // 2. Swap after the owned-inode lstat, before the move/removal: the foreign replacement keeps its name and bytes.
    const swap = await scenario("swap", "cutover", "swap");
    expect(swap.fixture.fired, swap.trace).toBe(true);
    expect(idOf(fs.lstatSync(path.join(swap.root, "state.db")))).toBe(swap.fixture.foreign);
    expect(fs.readFileSync(path.join(swap.root, "state.db"), "utf8")).toBe("foreign");
    expect(idOf(fs.lstatSync(path.join(swap.root, "moved-away.db")))).toBe(swap.fixture.owned);
    expect(dbNames(swap.root), swap.trace).toEqual(["state.db"]);
    expect(swap.json.retained).toContain("state.db");
    expect(swap.json.cleanupWarnings?.some(text => text.includes(`${swap.root}/state.db`)), swap.trace).toBe(true);
    expect(swap.json.cleanupWarnings!.length).toBeGreaterThan(0);
  }, 5 * CHILD_TIMEOUT);
});
