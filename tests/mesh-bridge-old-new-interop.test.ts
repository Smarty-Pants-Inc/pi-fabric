// Hosted-CI-only expected path: genuine Linux GitHub-hosted runner, after dist build.
// Real CLI teardown deletes product mirror/lease files: never run or spoof admission locally.
// Keep reason: OLD/new native stdio trust boundary, real owner admission and return ACKs.
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { hostedOnly } from "../scripts/hosted-only.mjs";

const HOSTED = hostedOnly("old/new mesh bridge stdio interop") && process.platform === "linux";
if (!HOSTED) console.log("INTEROP-SKIPPED reason=requires-genuine-github-hosted-linux");
const OLD = "069f594ff99429da11ea819b4930c012ca4cdf4e";
const REPO = path.resolve(import.meta.dirname, "..");
const sha = (bytes: string | Buffer) => createHash("sha256").update(bytes).digest("hex");

describe.skipIf(!HOSTED)("OLD/new built mesh-bridge interoperability (hosted Linux CI only)", () => {
  it("links NEW native stdio to OLD 069f594 agent and admits/ACKs all 192 commands", async () => {
    expect(HOSTED).toBe(true); // Guard executes before allocating fixtures or creating any actor.
    const base = fs.mkdtempSync(path.join(os.tmpdir(), "mbi-"));
    fs.chmodSync(base, 0o700);
    const steps: unknown[] = [];
    const save = (name: string, value: unknown) => fs.writeFileSync(path.join(base, name), JSON.stringify(value, null, 2) + "\n");
    let ordinal = 0;
    // Each foreground step owns a distinct Linux process group, records PID/start/argv,
    // has a hard TERM->KILL deadline, and joins actual close before returning custody.
    const run = async (command: string, args: string[], cwd: string, limit: number,
      extraEnv: NodeJS.ProcessEnv = {}): Promise<string> => {
      const id = ++ordinal, started = Date.now();
      const child = spawn(command, args, { cwd, env: { ...process.env, ...extraEnv }, detached: true,
        stdio: ["ignore", "pipe", "pipe"] });
      const pid = child.pid;
      save(`step-${id}-owned.json`, { pid, command: [command, ...args], cwd, deadlineMs: limit,
        stat: pid ? fs.readFileSync(`/proc/${pid}/stat`, "utf8") : null });
      let stdout = "", stderr = "", timedOut = false, spawnError: Error | undefined;
      child.stdout.setEncoding("utf8"); child.stderr.setEncoding("utf8");
      child.stdout.on("data", (chunk: string) => { stdout += chunk; });
      child.stderr.on("data", (chunk: string) => { stderr += chunk; });
      child.once("error", error => { spawnError = error; });
      const signal = (name: NodeJS.Signals) => {
        if (!pid) return;
        try { process.kill(-pid, name); } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
        }
      };
      let force: ReturnType<typeof setTimeout> | undefined;
      const timer = setTimeout(() => { timedOut = true; signal("SIGTERM"); force = setTimeout(() => signal("SIGKILL"), 8_000); }, limit);
      const terminal = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(resolve => {
        child.once("close", (code, signal) => resolve({ code, signal }));
      });
      clearTimeout(timer); if (force) clearTimeout(force);
      fs.writeFileSync(path.join(base, `step-${id}.stdout`), stdout);
      fs.writeFileSync(path.join(base, `step-${id}.stderr`), stderr);
      const receipt = { id, pid, command: [command, ...args], started, ended: Date.now(), ...terminal, timedOut, error: spawnError?.message };
      steps.push(receipt); save("steps.json", steps);
      // A closed leader alone is not descendant cleanup. Refuse handback if its group
      // still exists; terminate only that recorded group, never process-name patterns.
      let groupAlive = false;
      if (pid) try { process.kill(-pid, 0); groupAlive = true; } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
      }
      if (groupAlive) {
        signal("SIGKILL");
        // One finite cleanup observation after the recorded group's forced stop.
        await new Promise(resolve => setTimeout(resolve, 2_000));
        let remains = false;
        try { process.kill(-pid!, 0); remains = true; } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
        }
        save(`step-${id}-cleanup.json`, { pid, forced: true, groupStillExists: remains });
        throw new Error(`Owned group ${pid} required forced cleanup (still exists=${remains}); evidence: ${base}`);
      }
      if (timedOut || spawnError || terminal.code !== 0) throw new Error(`Step ${id} failed: ${JSON.stringify(receipt)}\n${stderr}\nEvidence: ${base}`);
      return stdout;
    };
    try {
      const head = (await run("git", ["rev-parse", "HEAD"], REPO, 10_000)).trim();
      expect(head).toMatch(/^[0-9a-f]{40}$/);
      // Fail closed on dirty build inputs, rather than label unrelated current dist as HEAD.
      await run("git", ["diff", "--quiet", "HEAD", "--", "src", "native", "package.json", "bun.lock", "scripts"], REPO, 10_000);
      const source = await run("git", ["show", `${head}:src/mesh/bridge.ts`], REPO, 10_000);
      const sourceSha = sha(source);
      expect(sha(fs.readFileSync(path.join(REPO, "src/mesh/bridge.ts")))).toBe(sourceSha);
      const oldRelease = path.join(base, "old"), newRelease = path.join(base, "new"), fixture = path.join(base, "fixture");
      for (const dir of [oldRelease, newRelease, fixture]) fs.mkdirSync(dir, { mode: 0o700 });
      for (const [commit, release] of [[OLD, oldRelease], [head, newRelease]] as const) {
        const archive = path.join(base, `${commit}.tar`);
        await run("git", ["archive", "--format=tar", `--output=${archive}`, commit], REPO, 20_000);
        await run("tar", ["-xf", archive, "-C", release], REPO, 20_000);
        // These provenance markers belong ONLY to private extracted test roots.
        fs.writeFileSync(path.join(release, "COMMIT"), commit + "\n");
        const lock = fs.readFileSync(path.join(release, "bun.lock"));
        if (!lock.equals(fs.readFileSync(path.join(REPO, "bun.lock")))) {
          throw new Error(`Cannot reuse current dependencies for ${commit}: bun.lock differs. CI needs a separate frozen-lockfile dependency preparation for OLD ${OLD}; no install/fetch is performed by this test.`);
        }
      }
      // Copy the FULL dependency closure, preserving only confined relative symlinks.
      // No prepack, dependency mutation, live HOME copy, install or network fallback.
      const deps = fs.realpathSync(path.join(REPO, "node_modules"));
      const checkDeps = (dir: string): void => {
        for (const name of fs.readdirSync(dir)) {
          const file = path.join(dir, name), stat = fs.lstatSync(file);
          if (stat.isSymbolicLink()) {
            const link = fs.readlinkSync(file), target = fs.realpathSync(file);
            if (path.isAbsolute(link) || !target.startsWith(deps + path.sep)) throw new Error(`Unconfined dependency link: ${file}`);
          } else if (stat.isDirectory()) checkDeps(file);
          else if (!stat.isFile()) throw new Error(`Special dependency entry: ${file}`);
        }
      };
      checkDeps(deps);
      for (const release of [oldRelease, newRelease]) fs.cpSync(deps, path.join(release, "node_modules"), { recursive: true, verbatimSymlinks: true });
      fs.cpSync(path.join(REPO, "dist"), path.join(newRelease, "dist"), { recursive: true, verbatimSymlinks: true });
      // CI fetched the exact native toolchain before its normal build. Require that
      // cached pin for OLD too; never fall back to an unpinned compiler or fetch it here.
      await run("bun", ["run", "build"], oldRelease, 120_000, { FABRIC_NATIVE_CC_PINNED: "1" });
      await run("bun", ["run", "assert:build-artifacts"], newRelease, 30_000);
      save("preparation.json", { oldCommit: OLD, newCommit: head, newSourceSha: sourceSha,
        lockSha256: sha(fs.readFileSync(path.join(REPO, "bun.lock"))), oldRelease, newRelease, fixture,
        newDist: "complete current CI-built dist closure copied after HEAD/source/lock checks" });
      await run(process.execPath, [path.join(newRelease, "scripts/bridge-fence-interop.mjs"),
        "--old-release", oldRelease, "--new-release", newRelease, "--new-commit", head,
        "--new-source-sha", sourceSha, "--private-fixture", fixture], newRelease, 90_000);
      const result = JSON.parse(fs.readFileSync(path.join(fixture, "result.json"), "utf8"));
      expect(result).toMatchObject({ ok: true, oldCommit: OLD, newCommit: head, newSourceSha: sourceSha,
        transport: { linked: true }, stopReceipt: { code: 0, signal: null }, correctness: {
          sent: 192, delivered: 192, byDirection: { toLocal: 96, toRemote: 96 }, acks: 192, nativeAdmissions: 192,
          drops: 0, duplicates: 0, outOfOrder: 0, invalid: 0 } });
      const provenance = JSON.parse(fs.readFileSync(path.join(fixture, "provenance.json"), "utf8"));
      expect(provenance.map((entry: { commit: string }) => entry.commit)).toEqual([result.oldCommit, result.newCommit]);
      expect(provenance[1].bridgeSourceSha256).toBe(result.newSourceSha);
      save("test-result.json", { ok: true, head, sourceSha, result: path.join(fixture, "result.json"), steps });
      // Only validated native counts, ACKs, closure and exact provenance earn RAN.
      console.log(`INTEROP-RAN old=${result.oldCommit} new=${result.newCommit} delivered=192/192 acks=ok`);
      console.log(`OLD/new bridge interoperability evidence retained: ${base}`);
    } catch (error) {
      save("test-result.json", { ok: false, error: error instanceof Error ? error.stack : String(error), steps });
      console.error(`OLD/new bridge failure evidence retained: ${base}`);
      throw error;
    }
    // Intentionally retain JSON/stdout/stderr/native journals on both success and
    // failure until the CI runner's own temporary-root lifecycle ends. No root cleanup.
  }, 300_000);
});
