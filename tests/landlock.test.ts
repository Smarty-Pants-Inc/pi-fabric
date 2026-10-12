import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { exec } from "node:child_process";
import { createHash } from "node:crypto";
import type { BashOperations } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { executeFile } from "../src/agents/transports/process-utils.js";
import { groupOperations, LandlockBashConfinement, type LandlockSettings } from "../src/core/landlock.js";

const helper = path.resolve("dist/native/fabric-landlock");
const quote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`;
const roots: string[] = [];
const confinements: LandlockBashConfinement[] = [];
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-landlock-routes-"));
  roots.push(root);
  const cwd = path.join(root, "lane");
  const sibling = path.join(root, "sibling");
  const tmp = path.join(root, "tmp");
  for (const directory of [cwd, sibling, tmp]) fs.mkdirSync(directory, { mode: 0o700 });
  const victim = path.join(sibling, "victim");
  fs.writeFileSync(victim, "protected");
  vi.stubEnv("TMPDIR", tmp);
  vi.stubEnv("SMARTY_ROLE", "task-agent@reviewed-policy");
  const confinement = new LandlockBashConfinement(cwd);
  confinements.push(confinement);
  const audit = () => fs.readFileSync(path.join(cwd, ".pi/landlock-audit.jsonl"), "utf8")
    .trim().split("\n").map(line => JSON.parse(line));
  return { root, cwd, victim, confinement, audit };
};

const execOperations = (executable: string): BashOperations => ({
  exec: (command, cwd, options) => new Promise((resolve, reject) => {
    exec(`${quote(executable)} -c ${quote(command)}`, { cwd, env: options.env, timeout: 5_000 },
      (error, stdout, stderr) => {
        options.onData(Buffer.from(stdout + stderr));
        if (error && typeof error.code !== "number") reject(error);
        else resolve({ exitCode: typeof error?.code === "number" ? error.code : 0 });
      });
  }),
});
// These are test-only Bash operations adapters, not evidence that production
// actor/task/exec launchers are Landlock-enforced (smarty-dev#7935).
const routes = [
  ["Bash exec adapter", execOperations],
  ["Bash process-group spawn", (executable: string) => groupOperations(executable, ["-c"])],
] as const;

const invoke = async (ops: BashOperations, command: string, cwd: string, env?: NodeJS.ProcessEnv) => {
  let output = "";
  const result = await ops.exec(command, cwd, { ...(env ? { env } : {}), timeout: 5, onData: data => { output += data.toString(); } });
  return { ...result, output };
};

afterEach(() => {
  for (const confinement of confinements.splice(0)) confinement.close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
});

describe.skipIf(process.platform !== "linux")("Bash Landlock operations wrapper", () => {
  beforeAll(async () => { expect(Number((await executeFile(helper, ["--abi"])).stdout)).toBeGreaterThanOrEqual(4); });

  describe.each(routes)("%s", (_route, operations) => {
    it.each([false, true])("consumes prefix/env requests; root allowEscape=%s controls writes and logging", async allowEscape => {
      const h = fixture();
      const settings: LandlockSettings = { mode: "enforce", disabled: false, allowEscape };
      const ops = h.confinement.operations(operations(helper), operations("/bin/sh"), "/bin/sh",
        h.cwd, () => settings);
      for (const request of ["prefix", "explicit env", "inherited env"] as const) {
        fs.writeFileSync(h.victim, "protected");
        const body = `printf '%s\\n' "\${PI_FABRIC_LANDLOCK_ESCAPE-unset}"; printf changed > ${quote(h.victim)}`;
        const command = request === "prefix" ? `PI_FABRIC_LANDLOCK_ESCAPE=1 PI_FABRIC_LANDLOCK_ESCAPE=1 ${body}` : body;
        vi.stubEnv("PI_FABRIC_LANDLOCK_ESCAPE", request === "inherited env" ? "1" : undefined);
        const env = request === "inherited env" ? undefined : {
          ...process.env, ...(request === "explicit env" ? { PI_FABRIC_LANDLOCK_ESCAPE: "1" } : {}),
        };
        const result = await invoke(ops, command, h.cwd, env);
        expect(result.output).toContain("unset\n");
        expect(result.exitCode === 0).toBe(allowEscape);
        expect(fs.readFileSync(h.victim, "utf8")).toBe(allowEscape ? "changed" : "protected");
        const row = h.audit().at(-1);
        expect(row.event).toBe(allowEscape ? "escape" : "enforce");
        expect(row.commandSha256).toBe(createHash("sha256").update(command).digest("hex"));
        expect(result.output.includes("Landlock escape: unconfined")).toBe(allowEscape);
        expect(JSON.stringify(row)).not.toContain(body);
      }
      vi.stubEnv("PI_FABRIC_LANDLOCK_ESCAPE", undefined);
      expect((await invoke(ops, `printf denied > ${quote(h.victim)}`, h.cwd)).exitCode).not.toBe(0);
      expect(h.audit().at(-1).event).toBe("enforce");
    });

    it("direct native helper strips assignments and inherited env without trusting a grant-shaped env", async () => {
      const h = fixture();
      const pin = fs.statSync(h.cwd, { bigint: true });
      const env = { ...process.env, PI_FABRIC_LANDLOCK_SHELL: "/bin/sh",
        PI_FABRIC_LANDLOCK_WRITES: `${pin.dev}:${pin.ino}:${h.cwd}`, PI_FABRIC_LANDLOCK_ESCAPE: "1" };
      const body = `printf '%s\\n' "\${PI_FABRIC_LANDLOCK_ESCAPE-unset}"; printf changed > ${quote(h.victim)}`;
      const result = await invoke(operations(helper), `PI_FABRIC_LANDLOCK_ESCAPE=1 PI_FABRIC_LANDLOCK_ESCAPE=1 ${body}`, h.cwd, env);
      expect(result.exitCode).not.toBe(0);
      expect(result.output).toContain("unset\n");
      expect(fs.readFileSync(h.victim, "utf8")).toBe("protected");
    });
  });

  it.each(["actor", "task-agent"])("Bash descendants with %s role inherit confinement across exec and spawn", async role => {
    const h = fixture();
    // A benign worker fixture executes actual Node execFileSync/spawnSync child paths;
    // no model, credentials or agent session is launched.
    const source = `const cp = require('node:child_process');
const fs = require('node:fs');
if (process.env.PI_FABRIC_LANDLOCK_ESCAPE !== undefined) throw Error('reserved env leaked');
const victim = ${JSON.stringify(h.victim)};
const body = 'PI_FABRIC_LANDLOCK_ESCAPE=1 printf descendant > ' + ${JSON.stringify(quote(h.victim))};
const run = command => cp.spawnSync('/bin/sh', ['-c', command], {encoding:'utf8', env:{...process.env, PI_FABRIC_LANDLOCK_ESCAPE:'1'}});
const result = run(body);
let execAllowed = true;
try { cp.execFileSync('/bin/sh', ['-c', body], {stdio:'ignore'}); } catch { execAllowed = false; }
console.log(JSON.stringify({spawnAllowed:result.status===0, execAllowed, role:process.env.SMARTY_ROLE}));`;
    for (const allowEscape of [false, true]) {
      fs.writeFileSync(h.victim, "protected");
      const ops = h.confinement.operations(groupOperations(helper, ["-c"]), groupOperations("/bin/sh", ["-c"]),
        "/bin/sh", h.cwd, () => ({ mode: "enforce", disabled: false, allowEscape }));
      const result = await invoke(ops, `PI_FABRIC_LANDLOCK_ESCAPE=1 ${quote(process.execPath)} -e ${quote(source)}`,
        h.cwd, { ...process.env, SMARTY_ROLE: role });
      expect(result.exitCode).toBe(0);
      expect(result.output).toContain(JSON.stringify({ spawnAllowed: allowEscape, execAllowed: allowEscape, role }));
      expect(fs.readFileSync(h.victim, "utf8")).toBe(allowEscape ? "descendant" : "protected");
      expect(h.audit().at(-1).event).toBe(allowEscape ? "escape" : "enforce");
    }
  });

  it("rechecks authority for every exec on a reused wrapper, including grant revocation", async () => {
    const h = fixture();
    const settings: LandlockSettings = { mode: "enforce", disabled: false, allowEscape: true };
    const ops = h.confinement.operations(groupOperations(helper, ["-c"]), groupOperations("/bin/sh", ["-c"]),
      "/bin/sh", h.cwd, () => settings);
    settings.allowEscape = false;
    expect((await invoke(ops, `PI_FABRIC_LANDLOCK_ESCAPE=1 printf bad > ${quote(h.victim)}`, h.cwd)).exitCode).not.toBe(0);
    expect(h.audit()[0].event).toBe("enforce");
  });

  it("refuses granted environment escapes before spawn if mandatory logging fails", async () => {
    const h = fixture();
    fs.mkdirSync(path.join(h.cwd, ".pi"));
    fs.symlinkSync(h.victim, path.join(h.cwd, ".pi/landlock-audit.jsonl"));
    const ops = h.confinement.operations(groupOperations(helper, ["-c"]), groupOperations("/bin/sh", ["-c"]),
      "/bin/sh", h.cwd, () => ({ mode: "enforce", disabled: false, allowEscape: true }));
    await expect(invoke(ops, `printf changed > ${quote(h.victim)}`, h.cwd,
      { ...process.env, PI_FABRIC_LANDLOCK_ESCAPE: "1" })).rejects.toThrow();
    expect(fs.readFileSync(h.victim, "utf8")).toBe("protected");
  });
});
