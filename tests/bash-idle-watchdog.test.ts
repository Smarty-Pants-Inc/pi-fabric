import { createBashTool } from "@earendil-works/pi-coding-agent";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { applyRunBashDefaults, BASH_IDLE_EXIT_CODE, BASH_IDLE_MARKER, BASH_IDLE_TERM_GRACE_S } from "../src/guards/actor-bash-timeout.js";

// Direct defaults-helper -> real Pi bash seam, scaled from 180 s to 2 s, NOT AgentManager E2E.
// CI has no authorized live model/account or 1.48M-commit replay fixture. The PR body names the
// real process-task AgentManager #6137 proof replay and the owner plan to rerun it after build.
const run = async (command: string, env: Record<string, string> = { PI_FABRIC_BASH_IDLE_S: "2" }, timeout?: number) => {
  const input: { command: string; timeout?: number } = { command, ...(timeout === undefined ? {} : { timeout }) };
  applyRunBashDefaults(env, input);
  const started = Date.now();
  try {
    const result = await createBashTool(process.cwd()).execute("call-1", input, undefined, undefined);
    const part = result.content[0];
    return { ok: true, seconds: (Date.now() - started) / 1000, text: part?.type === "text" ? part.text : "" };
  } catch (error) {
    return { ok: false, seconds: (Date.now() - started) / 1000, text: (error as Error).message };
  }
};

const quote = (value: string): string => `'${value.replaceAll("'", `'\\''`)}'`;
const roots: string[] = [];
const alive = (pid: number): boolean => {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    return !["Z", "X"].includes(stat.slice(stat.lastIndexOf(")") + 2).split(" ")[0]!);
  } catch { return false; }
};
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-idle-"));
  roots.push(root);
  const script = path.join(root, "child.cjs");
  fs.writeFileSync(script, `
const fs=require("fs"),path=require("path");
const[root,role]=process.argv.slice(2);
const stat=fs.readFileSync("/proc/self/stat","utf8").slice(fs.readFileSync("/proc/self/stat","utf8").lastIndexOf(")")+2).split(" ");
fs.writeFileSync(path.join(root,role+".pid"),JSON.stringify({pid:process.pid,pgid:Number(stat[2])}));
const record=event=>fs.appendFileSync(path.join(root,role+".events"),JSON.stringify({event,at:Date.now()})+"\\n");
process.on("SIGTERM",()=>{record("TERM");if(role==="parent")process.exit(0);process.stdout.write("term\\n")});
record("ready");
if(role==="parent")process.stdout.write("started\\n");
setInterval(()=>record("alive"),100);
`);
  const command = (role: string) => `${quote(process.execPath)} ${quote(script)} ${quote(root)} ${quote(role)}`;
  const info = (role: string): { pid: number; pgid: number } => JSON.parse(fs.readFileSync(path.join(root, `${role}.pid`), "utf8"));
  const events = (role: string): Array<{ event: string; at: number }> => fs.readFileSync(path.join(root, `${role}.events`), "utf8").trim().split("\n").map(line => JSON.parse(line));
  return { root, command, info, events };
};

afterEach(() => {
  // A failing assertion must not leak any test-owned process.
  for (const root of roots.splice(0)) {
    for (const name of fs.readdirSync(root).filter(name => name.endsWith(".pid"))) {
      const { pid } = JSON.parse(fs.readFileSync(path.join(root, name), "utf8")) as { pid: number };
      if (alive(pid)) try { process.kill(pid, "SIGKILL"); } catch { /* gone */ }
    }
    fs.rmSync(root, { recursive: true, force: true });
  }
});

describe.skipIf(process.platform === "win32")("bash idle watchdog (smarty-dev#6137)", () => {
  it("kills a silent command after N s plus bounded TERM grace with the timeout result", { timeout: 20_000 }, async () => {
    const result = await run("sleep 300");
    expect(result.ok).toBe(false);
    expect(result.seconds).toBeGreaterThanOrEqual(1.9 + BASH_IDLE_TERM_GRACE_S);
    expect(result.seconds).toBeLessThan(10);
    expect(result.text).toContain("[pi-fabric] bash idle timeout: no output for 2 s; killed; rerun with a bounded range or a command that prints progress");
    expect(result.text).toContain(`Command exited with code ${BASH_IDLE_EXIT_CODE}`);
  });

  it("watches a caller command beginning with the public marker comment", { timeout: 20_000 }, async () => {
    const result = await run(`${BASH_IDLE_MARKER}\nsleep 300`);
    expect(result.ok).toBe(false);
    expect(result.text).toContain("no output for 2 s");
    expect(result.text).toContain(`Command exited with code ${BASH_IDLE_EXIT_CODE}`);
    expect(result.seconds).toBeLessThan(10);
  });

  it("does not kill a long command that keeps printing", { timeout: 20_000 }, async () => {
    const result = await run("for i in 1 2 3 4 5 6; do echo tick $i; sleep 1; done; echo done >&2");
    expect(result).toMatchObject({ ok: true, text: "tick 1\ntick 2\ntick 3\ntick 4\ntick 5\ntick 6\ndone\n" });
    expect(result.seconds).toBeGreaterThan(5.5);
  });

  it.skipIf(process.platform !== "linux")("sends TERM before KILL and preserves all 5 s of grace even after TERM output", { timeout: 20_000 }, async () => {
    const f = fixture();
    const started = Date.now();
    const result = await run(f.command("stubborn"));
    const events = f.events("stubborn");
    const term = events.find(event => event.event === "TERM")!;
    expect(term).toBeDefined();
    expect(term.at - started).toBeGreaterThanOrEqual(1900);
    const lastAlive = events.filter(event => event.event === "alive").at(-1)!;
    expect(lastAlive.at - term.at).toBeGreaterThanOrEqual(4700);
    expect(Date.now() - term.at).toBeGreaterThanOrEqual(4900);
    expect(result.seconds).toBeLessThan(10);
    expect(result.text).toContain("term\n");
    expect(result.text).toContain(`Command exited with code ${BASH_IDLE_EXIT_CODE}`);
    expect(alive(f.info("stubborn").pid)).toBe(false);
  });

  it.skipIf(process.platform !== "linux")("kills setsid and reparented in-group children without any ps executable", { timeout: 20_000 }, async () => {
    const f = fixture();
    const emptyPath = path.join(f.root, "empty-bin");
    fs.mkdirSync(emptyPath);
    const setsid = ["/usr/bin/setsid", "/bin/setsid"].find(file => fs.existsSync(file))!;
    expect(setsid).toBeDefined();
    const result = await run(`PATH=${quote(emptyPath)}; export PATH; ${quote(setsid)} ${f.command("setsid")} & (${f.command("orphan")} &); ${f.command("parent")}`);
    expect(result.text).toContain("started\n");
    expect(result.text).toContain("no output for 2 s");
    const parent = f.info("parent"), orphan = f.info("orphan"), detached = f.info("setsid");
    expect(orphan.pgid).toBe(parent.pgid);
    expect(detached.pgid).toBe(detached.pid);
    expect(detached.pgid).not.toBe(parent.pgid);
    for (const role of ["parent", "orphan", "setsid"]) {
      expect(f.events(role).some(event => event.event === "TERM")).toBe(true);
      expect(alive(f.info(role).pid)).toBe(false);
    }
  });

  it.skipIf(process.platform !== "linux")("Pi's hard total timeout still kills the newly detached command group", { timeout: 20_000 }, async () => {
    const f = fixture();
    const result = await run(f.command("stubborn"), { PI_FABRIC_BASH_IDLE_S: "30" }, 1);
    expect(result.ok).toBe(false);
    expect(result.text).toContain("timed out");
    expect(result.seconds).toBeLessThan(4);
    expect(alive(f.info("stubborn").pid)).toBe(false);
  });

  it("keeps the command's exit code, output, quoting and heredocs, and returns at shell exit", { timeout: 20_000 }, async () => {
    expect(await run("echo out; echo err >&2; exit 3")).toMatchObject({ ok: false, text: "out\nerr\n\n\nCommand exited with code 3" });
    expect(await run(`printf '%s\\n' "it's" "$((1+2))"; cat <<'EOF'\n$HOME\nEOF`)).toMatchObject({ ok: true, text: "it's\n3\n$HOME\n" });
    const background = await run("sleep 30 >/dev/null 2>&1 & echo $!");
    expect(background.ok).toBe(true);
    expect(background.seconds).toBeLessThan(2);
    process.kill(Number(background.text.trim()));
  });

  it("still kills a silent call that passed its own long timeout", { timeout: 20_000 }, async () => {
    const result = await run("sleep 300", { PI_FABRIC_BASH_IDLE_S: "2" }, 1000);
    expect(result.ok).toBe(false);
    expect(result.text).toContain("no output for 2 s");
    expect(result.seconds).toBeLessThan(10);
  });

  it("leaves a Main (no worker environment) and bashIdleSeconds 0 unchanged", { timeout: 20_000 }, async () => {
    expect(await run("sleep 3; echo late", {})).toMatchObject({ ok: true, text: "late\n" });
    expect(await run("sleep 3; echo late", { PI_FABRIC_BASH_IDLE_S: "0" })).toMatchObject({ ok: true, text: "late\n" });
  });
});
