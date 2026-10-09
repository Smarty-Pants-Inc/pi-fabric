import { createBashTool } from "@earendil-works/pi-coding-agent";
import { execFileSync } from "node:child_process";
import { describe, expect, it } from "vitest";
import { applyRunBashDefaults, BASH_IDLE_EXIT_CODE } from "../src/guards/actor-bash-timeout.js";

// smarty-dev#6137: the idle watchdog through Pi's real bash tool, scaled down from 180 s to 2 s.
const run = async (command: string, env: Record<string, string> = { PI_FABRIC_BASH_IDLE_S: "2" }) => {
  const input: { command: string; timeout?: number } = { command };
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

const alive = (pattern: string): boolean =>
  execFileSync("ps", ["-A", "-o", "args="], { encoding: "utf8" }).split("\n").some((line) => line.trim() === pattern);

describe.skipIf(process.platform === "win32")("bash idle watchdog (smarty-dev#6137)", () => {
  it("kills a silent command at ~N s with the timeout result", { timeout: 20_000 }, async () => {
    const result = await run("sleep 300");
    expect(result.ok).toBe(false);
    expect(result.seconds).toBeGreaterThanOrEqual(1.9);
    expect(result.seconds).toBeLessThan(6);
    expect(result.text).toContain("[pi-fabric] bash idle timeout: no output for 2 s; killed; rerun with a bounded range or a command that prints progress");
    expect(result.text).toContain(`Command exited with code ${BASH_IDLE_EXIT_CODE}`);
  });

  it("does not kill a long command that keeps printing", { timeout: 20_000 }, async () => {
    const result = await run("for i in 1 2 3 4 5 6; do echo tick $i; sleep 1; done; echo done >&2");
    expect(result).toMatchObject({ ok: true, text: "tick 1\ntick 2\ntick 3\ntick 4\ntick 5\ntick 6\ndone\n" });
    expect(result.seconds).toBeGreaterThan(5.5);
  });

  it("kills setsid and background children too", { timeout: 20_000 }, async () => {
    const tag = `sleep ${300 + Math.floor(Math.random() * 600)}`;
    const result = await run(`setsid ${tag} & (${tag}.5 &); echo started; sleep 300`);
    expect(result.text).toMatch(/^started\n[\s\S]*no output for 2 s/);
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(alive(tag)).toBe(false);
    expect(alive(`${tag}.5`)).toBe(false);
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
    const input = { command: "sleep 300", timeout: 1000 };
    applyRunBashDefaults({ PI_FABRIC_BASH_IDLE_S: "2" }, input);
    expect(input.timeout).toBe(1000);
    const started = Date.now();
    await expect(createBashTool(process.cwd()).execute("call-1", input, undefined, undefined)).rejects.toThrow(/no output for 2 s/);
    expect(Date.now() - started).toBeLessThan(6000);
  });

  it("leaves a Main (no worker environment) and bashIdleSeconds 0 unchanged", { timeout: 20_000 }, async () => {
    expect(await run("sleep 3; echo late", {})).toMatchObject({ ok: true, text: "late\n" });
    expect(await run("sleep 3; echo late", { PI_FABRIC_BASH_IDLE_S: "0" })).toMatchObject({ ok: true, text: "late\n" });
  });
});
