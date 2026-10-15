import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import { execFileSync } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

// smarty-dev#8606: production compiled worker -> real Pi -> production compiled
// Fabric extension, with a profile guard extension that refuses `git stash` in
// a linked worktree on tool_call (the fleet guard's rule, judged from ctx.cwd
// or a leading cd). Only inference is substituted by a loopback endpoint.
// fabric_exec's pi.bash must meet the same guard as the native bash tool.
const cleanups: Array<() => Promise<unknown> | void> = [];
afterEach(async () => {
  try { for (const close of cleanups.splice(0).reverse()) await close(); }
  finally { vi.unstubAllEnvs(); }
});
const GUARD_REASON = "test guard refused git stash in a linked worktree (#8606)";
const shellQuote = (value: string) => "'" + value.replaceAll("'", "'\\''") + "'";

/** A Fabric install whose pi-coding-agent is a private copy, like a release tree beside a separate Pi runtime. */
const privateRealmFabric = (root: string): string => {
  const parent = path.join(root, "fabric-release");
  fs.mkdirSync(path.join(parent, "node_modules", "@earendil-works"), { recursive: true });
  fs.cpSync(path.resolve("dist"), path.join(parent, "dist"), { recursive: true });
  fs.copyFileSync(path.resolve("package.json"), path.join(parent, "package.json"));
  for (const entry of fs.readdirSync(path.resolve("node_modules"))) {
    if (entry === "@earendil-works") continue;
    fs.symlinkSync(path.resolve("node_modules", entry), path.join(parent, "node_modules", entry), "junction");
  }
  for (const entry of fs.readdirSync(path.resolve("node_modules/@earendil-works"))) {
    const source = fs.realpathSync(path.resolve("node_modules/@earendil-works", entry));
    const target = path.join(parent, "node_modules/@earendil-works", entry);
    if (entry === "pi-coding-agent") fs.cpSync(source, target, { recursive: true, verbatimSymlinks: true });
    else fs.symlinkSync(source, target, "junction");
  }
  return parent;
};

type Launch = "shim" | "native" | "native-private-realm" | "shim-cwd-argument" | "shim-allowed";

describe.skipIf(process.platform === "win32")("fabric_exec pi.bash in a real process worker obeys profile tool_call guards (#8606)", () => {
  // shim-allowed: a command the guard allows still runs in a worker run, where Fabric's own
  // tool_call hook adds the run defaults (timeout cap, idle watchdog): they are handed back to
  // the preflight, not a rewrite it refuses (CODE_REVIEW P1 on 692c62a0).
  it.each(["shim", "native", "native-private-realm", "shim-cwd-argument", "shim-allowed"] as const)("%s: pi.bash git stash in a linked worktree is refused with the guard's reason", async (launch: Launch) => {
    expect(fs.existsSync("dist/worker.js"), "fresh build required").toBe(true);
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-8606-guard-")));
    cleanups.push(() => fs.promises.rm(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }));
    const main = path.join(root, "main"); const linked = path.join(root, "linked");
    execFileSync("git", ["init", "-q", main]);
    execFileSync("git", ["-C", main, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-q", "--allow-empty", "-m", "init"]);
    execFileSync("git", ["-C", main, "worktree", "add", "-q", "--detach", linked]);
    const executed = path.join(root, "stash-executed");
    const refusals = path.join(root, "refusals.jsonl");
    const agentDir = path.join(root, "agent"); fs.mkdirSync(agentDir);
    const guard = path.join(root, "guard", "stash-guard.ts"); fs.mkdirSync(path.dirname(guard));
    fs.writeFileSync(guard, [
      "import fs from 'node:fs';",
      "export default function(pi) {",
      "  pi.on('tool_call', (event, ctx) => {",
      "    if (event.toolName !== 'bash') return undefined;",
      "    const command = String(event.input?.command ?? '');",
      "    if (!/\\bgit\\s+stash\\b/.test(command)) return undefined;",
      // Like the fleet guard: judged against ctx.cwd unless the command names its own cd target.
      `    const linked = ${JSON.stringify(linked)};`,
      "    if (ctx.cwd !== linked && !command.includes(\"cd '\" + linked + \"' && \")) return undefined;",
      `    fs.appendFileSync(${JSON.stringify(refusals)}, JSON.stringify({ toolCallId: event.toolCallId, command, cwd: ctx.cwd }) + '\\n');`,
      `    return { block: true, reason: ${JSON.stringify(GUARD_REASON)} };`,
      "  });",
      "}",
    ].join("\n") + "\n");
    const toolMessages: unknown[] = [];
    let requests = 0;
    const allowed = path.join(root, "allowed-executed");
    const bash = launch === "shim-cwd-argument"
      ? `{ cmd: ${JSON.stringify("git stash list > " + shellQuote(executed))}, cwd: ${JSON.stringify(linked)} }`
      : launch === "shim-allowed" ? `{ cmd: ${JSON.stringify("printf ok > " + shellQuote(allowed))} }`
        : `{ cmd: ${JSON.stringify("git stash list > " + shellQuote(executed))} }`;
    const server = http.createServer((request, response) => {
      let body = "";
      request.on("data", (data) => { body += data; });
      request.on("end", () => {
        const payload = JSON.parse(body);
        const tool = [...payload.messages].reverse().find((message: { role: string }) => message.role === "tool");
        if (tool) toolMessages.push(tool);
        const delta = ++requests === 1
          ? { role: "assistant", tool_calls: [{ index: 0, id: "probe-8606", type: "function", function: { name: "fabric_exec",
            arguments: JSON.stringify({ code: `return await pi.bash(${bash});` }) } }] }
          : { role: "assistant", content: "probe finished" };
        response.writeHead(200, { "Content-Type": "text/event-stream" });
        for (const [part, finish] of [[delta, null], [{}, requests === 1 ? "tool_calls" : "stop"]] as const) {
          response.write("data: " + JSON.stringify({ id: "offline", object: "chat.completion.chunk", created: 1, model: payload.model, choices: [{ index: 0, delta: part, finish_reason: finish }] }) + "\n\n");
        }
        response.end("data: [DONE]\n\n");
      });
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    cleanups.push(() => new Promise<void>((resolve) => { server.closeAllConnections(); server.close(() => resolve()); }));
    fs.writeFileSync(path.join(agentDir, "models.json"), JSON.stringify({ providers: { proof: {
      baseUrl: "http://127.0.0.1:" + (server.address() as { port: number }).port + "/v1", apiKey: "offline-only", api: "openai-completions",
      models: [{ id: "offline", name: "offline", reasoning: false, input: ["text"], contextWindow: 32000, maxTokens: 1024, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
    } } }));
    fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ extensions: [guard], defaultProjectTrust: "never", enableInstallTelemetry: false, compaction: { enabled: false }, retry: { enabled: false } }));
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ fullCodeMode: true,
      agents: { retainRuns: true, nice: 19, sessionExport: false }, mesh: { enabled: false }, mcp: { enabled: false }, memory: { enabled: false },
      jev: { enabled: false }, residency: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false } }));
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir); vi.stubEnv("PI_OFFLINE", "1"); vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined);
    const fabric = launch === "native-private-realm" ? privateRealmFabric(root) : path.resolve(".");
    const cli = fs.realpathSync(path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/cli.js"));
    let piBinary = cli;
    if (launch.startsWith("shim")) {
      piBinary = path.join(root, "pi-shell-shim");
      fs.writeFileSync(piBinary, "#!/bin/sh\nexec " + shellQuote(process.execPath) + " " + shellQuote(cli) + ' "$@"\n'); fs.chmodSync(piBinary, 0o700);
    }
    // The cwd case runs the session in the main checkout and moves only pi.bash into the linked worktree.
    const sessionCwd = launch === "shim-cwd-argument" ? main : linked;
    const manager = new AgentManager(sessionCwd, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 60_000, retainRuns: true, budgetUsd: 0, deniedModels: [], nice: 19, sessionExport: false }, {
      workerPath: path.join(fabric, "dist/worker.js"), fabricExtensionPath: path.join(fabric, "dist/index.js"), piBinary,
      runRoot: path.join(root, "runs"), fullCodeMode: true,
    });
    cleanups.push(() => manager.close());
    const result = await manager.run({ task: "probe8606: run git stash list through fabric_exec pi.bash", model: "proof/offline", transport: "process", extensions: true, tools: ["fabric_exec", "bash"] });
    let page = manager.readLog(result.id, { lines: 5000 });
    let rows = page.events;
    while (page.hasMore) {
      page = manager.readLog(result.id, { lines: 5000, before: page.before!, beforeGeneration: page.generation! });
      rows = [...page.events, ...rows];
    }
    const log = (rows.map(({ raw }) => raw).join("\n") + (rows.length ? "\n" : "")).slice(-6000);
    const evidence = { launch, sessionCwd: path.basename(sessionCwd), status: result.status, error: result.error, text: result.text, requests, toolMessages,
      stashExecuted: fs.existsSync(executed), allowedExecuted: fs.existsSync(allowed), guardRefusals: fs.existsSync(refusals) ? fs.readFileSync(refusals, "utf8").trim().split("\n").map((line) => JSON.parse(line)) : [] };
    if (process.env.TASK_OUT) fs.writeFileSync(path.join(process.env.TASK_OUT, "probe8606-" + launch + ".json"), JSON.stringify(evidence, null, 2).replaceAll(root, "<tmp>") + "\n");
    expect(result.status, (result.error ?? "") + "\n" + log).toBe("completed");
    expect(requests, log).toBe(2);
    if (launch === "shim-allowed") {
      expect(evidence.allowedExecuted, JSON.stringify(evidence)).toBe(true);
      expect(JSON.stringify(toolMessages)).not.toContain("rewrote a shell command");
      expect(evidence.guardRefusals).toEqual([]);
      return;
    }
    expect(evidence.stashExecuted, JSON.stringify(evidence)).toBe(false);
    expect(JSON.stringify(toolMessages), JSON.stringify(evidence)).toContain(GUARD_REASON);
    // Exactly one preflight reached the guard: the nested pi.bash call (a native
    // task may see it inside the bash idle-watchdog wrapper).
    expect(evidence.guardRefusals).toHaveLength(1);
    expect(evidence.guardRefusals[0].command).toContain("git stash list > " + shellQuote(executed));
  }, 90_000);
});
