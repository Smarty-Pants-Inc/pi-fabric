import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { afterAll, describe, expect, it } from "vitest";

const projectRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const cli = path.join(projectRoot, "bench", "prewalk", "verify-prewalk-canary.mjs");
const roots: string[] = [];
const tempRoot = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "prewalk-verifier-"));
  roots.push(root);
  return root;
};
afterAll(() => {
  for (const root of roots) fs.rmSync(root, { recursive: true, force: true });
});

const statusText = (dist: string, options: { loaded?: string; disk?: string; stale?: string } = {}) =>
  [
    "state: armed",
    "planRequired: false",
    "runtime:",
    "  entry:",
    `    path: ${path.join(dist, "index.js")}`,
    `    loadedSha256: ${(options.loaded ?? "a").repeat(64)}`,
    `    diskSha256: ${(options.disk ?? options.loaded ?? "a").repeat(64)}`,
    `    stale: ${options.stale ?? "false"}`,
    "  lazyRuntime:",
    `    path: ${path.join(dist, "fabric-runtime-state.js")}`,
    `    loadedSha256: ${"b".repeat(64)}`,
    `    diskSha256: ${"b".repeat(64)}`,
    "    stale: false",
  ].join("\n");

interface RunOptions {
  dist: string;
  status?: string;
  prewalkEntries?: number;
  rpc?: boolean;
  modelSelects?: Array<Record<string, unknown>>;
  recovery?: "ok" | "duplicate" | "missing" | "tool-error" | "wrong-model";
  requestContract?: { path: string; sha256: string };
  requestContexts?: (contractSha256: string) => Array<Record<string, unknown>>;
  assistantUsage?: Record<string, unknown>;
}

const RECOVERY_MARKER = "RECOVERY-MARKER: read-only";

const recoveryEpisode = (kind: NonNullable<RunOptions["recovery"]>) => {
  const episode: Array<Record<string, unknown>> = [
    {
      type: "message_end",
      message: {
        role: "assistant",
        provider: "z",
        model: "exec",
        content: [{ type: "text", text: "stopping" }],
        stopReason: "aborted",
        timestamp: 3,
      },
    },
  ];
  if (kind !== "missing") {
    episode.push({
      type: "message_end",
      message: { role: "user", content: [{ type: "text", text: RECOVERY_MARKER }], timestamp: 4 },
    });
  }
  if (kind === "duplicate") {
    episode.push({
      type: "message_end",
      message: { role: "user", content: [{ type: "text", text: RECOVERY_MARKER }], timestamp: 5 },
    });
  }
  episode.push(
    {
      type: "message_end",
      message: {
        role: "assistant",
        provider: "a",
        model: "m",
        content: [{ type: "toolCall", id: "call-r1", name: "read", arguments: {} }],
        stopReason: "toolUse",
        timestamp: 6,
      },
    },
    {
      type: "message_end",
      message: {
        role: "toolResult",
        toolCallId: "call-r1",
        toolName: "read",
        content: [{ type: "text", text: "file" }],
        isError: kind === "tool-error",
        timestamp: 7,
      },
    },
    {
      type: "message_end",
      message: {
        role: "assistant",
        provider: kind === "wrong-model" ? "z" : "a",
        model: kind === "wrong-model" ? "exec" : "m",
        content: [{ type: "text", text: "done" }],
        stopReason: "stop",
        timestamp: 8,
      },
    },
  );
  return episode;
};

const makeRun = (options: RunOptions) => {
  const root = tempRoot();
  const run = path.join(root, "cell");
  fs.mkdirSync(path.join(run, "sessions"), { recursive: true });
  const requestContexts =
    options.requestContract && options.requestContexts
      ? options.requestContexts(options.requestContract.sha256)
      : [];
  fs.writeFileSync(
    path.join(run, "started.json"),
    JSON.stringify(
      {
        startedAt: new Date().toISOString(),
        mode: options.rpc ? "rpc" : "json",
        turns: 16,
        rpcRuns: options.rpc ? 2 : null,
        runtimeHashes: {},
        extensions: [],
        requestContract: options.requestContract ?? null,
      },
      null,
      2,
    ) + "\n",
  );
  fs.writeFileSync(
    path.join(run, "finished.json"),
    JSON.stringify(
      {
        ok: true,
        problems: [],
        mode: options.rpc ? "rpc" : "json",
        turns: 16,
        rpcRuns: options.rpc ? 2 : null,
        settlements: options.rpc ? 2 : null,
        wallMs: 1,
        telemetryShutdown: true,
      },
      null,
      2,
    ) + "\n",
  );
  const events = [
    ...(options.rpc ? [] : [{ type: "session", version: 3, id: "session-1", timestamp: "2026-09-20T00:00:00.000Z" }]),
    {
      type: "message_end",
      message: {
        role: "toolResult",
        toolCallId: "call-1",
        toolName: "fabric_exec",
        content: [{ type: "text", text: options.status ?? statusText(options.dist) }],
        isError: false,
        timestamp: 1,
      },
    },
    {
      type: "message_end",
      message: {
        role: "assistant",
        provider: "a",
        model: "m",
        content: [{ type: "text", text: "done" }],
        usage: {
          input: 1,
          output: 1,
          cacheRead: 0,
          cacheWrite: 0,
          totalTokens: 2,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0.1 },
          ...(options.assistantUsage ?? {}),
        },
        stopReason: "stop",
        timestamp: 2,
      },
    },
    ...(options.recovery ? recoveryEpisode(options.recovery) : []),
    { type: "agent_settled" },
  ];
  fs.writeFileSync(path.join(run, "events.jsonl"), events.map((event) => JSON.stringify(event)).join("\n") + "\n");
  const telemetry = [
    { type: "session_start", at: 1, sessionId: "session-1", model: "a/m" },
    ...(options.modelSelects ?? [
      { type: "model_select", at: 2, model: "a/e", previous: "a/m" },
      { type: "model_select", at: 3, model: "a/m", previous: "a/e" },
    ]),
    ...requestContexts,
    { type: "session_shutdown", at: 4 },
  ];
  fs.writeFileSync(path.join(run, "telemetry.jsonl"), telemetry.map((record) => JSON.stringify(record)).join("\n") + "\n");
  if (options.rpc) {
    fs.writeFileSync(
      path.join(run, "rpc.json"),
      JSON.stringify(
        {
          mode: "rpc",
          rpcRuns: 2,
          settlements: 2,
          commands: [],
          state: { sessionId: "session-1", isStreaming: false, pendingMessageCount: 0 },
          stats: null,
          errors: [],
        },
        null,
        2,
      ) + "\n",
    );
  }
  const entries = [];
  for (let index = 0; index < (options.prewalkEntries ?? 0); index += 1) {
    entries.push({
      type: "custom_message",
      id: `p${index}`,
      parentId: null,
      timestamp: "2026-09-20T00:00:00.000Z",
      customType: index === 0 ? "pi-fabric-prewalk-armed" : "pi-fabric-prewalk-continue",
      content: "x",
      display: false,
      details: {},
    });
  }
  fs.writeFileSync(
    path.join(run, "sessions", "session-1.jsonl"),
    entries.map((entry) => JSON.stringify(entry)).join("\n") + (entries.length > 0 ? "\n" : ""),
  );
  if (options.recovery) {
    fs.writeFileSync(
      path.join(run, "probe.jsonl"),
      [
        { type: "snapshot", seq: 1, label: "before_cancel", sourceHash: "e".repeat(64) },
        { type: "snapshot", seq: 2, label: "shutdown", sourceHash: "e".repeat(64) },
      ].map((record) => JSON.stringify(record)).join("\n") + "\n",
    );
  }
  return run;
};

const runCli = (args: string[]) =>
  spawnSync(process.execPath, [cli, ...args], { cwd: projectRoot, encoding: "utf8", timeout: 30_000 });

const distFor = () => {
  const dist = path.join(tempRoot(), "dist");
  fs.mkdirSync(dist, { recursive: true });
  return dist;
};

describe("verify-prewalk-canary", () => {
  const artifactReceipt = (run: string, sha: string, counts: Record<string, number>, ok: boolean) => ({
    spec: { path: "/tmp/spec.json", sha256: "f".repeat(64) },
    cwd: run,
    testFile: "tests/native.test.mjs",
    command: ["node", "--test", "--test-reporter=tap", "tests/native.test.mjs"],
    exitCode: ok ? 0 : 1,
    signal: null,
    counts,
    countsObserved: true,
    artifact: { path: "tests/native.test.mjs", sha256Before: sha, sha256After: sha, sizeBytes: 4, unchangedDuringCheck: true },
    ok,
    startedAt: "2026-09-21T00:00:00.000Z",
    finishedAt: "2026-09-21T00:00:01.000Z",
  });

  const writeArtifact = (run: string, text = "// artifact\n") => {
    const file = path.join(run, "tests", "native.test.mjs");
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, text);
    return createHash("sha256").update(fs.readFileSync(file)).digest("hex");
  };

  const writeReceipt = (run: string, receipt: unknown) => {
    const file = path.join(run, "task-check.json");
    fs.writeFileSync(file, JSON.stringify(receipt, null, 2) + "\n");
    return file;
  };

  it("reports a failing artifact on its own axis while the lifecycle stays clean", () => {
    const dist = distFor();
    const run = makeRun({ dist });
    const sha = writeArtifact(run);
    const receipt = writeReceipt(run, artifactReceipt(run, sha, { tests: 7, pass: 3, fail: 4, skipped: 0, todo: 0 }, false));
    const result = runCli(["--run", run, "--dist", dist, "--task-check-report", receipt]);
    expect(result.status).toBe(1);
    const report = JSON.parse(result.stdout) as {
      checks: Array<{ name: string; status: string; detail: unknown }>;
      axes: Record<string, Array<{ name: string; status: string }>>;
    };
    const artifact = report.checks.find((check) => check.name === "task-verification");
    expect(artifact?.status).toBe("fail");
    expect(JSON.stringify(artifact?.detail)).toContain("tests failed: 4 of 7");
    expect((report.axes.lifecycle ?? []).every((entry) => entry.status === "pass")).toBe(true);
    expect(report.axes.artifact).toEqual([{ name: "task-verification", status: "fail" }]);
  });

  it("passes the artifact axis and re-binds the content hash, then fails once the artifact changes", () => {
    const dist = distFor();
    const run = makeRun({ dist });
    const sha = writeArtifact(run);
    const receipt = writeReceipt(run, artifactReceipt(run, sha, { tests: 7, pass: 7, fail: 0, skipped: 0, todo: 0 }, true));
    const green = runCli(["--run", run, "--dist", dist, "--task-check-report", receipt]);
    expect(green.status).toBe(0);
    expect(JSON.parse(green.stdout).axes.artifact).toEqual([{ name: "task-verification", status: "pass" }]);

    fs.writeFileSync(path.join(run, "tests", "native.test.mjs"), "// artifact edited after verification\n");
    const stale = runCli(["--run", run, "--dist", dist, "--task-check-report", receipt]);
    expect(stale.status).toBe(1);
    const detail = JSON.stringify(JSON.parse(stale.stdout).checks.find((c: { name: string }) => c.name === "task-verification")?.detail);
    expect(detail).toContain("artifact content changed after the check ran");
  });

  it("leaves missing artifact evidence unobserved instead of green", () => {
    const dist = distFor();
    const run = makeRun({ dist });
    const missing = runCli(["--run", run, "--dist", dist, "--task-check-report", path.join(run, "absent.json")]);
    expect(missing.status).toBe(1);
    expect(JSON.parse(missing.stdout).checks.find((c: { name: string }) => c.name === "task-verification")?.status).toBe(
      "unobserved",
    );
    // Without the flag the axis is simply not requested; no check is invented.
    const optOut = runCli(["--run", run, "--dist", dist]);
    expect(JSON.parse(optOut.stdout).checks.map((check: { name: string }) => check.name)).not.toContain("task-verification");
  });

  it("cannot pass on receipt hashes when no work directory resolves", () => {
    const dist = distFor();
    const run = makeRun({ dist });
    const sha = writeArtifact(run);
    const receipt = writeReceipt(run, {
      ...artifactReceipt(run, sha, { tests: 7, pass: 7, fail: 0, skipped: 0, todo: 0 }, true),
      cwd: "relative/work",
    });
    // makeRun's started.json records no cwd either, so nothing is resolvable.
    const unresolved = runCli(["--run", run, "--dist", dist, "--task-check-report", receipt]);
    expect(unresolved.status).toBe(1);
    const unresolvedReport = JSON.parse(unresolved.stdout) as {
      checks: Array<{ name: string; status: string; detail: unknown }>;
    };
    const unobserved = unresolvedReport.checks.find((check) => check.name === "task-verification");
    expect(unobserved?.status).toBe("unobserved");
    expect(JSON.stringify(unobserved?.detail)).toContain("work directory");

    // An explicit absolute --work-dir re-binds the artifact content and can pass.
    const resolved = runCli(["--run", run, "--dist", dist, "--task-check-report", receipt, "--work-dir", run]);
    expect(resolved.status).toBe(0);
    expect(JSON.parse(resolved.stdout).axes.artifact).toEqual([{ name: "task-verification", status: "pass" }]);

    fs.writeFileSync(path.join(run, "tests", "native.test.mjs"), "// artifact edited after verification\n");
    const stale = runCli(["--run", run, "--dist", dist, "--task-check-report", receipt, "--work-dir", run]);
    expect(stale.status).toBe(1);
    const staleDetail = JSON.stringify(
      JSON.parse(stale.stdout).checks.find((check: { name: string }) => check.name === "task-verification")?.detail,
    );
    expect(staleDetail).toContain("artifact content changed after the check ran");
  });

  it("verifies write-scope compliance separately from the artifact verdict", () => {
    const dist = distFor();
    const run = makeRun({ dist });
    const clean = path.join(run, "scope-clean.json");
    fs.writeFileSync(clean, JSON.stringify({ marker: "SCOPE-CHANGE", ok: true, writesAfter: [] }) + "\n");
    const green = runCli(["--run", run, "--dist", dist, "--scope-report", clean]);
    expect(green.status).toBe(0);
    expect(JSON.parse(green.stdout).axes.scope).toEqual([{ name: "scope-no-writes-after-marker", status: "pass" }]);

    const dirty = path.join(run, "scope-dirty.json");
    fs.writeFileSync(dirty, JSON.stringify({ marker: "SCOPE-CHANGE", ok: true, writesAfter: ["tests/x.mjs"] }) + "\n");
    const red = runCli(["--run", run, "--dist", dist, "--scope-report", dirty]);
    expect(red.status).toBe(1);
    const report = JSON.parse(red.stdout) as { checks: Array<{ name: string; status: string }>; axes: Record<string, unknown> };
    expect(report.checks.find((check) => check.name === "scope-no-writes-after-marker")?.status).toBe("fail");
    expect(report.checks.find((check) => check.name === "runtime-identity")?.status).toBe("pass");

    const unreadable = path.join(run, "scope-broken.json");
    fs.writeFileSync(unreadable, "{not json\n");
    const broken = runCli(["--run", run, "--dist", dist, "--scope-report", unreadable]);
    expect(broken.status).toBe(1);
    expect(JSON.parse(broken.stdout).checks.find((c: { name: string }) => c.name === "scope-no-writes-after-marker")?.status).toBe(
      "unobserved",
    );
  });

  it("passes a complete json-mode cell with structured identity and persisted counts", () => {
    const dist = distFor();
    const run = makeRun({ dist, prewalkEntries: 2 });
    const result = runCli(["--run", run, "--main", "a/m", "--executor", "a/e", "--expect-prewalk", "2", "--dist", dist]);
    expect(result.status).toBe(0);
    const report = JSON.parse(result.stdout) as { ok: boolean; checks: Array<{ name: string; status: string }> };
    expect(report.ok).toBe(true);
    expect(report.checks.map((check) => check.name)).toEqual([
      "recording-complete",
      "finished-ok",
      "runtime-identity",
      "in-place-roundtrip",
      "persisted-prewalk",
    ]);
    expect(report.checks.every((check) => check.status === "pass")).toBe(true);
  });

  it("recognizes a status nested in the real batched Fabric envelope, not its descriptions or appendix", () => {
    const dist = distFor();
    const status = [
      "descriptions:", "  - name: plan", "    inputSchema:", "      required:", "        - outcome",
      'helper: "<multi-line string, see section: helper>"', "status:",
      ...statusText(dist).split("\n").map((line) => `  ${line}`),
      "", "--- helper (100 chars) ---", "not a status line", statusText("/quoted/not-runtime"),
    ].join("\n");
    const run = makeRun({ dist, status });
    const result = runCli(["--run", run, "--dist", dist]);
    expect(result.status).toBe(0);
    expect(JSON.parse(result.stdout).checks.find((c: { name: string }) => c.name === "runtime-identity").status).toBe("pass");
  });

  it("recognizes JSON status envelopes without treating boolean false as stale", () => {
    const dist = distFor();
    const artifact = (name: string) => ({ path: path.join(dist, name), loadedSha256: "a".repeat(64), diskSha256: "a".repeat(64), stale: false });
    const status = JSON.stringify({ status: { state: "armed", runtime: { entry: artifact("index.js"), lazyRuntime: artifact("fabric-runtime-state.js") } } });
    const run = makeRun({ dist, status });
    expect(runCli(["--run", run, "--dist", dist]).status).toBe(0);
  });

  it.each([
    (dist: string) => `helper: |\n${statusText(dist).split("\n").map((line) => `  ${line}`).join("\n")}`,
    (dist: string) => `descriptions:\n  runtime:\n    entry: ${dist}\n\n--- helper (100 chars) ---\n${statusText(dist)}`,
    (dist: string) => `status:\n${statusText(dist).split("\n").map((line) => `  ${line}`).join("\n")}\n  malformed line`,
  ])("never accepts quoted, description-only or malformed status evidence", (text) => {
    const dist = distFor();
    const run = makeRun({ dist, status: text(dist) });
    const result = runCli(["--run", run, "--dist", dist]);
    expect(result.status).toBe(1);
    expect(JSON.parse(result.stdout).checks.find((c: { name: string }) => c.name === "runtime-identity").status).toBe("unobserved");
  });

  it("does not let an earlier good identity hide a later stale observation", () => {
    const dist = distFor();
    const run = makeRun({ dist });
    const eventsPath = path.join(run, "events.jsonl");
    const events = fs.readFileSync(eventsPath, "utf8").trim().split("\n").map((line) => JSON.parse(line));
    const status = events.find((event) => event.message?.role === "toolResult");
    events.push({ ...status, message: { ...status.message, toolCallId: "call-stale", content: [{ type: "text", text: statusText(dist, { stale: "true" }) }] } });
    fs.writeFileSync(eventsPath, events.map((event) => JSON.stringify(event)).join("\n") + "\n");
    const result = runCli(["--run", run, "--dist", dist]);
    expect(result.status).toBe(1);
    expect(JSON.parse(result.stdout).checks.find((c: { name: string }) => c.name === "runtime-identity").status).toBe("fail");
  });

  it("fails on a stale loaded identity and never rewrites the run directory", () => {
    const dist = distFor();
    const run = makeRun({ dist, status: statusText(dist, { loaded: "a", disk: "c", stale: "true" }) });
    const before = fs.readFileSync(path.join(run, "events.jsonl"));
    const result = runCli(["--run", run, "--dist", dist]);
    expect(result.status).toBe(1);
    const report = JSON.parse(result.stdout) as { ok: boolean; checks: Array<{ name: string; status: string; detail: unknown }> };
    const identity = report.checks.find((check) => check.name === "runtime-identity");
    expect(identity?.status).toBe("fail");
    expect(JSON.stringify(identity?.detail)).toContain("stale=true");
    expect(fs.readFileSync(path.join(run, "events.jsonl")).equals(before)).toBe(true);
  });

  it("treats a missing status result as unobserved, which blocks ok", () => {
    const dist = distFor();
    const run = makeRun({ dist, status: "state: armed\nplanRequired: false" });
    const result = runCli(["--run", run, "--dist", dist]);
    expect(result.status).toBe(1);
    const report = JSON.parse(result.stdout) as { ok: boolean; checks: Array<{ name: string; status: string }> };
    expect(report.checks.find((check) => check.name === "runtime-identity")?.status).toBe("unobserved");
    expect(report.ok).toBe(false);
  });

  it("fails the persisted expectation and the roundtrip when the evidence disagrees", () => {
    const dist = distFor();
    const run = makeRun({
      dist,
      prewalkEntries: 1,
      modelSelects: [{ type: "model_select", at: 2, model: "a/e", previous: "a/m" }],
    });
    const result = runCli(["--run", run, "--main", "a/m", "--executor", "a/e", "--expect-prewalk", "2", "--dist", dist]);
    expect(result.status).toBe(1);
    const report = JSON.parse(result.stdout) as { checks: Array<{ name: string; status: string }> };
    expect(report.checks.find((check) => check.name === "persisted-prewalk")?.status).toBe("fail");
    expect(report.checks.find((check) => check.name === "in-place-roundtrip")?.status).toBe("fail");
  });

  it("passes an rpc cell whose rpc state matches the telemetry session identity", () => {
    const dist = distFor();
    const run = makeRun({ dist, rpc: true });
    const result = runCli(["--run", run, "--dist", dist]);
    expect(result.status).toBe(0);
    const report = JSON.parse(result.stdout) as { checks: Array<{ name: string; status: string }> };
    expect(report.checks.find((check) => check.name === "session-identity")?.status).toBe("pass");
  });

  it("refuses to overwrite an existing --json report", () => {
    const dist = distFor();
    const run = makeRun({ dist });
    const out = path.join(tempRoot(), "report.json");
    fs.writeFileSync(out, "{}");
    const result = runCli(["--run", run, "--dist", dist, "--json", out]);
    expect(result.status).toBe(2);
    expect(result.stderr).toContain("EEXIST");
  });

  it("passes the recovery verdict only when the reply, read-only tools and preserved work are all observed", () => {
    const dist = distFor();
    const run = makeRun({ dist, recovery: "ok" });
    const result = runCli(["--run", run, "--main", "a/m", "--recovery-marker", RECOVERY_MARKER, "--dist", dist]);
    expect(result.status).toBe(0);
    const report = JSON.parse(result.stdout) as { ok: boolean; checks: Array<{ name: string; status: string }> };
    expect(report.ok).toBe(true);
    for (const name of [
      "recovery-abort-recorded",
      "recovery-delivered-once",
      "recovery-tools",
      "recovery-reply",
      "recovery-no-writes-after-cancel",
      "recovery-work-preserved",
    ]) {
      expect(report.checks.find((check) => check.name === name)?.status).toBe("pass");
    }
  });

  it("fails duplicate and missing recovery deliveries", () => {
    const dist = distFor();
    for (const kind of ["duplicate", "missing"] as const) {
      const run = makeRun({ dist, recovery: kind });
      const result = runCli(["--run", run, "--main", "a/m", "--recovery-marker", RECOVERY_MARKER, "--dist", dist]);
      expect(result.status).toBe(1);
      const report = JSON.parse(result.stdout) as { checks: Array<{ name: string; status: string }> };
      expect(report.checks.find((check) => check.name === "recovery-delivered-once")?.status).toBe("fail");
    }
  });

  it("fails a failed recovery tool and a non-Main recovery reply", () => {
    const dist = distFor();
    for (const [kind, name] of [
      ["tool-error", "recovery-tools"],
      ["wrong-model", "recovery-reply"],
    ] as const) {
      const run = makeRun({ dist, recovery: kind });
      const result = runCli(["--run", run, "--main", "a/m", "--recovery-marker", RECOVERY_MARKER, "--dist", dist]);
      expect(result.status).toBe(1);
      const report = JSON.parse(result.stdout) as { checks: Array<{ name: string; status: string }> };
      expect(report.checks.find((check) => check.name === name)?.status).toBe("fail");
    }
  });
});

const writeContract = (body: Record<string, unknown>) => {
  const root = tempRoot();
  const file = path.join(root, "contract.json");
  const text = JSON.stringify(body, null, 2) + "\n";
  fs.writeFileSync(file, text);
  return { path: file, sha256: createHash("sha256").update(text).digest("hex") };
};

const contextRecord = (
  requestIndex: number,
  model: string,
  sha256: string,
  matches: Record<string, number[][]>,
) => ({
  type: "request_context",
  at: 10 + requestIndex,
  requestIndex,
  model,
  requestEvidence: { layout: "context", truncated: false, matches, contractSha256: sha256 },
});

describe("request contract checks", () => {
  const contractBody = {
    markers: { task: "TASK", plan: "PLAN", steer1: "STEER-1", steer2: "STEER-2" },
    exactlyOnce: ["plan"],
    present: ["task"],
    ordered: ["steer1", "steer2"],
    absentBefore: ["plan"],
  };

  it("passes when opt-in evidence proves the executor payload contract", () => {
    const dist = distFor();
    const contract = writeContract(contractBody);
    const run = makeRun({
      dist,
      requestContract: contract,
      requestContexts: (sha) => [
        contextRecord(1, "a/m", sha, { task: [[0, 0, 0]], plan: [], steer1: [], steer2: [] }),
        contextRecord(2, "a/e", sha, { task: [[1, 0, 0]], plan: [[1, 0, 10]], steer1: [[2, 0, 0]], steer2: [] }),
        contextRecord(3, "a/e", sha, { task: [[1, 0, 0]], plan: [[1, 0, 10]], steer1: [[2, 0, 0]], steer2: [[3, 0, 0]] }),
      ],
    });
    const result = runCli(["--run", run, "--main", "a/m", "--executor", "a/e", "--dist", dist]);
    expect(result.status).toBe(0);
    const report = JSON.parse(result.stdout) as { ok: boolean; checks: Array<{ name: string; status: string }> };
    expect(report.ok).toBe(true);
    expect(report.checks.find((check) => check.name === "request-contract-evidence")?.status).toBe("pass");
    expect(report.checks.find((check) => check.name === "request-contract-payload")?.status).toBe("pass");
  });

  it("fails the payload check on duplicates and order violations", () => {
    const dist = distFor();
    const contract = writeContract(contractBody);
    const run = makeRun({
      dist,
      requestContract: contract,
      requestContexts: (sha) => [
        contextRecord(1, "a/m", sha, { task: [[0, 0, 0]], plan: [], steer1: [], steer2: [] }),
        contextRecord(2, "a/e", sha, { task: [[1, 0, 0]], plan: [[1, 0, 10], [1, 0, 20]], steer1: [], steer2: [[2, 0, 0]] }),
        contextRecord(3, "a/e", sha, { task: [[1, 0, 0]], plan: [[1, 0, 10]], steer1: [[3, 0, 0]], steer2: [] }),
      ],
    });
    const result = runCli(["--run", run, "--main", "a/m", "--executor", "a/e", "--dist", dist]);
    expect(result.status).toBe(1);
    const report = JSON.parse(result.stdout) as { checks: Array<{ name: string; status: string; detail: unknown }> };
    const payload = report.checks.find((check) => check.name === "request-contract-payload");
    expect(payload?.status).toBe("fail");
    const detail = JSON.stringify(payload?.detail);
    expect(detail).toContain("exactly once");
    expect(detail).toContain("appears before the previous ordered marker");
  });

  it("blocks ok when a declared contract has no request_context records", () => {
    const dist = distFor();
    const contract = writeContract(contractBody);
    const run = makeRun({ dist, requestContract: contract });
    const result = runCli(["--run", run, "--main", "a/m", "--executor", "a/e", "--dist", dist]);
    expect(result.status).toBe(1);
    const report = JSON.parse(result.stdout) as { checks: Array<{ name: string; status: string }> };
    expect(report.checks.find((check) => check.name === "request-contract-evidence")?.status).toBe("unobserved");
    expect(report.checks.find((check) => check.name === "request-contract-payload")?.status).toBe("unobserved");
  });

  it("fails the evidence check when a recorded sha differs from the contract file", () => {
    const dist = distFor();
    const contract = writeContract(contractBody);
    const run = makeRun({
      dist,
      requestContract: contract,
      requestContexts: () => [
        contextRecord(1, "a/e", "0".repeat(64), { task: [], plan: [[1, 0, 10]], steer1: [], steer2: [] }),
      ],
    });
    const result = runCli([
      "--run", run, "--main", "a/m", "--executor", "a/e",
      "--request-contract", contract.path, "--dist", dist,
    ]);
    expect(result.status).toBe(1);
    const report = JSON.parse(result.stdout) as { checks: Array<{ name: string; status: string }> };
    expect(report.checks.find((check) => check.name === "request-contract-evidence")?.status).toBe("fail");
  });

  it("fails rather than false-passes when recorded marker positions are malformed", () => {
    const dist = distFor();
    const contract = writeContract(contractBody);
    const run = makeRun({
      dist,
      requestContract: contract,
      requestContexts: (sha) => [
        contextRecord(1, "a/m", sha, { task: [[0, 0, 0]], plan: [], steer1: [], steer2: [] }),
        contextRecord(2, "a/e", sha, { task: [[1, 0]], plan: [[1, 0, 10]], steer1: [[2, 0, 0]], steer2: [[3, 0, 0]] }),
      ],
    });
    const result = runCli(["--run", run, "--main", "a/m", "--executor", "a/e", "--dist", dist]);
    expect(result.status).toBe(1);
    const report = JSON.parse(result.stdout) as { checks: Array<{ name: string; status: string; detail: unknown }> };
    const evidence = report.checks.find((check) => check.name === "request-contract-evidence");
    const payload = report.checks.find((check) => check.name === "request-contract-payload");
    expect(evidence?.status).toBe("fail");
    expect(JSON.stringify(evidence?.detail)).toContain("positions malformed");
    expect(payload?.status).toBe("fail");
    expect(JSON.stringify(payload?.detail)).toContain("positions malformed");
  });
});

describe("metrics and human report", () => {
  it("derives per-model usage, phases and observed failures as informational metrics", () => {
    const dist = distFor();
    const run = makeRun({ dist });
    const result = runCli(["--run", run, "--main", "a/m", "--executor", "a/e", "--dist", dist]);
    expect(result.status).toBe(0);
    const report = JSON.parse(result.stdout) as {
      ok: boolean;
      metrics: {
        available: boolean;
        wallMs: number | null;
        assistantCount: number;
        perModel: Record<string, { requests: number; input: number; output: number; cacheRead: number; cacheWrite: number; totalTokens: number; recordedCostEstimateUsd: number }>;
        phases: { handoffAt: number | null; returnAt: number | null };
        reasoningByModel: Record<string, number> | null;
        observedFailures: { assistantErrorOrAborted: number; toolResultErrors: number };
      };
    };
    expect(report.metrics.available).toBe(true);
    expect(report.metrics.perModel["a/m"]).toMatchObject({
      requests: 1, input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2, recordedCostEstimateUsd: 0.1,
    });
    expect(report.metrics.reasoningByModel).toBeNull();
    expect(report.metrics.observedFailures.assistantErrorOrAborted).toBe(0);
    expect(report.metrics.observedFailures.toolResultErrors).toBe(0);
  });

  it("reports a reasoning breakdown without adding it into usage totals", () => {
    const dist = distFor();
    const run = makeRun({ dist, assistantUsage: { reasoning: 7 } });
    const result = runCli(["--run", run, "--main", "a/m", "--executor", "a/e", "--dist", dist]);
    expect(result.status).toBe(0);
    const report = JSON.parse(result.stdout) as {
      metrics: { perModel: Record<string, { totalTokens: number }>; reasoningByModel: Record<string, number> | null };
    };
    expect(report.metrics.reasoningByModel).toEqual({ "a/m": 7 });
    expect(report.metrics.perModel["a/m"]?.totalTokens).toBe(2);
  });

  it("keeps incomplete usage unavailable instead of fabricating zeros", () => {
    const dist = distFor();
    const run = makeRun({ dist, recovery: "ok" });
    const result = runCli(["--run", run, "--main", "a/m", "--recovery-marker", RECOVERY_MARKER, "--dist", dist]);
    expect(result.status).toBe(0);
    const report = JSON.parse(result.stdout) as { ok: boolean; metrics: { available: boolean; reason: string } };
    expect(report.ok).toBe(true);
    expect(report.metrics.available).toBe(false);
    expect(report.metrics.reason).toContain("usage");
  });

  it("writes a human report to a new file and refuses to overwrite it", () => {
    const dist = distFor();
    const run = makeRun({ dist });
    const out = path.join(tempRoot(), "report.md");
    const result = runCli(["--run", run, "--dist", dist, "--report", out]);
    expect(result.status).toBe(0);
    const text = fs.readFileSync(out, "utf8");
    expect(text).toContain("Prewalk canary verification");
    expect(text).toContain("a/m");
    const second = runCli(["--run", run, "--dist", dist, "--report", out]);
    expect(second.status).toBe(2);
    expect(second.stderr).toContain("EEXIST");
  });
});
