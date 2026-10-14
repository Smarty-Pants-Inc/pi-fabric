import { spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { fileURLToPath, pathToFileURL } from "node:url";
import { describe, expect, it } from "vitest";
import { detachedPreview } from "../src/core/preview.js";
import { truncateMiddle } from "../src/util.js";
import { boundModelOutput } from "../src/output-budget.js";
import { FabricWorkflowItemTransitions } from "../src/activity/workflow-items.js";
import { buildThinkingDigest } from "../src/agents/thinking-transfer.js";
import { snippetTask } from "../src/agents/fork-seed.js";
import { buildActorContext } from "../src/actors/context.js";
import { ActorLogStore } from "../src/actors/log-store.js";
import { recordsInboxMessage } from "../src/records/inbox.js";
import { montyErrorText } from "../src/runtime/monty-source.js";
import { ShellMonitor } from "../src/core/shell-monitor.js";
import { branch, hostedRun, inboxHarness, recall, expand, actorMessage, record, type ProbeMode } from "./fixtures/other-preview-heap-probe.js";

// Keep reason: P0 #10720 is retained heap/data custody, not a cheap user-flow
// assertion. Children run real public entry points and keep their outputs live.
// No generated modules, installs, artifact writes, or filesystem cleanup.
const fixture = fileURLToPath(new URL("./fixtures/other-preview-heap-probe.ts", import.meta.url));
const cli = fileURLToPath(new URL("jiti-cli.mjs", pathToFileURL(createRequire(import.meta.url).resolve("jiti"))));
const MiB = 1024 * 1024;
function measure(mode: ProbeMode, timeout = 120_000) {
  const result = spawnSync(process.execPath, ["--expose-gc", cli, fixture, mode],
    { encoding: "utf8", timeout, maxBuffer: MiB, env: { ...process.env, JITI_FS_CACHE: "0", NODE_DISABLE_COMPILE_CACHE: "1" } });
  expect(result.error).toBeUndefined();
  expect(result.status, result.stderr || result.stdout).toBe(0);
  return JSON.parse(result.stdout.trim().split("\n").at(-1)!) as { mode: string; inputs: number; inputBytes: number; retainedBytes: number };
}

describe("detached other previews: retained heap", () => {
  it("detects the 100-parent sliced-string retention counterexample", () => {
    const result = measure("control");
    expect(result).toMatchObject({ inputs: 100, inputBytes: 4 * MiB });
    expect(result.retainedBytes).toBeGreaterThan(350 * MiB);
  }, 130_000);
  it.each([
    ["workflow", 2], ["completion", 4], ["completion-pressure", 4], ["hosted", 32],
    ["strings", 12], ["actors", 6], ["memory", 8], ["shell", 4], ["observation", 4],
  ] as const)("%s does not retain 100 unique 4 MiB parents (budget %i MiB)", (mode, budget) => {
    const result = measure(mode);
    expect(result).toMatchObject({ inputs: 100, inputBytes: 4 * MiB });
    expect(result.retainedBytes).toBeLessThan(budget * MiB);
  }, 130_000);

  const python = spawnSync("python3", ["-I", "-B", "-c", "import sys; print(sys.executable)"], { encoding: "utf8", timeout: 10_000 });
  it.skipIf(python.status !== 0)("CPython logs retain only their log budget", () => {
    expect(measure("cpython").retainedBytes).toBeLessThan(8 * MiB);
  }, 130_000);
  const require = createRequire(import.meta.url);
  let hasMonty = false;
  try {
    const native = createRequire(require.resolve("@pydantic/monty/node"));
    const triple = process.platform === "darwin" ? `darwin-${process.arch}` : process.platform === "linux" ? `linux-${process.arch}-gnu` : "win32-x64-msvc";
    native.resolve(`@pydantic/monty-${triple}/${process.platform === "win32" ? "monty.exe" : "monty"}`);
    hasMonty = true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "MODULE_NOT_FOUND") throw error;
  }
  it.skipIf(!hasMonty)("native Monty print callbacks retain only their log budget", () => {
    expect(measure("monty").retainedBytes).toBeLessThan(8 * MiB);
  }, 130_000);
});

describe("other preview compatibility and custody counterexamples", () => {
  it("copies even already-bounded previews and preserves UTF-16 surrogate cuts", () => {
    const text = "ab😀tail";
    expect(detachedPreview(text, 3)).toBe("ab\ud83d");
    expect(detachedPreview(text.slice(0, 3), 100)).toBe("ab\ud83d");
    expect(detachedPreview(text, 0)).toBe("");
    expect(truncateMiddle("short", 10)).toBe("short");
    const value = "😀".repeat(1000);
    const max = 101;
    const marker = `\n\n... ${value.length - max} characters omitted by Pi Fabric ...\n\n`;
    const available = Math.max(0, max - marker.length);
    expect(truncateMiddle(value, max)).toBe(value.slice(0, Math.ceil(available / 2)) + marker + value.slice(value.length - Math.floor(available / 2)));
    expect(truncateMiddle(value, 1)).toBe(`\n\n... ${value.length - 1} characters omitted by Pi Fabric ...\n\n`);
  });

  it("keeps cached labels after duplicate transitions and exact trim/cut behavior", () => {
    const events: unknown[] = [];
    const transitions = new FabricWorkflowItemTransitions("run", (_channel, event) => events.push(event));
    transitions.record({ id: "item", status: "running" }, "first");
    transitions.record({ id: "item", status: "running" }, "  " + "a".repeat(119) + "😀tail  ");
    expect(events).toHaveLength(1);
    transitions.finish(true);
    expect(events.at(-1)).toMatchObject({ itemId: "item", from: "running", to: "completed", label: "a".repeat(119) + "\ud83d" });
  });

  it("bounds cached label bytes without losing stable item transitions", () => {
    const events: Array<{ itemId: string; label?: string; to: string }> = [];
    const transitions = new FabricWorkflowItemTransitions("run", (_channel, event) => events.push(event as typeof events[number]));
    for (let index = 0; index < 10_000; index++) transitions.record({ id: String(index), status: "running" }, "label".repeat(24));
    events.length = 0;
    transitions.finish(true);
    expect(events).toHaveLength(10_000);
    expect(new Set(events.map(event => event.itemId)).size).toBe(10_000);
    expect(events.every(event => event.to === "completed")).toBe(true);
    expect(events.reduce((bytes, event) => bytes + 2 * (event.label?.length ?? 0), 0)).toBeLessThanOrEqual(2 * MiB);
  });

  it("preserves hosted progress tails, error heads and partial surrogate cuts", () => {
    const run = hostedRun("run");
    const text = "lead" + "😀" + "x".repeat(99_999);
    run.reporter.progress({ text, currentTool: "a".repeat(199) + "😀end" });
    expect(run.record.text).toBe(text.slice(-100_000));
    expect(run.record.currentTool).toBe("a".repeat(199) + "\ud83d");
    run.reporter.fail({ error: "e".repeat(19_999) + "😀end" });
    expect(run.record.error).toBe("e".repeat(19_999) + "\ud83d");
    expect(run.record.currentTool).toBeUndefined();
    run.close();
  });

  it("keeps inbox summaries and full custody identities exact, clipping display names only", () => {
    const h = inboxHarness();
    const text = "x".repeat(3999) + "😀end";
    const name = "n".repeat(79) + "😀long";
    const identity = "root-" + "i".repeat(200);
    h.inbox.enqueue({ id: "id", name, status: "completed", text, startedAt: 1, finishedAt: 1,
      completionDelivery: { status: "undelivered", addressedTo: identity, redeliveredFrom: identity } });
    h.boundary();
    expect(h.messages[0]!.content).toContain(`Agent ${name.slice(0, 80)} (id) completed`);
    expect(h.messages[0]!.content).toContain(`[re-delivered from dead Main session ${identity}]`);
    expect(h.messages[0]!.content).toContain(text.slice(0, 4000) + "\n[truncated; use agents.wait({id}) for the full result]");
    h.inbox.close();
  });

  it("pressure never evicts unread IDs/outcomes or preparation/delivery callbacks", () => {
    const h = inboxHarness();
    const prepared: string[] = [], delivered: string[] = [];
    const text = "x".repeat(50_000);
    for (let index = 0; index < 300; index++) {
      const id = String(index);
      h.inbox.enqueue({ id, name: "worker", status: "failed", text, partialText: text, error: text,
        warnings: Array(20).fill(text) as string[], startedAt: 1 }, () => delivered.push(id), () => prepared.push(id));
    }
    expect(delivered).toEqual([]);
    h.inbox.acknowledge("17");
    for (let batch = 0; batch < 10; batch++) h.boundary();
    const ids = h.messages.flatMap(message => message.details.ids);
    expect(ids).toHaveLength(299);
    expect(new Set(ids).size).toBe(299);
    expect(ids).not.toContain("17");
    expect(delivered).toEqual(ids);
    expect(prepared).toEqual(ids);
    h.inbox.close();
  });

  it("pressure cannot consume an unclaimed carrier without a durable receipt", () => {
    const h = inboxHarness(false);
    let delivered = 0;
    for (let index = 0; index < 100; index++) h.inbox.enqueue({ id: String(index), name: "worker", status: "completed", startedAt: 1,
      text: "x".repeat(50_000), warnings: Array(20).fill("w".repeat(5000)) as string[] }, () => { delivered++; });
    for (let batch = 0; batch < 4; batch++) h.boundary();
    expect(h.messages.flatMap(message => message.details.ids)).toHaveLength(100);
    expect(delivered).toBe(0);
    h.inbox.close();
    expect(delivered).toBe(0);
  });

  it("retains full artifacts and safety-critical receipts while cutting only prose", async () => {
    const output = "x".repeat(2000);
    let artifact = "";
    const normal = await boundModelOutput(output, 80, output, async full => { artifact = full; return "/artifact"; });
    expect(artifact).toBe(output);
    expect(normal.text.length).toBeLessThanOrEqual(80);
    expect(normal.text).toContain("saved to: /artifact]");
    const priority = "DO NOT RETRY id=one id=two";
    const bounded = await boundModelOutput(output, 4, output, async () => { throw new Error("sink unavailable"); }, { text: priority, sections: [output] });
    expect(bounded.text).toBe(priority);
    const tiny = await boundModelOutput(output, 1, output, async () => "/artifact");
    expect(tiny.text).toBe("]");
  });

  it("preserves inherited snippets, thinking first-line trim and traceback markers", () => {
    const text = "a".repeat(3999) + "😀end";
    expect(snippetTask(branch(text), 1, "task")).toContain(text.slice(0, 4000) + "…");
    const digest = buildThinkingDigest(branch("  first line  \n" + "later".repeat(1000)), { source: { provider: "fixture", modelId: "source" }, target: { provider: "other", modelId: "target" } });
    expect(digest?.content).toContain("- [entry assistant] first line");
    expect(digest?.content).not.toContain("later");
    const error = "e".repeat(7900) + "discard".repeat(10000) + "z".repeat(8000);
    expect(montyErrorText(new Error(error))).toBe(error.slice(0, 7900) + "\n[Python traceback truncated]\n" + error.slice(-8000));
  });

  it("keeps actor whitespace/first-line behavior and exact serialized-data prefixes", () => {
    const ctx = buildActorContext([{ role: "user", content: " one\n two\tthree " }, { role: "toolResult", isError: true, content: "bad\nnot in preview" }], 2, 1000);
    expect(ctx.digest).toMatchObject({ lastUserRequest: "one two three", lastError: "bad", openErrors: 1 });
    expect(ctx.transcript).toEqual(["user: one two three", "result : bad [ERR]"]);
    const store = new ActorLogStore({ maxEventBytes: 8192 }, { eventContextChars: 3 }, { actorRunArchiveMs: 1 });
    const history: ReturnType<typeof actorMessage>[] = [];
    const message = actorMessage(1, "ab😀tail");
    store.recordMessage(history, message);
    expect(message.text).toBe("ab\ud83d\n[actor message truncated]");
    const data = { text: '<&"😀' + "x".repeat(5000) };
    const visible = recordsInboxMessage([record("safe <&>", data)]).content;
    const expected = JSON.stringify(data).slice(0, 2048).replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
    expect(visible).toContain(`<data>${expected}</data>`);
    expect(visible).toContain("safe &lt;&amp;&gt;");
  });

  it("keeps memory's surrogate-safe snippet and page cursor behavior", async () => {
    const text = "x".repeat(255) + "😀tail";
    const page = await expand(text, 256);
    expect(page.error).toBeUndefined();
    expect(page.entries[0]!.text).toBe("x".repeat(255));
    expect(page.entries[0]!.textRange).toEqual({ start: 0, end: 255, total: text.length, complete: false });
    expect(page.next?.args.textOffset).toBe(255);
    expect(recall("a".repeat(479) + "😀tail").hits[0]).toMatchObject({ snippet: "a".repeat(479) + "…", truncated: true });
  });

  it("keeps shell normalization, duplicate suppression, marker and CRLF framing", () => {
    const batches: Array<{ lines: string[]; omitted: number }> = [];
    const monitor = new ShellMonitor({ delivery: "ui", timeoutMs: 1000, intervalMs: 1000 }, batch => batches.push(batch));
    monitor.append(Buffer.from("  one\r\none\n" + "a".repeat(479) + "😀tail\n"));
    monitor.close();
    expect(batches).toEqual([{ lines: ["one", "a".repeat(479) + "\ud83d [line truncated]"], omitted: 0 }]);
  });
});
