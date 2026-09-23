import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { pathToFileURL } from "node:url";
import { setImmediate as yieldToHost } from "node:timers/promises";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { encodeCwdDir } from "../src/memory/discovery.js";
import { createMemorySourceRegistry } from "../src/memory/portable.js";
import { MemoryProvider, type MemoryProviderContext } from "../src/providers/memory-provider.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { messageEntry, sessionHeader, userMessage, writeSessionFile } from "./fixtures/memory.js";

// The worker is a published build entry. Exercise its real default URL, not a test-only loader.
// CI runs affected tests before `bun run build`, so skip when dist/ is absent.
const workerFile = path.resolve("dist/memory/worker-provider.js");
const hasDistWorker = fs.existsSync(workerFile);
const WorkerMemoryProvider = hasDistWorker
  ? (await import(pathToFileURL(workerFile).href) as typeof import("../src/memory/worker-provider.js")).WorkerMemoryProvider
  : undefined;
const exec = promisify(execFile);
interface Page {
  total: number;
  hits: Array<{ entryId: string; snippet: string; follow: { args: Record<string, unknown> } }>;
  next: { args: Record<string, unknown> } | null;
  coverage: { complete: boolean; reasons: string[] };
  error?: { code: string };
}

let root: string;
let context: MemoryProviderContext;
const providers: Array<InstanceType<NonNullable<typeof WorkerMemoryProvider>>> = [];
const invocation = (): FabricInvocationContext => ({
  cwd: context.cwd, signal: undefined, parentToolCallId: "memory-integration", nestedToolCallId: "recall",
  extensionContext: {} as FabricInvocationContext["extensionContext"], update: vi.fn(),
});
const makeProvider = (overrides: Partial<MemoryProviderContext> = {}) => {
  if (!WorkerMemoryProvider) throw new Error("dist/memory/worker-provider.js is required");
  const provider = new WorkerMemoryProvider({ ...context, ...overrides });
  providers.push(provider);
  return provider;
};
const seed = (id: string, texts: string[]) => writeSessionFile(
  path.join(root, "sessions", encodeCwdDir(context.cwd)), `${id}.jsonl`, [
    sessionHeader(id, context.cwd),
    ...texts.map((text, index) => messageEntry(`${id}-${index}`, index ? `${id}-${index - 1}` : null,
      "2026-09-18T00:00:00.000Z", userMessage(text))),
  ],
);
beforeEach(() => {
  root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-memory-worker-"));
  context = {
    agentDir: root, cwd: path.join(root, "project"),
    config: { ...DEFAULT_FABRIC_CONFIG.memory, indexDir: path.join(root, "index"), hotSessions: 0 },
  };
});
afterEach(async () => {
  await Promise.all(providers.splice(0).map(provider => provider.close()));
  fs.rmSync(root, { recursive: true, force: true });
});

describe.skipIf(!hasDistWorker)("compiled file memory worker", () => {
  it("keeps the host event loop available during thirteen cold-session recalls", async () => {
    const files = Array.from({ length: 13 }, (_, index) => seed(`session-${index}`,
      Array.from({ length: 50 }, () => "brand name thương hiệu Xuân Anh ".repeat(40))));
    const memory = makeProvider();
    let completed = 0;
    const recalls = files.map(file => memory.invoke("recall", {
      scope: `session:${file}`, query: "brand name thương hiệu", pageSize: 3,
    }, invocation()).then(value => { completed++; return value as Page; }));
    await yieldToHost();
    expect(completed).toBeLessThan(13);
    for (const result of await Promise.all(recalls)) {
      expect(result.total).toBe(50);
      expect(result.hits).toHaveLength(3);
      expect(result.coverage.complete).toBe(true);
    }
  });

  it("matches inline retrieval, pagination, regex, expansion, and listing without altering source", async () => {
    const file = seed("history", ["brand first", "brand second", "unrelated", "brand last"]);
    const original = fs.readFileSync(file, "utf8");
    const memory = makeProvider();
    const inline = new MemoryProvider(context);
    const requests = [
      { scope: "project", query: "brand" },
      { scope: `session:${file}`, query: "brand", pageSize: 1 },
      { scope: `session:${file}`, query: "brand second", queryMode: "phrase" },
      { scope: `session:${file}`, query: "brand", queryMatch: "all", role: "user" },
      { scope: `session:${file}`, query: "br.nd", queryMode: "regex" },
      { scope: `session:${file}`, entryRange: { first: 1, last: 2 } },
    ];
    for (const args of requests) {
      expect(await memory.invoke("recall", args, invocation()))
        .toEqual(await inline.invoke("recall", args, invocation()));
    }
    let page = await memory.invoke("recall", requests[1]!, invocation()) as Page;
    const ids = page.hits.map(hit => hit.entryId);
    while (page.next) {
      const args = page.next.args;
      page = await memory.invoke("recall", args, invocation()) as Page;
      expect(page).toEqual(await inline.invoke("recall", args, invocation()));
      ids.push(...page.hits.map(hit => hit.entryId));
    }
    expect(new Set(ids).size).toBe(3);
    const expand = { session: file, entryIds: ["history-1"], maxChars: 8 };
    expect(await memory.invoke("expand", expand, invocation())).toEqual(await inline.invoke("expand", expand, invocation()));
    expect(await memory.invoke("sessions", { scope: "project" }, invocation()))
      .toEqual(await inline.invoke("sessions", { scope: "project" }, invocation()));
    expect(fs.readFileSync(file, "utf8")).toBe(original);
  });

  it("preserves same-size rewrite invalidation and rejects stale follow pointers", async () => {
    const file = seed("rewrite", ["brand old"]);
    const memory = makeProvider();
    const page = await memory.invoke("recall", { scope: `session:${file}`, query: "brand" }, invocation()) as Page;
    const stat = fs.statSync(file);
    fs.writeFileSync(file, fs.readFileSync(file, "utf8").replace("brand old", "brand new"));
    fs.utimesSync(file, stat.atime, stat.mtime);
    expect(await memory.invoke("expand", page.hits[0]!.follow.args, invocation()))
      .toMatchObject({ error: { code: "stale_pointer" } });
    const updated = await memory.invoke("recall", { scope: `session:${file}`, query: "new" }, invocation()) as Page;
    expect(updated.total).toBe(1);
    expect(updated.hits[0]!.snippet).toContain("brand new");
  });

  it("honors live branch navigation, all branches, and persisted branches without a live getter", async () => {
    const file = writeSessionFile(path.join(root, "sessions", encodeCwdDir(context.cwd)), "current.jsonl", [
      sessionHeader("current", context.cwd),
      messageEntry("root", null, "2026-09-18T00:00:00.000Z", userMessage("root")),
      messageEntry("chosen", "root", "2026-09-18T00:00:01.000Z", userMessage("visible brand")),
      messageEntry("sibling", "root", "2026-09-18T00:00:02.000Z", userMessage("abandoned brand")),
    ]);
    let leafId = "chosen";
    const memory = makeProvider({
      sessionFile: file, sessionId: "current",
      getLiveBranch: () => ({ leafId, entries: [{ id: "root" }, { id: leafId, uncloneable: () => {} }] }),
    });
    const query = { query: "brand", scope: `session:${file}` };
    const first = await memory.invoke("recall", query, invocation()) as Page;
    expect(first.hits.map(hit => hit.entryId)).toEqual(["chosen"]);
    expect((await memory.invoke("recall", { ...query, branches: "all" }, invocation()) as Page).total).toBe(2);
    leafId = "sibling";
    expect(await memory.invoke("expand", first.hits[0]!.follow.args, invocation())).toMatchObject({ error: { code: "stale_pointer" } });
    expect((await memory.invoke("recall", query, invocation()) as Page).hits.map(hit => hit.entryId)).toEqual(["sibling"]);
    const persisted = makeProvider({ sessionFile: file, sessionId: "current" });
    expect((await persisted.invoke("recall", query, invocation()) as Page).hits.map(hit => hit.entryId)).toEqual(["sibling"]);
  });

  it("retains privacy policy and fail-closed ambiguous-session errors", async () => {
    const file = writeSessionFile(path.join(root, "sessions", encodeCwdDir(context.cwd)), "privacy.jsonl", [
      sessionHeader("duplicate", context.cwd),
      messageEntry("assistant", null, "2026-09-18T00:00:00.000Z", {
        role: "assistant", content: [{ type: "thinking", thinking: "privatethought" }, { type: "text", text: "publicanswer" }],
      }),
    ]);
    const memory = makeProvider();
    expect((await memory.invoke("recall", { scope: `session:${file}`, query: "privatethought" }, invocation()) as Page).total).toBe(0);
    expect((await memory.invoke("recall", { scope: `session:${file}`, query: "publicanswer" }, invocation()) as Page).total).toBe(1);
    writeSessionFile(path.dirname(file), "duplicate.jsonl", [sessionHeader("duplicate", context.cwd)]);
    expect(await memory.invoke("recall", { scope: "session:duplicate" }, invocation()))
      .toMatchObject({ error: { code: "ambiguous_session" }, hits: [] });
  });

  it("keeps authorized portable adapters in the host rather than cloning their callbacks", async () => {
    const sources = createMemorySourceRegistry();
    const authorize = vi.fn(() => true);
    const loadSession = vi.fn(async () => ({
      sessionKey: "one", revision: "1", records: [
        sessionHeader("one", context.cwd),
        messageEntry("answer", null, "2026-09-18T00:00:00.000Z", userMessage("host brand")),
      ],
    }));
    sources.register({
      id: "host", interfaceVersion: 1, authorize,
      async listSessions() { return [{ sessionKey: "one", revision: "1" }]; }, loadSession,
    });
    const memory = makeProvider({ sources });
    const args = { source: "host", scope: "session:one", query: "brand" };
    const result = await memory.invoke("recall", args, invocation()) as Page;
    expect(result).toEqual(await new MemoryProvider({ ...context, sources }).invoke("recall", args, invocation()));
    expect(result.hits).toContainEqual(expect.objectContaining({ entryId: "answer" }));
    expect(authorize).toHaveBeenCalled();
    expect(loadSession).toHaveBeenCalled();
  });

  it("does not keep a Node host alive after the last request, even without explicit close", async () => {
    const workerFixture = new URL("./fixtures/memory-worker.mjs", import.meta.url).href;
    const program = `import { WorkerMemoryProvider } from ${JSON.stringify(pathToFileURL(workerFile).href)};
      const provider = new WorkerMemoryProvider(${JSON.stringify(context)}, new URL(${JSON.stringify(workerFixture)}));
      await provider.invoke("recall", {}, { cwd: ${JSON.stringify(context.cwd)}, update() {} });
      console.log("idle worker released the host");`;
    const result = await exec(process.execPath, ["--input-type=module", "-e", program], { timeout: 5_000 });
    expect(result.stdout).toContain("idle worker released the host");
  });
});
