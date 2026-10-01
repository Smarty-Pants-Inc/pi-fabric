import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { validateToolArguments } from "@earendil-works/pi-ai";
import { afterEach, describe, expect, it, vi } from "vitest";
import { directiveSchema } from "../src/actors/manager.js";
import { normalizeAgentRunRequest } from "../src/agents/request.js";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { ownsRunReplyTool } from "../src/core/reply-tool-identity.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import replyTool, { REPLY_TOOL } from "../src/worker/reply-tool.js";

// smarty-dev#967: a directive reply is the arguments of one fabric_reply call.
describe("fabric_reply worker hook", () => {
  const roots: string[] = [];
  afterEach(() => {
    vi.restoreAllMocks();
    delete process.env.PI_FABRIC_REPLY_SCHEMA_FILE;
    delete process.env.PI_FABRIC_REPLY_FILE;
    delete process.env.PI_FABRIC_REPLY_HOOK;
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  });

  const load = () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-reply-"));
    roots.push(root);
    const schemaFile = path.join(root, "schema.json");
    const replyFile = path.join(root, "reply.json");
    fs.writeFileSync(schemaFile, JSON.stringify(directiveSchema));
    process.env.PI_FABRIC_REPLY_SCHEMA_FILE = schemaFile;
    process.env.PI_FABRIC_REPLY_FILE = replyFile;
    const tools: Array<Record<string, any>> = [];
    const handlers: Array<[string, (event: any) => any]> = [];
    replyTool({
      registerTool: (tool: Record<string, any>) => tools.push(tool),
      on: (name: string, handler: (event: any) => any) => handlers.push([name, handler]),
    } as never);
    return { tool: tools[0]!, count: tools.length, replyFile, handlers };
  };

  it("#2479 persists structured reply before reporting delivery", async () => {
    const { tool, replyFile } = load();
    const descriptors = new Map<number, string>(), events: string[] = [];
    const open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs), rename = fs.renameSync.bind(fs);
    vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => { const fd = open(file, flags, mode); descriptors.set(fd, String(file)); return fd; });
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => { events.push(`sync:${descriptors.get(fd)}`); sync(fd); });
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => { events.push(`rename:${to}`); rename(from, to); });
    await tool.execute("durable-reply", { action: "message", message: "Audit." });
    const published = events.indexOf(`rename:${replyFile}`);
    expect(events.slice(0, published)).toContain(`sync:${replyFile}.${process.pid}.tmp`);
    if (process.platform !== "win32") expect(events.slice(published + 1)).toContain(`sync:${path.dirname(replyFile)}`);
  });

  // smarty-dev#1469: Mains and task agents never load this hook with its env, so they get no guard.
  it("adds nothing outside a reply run", () => {
    const tools: unknown[] = [];
    const handlers: unknown[] = [];
    replyTool({ registerTool: (tool: unknown) => tools.push(tool), on: (name: string) => handlers.push(name) } as never);
    expect(tools).toEqual([]);
    expect(handlers).toEqual([]);
  });

  // smarty-dev#1469: a directive actor's bash call that cuts an issue or PR comment list is blocked
  // with the fix; the full list, a single comment and other tools pass.
  it("blocks a cut comment-list read in a directive run and names the fix", () => {
    const { handlers } = load();
    expect(handlers.map(([name]) => name)).toEqual(["tool_call"]);
    const guard = handlers[0]![1];
    const list = "gh api repos/o/r/issues/1201/comments --jq '.[] | {id, user: .user.login, created_at, first: (.body | split(\"\\n\")[0])}'";
    const blocked = guard({ toolName: "bash", toolCallId: "t1", input: { command: `${list} | tail -n 3` } });
    expect(blocked).toMatchObject({ block: true });
    expect(blocked.reason).toContain("--jq '.[] | {id, user: .user.login, created_at, first: (.body | split(\"\\n\")[0])}'");
    expect(blocked.reason).toMatch(/owner's answer can be any comment/);
    expect(guard({ toolName: "bash", toolCallId: "t2", input: { command: list } })).toBeUndefined();
    expect(guard({ toolName: "bash", toolCallId: "t3", input: { command: "gh api repos/o/r/issues/comments/5 --jq .body | head -30" } })).toBeUndefined();
    expect(guard({ toolName: "read", toolCallId: "t4", input: { path: "x/issues/1/comments | tail -3" } })).toBeUndefined();
  });

  it("never lets a task agent's run request ask for the directive hook", () => {
    const request = normalizeAgentRunRequest(
      { task: "t", schema: directiveSchema, replyTool: true }, { runner: "pi", timeoutMs: 1 },
    );
    expect(request).not.toHaveProperty("replyTool");
  });

  it("delivers one reply, ends the run, and refuses a second call", async () => {
    const { tool, count, replyFile } = load();
    expect(count).toBe(1);
    expect(tool.name).toBe(REPLY_TOOL);
    expect(tool.parameters).toEqual(directiveSchema);
    const result = await tool.execute("call-1", { action: "message", message: "Look at #80." });
    expect(result.terminate).toBe(true);
    expect(JSON.parse(fs.readFileSync(replyFile, "utf8"))).toEqual({ action: "message", message: "Look at #80." });
    await expect(tool.execute("call-2", { action: "silent" })).rejects.toThrow(/already replied/);
    expect(JSON.parse(fs.readFileSync(replyFile, "utf8")).action).toBe("message");
  });

  // review/astra F1 on #85: Fabric captures, and so hides, every extension tool in full-code and
  // Schema enforce modes. It leaves out only this run's reply tool, from the exact hook file.
  it("is never captured, while another extension's fabric_reply still is", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-reply-"));
    roots.push(root);
    const hook = path.join(root, "reply-tool.js");
    const other = path.join(root, "other-extension.js");
    fs.writeFileSync(hook, "");
    fs.writeFileSync(other, "");
    process.env.PI_FABRIC_REPLY_FILE = path.join(root, "reply.json");
    process.env.PI_FABRIC_REPLY_HOOK = fs.realpathSync(hook);
    const registered = (name: string, file: string) => ({
      definition: { name, label: name, description: name, parameters: {}, execute: async () => ({ content: [], details: {} }) },
      sourceInfo: { path: file, source: "extension" },
    });
    const catalog = new CapturedToolCatalog();
    catalog.replace(
      [registered("fabric_reply", hook), registered("fabric_reply", other), registered("todo", other)] as never,
      {} as never, { ...DEFAULT_FABRIC_CONFIG.capture, enabled: true, hideFromModel: true }, path.join(root, "fabric.js"),
    );
    // The spoof keeps its name in the catalog (the map is by name): only the hook's entry is skipped.
    expect(catalog.list().map((entry) => [entry.name, entry.sourceInfo.path])).toEqual([["fabric_reply", other], ["todo", other]]);
    delete process.env.PI_FABRIC_REPLY_HOOK;                             // outside a reply run
    catalog.replace([registered("fabric_reply", hook)] as never, {} as never,
      { ...DEFAULT_FABRIC_CONFIG.capture, enabled: true, hideFromModel: true }, path.join(root, "fabric.js"));
    expect(catalog.list().map((entry) => entry.name)).toEqual(["fabric_reply"]);
  });

  // review/astra on #85: the native call gate lets through only this run's reply tool.
  it("owns the reply tool only from the exact hook file of a reply run", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-reply-"));
    roots.push(root);
    const hook = path.join(root, "reply-tool.js");
    const other = path.join(root, "other-extension.js");
    fs.writeFileSync(hook, "");
    fs.writeFileSync(other, "");
    const env = { PI_FABRIC_REPLY_FILE: path.join(root, "reply.json"), PI_FABRIC_REPLY_HOOK: fs.realpathSync(hook) };
    expect(ownsRunReplyTool([{ name: "fabric_reply", sourceInfo: { path: hook } }], env)).toBe(true);
    expect(ownsRunReplyTool([{ name: "fabric_reply", sourceInfo: { path: other } }], env)).toBe(false);
    expect(ownsRunReplyTool([{ name: "other_tool", sourceInfo: { path: hook } }], env)).toBe(false);
    expect(ownsRunReplyTool([{ name: "fabric_reply", sourceInfo: { path: hook } }], {})).toBe(false);
  });

  it("has Pi reject arguments outside the directive schema at the tool boundary", () => {
    const { tool } = load();
    const call = (args: unknown) => validateToolArguments(tool as never, { type: "toolCall", id: "c", name: REPLY_TOOL, arguments: args } as never);
    expect(call({ action: "silent" })).toEqual({ action: "silent" });
    expect(() => call({ action: "shout" })).toThrow();
    expect(() => call({ action: "silent", note: "extra" })).toThrow();
    expect(() => call({})).toThrow();
  });
});
