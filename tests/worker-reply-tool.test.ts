import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { validateToolArguments } from "@earendil-works/pi-ai";
import { afterEach, describe, expect, it } from "vitest";
import { directiveSchema } from "../src/actors/manager.js";
import replyTool, { REPLY_TOOL } from "../src/worker/reply-tool.js";

// smarty-dev#967: a directive reply is the arguments of one fabric_reply call.
describe("fabric_reply worker hook", () => {
  const roots: string[] = [];
  afterEach(() => {
    delete process.env.PI_FABRIC_REPLY_SCHEMA_FILE;
    delete process.env.PI_FABRIC_REPLY_FILE;
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
    replyTool({ registerTool: (tool: Record<string, any>) => tools.push(tool) } as never);
    return { tool: tools[0]!, count: tools.length, replyFile };
  };

  it("adds nothing outside a reply run", () => {
    const tools: unknown[] = [];
    replyTool({ registerTool: (tool: unknown) => tools.push(tool) } as never);
    expect(tools).toEqual([]);
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

  it("has Pi reject arguments outside the directive schema at the tool boundary", () => {
    const { tool } = load();
    const call = (args: unknown) => validateToolArguments(tool as never, { type: "toolCall", id: "c", name: REPLY_TOOL, arguments: args } as never);
    expect(call({ action: "silent" })).toEqual({ action: "silent" });
    expect(() => call({ action: "shout" })).toThrow();
    expect(() => call({ action: "silent", note: "extra" })).toThrow();
    expect(() => call({})).toThrow();
  });
});
