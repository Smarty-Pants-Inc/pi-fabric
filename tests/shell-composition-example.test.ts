import fs from "node:fs";
import { describe, expect, it, vi } from "vitest";

// Execute the shipped guidance itself so the docs and the regression cannot drift.
const example = fs.readFileSync(new URL("../docs/shell-composition.md", import.meta.url), "utf8").match(/```ts\n([\s\S]*?)\n```/)![1]!;
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor as new (...args: string[]) => (...args: any[]) => Promise<unknown>;
const runExample = async (pages: Array<{ text: string; next: number; eof: boolean; omittedBytes: number }>) => {
  let code = "";
  const jev = { spawn: async (request: { program: { code: string } }) => { code = request.program.code; return { id: "controller" }; },
    evaluate: vi.fn(async () => { throw new Error("A completed bridge must not request a decision"); }) };
  await new AsyncFunction("jev", example)(jev);
  const read = vi.fn();
  const tools = { call: async (request: { ref: string; args: Record<string, unknown> }) => {
    if (request.ref === "sessions.open") return { id: "bridge" };
    if (request.ref === "sessions.write") return {};
    if (request.ref === "sessions.read") { read(request.args); const page = pages.shift(); if (!page) throw new Error("Unexpected extra read"); return page; }
    throw new Error(`Unexpected action: ${request.ref}`);
  } };
  return { result: await new AsyncFunction("tools", "jev", code)(tools, jev), read, jev };
};

describe("shipped realtime shell composition example", () => {
  it("A23 consumes a complete response on the EOF page before treating exit as failure", async () => {
    const { result, read, jev } = await runExample([{ text: '{"done":true}\n', next: 14, eof: true, omittedBytes: 0 }]);
    expect(result).toEqual({ done: true }); expect(read).toHaveBeenCalledOnce(); expect(jev.evaluate).not.toHaveBeenCalled();
  });
  it("A23 rejects disclosed retention gaps instead of joining partial response evidence", async () => {
    await expect(runExample([
      { text: '{"done":', next: 8, eof: false, omittedBytes: 0 },
      { text: 'true}\n', next: 20, eof: false, omittedBytes: 6 },
    ])).rejects.toThrow(/bridge output gap/);
  });
  it("still rejects EOF without a complete response", async () => {
    await expect(runExample([{ text: '{"done":', next: 8, eof: true, omittedBytes: 0 }])).rejects.toThrow("bridge exited");
  });
});
