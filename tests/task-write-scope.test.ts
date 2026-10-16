import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { FabricInvocationContext } from "../src/protocol.js";
import fabricWriteGuard, { writePolicyDenial } from "../src/agents/write-guard.js";
import { PiToolsProvider } from "../src/providers/pi-tools-provider.js";

const saved = process.env.PI_FABRIC_WRITE_POLICY;
afterEach(() => {
  if (saved === undefined) delete process.env.PI_FABRIC_WRITE_POLICY;
  else process.env.PI_FABRIC_WRITE_POLICY = saved;
});
const fixture = () => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "task-write-scope-")));
  const work = path.join(root, "work"), tmp = path.join(root, "tmp"), outside = path.join(root, "outside");
  for (const dir of [work, tmp, outside]) fs.mkdirSync(dir);
  fs.writeFileSync(path.join(outside, "target"), "original");
  fs.symlinkSync(outside, path.join(work, "escape"), process.platform === "win32" ? "junction" : "dir");
  return { root, work, tmp, outside, policy: { readOnly: false, writableRoots: [work, tmp], shell: "deny" as const } };
};
const diagnostic = (reason: string | undefined, target: string, roots: string[]) => {
  expect(reason).toContain(`refused path: ${JSON.stringify(target)}`);
  expect(reason).toContain(`allowed roots: ${JSON.stringify(roots)}`);
};

describe("task write-scope error receipts (smarty-dev#10544)", () => {
  it.each(["write", "edit"])("%s hook refuses outside, absent-parent, and symlink paths with the actual grant", (toolName) => {
    const { work, outside, root, policy } = fixture();
    process.env.PI_FABRIC_WRITE_POLICY = JSON.stringify(policy);
    let hook: ((event: { toolName: string; input: { path: string } }, context: { cwd: string }) => { block: boolean; reason: string } | undefined) | undefined;
    fabricWriteGuard({ on: (_name: string, handler: typeof hook) => { hook = handler; } } as unknown as ExtensionAPI);
    for (const target of [path.join(outside, "target"), path.join(root, "absent", "parent", "file"), path.join(work, "escape", "target")]) {
      const result = hook!({ toolName, input: { path: target } }, { cwd: work });
      expect(result?.block).toBe(true);
      diagnostic(result?.reason, target, policy.writableRoots);
    }
    expect(fs.readFileSync(path.join(outside, "target"), "utf8")).toBe("original");
    expect(fs.existsSync(path.join(root, "absent"))).toBe(false);
    expect(hook!({ toolName, input: { path: "new/parent/file" } }, { cwd: work })).toBeUndefined();
  });

  it("a read-only refusal identifies the path and empty grant", () => {
    diagnostic(writePolicyDenial({ readOnly: true, writableRoots: [], shell: "deny" }, "write", { path: "report.txt" }, process.cwd()), "report.txt", []);
  });

  it("nested provider refuses escapes; valid missing-parent writes and edits still work", async () => {
    const { work, tmp, outside, root, policy } = fixture();
    process.env.PI_FABRIC_WRITE_POLICY = JSON.stringify(policy);
    const provider = new PiToolsProvider(work, undefined, undefined, { powerShellToolDefinitionFactory: undefined, allowUnhookedTools: true });
    const context = { cwd: work, extensionContext: { cwd: work }, nestedToolCallId: "scope", update() {} } as unknown as FabricInvocationContext;
    try {
      for (const target of [path.join(outside, "target"), path.join(root, "absent", "parent", "file"), path.join(work, "escape", "target")]) {
        for (const toolName of ["write", "edit"]) {
          await expect(provider.invoke(toolName, { path: target, content: "forbidden", edits: [{ oldText: "original", newText: "forbidden" }] }, context))
            .rejects.toThrow(`refused path: ${JSON.stringify(target)}; allowed roots: ${JSON.stringify(policy.writableRoots)}`);
        }
      }
      expect(fs.readFileSync(path.join(outside, "target"), "utf8")).toBe("original");
      expect(fs.existsSync(path.join(root, "absent"))).toBe(false);
      for (const target of [path.join(work, "new", "parent", "file"), path.join(tmp, "allowed")]) {
        await provider.invoke("write", { path: target, content: "allowed" }, context);
        expect(fs.readFileSync(target, "utf8")).toBe("allowed");
        await provider.invoke("edit", { path: target, edits: [{ oldText: "allowed", newText: "edited" }] }, context);
        expect(fs.readFileSync(target, "utf8")).toBe("edited");
      }
    } finally {
      await provider.close();
    }
  });
});
