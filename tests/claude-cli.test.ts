import { describe, expect, it } from "vitest";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { buildClaudeArguments, claudeUserMessage, discoverClaudeModels } from "../src/agents/claude-cli.js";

describe("Claude stream-json messages", () => {
  it("distinguishes a native resolved target from an alias display fallback", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-claude-catalog-"));
    const binary = path.join(root, "catalog.mjs");
    fs.writeFileSync(binary, `process.stdin.setEncoding("utf8");
let input = "";
process.stdin.on("data", chunk => { input += chunk; });
process.stdin.on("end", () => {
  const request = JSON.parse(input);
  console.log(JSON.stringify({ type: "control_response", response: { subtype: "success", request_id: request.request_id, response: { models: [
    { value: "shortcut", displayName: "Unresolved" },
    { value: "haiku", resolvedModel: "claude-haiku-test" }
  ] } } }));
});`);
    try {
      const models = await discoverClaudeModels(binary, root);
      expect(models[0]).toMatchObject({ value: "shortcut", resolvedModel: "shortcut", resolvedModelKnown: false });
      expect(models[1]).toMatchObject({ value: "haiku", resolvedModel: "claude-haiku-test" });
      expect(models[1]?.resolvedModelKnown).not.toBe(false);
    } finally { fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }); }
  });

  it("maps Fabric image blocks to Claude base64 content blocks", () => {
    const message = claudeUserMessage("Inspect this", [
      { type: "image", data: "aGVsbG8=", mimeType: "image/png" },
    ]);
    expect(message).toMatchObject({
      type: "user",
      message: {
        role: "user",
        content: [
          { type: "text", text: "Inspect this" },
          {
            type: "image",
            source: {
              type: "base64",
              media_type: "image/png",
              data: "aGVsbG8=",
            },
          },
        ],
      },
    });
  });
});

describe("Claude session persistence", () => {
  const options = { tools: ["read"], extensions: false, persistentSession: false };

  it("disables transcript persistence by default", () => {
    expect(buildClaudeArguments(options)).toContain("--no-session-persistence");
  });

  it("omits the disable flag when explicitly opted in", () => {
    expect(buildClaudeArguments({ ...options, persistentSession: true })).not.toContain("--no-session-persistence");
  });
});
