import fs from "node:fs";
import os from "node:os";
import fsp, { rm, stat } from "node:fs/promises";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import {
  boundModelOutput,
  formatResidentOutcomePriority,
  MAX_FAILURE_MODEL_OUTPUT_CHARS,
  modelOutputBudget,
} from "../src/output-budget.js";

describe("modelOutputBudget", () => {
  it("caps failures without reducing successful or stricter configured budgets", () => {
    expect(modelOutputBudget(50_000, true)).toBe(50_000);
    expect(modelOutputBudget(50_000, false)).toBe(MAX_FAILURE_MODEL_OUTPUT_CHARS);
    expect(modelOutputBudget(10_000, false)).toBe(10_000);
  });
});

describe("priority model output", () => {
  const receipts = [
    { state: "committed" as const, operation: "createActor", entityKind: "actor" as const, requestId: "request-one", id: "actor-one", ownerHostId: "owner-one" },
    { state: "committed" as const, operation: "spawn", entityKind: "agent" as const, requestId: "request-two", id: "agent-two", ownerHostId: "owner-two" },
  ];
  const text = formatResidentOutcomePriority(receipts);

  it("keeps all reconciliation facts first, budgets logs/cause independently and preserves the full artifact", async () => {
    const sections = [`logs-start ${"log detail; ".repeat(3_000)} logs-end`, `cause-start ${"cause detail; ".repeat(3_000)} cause-end`, "short progress"];
    const full = [text, ...sections].join("\n\n");
    const writer = vi.fn(async () => "/tmp/full.txt");
    const result = await boundModelOutput(sections.join("\n\n"), 2_000, full, writer, { text, sections });
    expect(result.text.length).toBeLessThanOrEqual(2_000);
    expect(result.text.startsWith(text + "\n\n")).toBe(true);
    for (const marker of ["logs-start", "logs-end", "cause-start", "cause-end", "short progress"]) expect(result.text).toContain(marker);
    expect(result.text).toContain("saved to: /tmp/full.txt]");
    expect(writer).toHaveBeenCalledExactlyOnceWith(full);
    expect(result.omittedChars).toBeGreaterThan(0);
  });

  it("does not truncate a priority block or drop any ID even when receipts alone exceed the soft budget", async () => {
    const many = Array.from({ length: 300 }, (_, index) => ({ ...receipts[index % 2]!, requestId: `request-${index}`, id: `entity-${index}`, ownerHostId: `owner-${index}` }));
    const priority = formatResidentOutcomePriority(many);
    const sections = ["guest prose must not replace receipts".repeat(1_000)];
    const full = [priority, ...sections].join("\n\n");
    const result = await boundModelOutput(sections[0]!, 1_000, full, async () => "/tmp/full.txt", { text: priority, sections });
    expect(result.text).toBe(priority);
    expect(result.artifactPath).toBe("/tmp/full.txt");
    for (const receipt of many) for (const id of [receipt.requestId, receipt.id, receipt.ownerHostId]) expect(result.text).toContain(id);
    expect(result.text).toContain("ResidentOutcomeUnknownError"); expect(result.text).toContain("Do not retry or reassign");
  });

  it("keeps priority facts when artifact writes fail and handles allocations smaller than a truncation marker", async () => {
    const sections = ["logs ".repeat(1_000), "cause ".repeat(1_000)];
    const full = [text, ...sections].join("\n\n");
    const writer = async () => { throw new Error("disk full"); };
    for (const budget of [text.length, text.length + 5, text.length + 100, 2_000]) {
      const result = await boundModelOutput(sections.join("\n\n"), budget, full, writer, { text, sections });
      expect(result.text.length).toBeLessThanOrEqual(budget);
      expect(result.text.startsWith(text)).toBe(true);
      expect(result.artifactPath).toBeUndefined();
    }
  });

  it("keeps priority output first without artifact allocation when everything fits", async () => {
    const sections = ["small logs", "small cause"];
    const full = [text, ...sections].join("\n\n");
    const writer = vi.fn(async () => "/tmp/full.txt");
    const result = await boundModelOutput(sections.join("\n\n"), 2_000, full, writer, { text, sections });
    expect(result.text).toBe(full); expect(writer).not.toHaveBeenCalled();
  });

  it("labels unavailable fence facts as unknown without inventing entity/owner IDs", () => {
    const priority = formatResidentOutcomePriority([{ state: "unknown", operation: "spawn", entityKind: "agent", requestId: "unreadable-fence" }]);
    expect(priority).toContain("state=unknown"); expect(priority).toContain("requestId=unreadable-fence");
    expect(priority).toContain("agentId=not yet known, ownerHostId=not yet known");
    expect(priority).toContain("Do not retry or reassign");
  });
});

describe("boundModelOutput", () => {
  it("leaves output under budget untouched", async () => {
    const writer = vi.fn(async () => "/tmp/full.txt");
    await expect(boundModelOutput("small", 1_000, "small", writer)).resolves.toEqual({
      text: "small",
      originalChars: 5,
      omittedChars: 0,
    });
    expect(writer).not.toHaveBeenCalled();
  });

  it("bounds visible text and links the complete artifact", async () => {
    const full = `start-${"x".repeat(4_000)}-end`;
    const writer = vi.fn(async () => "/tmp/pi-fabric-output/output.txt");
    const result = await boundModelOutput(full, 1_000, full, writer);

    expect(result.text.length).toBeLessThanOrEqual(1_000);
    expect(result.text).toContain("start-");
    expect(result.text).toContain("-end");
    expect(result.text).toContain("Full output (4010 chars) saved to:");
    expect(result.artifactPath).toBe("/tmp/pi-fabric-output/output.txt");
    expect(result.omittedChars).toBeGreaterThan(0);
    expect(writer).toHaveBeenCalledWith(full);
  });
  it("never cuts the artifact path when the budget barely fits the suffix", async () => {
    const full = "x".repeat(4_000);
    const artifactPath = "/tmp/pi-fabric-output/output.txt";
    const writer = vi.fn(async () => artifactPath);
    const result = await boundModelOutput(full, 100, full, writer);

    expect(result.text.length).toBeLessThanOrEqual(100);
    expect(result.text).toContain(artifactPath);
    expect(result.artifactPath).toBe(artifactPath);
    expect(result.omittedChars).toBeGreaterThan(0);
  });


  it("persists retrievable artifacts with private POSIX permissions", async () => {
    const result = await boundModelOutput("x".repeat(4_000), 1_000);
    expect(result.artifactPath).toBeDefined();
    const info = await stat(result.artifactPath!);
    if (process.platform !== "win32") {
      expect(info.mode & 0o777).toBe(0o600);
    }
    await rm(path.dirname(result.artifactPath!), { recursive: true, force: true });
  });

  it("removes the allocated directory when the real artifact write fails", async () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "output-failure-test-"));
    const tmp = vi.spyOn(os, "tmpdir").mockReturnValue(tempRoot);
    const write = vi.spyOn(fsp, "writeFile").mockRejectedValue(new Error("disk full"));
    try {
      const result = await boundModelOutput("x".repeat(4_000), 1_000);
      expect(result.artifactPath).toBeUndefined();
      expect(fs.readdirSync(tempRoot)).toEqual([]);
    } finally {
      write.mockRestore();
      tmp.mockRestore();
      fs.rmSync(tempRoot, { recursive: true, force: true });
    }
  });

  it("stays bounded if artifact persistence fails", async () => {
    const writer = vi.fn(async () => { throw new Error("disk full"); });
    const result = await boundModelOutput("x".repeat(4_000), 1_000, undefined, writer);

    expect(result.text.length).toBeLessThanOrEqual(1_000);
    expect(result.artifactPath).toBeUndefined();
    expect(result.text).toContain("characters omitted by Pi Fabric");
  });
});
