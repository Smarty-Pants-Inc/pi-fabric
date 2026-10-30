import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";
import { describe, expect, it } from "vitest";

const harness = path.resolve(import.meta.dirname, "../scripts/probe-idle-main-batches.mjs");
const fixture = () => fs.mkdtempSync(path.join(os.tmpdir(), "idle-batch-probe-test-"));
const run = (body: string) => {
  const child = spawnSync(process.execPath, ["--input-type=module", "-e",
    `import * as probe from ${JSON.stringify(pathToFileURL(harness).href)};\n${body}`], { encoding: "utf8" });
  expect(child.error).toBeUndefined();
  expect(child.status, child.stderr).toBe(0);
  return JSON.parse(child.stdout);
};

// Keep: recorded attribution defects F2/F3 and evidence data safety defect F4.
// Every database/output is created by this test; no live mesh or prior evidence.
describe("idle Main batch probe", () => {
  it("binds actual native transactions through descriptor paths; excludes another state.db but counts the mesh", () => {
    const root = fixture();
    const result = run(`
      import fs from "node:fs";
      import path from "node:path";
      import { DatabaseSync } from "node:sqlite";
      const root = ${JSON.stringify(root)};
      const file = path.join(root, "state.db");
      new DatabaseSync(file).close();
      fs.mkdirSync(path.join(root, "other"));
      const rows = [];
      const restore = probe.captureTransactions(probe.databaseIdentity(file),
        () => ({ phase: "idle", batchId: 1 }), row => rows.push(row));
      const pin = process.platform === "linux" ? fs.openSync(root, "r") : undefined;
      const opened = pin === undefined ? file : path.join("/proc/self/fd", String(pin), "state.db");
      const mesh = new DatabaseSync(opened);
      const other = new DatabaseSync(path.join(root, "other", "state.db"));
      const memory = new DatabaseSync(":memory:");
      for (const db of [other, mesh, memory, mesh]) {
        db.exec("BEGIN IMMEDIATE"); db.exec("COMMIT");
      }
      mesh.close(); other.close(); memory.close(); restore();
      if (pin !== undefined) fs.closeSync(pin);
      const calls = [{ id: 1, phase: "idle", stack: ["at caller (<candidate>/caller.ts:8:9)"],
        ops: [], prepared: [], hookRuns: [], results: [], succeeded: true }];
      console.log(JSON.stringify({ rows, sites: probe.aggregateSites({ calls, transactions: rows, locks: [] }),
        expected: probe.databaseIdentity(file) }));
    `);
    const begins = result.rows.filter((row: any) => row.verb === "BEGIN IMMEDIATE");
    expect(begins.map((row: any) => row.database.scope)).toEqual([
      "other-sqlite", "mesh-state", "unknown-sqlite", "mesh-state",
    ]);
    expect(begins.every((row: any) => row.succeeded)).toBe(true);
    expect(begins[1].database).toMatchObject(result.expected);
    expect(begins[1].database.connectionId).toBe(begins[3].database.connectionId);
    expect(begins[0].database.connectionId).not.toBe(begins[1].database.connectionId);
    expect(result.sites[0].transactions).toBe(2);
  });

  it("combines different stacks at one immediate source location while keeping distinct callers separate", () => {
    const result = run(`
      const frames = [
        ["at first (<candidate>/caller.ts:8:9)", "at parentA (<candidate>/a.ts:2:3)"],
        ["at renamed (<candidate>/caller.ts:8:9)", "at async parentB (<candidate>/b.ts:4:5)"],
        ["at <candidate>/caller.ts:8:9", "at parentA (<candidate>/a.ts:2:3)"],
        ["at async <candidate>/caller.ts:8:9", "at parentB (<candidate>/b.ts:4:5)"],
        ["at next (<candidate>/caller.ts:9:9)", "at parentA (<candidate>/a.ts:2:3)"],
        ["at <candidate>/caller.ts:8:10", "at parentA (<candidate>/a.ts:2:3)"],
      ];
      const calls = frames.map((stack, i) => ({ id: i + 1, phase: "idle", stack,
        ops: [{ kind: "put", key: "test/key" }], prepared: [[{ kind: "put", key: "test/prepared" }]],
        hookRuns: [{ name: "prepare" }], results: [{ applied: true }], succeeded: true }));
      console.log(JSON.stringify(probe.aggregateSites({ calls, transactions: [], locks: [] })));
    `);
    expect(result).toHaveLength(3);
    expect(result[0]).toMatchObject({ callSite: "<candidate>/caller.ts:8:9", count: 4,
      inputOps: 4, preparedOps: 4, resultOps: 4, appliedOps: 4, hooks: { prepare: 4 },
      keys: { "put test/key": 4, "put test/prepared": 4 } });
    expect(result[0].stacks).toHaveLength(4);
    expect(result[0].stacks.map((variant: any) => variant.count)).toEqual([1, 1, 1, 1]);
    expect(result[0].stacks.map((variant: any) => variant.stack[0])).toEqual([
      "at first (<candidate>/caller.ts:8:9)",
      "at renamed (<candidate>/caller.ts:8:9)",
      "at <candidate>/caller.ts:8:9",
      "at async <candidate>/caller.ts:8:9",
    ]);
    expect(result[0].stacks[0].stack[1]).toContain("parentA");
    expect(result[0].stacks[1].stack[1]).toContain("parentB");
    expect(result.slice(1).map((site: any) => site.callSite)).toEqual([
      "<candidate>/caller.ts:9:9", "<candidate>/caller.ts:8:10",
    ]);
  });

  it.each([".json", ".migration.json", ".ready.json", ".capture.json", ".rpc.json", ".stderr.log"])(
    "refuses the CLI before startup when a fresh summary has an existing %s artifact", suffix => {
      const root = fixture();
      const out = path.join(root, "report.json"), sentinel = path.join(root, "report" + suffix);
      fs.writeFileSync(sentinel, "preserve this evidence\n", { mode: 0o600 });
      const child = spawnSync(process.execPath, [harness, "--import-root", path.join(root, "unused-candidate"),
        "--pi-cli", path.join(root, "unused-cli"), "--out", out, "--seconds", "2"], { encoding: "utf8" });
      expect(child.status).not.toBe(0);
      expect(child.stderr).toContain("use fresh evidence paths");
      expect(fs.readFileSync(sentinel, "utf8")).toBe("preserve this evidence\n");
      expect(fs.readdirSync(root)).toEqual([path.basename(sentinel)]);
    },
  );

  it("preserves evidence that appears after preflight too", () => {
    const root = fixture();
    const result = run(`
      import fs from "node:fs";
      const files = probe.prepareOutput(${JSON.stringify(path.join(root, "report.json"))});
      fs.writeFileSync(files.capture, "sentinel");
      let code;
      try { probe.save(files.capture, { replacement: true }); } catch (error) { code = error.code; }
      console.log(JSON.stringify({ code, content: fs.readFileSync(files.capture, "utf8") }));
    `);
    expect(result).toEqual({ code: "EEXIST", content: "sentinel" });
  });

  it.skipIf(process.platform === "win32")("writes fresh private artifacts under permissive umask without changing a shared parent", () => {
    const root = fixture();
    const result = run(`
      import fs from "node:fs";
      import path from "node:path";
      process.umask(0);
      const root = ${JSON.stringify(root)};
      const shared = path.join(root, "shared");
      fs.mkdirSync(shared, { mode: 0o777 });
      let refused = false;
      try { probe.prepareOutput(path.join(shared, "report.json")); } catch { refused = true; }
      const files = probe.prepareOutput(path.join(shared, "private", "report.json"));
      for (const file of Object.values(files)) probe.save(file, { fresh: true });
      console.log(JSON.stringify({ refused, shared: fs.statSync(shared).mode & 0o777,
        parent: fs.statSync(path.dirname(files.summary)).mode & 0o777,
        modes: Object.values(files).map(file => fs.statSync(file).mode & 0o777),
        contents: Object.values(files).map(file => JSON.parse(fs.readFileSync(file, "utf8"))) }));
    `);
    expect(result.refused).toBe(true);
    expect(result.shared).toBe(0o777);
    expect(result.parent).toBe(0o700);
    expect(result.modes).toEqual(Array(6).fill(0o600));
    expect(result.contents).toEqual(Array(6).fill({ fresh: true }));
  });
});
