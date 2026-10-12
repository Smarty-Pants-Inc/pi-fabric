import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { runDecisionsCli } from "../src/cli/decisions.js";
import { MeshStore } from "../src/mesh/store.js";
import { DecisionStore } from "../src/decisions/store.js";

const roots: string[] = [];

afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-decisions-cli-"));
  roots.push(root);
  const store = new DecisionStore(new MeshStore(root, 64 * 1024, 500), {
    id: "session:main", name: "main", kind: "main", sessionId: "main",
  });
  return { root, store };
};

const run = async (argv: string[], env: NodeJS.ProcessEnv = {}) => {
  let stdout = "";
  let stderr = "";
  const code = await runDecisionsCli(argv, {
    stdout: { write: (chunk: string) => { stdout += chunk; return true; } },
    stderr: { write: (chunk: string) => { stderr += chunk; return true; } },
    env,
  });
  return { code, stdout, stderr };
};

describe("pi-fabric decisions CLI", () => {
  it("lists open decisions and answers one with via cli and the OS user", async () => {
    const { root, store } = await fixture();
    const approval = await store.raise({
      kind: "approval",
      title: "demo.write requests write access",
      options: [{ id: "approve", label: "Approve once" }, { id: "deny", label: "Deny" }],
    });
    const listed = await run(["list", "--root", root]);
    expect(listed.code).toBe(0);
    expect(listed.stdout).toContain(approval.id);
    expect(listed.stdout).toContain("approve=Approve once");

    const json = await run(["list", "--json"], { PI_FABRIC_MESH_ROOT: root });
    expect(JSON.parse(json.stdout)).toEqual([expect.objectContaining({ id: approval.id, status: "open" })]);

    const answered = await run(["answer", "--root", root, approval.id, "--option", "approve", "--json"]);
    expect(answered.code).toBe(0);
    expect(JSON.parse(answered.stdout)).toMatchObject({
      status: "answered",
      answer: { optionId: "approve", via: "cli", answeredBy: os.userInfo().username },
    });
    expect((await run(["list", "--root", root])).stdout).toBe("No decisions\n");
    expect((await run(["list", "--root", root, "--status", "answered"])).stdout).toContain(approval.id);
  });

  it("answers text decisions and fails on non-open or invalid answers", async () => {
    const { root, store } = await fixture();
    const question = await store.raise({ title: "Why?" });
    expect((await run(["answer", "--root", root, question.id, "--option", "x"])).code).toBe(1);
    expect((await run(["answer", "--root", root, question.id, "--text", "because"])).code).toBe(0);
    const again = await run(["answer", "--root", root, question.id, "--text", "twice"]);
    expect(again.code).toBe(1);
    expect(again.stderr).toMatch(/answered, not open/);
  });

  it("returns usage errors for malformed invocations", async () => {
    const { root } = await fixture();
    expect((await run([])).code).toBe(2);
    expect((await run(["list"])).code).toBe(2);
    expect((await run(["list", "--root", root, "--status", "bogus"])).code).toBe(2);
    expect((await run(["answer", "--root", root, "dec_abcdefgh12"])).code).toBe(2);
    expect((await run(["frobnicate", "--root", root])).code).toBe(2);
    expect((await run(["list", "--root", root, "--nope", "x"])).code).toBe(2);
    const missing = await run(["list", "--root", path.join(root, "missing")]);
    expect(missing.code).toBe(1);
    expect(missing.stderr).toMatch(/No Fabric mesh root/);
  });
});
