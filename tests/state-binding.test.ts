import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it } from "vitest";
import { validationMessage } from "../src/core/action-arguments.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { StateProvider } from "../src/providers/state-provider.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import {
  normalizeStateBinding,
  observeGitWorkspace,
} from "../src/state/binding.js";
import { CURRENT_KEY, StateStore, STATE_TOPIC } from "../src/state/store.js";

const roots: string[] = [];
const identity: MeshIdentity = {
  id: "session:test",
  name: "main",
  kind: "main",
  sessionId: "test",
};

const tempDir = (prefix: string): string => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), prefix)));
  roots.push(root);
  return root;
};

const createMesh = (): MeshStore => new MeshStore(tempDir("pi-fabric-bound-mesh-"), 256 * 1024, 100);

const git = (cwd: string, args: string[]): string =>
  execFileSync(
    "git",
    ["-c", "user.name=fabric", "-c", "user.email=fabric@example.invalid", "-c", "commit.gpgsign=false", ...args],
    { cwd, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] },
  ).trim();

const createRepo = (): { cwd: string; head: string } => {
  const cwd = tempDir("pi-fabric-bound-repo-");
  git(cwd, ["init", "--quiet"]);
  fs.writeFileSync(path.join(cwd, "README.md"), "bound\n");
  git(cwd, ["add", "README.md"]);
  git(cwd, ["commit", "--quiet", "-m", "init"]);
  return { cwd, head: git(cwd, ["rev-parse", "HEAD"]) };
};

const contextFor = (cwd: string): FabricInvocationContext => ({
  cwd,
  signal: undefined,
  parentToolCallId: "test",
  nestedToolCallId: "nested",
  extensionContext: {} as ExtensionContext,
  update() {},
});

const transitionWithEvidence = async (store: StateStore, evidence: string[]) =>
  store.transition({ label: "apply", to: "applied", summary: "applied", evidence }, identity);

afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("state binding validation", () => {
  it("accepts bounded bindings and sorts keys", () => {
    expect(normalizeStateBinding(undefined)).toBeUndefined();
    expect(normalizeStateBinding({})).toBeUndefined();
    expect(
      normalizeStateBinding({ specDigest: "sha256:abc", commit: "deadbeef", "node.address-1_x": "" }),
    ).toEqual({ commit: "deadbeef", "node.address-1_x": "", specDigest: "sha256:abc" });
    expect(Object.keys(normalizeStateBinding({ b: "1", a: "2" })!)).toEqual(["a", "b"]);
  });

  it("fails closed on malformed bindings", () => {
    expect(() => normalizeStateBinding([])).toThrow(/must be an object/);
    expect(() => normalizeStateBinding("commit")).toThrow(/must be an object/);
    expect(() => normalizeStateBinding({ Commit: "x" })).toThrow(/must match/);
    expect(() => normalizeStateBinding({ "1commit": "x" })).toThrow(/must match/);
    expect(() => normalizeStateBinding({ [`a${"b".repeat(64)}`]: "x" })).toThrow(/must match/);
    expect(() => normalizeStateBinding({ commit: 1 })).toThrow(/binding\.commit must be a string/);
    expect(() => normalizeStateBinding({ commit: "x".repeat(513) })).toThrow(/exceeds 512/);
    const tooMany = Object.fromEntries(Array.from({ length: 17 }, (_, index) => [`k${index}`, "v"]));
    expect(() => normalizeStateBinding(tooMany)).toThrow(/at most 16/);
  });

  it("declares bounded binding input on state.verify", async () => {
    const provider = new StateProvider(createMesh(), identity);
    const schema = (await provider.describe("verify", contextFor(process.cwd())))!
      .inputSchema as Record<string, unknown>;
    expect(validationMessage(schema, { binding: { commit: "abc" } })).toBeUndefined();
    expect(validationMessage(schema, { binding: { commit: 1 } })).toBeDefined();
    expect(validationMessage(schema, { binding: { commit: "x".repeat(513) } })).toBeDefined();
    const tooMany = Object.fromEntries(Array.from({ length: 17 }, (_, index) => [`k${index}`, "v"]));
    expect(validationMessage(schema, { binding: tooMany })).toBeDefined();
  });
});

describe("git observation", () => {
  it("observes HEAD and cleanliness, and omits outside a work tree", async () => {
    const { cwd, head } = createRepo();
    expect(await observeGitWorkspace(cwd)).toEqual({ commit: head, dirty: false });
    fs.writeFileSync(path.join(cwd, "untracked.txt"), "x");
    expect(await observeGitWorkspace(cwd)).toEqual({ commit: head, dirty: true });
    expect(await observeGitWorkspace(tempDir("pi-fabric-bound-plain-"))).toBeUndefined();
  });

  it("omits observation in a repository without a HEAD commit", async () => {
    const cwd = tempDir("pi-fabric-bound-unborn-");
    git(cwd, ["init", "--quiet"]);
    expect(await observeGitWorkspace(cwd)).toBeUndefined();
  });
});

describe("bound state certificates", () => {
  it("records observed commit, issuer and requester on certified events and the durable head", async () => {
    const { cwd, head } = createRepo();
    const mesh = createMesh();
    const store = new StateStore(mesh);
    await transitionWithEvidence(store, ["echo ok"]);

    const report = await store.verify({
      cwd,
      identity,
      binding: { specDigest: "sha256:spec", nodeAddress: "node-a" },
      schemaMode: "audit",
    });

    expect(report.certified).toBe(true);
    expect(report.observed).toEqual({ commit: head, dirty: false });
    expect(report.requestedBy).toBe("host");
    const expected = {
      binding: { nodeAddress: "node-a", specDigest: "sha256:spec" },
      observed: { commit: head, dirty: false },
      issuer: "host",
      requestedBy: "host",
      schemaMode: "audit",
    };
    expect(report.certificate).toMatchObject({ ...expected, current: true });
    const certified = mesh.read({ topic: STATE_TOPIC }).find((event) => event.kind === "state.certified");
    expect(certified?.data).toMatchObject(expected);
    expect((mesh.get(CURRENT_KEY)?.value as { certificate?: unknown }).certificate).toMatchObject(expected);

    const state = store.get();
    expect(state.certification.current).toMatchObject(expected);
    expect(state.head?.certificate).toMatchObject(expected);
    expect(store.history({}).certifications[0]).toMatchObject(expected);
  });

  it("accepts a matching binding.commit and records dirty worktrees", async () => {
    const { cwd, head } = createRepo();
    fs.writeFileSync(path.join(cwd, "README.md"), "changed\n");
    const store = new StateStore(createMesh());
    await transitionWithEvidence(store, ["echo ok"]);

    const report = await store.verify({ cwd, identity, binding: { commit: head.toUpperCase() } });

    expect(report.certified).toBe(true);
    expect(report.certificate).toMatchObject({
      binding: { commit: head.toUpperCase() },
      observed: { commit: head, dirty: true },
    });
  });

  it("fails closed on a binding.commit mismatch without running evidence", async () => {
    const { cwd, head } = createRepo();
    const mesh = createMesh();
    const store = new StateStore(mesh);
    const marker = path.join(tempDir("pi-fabric-bound-marker-"), "ran");
    await transitionWithEvidence(store, [`"${process.execPath}" -e "require('fs').writeFileSync(process.argv[1], 'x')" "${marker}"`]);
    const certified = await store.verify({ cwd, identity });
    expect(certified.certified).toBe(true);
    fs.rmSync(marker);

    const wrong = "0".repeat(40);
    const report = await store.verify({ cwd, identity, binding: { commit: wrong } });

    expect(report.certified).toBe(false);
    expect(report.results).toEqual([]);
    expect(fs.existsSync(marker)).toBe(false);
    expect(report.failures).toEqual([
      expect.objectContaining({
        reason: "binding-mismatch",
        message: `binding.commit ${wrong} does not match observed HEAD ${head}`,
      }),
    ]);
    const violated = mesh.read({ topic: STATE_TOPIC }).filter((event) => event.kind === "state.violated").at(-1);
    expect(violated?.data).toMatchObject({
      binding: { commit: wrong },
      observed: { commit: head },
      issuer: "host",
      requestedBy: "host",
      reasons: [expect.objectContaining({ reason: "binding-mismatch" })],
    });
    // The failed bound verification revokes the previously current certificate.
    expect(store.get().certification.current).toBeNull();
  });

  it("fails closed when binding.commit cannot be observed", async () => {
    const store = new StateStore(createMesh());
    await transitionWithEvidence(store, ["echo ok"]);
    const report = await store.verify({
      cwd: tempDir("pi-fabric-bound-plain-"),
      identity,
      binding: { commit: "0".repeat(40) },
    });
    expect(report.certified).toBe(false);
    expect(report.observed).toBeUndefined();
    expect(report.failures).toEqual([expect.objectContaining({ reason: "binding-unobserved" })]);
  });

  it("certifies without observation outside git and rejects invalid bindings before running", async () => {
    const mesh = createMesh();
    const store = new StateStore(mesh);
    await transitionWithEvidence(store, ["echo ok"]);
    const cwd = tempDir("pi-fabric-bound-plain-");
    const report = await store.verify({ cwd, identity });
    expect(report.certified).toBe(true);
    expect(report.certificate?.observed).toBeUndefined();
    expect(report.certificate?.binding).toBeUndefined();
    expect(report.certificate?.issuer).toBe("host");

    const before = mesh.read({ topic: STATE_TOPIC }).length;
    await expect(store.verify({ cwd, identity, binding: { Bad: "x" } })).rejects.toThrow(/must match/);
    expect(mesh.read({ topic: STATE_TOPIC })).toHaveLength(before);
  });

  it("folds legacy certificates that carry no binding fields", async () => {
    const mesh = createMesh();
    const store = new StateStore(mesh);
    const { head } = await transitionWithEvidence(store, ["echo ok"]);
    await mesh.publish({
      topic: STATE_TOPIC,
      kind: "state.certified",
      from: identity,
      text: "state certified",
      data: {
        certificationStatus: "certified",
        targets: [{ transitionId: head.transitionId, label: head.label, to: head.to }],
        head: { transitionId: head.transitionId, label: head.label, to: head.to, version: head.version },
        evidenceDigest: "sha256:e",
        resultDigest: "sha256:r",
        binding: { Invalid: 1 },
        observed: { commit: "not-a-sha" },
        requestedBy: "someone",
        issuer: "guest",
        ts: 1,
      },
    });
    const current = store.get().certification.current;
    expect(current).toMatchObject({ current: true, evidenceDigest: "sha256:e" });
    for (const key of ["binding", "observed", "issuer", "requestedBy", "schemaMode"]) {
      expect(current).not.toHaveProperty(key);
    }
  });

  it("marks provider-originated verification as requested by the program with the schema mode", async () => {
    const { cwd, head } = createRepo();
    const mesh = createMesh();
    const provider = new StateProvider(mesh, identity, { schemaMode: "off" });
    const context = contextFor(cwd);
    await provider.invoke(
      "transition",
      { label: "apply", to: "applied", summary: "applied", evidence: ["echo ok"] },
      context,
    );
    const report = (await provider.invoke(
      "verify",
      { binding: { commit: head, criteriaDigest: "sha256:c" } },
      context,
    )) as Awaited<ReturnType<StateStore["verify"]>>;
    expect(report.certified).toBe(true);
    expect(report.certificate).toMatchObject({
      binding: { commit: head, criteriaDigest: "sha256:c" },
      observed: { commit: head },
      issuer: "host",
      requestedBy: "program",
      schemaMode: "off",
    });
    const state = (await provider.invoke("get", {}, context)) as {
      certification: { current: unknown };
    };
    expect(state.certification.current).toMatchObject({ requestedBy: "program", schemaMode: "off" });
    await expect(
      provider.invoke("verify", { binding: { commit: 7 } }, context),
    ).rejects.toThrow(/binding\.commit must be a string/);
  });
});
