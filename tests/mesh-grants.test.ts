import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import {
  createMeshGrant,
  listMeshGrants,
  meshCliArgv,
  meshPostCommand,
  MESH_GRANT_TOKEN_ENV,
  postWithMeshGrant,
  revokeMeshGrant,
  wrapDurableNotifyScript,
} from "../src/mesh/grants.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { MeshProvider } from "../src/providers/mesh-provider.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

const roots: string[] = [];
const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
const context = {} as FabricInvocationContext;
const participants: FabricParticipantSource = {
  list: () => [], get: () => undefined, self: () => undefined as never, peers: () => [],
  async refresh() {}, scheduleRefresh() {},
};
const cliEntry = fileURLToPath(new URL("../src/cli/index.ts", import.meta.url));
const bun = process.versions.bun ? process.execPath : "bun";

const tempRoot = (): string => {
  const root = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-mesh-grant-")));
  roots.push(root);
  return root;
};
const store = (): MeshStore => new MeshStore(path.join(tempRoot(), "mesh"), 64 * 1024, 100);

const cli = (args: string[], options: { env?: Record<string, string>; input?: string } = {}) => {
  const env = { ...process.env, ...options.env };
  delete env[MESH_GRANT_TOKEN_ENV];
  Object.assign(env, options.env);
  return spawnSync(bun, [cliEntry, ...args], { encoding: "utf8", env, input: options.input ?? "", timeout: 30_000 });
};

afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("scoped external mesh grants", () => {
  it("stores only a hash in a private file and never lists tokens", async () => {
    const mesh = store();
    const { grant, token } = await createMeshGrant(mesh, { topic: "ci.hooks", ttlMs: 60_000, uses: 3, createdBy: identity });
    expect(token).toMatch(/^[A-Za-z0-9_-]{43}$/);
    const file = path.join(mesh.root, "grants.json");
    const stored = fs.readFileSync(file, "utf8");
    expect(stored).not.toContain(token);
    expect(stored).toMatch(/"hash": "[0-9a-f]{64}"/);
    if (process.platform !== "win32") expect(fs.statSync(file).mode & 0o777).toBe(0o600);
    const listed = listMeshGrants(mesh);
    expect(listed).toEqual([grant]);
    expect(JSON.stringify(listed)).not.toContain("hash");
  });

  it("posts untrusted events, decrements uses and refuses exhausted, expired, revoked or out-of-scope tokens", async () => {
    const mesh = store();
    const now = Date.now();
    const { grant, token } = await createMeshGrant(mesh, { topic: "ci.hooks", kind: "build", ttlMs: 60_000, uses: 2, createdBy: identity }, now);
    await expect(postWithMeshGrant(mesh, { token, topic: "other" }, now)).rejects.toThrow("does not cover topic");
    await expect(postWithMeshGrant(mesh, { token, kind: "deploy" }, now)).rejects.toThrow("does not cover kind");
    await expect(postWithMeshGrant(mesh, { token: "x".repeat(43) }, now)).rejects.toThrow("not valid");
    await expect(postWithMeshGrant(mesh, { token, data: "x".repeat(64 * 1024) }, now)).rejects.toThrow("exceeds 65536 bytes");
    const posted = await postWithMeshGrant(mesh, { token, data: { ok: true } }, now);
    expect(posted.event).toMatchObject({
      topic: "ci.hooks", kind: "build", data: { ok: true }, origin: "external", untrusted: true, grantId: grant.grantId,
    });
    expect(posted.grant.uses).toBe(1);
    await postWithMeshGrant(mesh, { token, topic: "ci.hooks", kind: "build" }, now);
    await expect(postWithMeshGrant(mesh, { token }, now)).rejects.toThrow("no uses left");
    await expect(postWithMeshGrant(mesh, { token }, now + 60_001)).rejects.toThrow("expired");
    expect(mesh.read({ topic: "ci.hooks" })).toHaveLength(2);

    const second = await createMeshGrant(mesh, { topic: "ci.hooks", ttlMs: 60_000, createdBy: identity });
    await expect(revokeMeshGrant(mesh, second.grant.grantId)).resolves.toEqual({ revoked: true });
    await expect(revokeMeshGrant(mesh, second.grant.grantId)).resolves.toEqual({ revoked: false });
    await expect(postWithMeshGrant(mesh, { token: second.token })).rejects.toThrow("not valid");
  });

  it("validates grant bounds and fails closed on damaged grant state", async () => {
    const mesh = store();
    await expect(createMeshGrant(mesh, { topic: "t", ttlMs: 59_999, createdBy: identity })).rejects.toThrow("ttlMs");
    await expect(createMeshGrant(mesh, { topic: "t", ttlMs: 31 * 86_400_000, createdBy: identity })).rejects.toThrow("ttlMs");
    await expect(createMeshGrant(mesh, { topic: "t", ttlMs: 60_000, uses: 10_001, createdBy: identity })).rejects.toThrow("uses");
    await expect(createMeshGrant(mesh, { topic: "bad topic", ttlMs: 60_000, createdBy: identity })).rejects.toThrow("Invalid Fabric mesh topic");
    const { token } = await createMeshGrant(mesh, { topic: "t", ttlMs: 60_000, createdBy: identity });
    fs.writeFileSync(path.join(mesh.root, "grants.json"), "{oops");
    await expect(postWithMeshGrant(mesh, { token })).rejects.toThrow("Failed to read Fabric mesh grants");
    expect(() => listMeshGrants(mesh)).toThrow("Failed to read Fabric mesh grants");
  });

  it("mesh.grant returns a ready-to-run command and refuses reserved topics", async () => {
    const mesh = store();
    const provider = new MeshProvider(mesh, identity, participants);
    await expect(provider.invoke("grant", { topic: "fabric.control.x", ttlMs: 60_000 }, context)).rejects.toThrow("reserved");
    const granted = await provider.invoke("grant", { topic: "inbox", ttlMs: 60_000, kind: "note" }, context) as {
      grantId: string; token: string; expiresAt: number; uses: number; command: string;
    };
    expect(granted).toMatchObject({ uses: 1, expiresAt: expect.any(Number) });
    const windows = process.platform === "win32";
    expect(granted.command).toContain(`${windows ? "$env:" : ""}${MESH_GRANT_TOKEN_ENV}='${granted.token}'`);
    expect(granted.command).toContain("mesh post --root");
    expect((await provider.describe("grant", context))?.risk).toBe("network");
    expect((await provider.describe("revoke", context))?.risk).toBe("write");
    await expect(provider.invoke("grants", {}, context)).resolves.toEqual([expect.not.objectContaining({ token: expect.anything() })]);
    // Run the returned line through a shell, as an outside process would.
    const shell = windows
      ? spawnSync("powershell.exe", ["-NoProfile", "-NonInteractive", "-Command", granted.command], { encoding: "utf8", timeout: 30_000 })
      : spawnSync("/bin/sh", ["-c", granted.command], { encoding: "utf8", timeout: 30_000 });
    expect(shell.status, shell.stderr).toBe(0);
    expect(mesh.read({ topic: "inbox" })).toEqual([expect.objectContaining({ kind: "note", untrusted: true, data: {} })]);
    await expect(provider.invoke("revoke", { grantId: granted.grantId }, context)).resolves.toEqual({ revoked: true });
  });
});

describe("mesh post command line", () => {
  it("quotes for a POSIX shell and for PowerShell on Windows", () => {
    const input = { root: "C:\\mesh root", token: "t'k", kind: "note" };
    expect(meshPostCommand(["node", "cli/index.js"], input, "linux")).toBe(
      `${MESH_GRANT_TOKEN_ENV}='t'\\''k' 'node' 'cli/index.js' mesh post --root 'C:\\mesh root' --kind 'note' --data '{}'`,
    );
    expect(meshPostCommand(["node.exe", "cli\\index.js"], input, "win32")).toBe(
      `$env:${MESH_GRANT_TOKEN_ENV}='t''k'; & 'node.exe' 'cli\\index.js' mesh post --root 'C:\\mesh root' --kind 'note' --data '{}'`,
    );
  });
});

describe("pi-fabric mesh post CLI", () => {
  it("posts with the token from the environment and reports usage errors", async () => {
    const mesh = store();
    const { token, grant } = await createMeshGrant(mesh, { topic: "hooks", ttlMs: 60_000, uses: 3, createdBy: identity });
    const posted = cli(["mesh", "post", "--root", mesh.root, "--kind", "push", "--data", '{"ref":"main"}'], {
      env: { [MESH_GRANT_TOKEN_ENV]: token },
    });
    expect(posted.status, posted.stderr).toBe(0);
    expect(JSON.parse(posted.stdout)).toMatchObject({ ok: true, topic: "hooks", kind: "push", grantId: grant.grantId, usesLeft: 2 });

    const stdin = cli(["mesh", "post", "--root", mesh.root, "--token", token, "--data-file", "-"], { input: '{"from":"stdin"}' });
    expect(stdin.status, stdin.stderr).toBe(0);
    expect(mesh.read({ topic: "hooks" }).map((event) => event.data)).toEqual([{ ref: "main" }, { from: "stdin" }]);

    const wrongTopic = cli(["mesh", "post", "--root", mesh.root, "--topic", "elsewhere"], { env: { [MESH_GRANT_TOKEN_ENV]: token } });
    expect(wrongTopic.status).toBe(1);
    expect(wrongTopic.stderr).toContain("does not cover topic");
    const badJson = cli(["mesh", "post", "--root", mesh.root, "--data", "{nope"], { env: { [MESH_GRANT_TOKEN_ENV]: token } });
    expect(badJson.status).toBe(2);
    const missingToken = cli(["mesh", "post", "--root", mesh.root]);
    expect(missingToken.status).toBe(2);
    expect(missingToken.stderr).toContain(MESH_GRANT_TOKEN_ENV);
    const missingRoot = cli(["mesh", "post", "--root", path.join(mesh.root, "absent")], { env: { [MESH_GRANT_TOKEN_ENV]: token } });
    expect(missingRoot.status).toBe(1);
    expect(fs.existsSync(path.join(mesh.root, "absent"))).toBe(false);
    const tooLarge = cli(["mesh", "post", "--root", mesh.root, "--data-file", "-"], {
      env: { [MESH_GRANT_TOKEN_ENV]: token }, input: JSON.stringify("x".repeat(70 * 1024)),
    });
    expect(tooLarge.status).toBe(1);
    expect(tooLarge.stderr).toContain("exceeds 65536 bytes");
    expect(mesh.read({ topic: "hooks" })).toHaveLength(2);
    expect(cli(["unknown"]).status).toBe(2);
  });
});

describe.skipIf(process.platform === "win32")("durable notify script wrapper", () => {
  const run = (script: string): ReturnType<typeof spawnSync> => {
    const file = path.join(tempRoot(), "task.sh");
    fs.writeFileSync(file, script, { mode: 0o600 });
    return spawnSync("/bin/bash", [file], { encoding: "utf8", timeout: 30_000 });
  };

  it("preserves the task exit code and publishes it through the CLI", async () => {
    const mesh = store();
    const { token } = await createMeshGrant(mesh, { topic: "tasks", kind: "task.completed", ttlMs: 60_000, createdBy: identity });
    const script = wrapDurableNotifyScript("echo working; exit 7", {
      argv: meshCliArgv(), root: mesh.root, token, kind: "task.completed", taskId: "task-1", description: "It's a \"test\"",
    });
    expect(script).not.toMatch(/--token/);
    const result = run(script);
    expect(result.status).toBe(7);
    expect(result.stdout).toBe("working\n");
    expect(mesh.read({ topic: "tasks" })).toEqual([expect.objectContaining({
      kind: "task.completed", untrusted: true,
      data: { taskId: "task-1", description: "It's a \"test\"", exitCode: 7 },
    })]);
  });

  it("a failing notify never changes the task result", () => {
    const failing = path.join(tempRoot(), "fail.sh");
    fs.writeFileSync(failing, "#!/bin/sh\necho noisy >&2\nexit 9\n", { mode: 0o755 });
    const script = wrapDurableNotifyScript("true", {
      argv: [failing], root: "/nonexistent", token: "t", kind: "k", taskId: "x",
    });
    const success = run(script);
    expect(success.status).toBe(0);
    expect(success.stderr).toBe("");
    const failed = run(wrapDurableNotifyScript("false", { argv: [failing], root: "/x", token: "t", kind: "k", taskId: "x" }));
    expect(failed.status).toBe(1);
  });
});
