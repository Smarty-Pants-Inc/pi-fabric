import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { recordedProjectLead, resolveProjectAgent } from "../src/topology/project-identity.js";

// Keep reason: launch metadata is a cross-repository routing trust boundary (#16723).
// Real filesystem permissions, git indexes and linked checkouts; no fs/git mocks.
describe("#16723 project lead marker trust", () => {
  const roots: string[] = [];
  const leadId = "session:11111111-1111-4111-8111-111111111111";
  const otherId = "session:22222222-2222-4222-8222-222222222222";
  const git = (cwd: string, ...args: string[]) => execFileSync("git", args, {
    cwd, stdio: "ignore", env: { ...process.env, GIT_CONFIG_GLOBAL: "/dev/null", GIT_CONFIG_NOSYSTEM: "1" },
  });
  afterEach(() => {
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  });
  const fixture = () => {
    const base = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-marker-trust-")));
    roots.push(base);
    const lane = path.join(base, "lane");
    fs.mkdirSync(lane);
    git(lane, "init", "-q");
    return { base, lane };
  };
  const marker = (cwd: string, id = leadId) => {
    const local = path.join(cwd, ".local");
    fs.mkdirSync(local, { mode: 0o700 });
    const file = path.join(local, "lead");
    fs.writeFileSync(file, `${id}\n`, { mode: 0o600 });
    return file;
  };

  it("refuses an indexed cross-repository marker, even with launcher-like permissions", () => {
    const { lane } = fixture();
    marker(lane);
    git(lane, "add", "--", ".local/lead"); // In the index is enough; no commit required.
    const lead = { id: leadId, startedAt: 1, role: "project-agent", repository: "code.example/team/control" };
    const recorded = recordedProjectLead(lane, {});
    const options = { repository: "code.example/team/lane", ...(recorded ? { leadId: recorded } : {}) };
    expect(recorded).toBeUndefined();
    expect(() => resolveProjectAgent([lead], lane, options)).toThrow(expect.objectContaining({
      code: "FABRIC_PROJECT_AGENT_UNRESOLVED",
    }));
  });

  it.skipIf(process.platform === "win32")("refuses a .local directory with mode 0755", () => {
    const { lane } = fixture();
    marker(lane);
    fs.chmodSync(path.join(lane, ".local"), 0o755);
    expect(recordedProjectLead(lane, {})).toBeUndefined();
  });

  it.skipIf(process.platform === "win32").each([0o620, 0o602])("refuses a writable marker (mode %i)", mode => {
    const { lane } = fixture();
    const file = marker(lane);
    fs.chmodSync(file, mode);
    expect(recordedProjectLead(lane, {})).toBeUndefined();
  });

  it.skipIf(process.getuid === undefined).each([true, false])("ignores nested markers (root marker present: %s)", rootMarker => {
    const { lane } = fixture();
    if (rootMarker) marker(lane);
    const sub = path.join(lane, "sub");
    fs.mkdirSync(sub);
    marker(sub, otherId);
    expect(recordedProjectLead(sub, {})).toBe(rootMarker ? leadId : undefined);
  });

  it("does not inherit a parent marker across a checkout boundary", () => {
    const { base, lane } = fixture();
    marker(base);
    const sub = path.join(lane, "sub");
    fs.mkdirSync(sub);
    expect(recordedProjectLead(sub, {})).toBeUndefined();
  });

  it("does not inherit a parent marker outside any checkout", () => {
    const { base } = fixture();
    marker(base);
    const plain = path.join(base, "plain");
    fs.mkdirSync(plain);
    expect(recordedProjectLead(plain, {})).toBeUndefined();
  });

  it("refuses a marker when git cannot read the checkout index", () => {
    const { lane } = fixture();
    marker(lane);
    fs.writeFileSync(path.join(lane, ".git", "index"), "invalid index\n");
    expect(recordedProjectLead(lane, {})).toBeUndefined();
  });

  it.skipIf(process.getuid === undefined)("reads only the linked checkout's root, not the common project root or nested cwd", () => {
    const { base, lane } = fixture();
    git(lane, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-q", "--allow-empty", "-m", "init");
    marker(lane, otherId);
    const linked = path.join(base, "linked");
    git(lane, "worktree", "add", "-q", "--detach", linked);
    marker(linked);
    const sub = path.join(linked, "sub");
    fs.mkdirSync(sub);
    marker(sub, otherId);
    expect(recordedProjectLead(sub, {})).toBe(leadId);
  });

  it.skipIf(process.getuid !== undefined)("refuses file authority when POSIX ownership cannot be checked", () => {
    const { lane } = fixture();
    marker(lane);
    expect(recordedProjectLead(lane, {})).toBeUndefined();
    expect(recordedProjectLead(lane, { SMARTY_LEAD_SESSION: leadId })).toBe(leadId);
  });

  it("still prefers strict explicit metadata over an untrusted marker", () => {
    const { lane } = fixture();
    marker(lane, otherId);
    git(lane, "add", "--", ".local/lead");
    expect(recordedProjectLead(lane, { SMARTY_LEAD_SESSION: leadId })).toBe(leadId);
    expect(() => recordedProjectLead(lane, { SMARTY_LEAD_SESSION: leadId.slice("session:".length) }))
      .toThrow(expect.objectContaining({ code: "FABRIC_PROJECT_LEAD_INVALID" }));
  });
});
