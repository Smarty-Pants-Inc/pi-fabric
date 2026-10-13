import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { readResidentOperatorEvidence, ROOT_PARTICIPANT_FRESH_MS } from "../src/residency/operator-safety.js";
import { actorTreeDeletionHooks, mainPublicationFenced } from "../src/topology/main-publication-fence.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { createHash } from "node:crypto";
import { fixture, type DeadRootFixture } from "./helpers/dead-root-fixture.js";

// pi-fabric#1031 round 1 P1: the live operator removal's final check (ResidentHost beforeOperatorCleanup, run under the
// kernel Main publication fence, the state transaction and the root participant key lock) uses the SAME Main-identity
// verdict as the offline removal (rootParticipantVerdict). Age alone never proves a Main dead; neither does a stale
// state row while a lease FILE is fresh (smarty-dev#7403 c6146802665). Real ResidentHost, real CLI request files, real
// flock fence; only the actor-tree removal itself is recorded and never performed (#4664).
type Call = { at: string; fenced: boolean; path?: string };
const GUIDANCE = "stop the Main, or let a host start finish it";
const STALE = () => Date.now() - 2 * ROOT_PARTICIPANT_FRESH_MS;
const GONE = { pid: 2147483647, host: os.hostname(), startTime: "1" };

afterEach(() => {
  delete actorTreeDeletionHooks.beforeCheck;
  delete actorTreeDeletionHooks.beforeDelete;
  vi.restoreAllMocks();
});

const record = (f: DeadRootFixture, dir: string, calls: Call[], beforeCheck?: () => void) => {
  const fenced = () => mainPublicationFenced(f.config.meshRoot, f.config.rootId);
  const inTree = (file: unknown) => {
    const resolved = path.resolve(String(file));
    if (/\.lock(\.dead\.[0-9a-f]+)?$/.test(path.basename(resolved))) return false; // metadata locks, as in actor-remove-live-fence
    return (resolved === path.resolve(dir) || resolved.startsWith(path.resolve(dir) + path.sep)) &&
      fs.existsSync(resolved) && fs.statSync(resolved).isDirectory();
  };
  const rmSync = fs.rmSync.bind(fs), rm = fs.promises.rm.bind(fs.promises);
  vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
    if (inTree(file)) calls.push({ at: "rm", path: path.resolve(String(file)), fenced: fenced() }); else rmSync(file, options);
  });
  vi.spyOn(fs.promises, "rm").mockImplementation(async (file, options) => {
    if (inTree(file)) calls.push({ at: "rm", path: path.resolve(String(file)), fenced: fenced() }); else await rm(file, options);
  });
  actorTreeDeletionHooks.beforeCheck = async () => { calls.push({ at: "beforeCheck", fenced: fenced() }); beforeCheck?.(); };
  actorTreeDeletionHooks.beforeDelete = async () => { calls.push({ at: "beforeDelete", fenced: fenced() }); };
};

/** Live operator removal through the real CLI request file; the final check's setup runs under the held fence. */
const operatorRemove = async (f: DeadRootFixture, seed: () => void, underFence?: () => void) => {
  const actor = await f.create("history");
  const dir = path.join(f.config.actorRoot, actor.id);
  fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, "session.jsonl"), "history\n");
  seed();
  const calls: Call[] = [];
  record(f, dir, calls, underFence);
  const cli = await f.cli(actor.id, ["--confirm-dead-root", f.config.rootId]);
  await f.host.actors.removalSettled(actor.id)?.catch(() => undefined);
  const removal = cli.out + cli.err;
  expect(fs.readFileSync(path.join(dir, "session.jsonl"), "utf8")).toBe("history\n"); // recorded, never performed
  expect(mainPublicationFenced(f.config.meshRoot, f.config.rootId)).toBe(false);
  return { calls, removal, dir, cli };
};
const refused = (result: Awaited<ReturnType<typeof operatorRemove>>, reason: RegExp) => {
  expect(result.calls.filter(call => call.at === "rm")).toEqual([]);
  expect(result.calls[0]).toEqual({ at: "beforeCheck", fenced: true });
  expect(result.removal).toContain(GUIDANCE);
  expect(result.removal).toMatch(reason);
};
const allowed = (result: Awaited<ReturnType<typeof operatorRemove>>) => {
  expect(result.cli).toMatchObject({ code: 0, err: "" });
  expect(result.calls).toEqual([{ at: "beforeCheck", fenced: true }, { at: "beforeDelete", fenced: true },
    { at: "rm", path: path.resolve(result.dir), fenced: true }]);
};
const hostKey = (id: string) => "topology/hosts/" + createHash("sha256").update(id).digest("hex");
/** A genuinely stale state.db host row (smarty-dev#7403: rows are 15-700 s old by design): seeded under a clock held at
 * `old` only for this one put (state-sqlite #applyPut stamps Date.now), restored immediately. */
const staleHostRow = async (f: DeadRootFixture) => {
  const old = STALE();
  const mesh = new MeshStore(f.config.meshRoot, 64 * 1024, 1_000);
  const clock = vi.spyOn(Date, "now").mockReturnValue(old);
  let row: Awaited<ReturnType<MeshStore["put"]>>;
  try {
    row = await mesh.put({ key: hostKey(f.config.rootId), identity: { id: f.config.rootId, name: "main", kind: "main" },
      value: { id: f.config.rootId, rootId: f.config.rootId, identity: { id: f.config.rootId }, expiresAt: old } });
  } finally { clock.mockRestore(); }
  expect(row.updatedAt).toBe(old);
};
const freshLeaseFile = (f: DeadRootFixture, id: string, expiresAt = Date.now() + 120_000) =>
  writeHostLease(f.config.meshRoot, { id, rootId: id, identityId: id, updatedAt: Date.now(), expiresAt, startedAt: Date.now() - 1_000 });

describe.skipIf(process.platform !== "linux")("live operator removal final check: Main identity (pi-fabric#1031)", () => {
  it("RED 1: a stale root record whose real Main is still running refuses under the fence; the tree is kept", async () => {
    const f = await fixture();
    try {
      const running = { pid: process.pid, host: os.hostname(), startTime: processStartTime(process.pid)! };
      refused(await operatorRemove(f, () => f.mainParticipant(STALE(), f.config.rootId, running)), /Main process \d+ is alive/);
    } finally { await f.close(); }
  }, 60_000);

  it("RED 2: no root participant record is unknown, never proof; refuses", async () => {
    const f = await fixture();
    try { refused(await operatorRemove(f, () => undefined), /no root participant record/); } finally { await f.close(); }
  }, 60_000);

  it("GREEN 3 (counterexample): a stale record whose pid and start identity are provably gone is removed, only under the fence", async () => {
    const f = await fixture();
    try { allowed(await operatorRemove(f, () => f.mainParticipant(STALE(), f.config.rootId, GONE))); } finally { await f.close(); }
  }, 60_000);

  it.each([["a gone pid without its start time", { pid: GONE.pid, host: GONE.host }], ["no pid", { host: GONE.host }]] as const)(
    "missing identity: %s refuses", async (_name, mainProcess) => {
      const f = await fixture();
      try {
        refused(await operatorRemove(f, () => f.mainParticipant(STALE(), f.config.rootId, mainProcess)), /identity unavailable/);
      } finally { await f.close(); }
    }, 60_000);

  // pi-fabric#1031 round 3 (c6148722668): a malformed recorded start time is no identity: unknown, never "reused".
  const BAD = ["not-a-proc-tick", " 1", "1 ", "+1", "-1", "1.0", "1e3", "0x1", "01", "18446744073709551616"];
  it.each(BAD.map(startTime => [startTime] as const))("malformed start time %j on the live current pid refuses", async startTime => {
    const f = await fixture();
    try {
      refused(await operatorRemove(f, () => f.mainParticipant(STALE(), f.config.rootId,
        { pid: process.pid, host: os.hostname(), startTime })), /identity unavailable \(malformed start time/);
    } finally { await f.close(); }
  }, 60_000);

  it("malformed start time on a gone pid refuses (unknown, not gone)", async () => {
    const f = await fixture();
    try {
      refused(await operatorRemove(f, () => f.mainParticipant(STALE(), f.config.rootId, { ...GONE, startTime: "not-a-proc-tick" })),
        /identity unavailable \(malformed start time/);
    } finally { await f.close(); }
  }, 60_000);

  it("control: the live current pid with a valid but different start tick is a reused pid; removed under the fence", async () => {
    const f = await fixture();
    try {
      const other = String(BigInt(processStartTime(process.pid)!) + 1n);
      allowed(await operatorRemove(f, () => f.mainParticipant(STALE(), f.config.rootId,
        { pid: process.pid, host: os.hostname(), startTime: other })));
    } finally { await f.close(); }
  }, 60_000);

  it("offline --main-stopped evidence (shared verdict): malformed is unknown, a valid different tick is stale/reused", async () => {
    const f = await fixture();
    try {
      const mesh = new MeshStore(f.config.meshRoot, 64 * 1024, 1_000);
      const verdict = (startTime: string) => {
        f.mainParticipant(STALE(), f.config.rootId, { pid: process.pid, host: os.hostname(), startTime });
        return readResidentOperatorEvidence(f.config, mesh, undefined, { mainStopped: true });
      };
      const bad = verdict("not-a-proc-tick");
      expect(bad.mainLiveness).toMatch(/^unknown: .*malformed start time/);
      expect(bad.liveLease).toBe(true);
      const reused = verdict(String(BigInt(processStartTime(process.pid)!) + 1n));
      expect(reused.mainLiveness).toMatch(/^stale:/);
      expect(reused.toolEvidence?.participants.every(p => /^gone: .* was reused/.test(p.identity))).toBe(true);
      expect(reused.liveLease).toBe(false);
    } finally { await f.close(); }
  }, 60_000);

  it("a fresh record refuses even when its pid is gone", async () => {
    const f = await fixture();
    try { refused(await operatorRemove(f, () => f.mainParticipant(Date.now(), f.config.rootId, GONE)), /fresh/); } finally { await f.close(); }
  }, 60_000);

  it("stale owner: a stale gone record whose other owner host has a fresh lease FILE refuses", async () => {
    const f = await fixture();
    try {
      refused(await operatorRemove(f, () => { f.mainParticipant(STALE(), "host:other", GONE); freshLeaseFile(f, "host:other"); }),
        /owner lease is live/);
    } finally { await f.close(); }
  }, 60_000);

  // smarty-dev#7403 c6146802665: state.db host rows are old by design while lease FILES are fresh.
  it("regression guard (already refused at baseline, not a new RED): a stale state host row plus a fresh root lease FILE written under the fence refuses, even with the pid gone", async () => {
    const f = await fixture();
    try {
      await staleHostRow(f);
      // Acceptance sees no live lease; the lease file turns fresh only after it, under the held fence.
      refused(await operatorRemove(f, () => f.mainParticipant(STALE(), f.config.rootId, GONE),
        () => freshLeaseFile(f, f.config.rootId)), /live root lease/);
    } finally { await f.close(); }
  }, 60_000);

  it("GREEN (counterexample): the stale state host row with an expired root lease FILE and the pid gone is removed", async () => {
    const f = await fixture();
    try {
      await staleHostRow(f);
      allowed(await operatorRemove(f, () => { f.mainParticipant(STALE(), f.config.rootId, GONE);
        freshLeaseFile(f, f.config.rootId, STALE()); }));
    } finally { await f.close(); }
  }, 60_000);
});
