import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import * as processIdentity from "../src/residency/process-identity.js";
import { fakeManagerHeapWorker } from "./fixtures/manager-heap-probe.js";

// Keep: recorded #722 stop/finishing catch, worker/manager trust boundary and
// data safety. Real manager stop, monitor and terminal-publication paths;
// recorded native/tree receipts only. No child, signal or deletion is executed.
vi.mock("node:child_process", async importOriginal => ({
  ...await importOriginal<typeof import("node:child_process")>(),
  spawn: vi.fn(() => { throw new Error("Unexpected real launch in actor-stop recorded probe"); }),
}));

const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};
const managers: AgentManager[] = [];
const releases: Array<() => void> = [];
const deletions: string[] = [];
const eventually = (check: () => void) => vi.waitFor(check, { timeout: 3_000, interval: 10 });

beforeEach(() => {
  deletions.length = 0;
  const record = (file: fs.PathLike) => { deletions.push(String(file)); };
  vi.spyOn(fs, "rmSync").mockImplementation(record);
  vi.spyOn(fs, "unlinkSync").mockImplementation(record);
  vi.spyOn(fs, "rmdirSync").mockImplementation(record);
  vi.spyOn(fs.promises, "rm").mockImplementation(async file => { record(file); });
  vi.spyOn(fs.promises, "unlink").mockImplementation(async file => { record(file); });
  vi.spyOn(fs.promises, "rmdir").mockImplementation(async file => { record(file); });
  vi.spyOn(process, "kill").mockImplementation((_pid, signal) => {
    if (signal !== 0) throw new Error("No signals authorized by recorded probe");
    return true;
  });
});

afterEach(async () => {
  for (const release of releases.splice(0)) release();
  try { await Promise.all(managers.splice(0).map(manager => manager.close())); }
  finally { vi.restoreAllMocks(); }
  // Fixture bytes remain in the test environment owner's private TMPDIR.
});

function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-stop-finishing-recorded-"));
  const onSettled = vi.fn();
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents,
    budgetUsd: 0, retainRuns: true, sessionExport: false, maxConcurrent: 1,
  }, { runRoot: path.join(root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    fullCodeMode: false, onSettled });
  managers.push(manager);
  return { root, manager, onSettled };
}

async function actorFixture(proof: "ESRCH" | "different-birth") {
  const pid = 2147483646;
  let gone = false, proofScans = 0;
  const closed = deferred(), tree = deferred(), stopEntered = deferred();
  const finishTree = () => { closed.resolve(); tree.resolve(); };
  releases.push(finishTree);
  vi.mocked(process.kill).mockImplementation((candidate, signal) => {
    if (signal !== 0) throw new Error("No signals authorized by recorded probe");
    if (candidate === pid && gone) {
      proofScans++;
      if (proof === "ESRCH") throw Object.assign(new Error("recorded primary exit"), { code: "ESRCH" });
    }
    return true;
  });
  const birth = processIdentity.processStartTime;
  vi.spyOn(processIdentity, "processStartTime").mockImplementation(candidate =>
    candidate === pid ? (gone && proof === "different-birth" ? "67890" : "12345") : birth(candidate));
  const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
    const handle = await fakeManagerHeapWorker(request, 64);
    const statusFile = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
    writeJsonAtomic(statusFile, { ...JSON.parse(fs.readFileSync(statusFile, "utf8")),
      status: "running", finishedAt: undefined, sessionId: String(pid), actorId: "recorded-actor" });
    return { ...handle, sessionId: String(pid), closed: closed.promise, treeClosed: tree.promise,
      // Primary liveness may end before the captured tree join completes.
      waitForClose: () => tree.promise, isAlive: async () => !gone,
      stop: async () => {
        gone = true;
        closed.resolve(); // Wakes production host PID/birth scan during drain.
        stopEntered.resolve();
        await tree.promise; // Primary/native close is NOT owned-tree exit.
      } };
  });
  const f = fixture();
  const handle = await f.manager.spawn({ task: "recorded actor activation", actorId: "recorded-actor",
    transport: "process", extensions: false });
  const directory = f.manager.runDirectory(handle.id)!;
  return { ...f, handle, directory, launch, finishTree, stopEntered,
    proofScans: () => proofScans,
    losePrimary: () => { gone = true; closed.resolve(); } };
}

describe("actor stop and finishing result custody (recorded #722)", () => {
  it.each(["ESRCH", "different-birth"] as const)("publishes stopped, not actor loss, when stop close triggers %s proof during tree drain", async proof => {
    const f = await actorFixture(proof);
    let stopReturned = false, waitReturned = false;
    const waiting = f.manager.wait(f.handle.id, { timeoutMs: 3_000 }).then(result => { waitReturned = true; return result; });
    const stopping = f.manager.stop(f.handle.id).then(result => { stopReturned = true; return result; });
    void waiting.catch(() => undefined); void stopping.catch(() => undefined);
    await f.stopEntered.promise;
    await eventually(() => expect(f.proofScans()).toBeGreaterThan(0));
    expect({ stopReturned, waitReturned }).toEqual({ stopReturned: false, waitReturned: false });
    expect(f.manager.isSettled(f.handle.id)).toBe(false);
    expect(f.manager.status(f.handle.id).status).toBe("running");
    expect(fs.existsSync(path.join(f.directory, "manager-result.json"))).toBe(false);
    expect(f.onSettled).not.toHaveBeenCalled();
    f.finishTree();
    const [stopped, waited] = await Promise.all([stopping, waiting]);
    for (const result of [stopped, waited, f.manager.status(f.handle.id)]) {
      expect(result).toMatchObject({ status: "stopped", error: "Agent stopped" });
      expect(result).not.toMatchObject({ error: expect.stringMatching(/Actor worker is proven gone/) });
    }
    expect(f.manager.isSettled(f.handle.id)).toBe(true);
    expect(f.onSettled).toHaveBeenCalledOnce();
    expect(f.launch).toHaveBeenCalledOnce();
  }, 5_000);

  it.each(["ESRCH", "different-birth"] as const)("preserves genuine %s primary loss observed before stop and never replays after exact tree exit", async proof => {
    const f = await actorFixture(proof);
    f.losePrimary();
    await eventually(() => expect(f.proofScans()).toBeGreaterThan(0));
    expect(f.manager.isSettled(f.handle.id)).toBe(false);
    let returned = false;
    const stopping = f.manager.stop(f.handle.id).then(result => { returned = true; return result; });
    void stopping.catch(() => undefined);
    await f.stopEntered.promise;
    expect(returned).toBe(false);
    expect(f.onSettled).not.toHaveBeenCalled();
    expect(fs.existsSync(path.join(f.directory, "manager-result.json"))).toBe(false);
    f.finishTree();
    const lost = await stopping;
    expect(lost).toMatchObject({ status: "failed", error: expect.stringMatching(/Actor worker is proven gone.*will not be automatically relaunched/) });
    expect(await f.manager.wait(f.handle.id)).toEqual(lost);
    expect(await f.manager.stop(f.handle.id)).toEqual(lost);
    expect(f.manager.status(f.handle.id)).toMatchObject(lost);
    expect(f.onSettled).toHaveBeenCalledOnce();
    expect(f.launch).toHaveBeenCalledOnce();
  }, 5_000);

});
