// Keep reason: #18131's committed-heartbeat / maintenance boundary cannot be reached cheaply
// in a live Main flow without a minute-long wait. Exercise real runtime wiring and persisted
// absence/alarm effects, not the spelling of the constructor options or mock-call counts.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

let runtime: FabricRuntimeState | undefined;
afterEach(async () => {
  try { await runtime?.shutdown(); }
  finally {
    runtime = undefined;
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
  }
});

describe("runtime committed-heartbeat presence cadence (#18131)", () => {
  it("first records root absence at 60s, then alarms on the next 60s pass, not at 25s", async () => {
    // Minimal real runtime envelope reused from participant-heartbeat-reload.test.ts.
    // No workers/providers need launching, and only Date is controlled: real I/O and promise
    // queues run normally. Manual committed heartbeats avoid a 120s wall-clock test.
    const cwd = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-presence-cadence-")));
    fs.chmodSync(cwd, 0o700);
    for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
    vi.stubEnv("SMARTY_AGENT", undefined);
    vi.stubEnv("HERDR_PANE_ID", undefined);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(cwd, "agent"));
    vi.stubEnv("PI_FABRIC_PROJECT_ROOT", cwd);
    const sessionId = "18131aaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    const pi = {
      on: () => () => {}, events: { emit() {}, on: () => () => {} },
      sendMessage() {}, appendEntry() {}, getThinkingLevel: () => "off", getSessionName: () => "cadence-main",
    } as unknown as ExtensionAPI;
    const context = {
      cwd, hasUI: false, mode: "rpc", model: { provider: "faux", id: "m1" },
      isIdle: () => true, hasPendingMessages: () => false, isProjectTrusted: () => true, abort() {},
      modelRegistry: { getAvailable: () => [], find: () => undefined },
      sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined, getBranch: () => [],
        getLeafId: () => null, getEntries: () => [] },
      ui: { setStatus() {}, notify() {} },
    } as unknown as ExtensionContext;
    const config = normalizeFabricConfig({ fullCodeMode: false,
      mesh: { enabled: true, root: path.join(cwd, "mesh"), stateBackend: "file", rootPresenceAlarmMs: 1 },
      agents: { enabled: false }, residency: { enabled: false }, mcp: { enabled: false },
      memory: { enabled: false }, jev: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false },
    });
    let directory: ParticipantDirectory | undefined;
    vi.spyOn(ParticipantDirectory.prototype, "start").mockImplementation(async function (this: ParticipantDirectory) {
      directory = this;
      // Do the real initial heartbeat without start()'s timer / process-exit withdrawal hook.
      // Constructor options, source registration, refresh and maintenance are not mocked.
      await this.refresh();
    });
    // Record-only retirement: this cadence test is not a withdrawal/delete test. Retain its
    // private scratch tree for the enclosing test sandbox rather than running product deletion.
    vi.spyOn(ParticipantDirectory.prototype, "quiesce").mockImplementation(async function (this: ParticipantDirectory) { this.suspend(); });
    vi.spyOn(ParticipantDirectory.prototype, "close").mockImplementation(async function (this: ParticipantDirectory) { this.suspend(); });
    const startedAt = Date.now();
    const clock = vi.spyOn(Date, "now").mockReturnValue(startedAt);
    runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
      extension: path.resolve("src/index.ts"), worker: path.join(cwd, "unused-worker.mjs"),
      residentHost: path.join(cwd, "unused-resident.mjs"), skills: cwd,
    } });
    await runtime.initialize(context, config);
    expect(directory).toBeDefined();
    expect(runtime.participantInfos({ kinds: ["root"], fresh: true })).toEqual(expect.arrayContaining([
      expect.objectContaining({ id: `session:${sessionId}`, status: "idle", stale: false }),
    ]));

    const orphanRoot = "session:absent-cadence-owner";
    const orphan: FabricParticipantRecord = {
      format: 1, id: "agent:cadence-orphan", rootId: orphanRoot,
      ownerHostId: orphanRoot, ownerIdentityId: orphanRoot, kind: "agent", name: "orphan",
      status: "idle", capabilities: [], runner: "pi", transport: "process", controlProtocol: "v1",
      startedAt, updatedAt: startedAt,
    };
    await runtime.mesh.put({ key: `topology/participants/${createHash("sha256").update(orphan.id).digest("hex")}`,
      value: orphan, identity: { id: orphanRoot, name: "absent", kind: "main", sessionId: "absent-cadence-owner" } });
    expect(runtime.participantInfos({ includeStale: true, fresh: true })).toEqual(expect.arrayContaining([
      expect.objectContaining({ id: orphan.id, stale: true }),
    ]));
    const absences = () => runtime!.mesh.listAll("topology/root-absence/", { fresh: true });
    const alarms = () => runtime!.mesh.read({ topic: "ops.owner", to: orphanRoot })
      .filter(event => event.kind === "root.presence.alarm");
    const heartbeatAt = async (elapsedMs: number) => {
      clock.mockReturnValue(startedAt + elapsedMs);
      await directory!.refresh();
      // The real maintenance queue is intentionally detached from the heartbeat's lease work.
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(runtime!.participantsConfirmedAt()).toBe(startedAt + elapsedMs);
    };

    await heartbeatAt(25_000);
    expect(absences()).toEqual([]);
    await heartbeatAt(59_999);
    expect(absences()).toEqual([]);
    await heartbeatAt(60_000);
    await vi.waitFor(() => expect(absences()).toEqual([
      expect.objectContaining({ value: { rootId: orphanRoot, firstAbsentAt: startedAt + 60_000 } }),
    ]));
    expect(alarms()).toEqual([]);
    await heartbeatAt(85_000); // The old 25s override would run another pass here.
    expect(alarms()).toEqual([]);
    await heartbeatAt(119_999);
    expect(alarms()).toEqual([]);
    await heartbeatAt(120_000);
    await vi.waitFor(() => expect(alarms()).toEqual([
      expect.objectContaining({ data: expect.objectContaining({ rootId: orphanRoot, firstAbsentAt: startedAt + 60_000 }) }),
    ]));
  }, 30_000);
});
