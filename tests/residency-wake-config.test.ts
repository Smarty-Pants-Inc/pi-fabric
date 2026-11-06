import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { findExecutable } from "../src/agents/transports/process-utils.js";
import { MeshStore } from "../src/mesh/store.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import type { FabricParticipantSource } from "../src/topology/types.js";
import { ResidencyClient } from "../src/residency/client.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { residentLaunchSpec } from "../src/residency/handover.js";
import { RESIDENT_HOST_FORMAT, type ResidentHostConfig, type ResidentHostOwner, type ResidentPiModelState } from "../src/residency/protocol.js";
import { assertResidentWakeConfig, canonicalResidentWakeConfig, ResidentWakeConfigMismatch,
  residentWakeConfigMatches, routesAt } from "../src/residency/wake-index.js";

describe("whole restart-config canonical equality", () => {
  it("ignores object ordering at every depth, but preserves array ordering", () => {
    const config = { actorRoot: "/actors", mesh: { actorScope: "project", grants: ["read", "write"] }, optional: undefined };
    const reordered = { mesh: { grants: ["read", "write"], actorScope: "project" }, actorRoot: "/actors" };
    expect(canonicalResidentWakeConfig(config)).toBe(canonicalResidentWakeConfig(reordered));
    expect(residentWakeConfigMatches(config, reordered)).toBe(true);
    expect(residentWakeConfigMatches(config, { ...reordered, mesh: { ...reordered.mesh, grants: ["write", "read"] } })).toBe(false);
  });
  it.each(["actorRoot", "sessionActorRoot", "mesh", "role", "piBinary", "futureAuthority"])("includes the entire %s field without an allowlist", field => {
    const config = { actorRoot: "/actors", sessionActorRoot: "/sessions", mesh: { actorScope: "project" }, role: "worker", piBinary: "/pi", futureAuthority: { grants: ["read"] } };
    expect(residentWakeConfigMatches(config, { ...config, [field]: "different" })).toBe(false);
    const removed: Record<string, unknown> = { ...config };
    delete removed[field];
    expect(residentWakeConfigMatches(config, removed)).toBe(false);
  });
  it("fails closed on missing, non-object, and non-JSON config", () => {
    const circular: Record<string, unknown> = {}; circular.self = circular;
    for (const value of [undefined, null, [], "config", circular]) expect(residentWakeConfigMatches(value, {})).toBe(false);
  });
});

const writeExecutable = (directory: string, name: string): string => {
  const file = path.join(directory, process.platform === "win32" ? `${name}.cmd` : name);
  fs.writeFileSync(file, process.platform === "win32" ? "@exit /b 0\r\n" : "#!/bin/sh\nexit 0\n", { mode: 0o700 });
  return file;
};
const binaryFields = ["piBinary", "claudeBinary", "vedaBinary"] as const;
const withSealedWakeFixture = async (run: (fixture: {
  root: string; bin: string; raw: ResidentHostConfig; client: ResidencyClient;
  spec: ReturnType<typeof residentLaunchSpec>;
  release: (name: string) => { workerPath: string; fabricExtensionPath: string; entry: string; hostPath: string };
  readConfig: () => ResidentHostConfig;
  writeConfig: (config: ResidentHostConfig) => void;
  retainRoutes: (config: ResidentHostConfig) => NonNullable<ReturnType<typeof routesAt>>;
  setModels: (state: ResidentPiModelState) => void;
  readyOwner: () => ResidentHostOwner;
}) => Promise<void> | void, optionalPresent = false): Promise<void> => {
  // realpath avoids platform temporary-directory symlinks, which real sealing rejects.
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-sealing-")));
  const previousPath = process.env.PATH;
  const previousPathExt = process.env.PATHEXT;
  let client: ResidencyClient | undefined;
  let mesh: MeshStore | undefined;
  try {
    const bin = path.join(root, "bin");
    fs.mkdirSync(bin);
    writeExecutable(bin, "pi");
    if (optionalPresent) for (const name of ["claude", "veda"]) writeExecutable(bin, name);
    process.env.PATH = bin; // No host-installed binaries (or helper commands) can leak in.
    if (process.platform === "win32") process.env.PATHEXT = ".CMD";
    for (const name of ["claude", "veda"]) {
      if (optionalPresent) expect(findExecutable(name)).toBeDefined();
      else expect(findExecutable(name)).toBeUndefined();
    }
    const release = (name: string) => {
      const dist = path.join(root, name, "dist");
      fs.mkdirSync(path.join(dist, "residency"), { recursive: true });
      const files = { workerPath: path.join(dist, "worker.js"), fabricExtensionPath: path.join(dist, "index.js"),
        entry: path.join(dist, "residency", "pi-entry.js"), hostPath: path.join(dist, "residency", "launcher.js") };
      for (const file of Object.values(files)) fs.writeFileSync(file, "export {};\n");
      return files;
    };
    const files = release("release-a");
    const raw: ResidentHostConfig & { futureAuthority: { grants: string[] } } = {
      format: RESIDENT_HOST_FORMAT, rootId: "session:wake-sealing", sessionId: "wake-sealing",
      mainName: "main", mainStartedAt: 1,
      cwd: root, projectRoot: root, meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"),
      residencyRoot: path.join(root, "mesh", "residency", "wake-sealing"), fullCodeMode: true,
      agents: structuredClone(DEFAULT_FABRIC_CONFIG.agents), mesh: structuredClone(DEFAULT_FABRIC_CONFIG.mesh),
      retention: structuredClone(DEFAULT_FABRIC_CONFIG.retention), workerPath: files.workerPath,
      fabricExtensionPath: files.fabricExtensionPath, piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
      futureAuthority: { grants: ["read"] },
    };
    fs.mkdirSync(raw.residencyRoot, { recursive: true });
    const configPath = path.join(raw.residencyRoot, "config.json");
    const readConfig = () => JSON.parse(fs.readFileSync(configPath, "utf8")) as ResidentHostConfig;
    // Only drift/migration cases use this; positive cases persist through the real Main client.
    const writeConfig = (value: ResidentHostConfig) => fs.writeFileSync(configPath, JSON.stringify(value));
    // A retained real file is enough: sealing validates paths/bytes, never executes it.
    const spec = residentLaunchSpec(raw, files.entry, writeExecutable(root, "runtime"));
    mesh = new MeshStore(raw.meshRoot, raw.mesh.maxEventBytes, raw.mesh.maxReadEvents);
    const mainClient = new ResidencyClient({ config: structuredClone(raw), mesh, hostPath: files.hostPath,
      participants: {} as FabricParticipantSource, mainAgent: { local: true, id: raw.rootId } as FabricMainAgentTarget });
    client = mainClient;
    const hostId = client.hostId;
    const retainRoutes = (config: ResidentHostConfig) => {
      const routes = { format: 1 as const, rootId: raw.rootId, hostId,
        configJson: canonicalResidentWakeConfig(config), actors: [] };
      fs.writeFileSync(path.join(raw.residencyRoot, "wake-routes.json"), JSON.stringify(routes));
      return routes;
    };
    const readyOwner = (): ResidentHostOwner => {
      // Real current-process liveness selects ensureHost's existing-owner path; no spawn or probe mock.
      const owner: ResidentHostOwner = { format: RESIDENT_HOST_FORMAT, hostId, pid: process.pid,
        processStartTime: processStartTime(process.pid), token: "fixture-owner", startedAt: 1, readyAt: 1 };
      fs.writeFileSync(path.join(raw.residencyRoot, "owner.json"), JSON.stringify(owner));
      return owner;
    };
    await run({ root, bin, raw, client, spec, release, readConfig, writeConfig, retainRoutes,
      setModels: state => { mainClient.options.piModelState = () => state; }, readyOwner });
  } finally {
    await client?.close();
    mesh?.closeState();
    if (previousPath === undefined) delete process.env.PATH;
    else process.env.PATH = previousPath;
    if (previousPathExt === undefined) delete process.env.PATHEXT;
    else process.env.PATHEXT = previousPathExt;
    fs.rmSync(root, { recursive: true, force: true });
  }
};
const expectWakeRefused = (root: string, snapshot: ResidentHostConfig): void => {
  expect(routesAt(root)).toBeUndefined();
  expect(() => assertResidentWakeConfig(root)).toThrow(ResidentWakeConfigMismatch);
  expect(() => assertResidentWakeConfig(root, snapshot)).toThrow(ResidentWakeConfigMismatch);
};
const wakeText = (root: string): NonNullable<ResidentHostConfig["agents"]["wakeText"]> => ({
  receiptDb: path.join(root, "receipts.sqlite"), maxChars: 200, trustedPublishers: [], repositories: {},
});

// Keep: recorded incident smarty-dev#19074, at the Main persistence -> launch-sealing
// -> retained wake-admission boundary. No pinning/matching mocks or launcher processes.
describe("Main-persisted sealed wake config (smarty-dev#19074)", () => {
  it.each(["syncPiModels", "updateWakeText", "updateModelGuidance", "ensureHost"] as const)(
    "%s still persists config with an unresolvable required pi selector while launch sealing refuses it", async method => {
      await withSealedWakeFixture(async ({ root, raw, client, spec, readConfig, readyOwner }) => {
        const missingPi = "pi-required-missing-for-wake-sealing-regression";
        expect(findExecutable(missingPi)).toBeUndefined();
        client.options.config.piBinary = missingPi; // Mutate only the client's cloned config, not the valid fixture spec.
        const launchError = new Error(`Resident handover cannot resolve binary: ${missingPi}`);
        expect(() => residentLaunchSpec(client.options.config, spec.entry, spec.runtime)).toThrow(launchError);
        expect(fs.existsSync(path.join(raw.residencyRoot, "config.json"))).toBe(false);
        if (method === "updateWakeText") expect(() => client.updateWakeText(wakeText(root))).not.toThrow();
        else if (method === "updateModelGuidance") expect(() => client.updateModelGuidance([])).not.toThrow();
        else if (method === "ensureHost") {
          const owner = readyOwner();
          await expect(client.ensureHost()).resolves.toEqual(owner);
        } else expect(() => client.syncPiModels()).not.toThrow();
        const saved = readConfig();
        expect(saved.piBinary).toBe(missingPi);
        expect(saved).toEqual(client.options.config);
        for (const field of ["claudeBinary", "vedaBinary"] as const) {
          expect(saved[field]).toBe(spec.config[field]);
          expect(fs.existsSync(saved[field])).toBe(false);
        }
        expect(() => residentLaunchSpec(saved, spec.entry, spec.runtime)).toThrow(launchError);
      });
    });

  it("syncPiModels persists absent literal claude/veda exactly as residentLaunchSpec and wake admits it", async () => {
    await withSealedWakeFixture(({ raw, client, spec, readConfig, retainRoutes }) => {
      expect(raw.claudeBinary).toBe("claude");
      expect(raw.vedaBinary).toBe("veda");
      for (const field of ["claudeBinary", "vedaBinary"] as const) {
        expect(path.dirname(spec.config[field])).toBe(path.dirname(spec.entry));
        expect(path.basename(spec.config[field])).toMatch(/^unavailable-[0-9a-f]{64}\.js$/);
        expect(fs.existsSync(spec.config[field])).toBe(false);
      }
      client.syncPiModels();
      // Baseline RED: the original client writes raw command names here.
      expect(readConfig()).toEqual(spec.config);
      expect(client.options.config).toEqual(spec.config);
      const routes = retainRoutes(spec.config);
      expect(routesAt(raw.residencyRoot)).toEqual(routes);
      expect(assertResidentWakeConfig(raw.residencyRoot)).toEqual(spec.config);
      expect(assertResidentWakeConfig(raw.residencyRoot, spec.config)).toEqual(spec.config);
    });
  });

  it("syncPiModels and residentLaunchSpec seal present PATH binaries to the same realpaths", async () => {
    await withSealedWakeFixture(({ raw, client, spec, readConfig, retainRoutes }) => {
      for (const field of binaryFields) expect(spec.config[field]).toBe(fs.realpathSync(findExecutable(raw[field])!));
      client.syncPiModels();
      expect(readConfig()).toEqual(spec.config);
      expect(client.options.config).toEqual(spec.config);
      const routes = retainRoutes(spec.config);
      expect(routesAt(raw.residencyRoot)).toEqual(routes);
      expect(assertResidentWakeConfig(raw.residencyRoot, spec.config)).toEqual(spec.config);
    }, true);
  });

  it.each(["syncPiModels", "updateWakeText", "updateModelGuidance", "ensureHost"] as const)(
    "%s seals binaries on its first config persistence", async method => {
      await withSealedWakeFixture(async ({ root, raw, client, spec, readConfig, retainRoutes, readyOwner }) => {
        expect(fs.existsSync(path.join(raw.residencyRoot, "config.json"))).toBe(false);
        if (method === "updateWakeText") client.updateWakeText(wakeText(root));
        else if (method === "updateModelGuidance") client.updateModelGuidance([]);
        else if (method === "ensureHost") {
          const owner = readyOwner();
          expect(await client.ensureHost()).toEqual(owner);
        } else client.syncPiModels();
        const saved = readConfig();
        for (const field of binaryFields) expect(saved[field]).toBe(spec.config[field]);
        expect(saved).toEqual(client.options.config);
        expect(residentLaunchSpec(saved, spec.entry, spec.runtime).config).toEqual(saved);
        const routes = retainRoutes(saved);
        expect(routesAt(raw.residencyRoot)).toEqual(routes);
        expect(assertResidentWakeConfig(raw.residencyRoot, saved)).toEqual(saved);
      });
    });

  it("repeated Main sync, model and wake-text writes retain sealed absence after claude/veda appear", async () => {
    await withSealedWakeFixture(async ({ root, bin, raw, client, spec, readConfig, retainRoutes, setModels, readyOwner }) => {
      client.syncPiModels();
      expect(readConfig()).toEqual(spec.config);
      const originalRoutes = retainRoutes(spec.config);
      for (const command of ["claude", "veda"] as const) {
        const appeared = writeExecutable(bin, command);
        expect(findExecutable(command)).toBe(appeared);
        expect(residentLaunchSpec(raw, spec.entry, spec.runtime).config[`${command}Binary`]).toBe(fs.realpathSync(appeared));
      }
      const check = () => {
        const saved = readConfig();
        for (const field of binaryFields) expect(saved[field]).toBe(spec.config[field]);
        expect(saved).toEqual(client.options.config);
        const routes = retainRoutes(saved); // The host refreshes its whole snapshot after intentional Main changes.
        expect(routesAt(raw.residencyRoot)).toEqual(routes);
        const admitted = assertResidentWakeConfig(raw.residencyRoot, saved);
        expect(admitted).toEqual(saved);
        expect(residentLaunchSpec(admitted, spec.entry, spec.runtime).config).toEqual(saved);
        for (const field of ["claudeBinary", "vedaBinary"] as const) expect(fs.existsSync(admitted[field])).toBe(false);
      };
      client.syncPiModels();
      client.syncPiModels();
      expect(routesAt(raw.residencyRoot)).toEqual(originalRoutes);
      check();
      const models: ResidentPiModelState = { available: [], aliases: {}, defaultModel: "test/model" };
      setModels(models);
      client.syncPiModels();
      expect(readConfig().piModels).toEqual(models);
      check();
      const guidance: NonNullable<ResidentHostConfig["modelGuidance"]> = [{ componentId: "test", component: "test",
        revision: 1, label: "model guidance", models: ["test/*"], content: "Use the sealed runner.", targets: ["main"], placement: "append" }];
      client.updateModelGuidance(guidance);
      expect(readConfig().modelGuidance).toEqual(guidance);
      check();
      client.updateModelGuidance([]);
      expect(readConfig().modelGuidance).toEqual([]);
      check();
      client.updateWakeText(wakeText(root));
      expect(readConfig().agents.wakeText).toEqual(wakeText(root));
      check();
      client.updateWakeText(undefined);
      expect(readConfig().agents.wakeText).toBeUndefined();
      check();
      const owner = readyOwner();
      expect(await client.ensureHost()).toEqual(owner);
      check();
    });
  });

  it("refuses pre-existing raw config against old sealed routes until the next same-release Main write repairs it", async () => {
    await withSealedWakeFixture(({ raw, client, spec, readConfig, writeConfig, retainRoutes }) => {
      writeConfig(raw);
      const routes = retainRoutes(spec.config);
      expect(residentWakeConfigMatches(raw, spec.config)).toBe(false);
      expectWakeRefused(raw.residencyRoot, spec.config);
      expect(readConfig()).toEqual(raw); // Wake admission must never normalize or repair persistence.
      client.syncPiModels();
      expect(readConfig()).toEqual(spec.config);
      expect(client.options.config).toEqual(spec.config);
      expect(routesAt(raw.residencyRoot)).toEqual(routes);
      expect(assertResidentWakeConfig(raw.residencyRoot, spec.config)).toEqual(spec.config);
    });
  });

  it.each(["actorRoot", "futureAuthority"] as const)("still refuses an exact %s drift after a Main write", async field => {
    await withSealedWakeFixture(({ root, raw, client, spec, readConfig, writeConfig, retainRoutes }) => {
      client.syncPiModels();
      retainRoutes(spec.config);
      expect(assertResidentWakeConfig(raw.residencyRoot, spec.config)).toEqual(spec.config);
      const changed = { ...readConfig(), [field]: field === "actorRoot" ? path.join(root, "other-actors") : { grants: ["all"] } };
      expect(residentWakeConfigMatches(changed, spec.config)).toBe(false);
      writeConfig(changed);
      expectWakeRefused(raw.residencyRoot, spec.config);
    });
  });

  it("still refuses a config sealed for another real release", async () => {
    await withSealedWakeFixture(({ raw, client, spec, release, writeConfig, retainRoutes }) => {
      client.syncPiModels();
      retainRoutes(spec.config);
      expect(assertResidentWakeConfig(raw.residencyRoot, spec.config)).toEqual(spec.config);
      const other = release("release-b");
      const foreign = residentLaunchSpec({ ...raw, workerPath: other.workerPath,
        fabricExtensionPath: other.fabricExtensionPath }, other.entry, spec.runtime);
      expect(foreign.releaseRoot).not.toBe(spec.releaseRoot);
      expect(residentWakeConfigMatches(foreign.config, spec.config)).toBe(false);
      writeConfig(foreign.config);
      expectWakeRefused(raw.residencyRoot, spec.config);
    });
  });

  it.each(binaryFields)("still refuses a changed %s selection in config.json", async field => {
    await withSealedWakeFixture(({ bin, raw, client, spec, readConfig, writeConfig, retainRoutes }) => {
      client.syncPiModels();
      retainRoutes(spec.config);
      expect(assertResidentWakeConfig(raw.residencyRoot, spec.config)).toEqual(spec.config);
      const changed = { ...readConfig(), [field]: fs.realpathSync(writeExecutable(bin, `other-${field}`)) };
      expect(residentWakeConfigMatches(changed, spec.config)).toBe(false);
      writeConfig(changed);
      expectWakeRefused(raw.residencyRoot, spec.config);
    });
  });

  it("a later PATH cannot reselect pi during Main persistence but an explicit selection drift still refuses", async () => {
    await withSealedWakeFixture(({ root, raw, client, spec, readConfig, writeConfig, retainRoutes }) => {
      client.syncPiModels();
      const routes = retainRoutes(spec.config);
      const otherBin = path.join(root, "other-bin");
      fs.mkdirSync(otherBin);
      const replacement = writeExecutable(otherBin, "pi");
      process.env.PATH = otherBin;
      expect(residentLaunchSpec(raw, spec.entry, spec.runtime).config.piBinary).toBe(fs.realpathSync(replacement));
      client.syncPiModels();
      expect(readConfig()).toEqual(spec.config);
      expect(routesAt(raw.residencyRoot)).toEqual(routes);
      expect(assertResidentWakeConfig(raw.residencyRoot)).toEqual(spec.config);
      writeConfig({ ...readConfig(), piBinary: replacement });
      expectWakeRefused(raw.residencyRoot, spec.config);
    });
  });

  it("keeps an already sealed restart snapshot valid and sealing idempotent", async () => {
    await withSealedWakeFixture(({ raw, client, spec, retainRoutes }) => {
      client.syncPiModels();
      const routes = retainRoutes(spec.config);
      expect(residentLaunchSpec(client.options.config, spec.entry, spec.runtime)).toEqual(spec);
      expect(routesAt(raw.residencyRoot)).toEqual(routes);
      expect(assertResidentWakeConfig(raw.residencyRoot)).toEqual(spec.config);
      expect(assertResidentWakeConfig(raw.residencyRoot, spec.config)).toEqual(spec.config);
    });
  });
});
