import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { collectHostReleases, formatReleaseReports, main } from "../src/releases-cli.js";
import { mainReleaseRecordDir, processStart } from "../src/lifecycle/release-process.js";

const roots: string[] = [];
afterEach(() => {
  roots.splice(0).forEach(root => fs.rmSync(root, { recursive: true, force: true }));
  vi.unstubAllEnvs();
});
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-release-census-"));
  roots.push(root);
  const procRoot = path.join(root, "proc");
  const profile = path.join(root, "profile");
  const active = path.join(root, "releases", "active");
  fs.mkdirSync(procRoot);
  fs.mkdirSync(profile);
  fs.mkdirSync(active, { recursive: true });
  fs.writeFileSync(path.join(active, "package.json"), JSON.stringify({ name: "pi-fabric" }));
  const settingsPath = path.join(profile, "settings.json");
  fs.writeFileSync(settingsPath, JSON.stringify({ packages: [active] }));
  const process = (pid: number, parent: number, args: string[], environment: Record<string, string> = {}, start = String(pid * 100)) => {
    const dir = path.join(procRoot, String(pid));
    fs.mkdirSync(dir);
    const fields = ["S", String(parent), ...Array<string>(17).fill("0"), start, "0"];
    fs.writeFileSync(path.join(dir, "stat"), `${pid} (pi (with spaces)) ${fields.join(" ")}`);
    fs.writeFileSync(path.join(dir, "cmdline"), args.join("\0") + "\0");
    fs.writeFileSync(path.join(dir, "environ"), Object.entries({ PI_CODING_AGENT_DIR: profile, ...environment }).map(([key, value]) => `${key}=${value}`).join("\0"));
  };
  const worker = (pid: number, parent: number, release: string, mainId = "session:session-one") => process(pid, parent,
    ["node", path.join(root, "releases", release, "dist", "worker.js"), "--id", `run-${pid}`, "--main-agent-id", mainId]);
  const record = (pid: number, sessionId: string, loaded = "old", start = String(pid * 100)) => {
    fs.mkdirSync(mainReleaseRecordDir(settingsPath), { recursive: true });
    fs.writeFileSync(path.join(mainReleaseRecordDir(settingsPath), `${pid}.json`), JSON.stringify({
      pid, start, sessionId, loadedRoot: path.join(root, "releases", loaded),
    }));
  };
  return { root, procRoot, settingsPath, process, worker, record };
};

describe("read-only release report", () => {
  it.each(["tilde", "file URL", "absolute"])("resolves an observed %s profile like the runtime", form => {
    const f = fixture();
    vi.stubEnv("HOME", f.root);
    vi.stubEnv("USERPROFILE", f.root);
    // The census's own profile must not replace the observed process's profile.
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(f.root, "observer-profile"));
    const profile = path.dirname(f.settingsPath);
    const observedProfile = form === "tilde" ? "~/profile"
      : form === "file URL" ? pathToFileURL(profile).href : profile;
    const environment = { PI_CODING_AGENT_DIR: observedProfile };
    f.process(10, 1, ["pi"], environment);
    f.record(10, "session-one");
    f.process(20, 10, ["node", path.join(f.root, "releases", "old", "dist", "worker.js"),
      "--id", "run-20", "--main-agent-id", "session:session-one"], environment);

    // Do not pass settingsPath: it would override the observed environment.
    const report = collectHostReleases({ procRoot: f.procRoot });
    expect(report.mains).toHaveLength(1);
    expect(report.mains[0]).toMatchObject({ pid: 10, mainId: "session:session-one",
      loaded: "old", active: "active", evidence: "runtime-record" });
    expect(report.mains[0]?.workers).toEqual([expect.objectContaining({
      pid: 20, mainId: "session:session-one", loaded: "old", active: "active",
    })]);
  });
  it.each(["non-Pi", "Pi"])("contains an invalid observed %s profile without losing a valid Main", kind => {
    const f = fixture();
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(f.root, "observer-profile"));
    f.process(10, 1, ["pi"]);
    f.record(10, "session-one");
    f.process(11, 1, kind === "Pi" ? ["pi"] : ["unrelated-service"],
      { PI_CODING_AGENT_DIR: "file:///tmp/profile%" });

    const report = collectHostReleases({ procRoot: f.procRoot });
    expect(report.mains.find(main => main.pid === 10)).toMatchObject({
      mainId: "session:session-one", loaded: "old", active: "active", evidence: "runtime-record",
    });
    expect(report.skippedProcesses).toBe(1);
    if (kind === "Pi") {
      expect(report.mains.find(main => main.pid === 11)).toMatchObject({
        loaded: "unknown", active: "unknown", evidence: "unknown",
      });
      expect(report.mains).toHaveLength(2);
    } else {
      expect(report.mains).toHaveLength(1);
    }
  });

  it("keeps invalid-profile workers visible without using the observer's selector", () => {
    const f = fixture();
    vi.stubEnv("PI_CODING_AGENT_DIR", path.dirname(f.settingsPath));
    f.process(10, 1, ["pi"]);
    f.record(10, "session-one");
    f.process(20, 10, ["node", path.join(f.root, "releases", "old", "dist", "worker.js"),
      "--id", "run-20", "--main-agent-id", "session:session-one"],
    { PI_CODING_AGENT_DIR: "file:///tmp/profile%" });

    const report = collectHostReleases({ procRoot: f.procRoot });
    expect(report.skippedProcesses).toBe(1);
    expect(report.mains).toHaveLength(1);
    expect(report.mains[0]).toMatchObject({ pid: 10, active: "active", workers: [
      { pid: 20, loaded: "old", active: "unknown" },
    ] });
  });

  it("groups live Main and actor workers using loaded paths, not the active selector", () => {
    const f = fixture();
    f.process(10, 1, ["pi"]);
    f.record(10, "session-one");
    f.worker(20, 10, "old");
    f.process(21, 10, ["node", path.join(f.root, "releases", "old", "dist", "worker.js"), "--id", "actor-run", "--main-agent-id", "session:session-one", "--actor-id", "actor-one"]);
    f.process(30, 20, ["pi"], { PI_FABRIC_PARENT_RUN: "run-20" });
    expect(processStart(10, f.procRoot)).toBe("1000");
    const report = collectHostReleases({ ...f, host: "host-one" });
    expect(report.mains).toHaveLength(1);
    expect(report.mains[0]).toMatchObject({ pid: 10, mainId: "session:session-one", loaded: "old", active: "active", evidence: "runtime-record" });
    expect(report.mains[0]?.workers).toHaveLength(2);
    expect(report.mains[0]?.workers[1]).toMatchObject({ actorId: "actor-one", loaded: "old" });
    expect(formatReleaseReports([report])).toContain("old=2");
  });

  it("joins detached resident workers to the recorded Main using session lineage ids", () => {
    const f = fixture();
    f.process(10, 1, ["pi"]);
    f.record(10, "session-one");
    const hostRoot = path.join(f.root, "resident");
    fs.mkdirSync(hostRoot);
    const config = path.join(hostRoot, "config.json");
    fs.writeFileSync(config, JSON.stringify({ rootId: "session:session-one", fabricExtensionPath: path.join(f.root, "releases", "new", "dist", "index.js") }));
    fs.writeFileSync(path.join(hostRoot, "owner.json"), JSON.stringify({ pid: 40, fabricExtensionPath: path.join(f.root, "releases", "old", "dist", "index.js") }));
    // Native Pi sets process.title, so Linux argv can be only "pi": owner metadata must suffice.
    f.process(40, 1, ["pi"], { PI_FABRIC_RESIDENT_CONFIG: config });
    f.worker(41, 40, "old", "session:session-one");
    const report = collectHostReleases(f);
    expect(report.mains).toHaveLength(1);
    expect(report.mains[0]).toMatchObject({ pid: 10, mainId: "session:session-one", workers: [{ pid: 41, mainId: "session:session-one" }], residentHosts: [{ pid: 40, mainId: "session:session-one", loaded: "old" }] });
  });

  it("labels legacy inference and unknowns honestly and rejects reused PID records", () => {
    const f = fixture();
    f.process(10, 1, ["pi"]);
    f.record(10, "wrong-session", "incorrect", "obsolete-start");
    f.worker(20, 10, "old");
    f.process(11, 1, ["pi"]);
    f.process(12, 1, ["pi"]);
    f.worker(22, 12, "older", "main:mixed");
    f.worker(23, 12, "newer", "main:mixed");
    f.worker(24, 1, "remote-old", "main:remote");
    f.record(99, "exited-session");
    const report = collectHostReleases(f);
    expect(report.mains.find(main => main.pid === 10)).toMatchObject({ loaded: "old", evidence: "worker-inferred" });
    expect(report.mains.find(main => main.pid === 11)).toMatchObject({ loaded: "unknown", evidence: "unknown" });
    expect(report.mains.find(main => main.pid === 12)).toMatchObject({ loaded: "unknown", evidence: "unknown" });
    expect(report.mains.find(main => main.mainId === "main:remote")).toMatchObject({ pid: null, loaded: "remote-old", evidence: "worker-inferred" });
    expect(JSON.stringify(report)).not.toContain("incorrect");
    expect(JSON.stringify(report)).not.toContain("exited-session");
  });

  it("recognizes Windows argv separators with synthetic proc facts on every platform", () => {
    const f = fixture();
    f.process(10, 1, ["C:\\pi-runtime\\node_modules\\pi-coding-agent\\dist\\cli.js"]);
    f.record(10, "session-one");
    f.process(20, 1, ["node", "C:\\fabric\\releases\\old\\dist\\worker.js", "--id", "windows-run", "--main-agent-id", "session:session-one"]);
    const report = collectHostReleases(f);
    expect(report.mains).toHaveLength(1);
    expect(report.mains[0]).toMatchObject({ mainId: "session:session-one", loaded: "old", workers: [{ runId: "windows-run", loaded: "old" }] });
  });

  it("aggregates offline snapshots per host and performs no writes", () => {
    const f = fixture();
    f.process(10, 1, ["pi"]);
    f.worker(20, 10, "old");
    const one = collectHostReleases({ ...f, host: "host-one" });
    const two = { ...one, host: "host-two" };
    const oneFile = path.join(f.root, "one.json");
    const twoFile = path.join(f.root, "two.json");
    fs.writeFileSync(oneFile, JSON.stringify([one]));
    fs.writeFileSync(twoFile, JSON.stringify(two));
    const before = fs.readdirSync(f.root, { recursive: true }).sort();
    let output = "";
    expect(main(["--snapshot", oneFile, "--snapshot", twoFile], { out: text => { output += text; return true; } })).toBe(0);
    expect(output).toContain("host-one");
    expect(output).toContain("host-two");
    expect(fs.readdirSync(f.root, { recursive: true }).sort()).toEqual(before);
    expect(() => main(["--bad"])).toThrow(/usage/);
  });
});
