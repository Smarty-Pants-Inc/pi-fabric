import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { resolveAgentDir } from "./core/agent-dir.js";
import { activeFabricRoot, releaseLabel } from "./core/agent-dir.js";
import { mainReleaseRecordDir, type MainReleaseProcess } from "./lifecycle/release-process.js";

interface ProcessInfo { pid: number; parent: number; args: string[]; env: Record<string, string>; start: string }
export interface WorkerRelease {
  pid: number;
  runId: string;
  mainId: string;
  loaded: string;
  active: string;
  actorId?: string;
}
export interface MainRelease {
  pid: number | null;
  mainId: string;
  loaded: string;
  evidence: "runtime-record" | "worker-inferred" | "unknown";
  active: string;
  workers: WorkerRelease[];
}
export interface HostReleaseReport { host: string; generatedAt: string; mains: MainRelease[]; skippedProcesses: number }

const selectedEnv = new Set(["PI_CODING_AGENT_DIR", "PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_MAIN_AGENT_ID", "PI_FABRIC_SESSION_ID"]);
const flag = (args: string[], name: string): string | undefined => {
  const index = args.indexOf(name);
  return index < 0 ? undefined : args[index + 1];
};
const activeLabel = (settings: string): string => {
  const root = activeFabricRoot(settings);
  return root ? releaseLabel(root) : "unknown";
};

/** Read only selected non-secret environment keys and native argv; never inspect prompt/history. */
export const collectHostReleases = (options: { procRoot?: string; settingsPath?: string; host?: string } = {}): HostReleaseReport => {
  const procRoot = options.procRoot ?? "/proc";
  const settings = options.settingsPath ?? path.join(resolveAgentDir(), "settings.json");
  const processes = new Map<number, ProcessInfo>();
  let skippedProcesses = 0;
  for (const entry of fs.readdirSync(procRoot)) {
    if (!/^[0-9]+$/.test(entry)) continue;
    const pid = Number(entry);
    try {
      const directory = path.join(procRoot, entry);
      const args = fs.readFileSync(path.join(directory, "cmdline"), "utf8").split("\0").filter(Boolean);
      const stat = fs.readFileSync(path.join(directory, "stat"), "utf8");
      const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
      const parent = Number(fields[1]);
      const start = fields[19];
      if (!start || args.length === 0 || fields[0] === "Z") continue;
      const env: Record<string, string> = {};
      // Hidepid may permit argv but deny environ. Keep reporting argv workers in that case.
      try {
        for (const pair of fs.readFileSync(path.join(directory, "environ"), "utf8").split("\0")) {
          const equal = pair.indexOf("=");
          const key = pair.slice(0, equal);
          if (selectedEnv.has(key)) env[key] = pair.slice(equal + 1);
        }
      } catch { /* argv is still useful */ }
      processes.set(pid, { pid, parent, args, env, start });
    } catch { skippedProcesses += 1; } // exited during snapshot or belongs to another OS user
  }
  const records = new Map<number, MainReleaseProcess>();
  for (const process of processes.values()) {
    const profileSettings = options.settingsPath ?? (process.env.PI_CODING_AGENT_DIR
      ? path.join(process.env.PI_CODING_AGENT_DIR, "settings.json") : settings);
    try {
      const record = JSON.parse(fs.readFileSync(path.join(mainReleaseRecordDir(profileSettings), `${process.pid}.json`), "utf8")) as MainReleaseProcess;
      if (record.pid === process.pid && record.start === process.start && typeof record.loadedRoot === "string" && typeof record.sessionId === "string") {
        records.set(process.pid, record);
      }
    } catch { /* Older Mains have no record. Do not label them with today's selector. */ }
  }
  const mains = new Map<string, MainRelease>();
  const mainsByPid = new Map<number, MainRelease>();
  const profile = (process: ProcessInfo): string => options.settingsPath ?? (process.env.PI_CODING_AGENT_DIR
    ? path.join(process.env.PI_CODING_AGENT_DIR, "settings.json") : settings);
  for (const process of processes.values()) {
    const record = records.get(process.pid);
    const pi = process.args.slice(0, 2).some(arg => path.basename(arg) === "pi" || /(?:pi-coding-agent|pi-runtime).*\/cli\.js$/.test(arg));
    if (!record && (!pi || process.env.PI_FABRIC_PARENT_RUN || process.env.PI_FABRIC_ACTOR_ID)) continue;
    const mainId = record ? `main:${record.sessionId}` : `pid:${process.pid}`;
    const main: MainRelease = {
      pid: process.pid, mainId,
      loaded: record ? releaseLabel(record.loadedRoot) : "unknown",
      evidence: record ? "runtime-record" : "unknown",
      active: activeLabel(profile(process)), workers: [],
    };
    mains.set(mainId, main);
    mainsByPid.set(process.pid, main);
  }
  for (const process of processes.values()) {
    const workerFile = process.args.find(arg => /\/releases\/[^/]+\/dist\/worker\.js$/.test(arg));
    if (!workerFile || !flag(process.args, "--id")) continue;
    // The worker entry path, not its child's Pi version or today's profile, proves its loaded Fabric.
    const loaded = releaseLabel(path.dirname(path.dirname(workerFile)));
    const lineage = flag(process.args, "--main-agent-id") ?? process.env.PI_FABRIC_MAIN_AGENT_ID ?? "unknown";
    let main = mains.get(lineage);
    if (!main) {
      // Native local parent ancestry covers legacy Mains; detached/remote workers retain lineage id.
      let ancestor = processes.get(process.parent);
      const visited = new Set<number>();
      while (ancestor && !visited.has(ancestor.pid)) {
        visited.add(ancestor.pid);
        main = mainsByPid.get(ancestor.pid);
        if (main) break;
        ancestor = processes.get(ancestor.parent);
      }
    }
    if (!main) {
      main = { pid: null, mainId: lineage, loaded: "unknown", evidence: "unknown", active: activeLabel(profile(process)), workers: [] };
      mains.set(lineage, main);
    }
    const actorId = flag(process.args, "--actor-id");
    main.workers.push({ pid: process.pid, runId: flag(process.args, "--id")!, mainId: lineage,
      loaded, active: activeLabel(profile(process)), ...(actorId ? { actorId } : {}),
    });
  }
  for (const main of mains.values()) {
    const releases = [...new Set(main.workers.map(worker => worker.loaded))];
    if (main.evidence === "unknown" && releases.length === 1) {
      main.loaded = releases[0]!;
      main.evidence = "worker-inferred";
    }
    main.workers.sort((a, b) => a.pid - b.pid);
  }
  return { host: options.host ?? os.hostname(), generatedAt: new Date().toISOString(),
    mains: [...mains.values()].sort((a, b) => (a.pid ?? Infinity) - (b.pid ?? Infinity) || a.mainId.localeCompare(b.mainId)), skippedProcesses,
  };
};

export const formatReleaseReports = (reports: HostReleaseReport[]): string => {
  const lines = ["HOST MAIN PID LOADED ACTIVE EVIDENCE WORKERS (loaded=count)"];
  for (const report of reports) for (const main of report.mains) {
    const counts = new Map<string, number>();
    for (const worker of main.workers) counts.set(worker.loaded, (counts.get(worker.loaded) ?? 0) + 1);
    lines.push([report.host, main.mainId, main.pid ?? "remote/detached", main.loaded, main.active, main.evidence,
      [...counts].map(([release, count]) => `${release}=${count}`).join(",") || "none"].join(" "));
  }
  return `${lines.join("\n")}\n`;
};

const usage = "usage: fabric-releases [--json] [--settings FILE] [--host NAME] [--snapshot FILE ...]";
export const main = (argv: string[], io = { out: (text: string) => process.stdout.write(text) }): number => {
  let json = false;
  const options: { settingsPath?: string; host?: string } = {};
  const snapshots: string[] = [];
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--help") { io.out(`${usage}\n`); return 0; }
    if (arg === "--json") { json = true; continue; }
    const value = argv[index + 1];
    if (!value || value.startsWith("--")) throw new Error(usage);
    if (arg === "--settings") options.settingsPath = value;
    else if (arg === "--host") options.host = value;
    else if (arg === "--snapshot") snapshots.push(value);
    else throw new Error(usage);
    index += 1;
  }
  const reports = snapshots.length ? snapshots.map(file => {
    const value = JSON.parse(fs.readFileSync(file, "utf8")) as HostReleaseReport | HostReleaseReport[];
    return Array.isArray(value) ? value : [value];
  }).flat() : [collectHostReleases(options)];
  io.out(json ? `${JSON.stringify(reports, null, 2)}\n` : formatReleaseReports(reports));
  return 0;
};
