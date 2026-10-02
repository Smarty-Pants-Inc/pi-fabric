import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { MeshStore } from "../../src/mesh/store.js";
import { RootRegistrationGuard } from "../../src/topology/root-registration.js";

const [root, role, branch] = process.argv.slice(2) as [string, string, string];
const lock = path.join(root, ".lock");
const ownerPath = path.join(lock, "owner");
const emit = (phase: string, code?: string): void => {
  process.stdout.write(JSON.stringify({ phase, code }) + "\n");
};
const wait = (): number => fs.readSync(0, Buffer.alloc(1), 0, 1, null);
const pause = (phase: string): void => {
  emit(phase);
  if (wait() !== 1) throw new Error("race controller closed before resuming worker");
};
const kill = process.kill;
let staleAssessed = false;
process.kill = (pid, signal) => {
  if (pid === 999999999) {
    staleAssessed = true;
    throw Object.assign(new Error("synthetic stale PID"), { code: "ESRCH" });
  }
  return kill(pid, signal);
};
if (role === "A") {
  let checks = 0;
  if (branch === "owner") {
    const read = fs.readFileSync;
    let checked = false;
    fs.readFileSync = ((...args: Parameters<typeof read>) => {
      const value = read(...args);
      if (String(args[0]) === ownerPath && staleAssessed && !checked) {
        checked = true;
        pause("checked-stale");
      }
      return value;
    }) as typeof read;
  } else {
    let checked = false;
    let missingReads = 0;
    const checkpoint = (): void => {
      if (!checked) { checked = true; pause("checked-stale"); }
    };
    const read = fs.readFileSync;
    fs.readFileSync = ((...args: Parameters<typeof read>) => {
      try { return read(...args); }
      catch (error) {
        if (String(args[0]) === ownerPath && ++missingReads === 2) checkpoint();
        throw error;
      }
    }) as typeof read;
    const stat = fs.statSync;
    fs.statSync = ((...args: Parameters<typeof stat>) => {
      const value = stat(...args);
      if (String(args[0]) === lock && ++checks === 2) checkpoint();
      return value;
    }) as typeof stat;
  }
  const rename = fs.renameSync;
  let moved = false;
  fs.renameSync = (source, target) => {
    rename(source, target);
    if (String(source) === lock && !moved) {
      moved = true;
      pause("moved-lock");
    }
  };
}
if (role === "B") {
  const list = fs.readdirSync;
  let scanned = false;
  fs.readdirSync = ((...args: Parameters<typeof list>) => {
    const value = list(...args);
    if (String(args[0]) === path.join(root, "root-registrations") && !scanned) {
      scanned = true;
      pause("scanned-before-commit");
    }
    return value;
  }) as typeof list;
}
const guard = new RootRegistrationGuard(new MeshStore(root, 64 * 1024, 100, { lockTimeoutMs: 150 }), {
  owner: { id: role, pid: process.pid, host: os.hostname(), startTime: "" },
});
try {
  await guard.claim({ sessionId: role, rootId: `session:${role}`, fabricSessionId: role, name: "shared-name" });
  emit("result", "admitted");
} catch (error) {
  emit("result", (error as { code?: string }).code ?? String(error));
}
// Keep each genuine owner live through the parent's final registration/liveness check.
wait();
