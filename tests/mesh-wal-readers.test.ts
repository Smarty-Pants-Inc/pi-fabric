import { spawn, type ChildProcess } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { formatWalReaders, walReaderPids } from "../src/mesh/wal-readers.js";

const CHILD = `
import { DatabaseSync } from "node:sqlite";
const db = new DatabaseSync(process.argv[1]);
db.exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE IF NOT EXISTS t(x); INSERT INTO t VALUES (1);");
db.exec("BEGIN");
db.prepare("SELECT count(*) AS n FROM t").get();
process.stdout.write("READY " + process.pid + "\\n");
process.stdin.resume();
process.stdin.on("end", () => { db.exec("COMMIT"); db.close(); process.exit(0); });
`;

let child: ChildProcess | undefined;
let dir: string | undefined;
afterEach(() => {
  child?.kill("SIGKILL");
  child = undefined;
  if (dir) fs.rmSync(dir, { recursive: true, force: true });
  dir = undefined;
});

describe.skipIf(process.platform !== "linux")("walReaderPids", () => {
  it("names the child process holding a WAL read mark", async () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), "wal-readers-"));
    const db = path.join(dir, "state.db");
    const proc = spawn(process.execPath, ["--input-type=module", "-e", CHILD, db], { stdio: ["pipe", "pipe", "pipe"] });
    child = proc;
    let stderr = "";
    proc.stderr!.on("data", (chunk) => { stderr += String(chunk); });
    const pid = await new Promise<number>((resolve, reject) => {
      let out = "";
      proc.stdout!.on("data", (chunk) => {
        out += String(chunk);
        const m = /READY (\d+)/.exec(out);
        if (m) resolve(Number(m[1]));
      });
      proc.on("exit", (code) => reject(new Error(`child exited ${code}: ${stderr}`)));
    });
    expect(pid).toBe(proc.pid);
    const readers = walReaderPids(db);
    const mine = readers.filter((r) => r.pid === pid);
    expect(mine.length).toBeGreaterThan(0);
    for (const r of mine) {
      expect(r.slot).toBeGreaterThanOrEqual(0);
      expect(r.slot).toBeLessThanOrEqual(4);
      expect(r.state).toMatch(/^[A-Za-z]$/);
      expect(r.ageMs).toBeGreaterThanOrEqual(0);
      expect(r.ageMs).toBeLessThan(10 * 60_000);
      expect(r.cmd).toBeTruthy();
    }
    expect(walReaderPids(path.join(dir, "missing.db"))).toEqual([]);
    expect(formatWalReaders(mine)).toMatch(new RegExp(`^pid ${pid} \\([A-Za-z], \\d+s, .+\\) slot [0-4]`));
    proc.stdin!.end();
    await new Promise((resolve) => proc.once("exit", resolve));
    child = undefined;
    expect(walReaderPids(db).filter((r) => r.pid === pid)).toEqual([]);
  }, 30_000);

  it("formats readers compactly and never throws", () => {
    expect(formatWalReaders([{ pid: 1234, slot: 2, state: "D", ageMs: 12 * 60_000, cmd: "node ..." }])).toBe("pid 1234 (D, 12m, node ...) slot 2");
    expect(formatWalReaders([])).toBe("no WAL readers found");
    expect(walReaderPids("\0bad")).toEqual([]);
  });
});
