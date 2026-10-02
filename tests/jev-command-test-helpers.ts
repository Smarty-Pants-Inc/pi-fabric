import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { expect, vi } from "vitest";

export const credentialRequest = { state: "offline fixture", questions: { yes: { type: "noul" as const, instructions: "Is this a fixture?" } } };
export const credentialResponse = { model: "jev-latest", answers: { yes: { type: "noul", noul: 1 } }, usage: { input_tokens: 1, output_tokens: 1 } };
export const delay = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
export function alive(pid: number): boolean {
  try { process.kill(pid, 0); return true; } catch { return false; }
}

/** Real owned children, with readiness recorded only AFTER installing SIGTERM handling. */
export function commandFixture(tree = false, parentIgnoresTerm = true, inheritStreams = true) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "jev-command-lifetime-"));
  const pidFile = path.join(root, "pid");
  const descendantFile = path.join(root, "descendant");
  const terminated = path.join(root, "term");
  const fresh = path.join(root, "fresh");
  const code = `
    const fs = require('node:fs');
    if (fs.existsSync(${JSON.stringify(fresh)})) { console.log('offline-fresh-key'); process.exit(0); }
    process.on('SIGTERM', () => { fs.writeFileSync(${JSON.stringify(terminated)}, 'received'); ${parentIgnoresTerm ? '' : 'process.exit(0);'} });
    ${tree ? `require('node:child_process').spawn(process.execPath, ['-e', ${JSON.stringify(`const fs = require('node:fs'); process.on('SIGTERM', () => {}); fs.writeFileSync(${JSON.stringify(descendantFile)}, String(process.pid)); setInterval(() => {}, 1000);`)}], { stdio: '${inheritStreams ? 'inherit' : 'ignore'}' });` : ""}
    fs.writeFileSync(${JSON.stringify(pidFile)}, String(process.pid));
    console.log('FAKE_SECRET_MUST_NOT_BE_USED'); console.error('FAKE_SECRET_MUST_NOT_LEAK');
    setInterval(() => {}, 1000);
  `;
  const pids = () => [descendantFile, pidFile].filter(file => fs.existsSync(file)).map(file => Number(fs.readFileSync(file, "utf8")));
  return {
    root, pidFile, terminated, fresh,
    command: [process.execPath, "-e", code],
    async ready() {
      await vi.waitFor(() => {
        expect(fs.existsSync(pidFile)).toBe(true);
        if (tree) expect(fs.existsSync(descendantFile)).toBe(true);
      }, { timeout: 4000, interval: 10 });
      return Number(fs.readFileSync(pidFile, "utf8"));
    },
    pids,
    async cleanup() {
      // Test-owned PID files only. Kill descendants before their parent can reap them.
      for (const pid of pids()) {
        if (alive(pid)) { try { process.kill(pid, "SIGKILL"); } catch { /* Already exited. */ } }
        await vi.waitFor(() => expect(alive(pid)).toBe(false), { timeout: 4000, interval: 10 });
      }
      fs.rmSync(root, { recursive: true, force: true });
    },
  };
}
