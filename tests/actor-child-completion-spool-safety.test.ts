import { execFile } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { expect, it } from "vitest";

// A synchronous FIFO read cannot be interrupted by a Vitest timer. Isolate each real
// native-watch reconciliation in an owned OS process, with an external kill deadline.
const run = (script: string): Promise<string> => new Promise((resolve, reject) => {
  execFile("bun", ["--eval", script], { timeout: 5_000, killSignal: "SIGKILL", maxBuffer: 32_768 },
    (error, stdout) => error ? reject(error) : resolve(stdout));
});
const cases = process.platform === "win32"
  ? ["oversized", "read-growth"] as const
  : ["fifo", "symlink-zero", "oversized", "open-race", "read-growth", "abandon-fifo", "source-fifo", "receipt-fifo"] as const;

it.each(cases)("refuses %s without blocking or consuming it; a normal completion still reconciles", async kind => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-spool-safety-"));
  try {
    const script = `
      import fs from "node:fs";
      import path from "node:path";
      import { execFileSync } from "node:child_process";
      import { ActorChildCompletionStore, ActorChildCompletionWatch } from ${JSON.stringify(path.resolve("src/actors/child-completions.ts"))};
      const root = ${JSON.stringify(root)}, kind = ${JSON.stringify(kind)};
      const session = path.join(root, "session.jsonl"); fs.writeFileSync(session, "");
      const store = new ActorChildCompletionStore(session);
      fs.mkdirSync(store.directory);
      const bad = "a".repeat(32), good = "b".repeat(32);
      const suffix = kind === "abandon-fifo" ? ".abandon" : kind === "source-fifo" ? ".source" : kind === "receipt-fifo" ? ".receipt" : ".json";
      const unsafe = path.join(store.directory, bad + suffix);
      const spawner = { id: "actor:owner", kind: "actor", runId: "c".repeat(32) };
      const result = id => ({ id, name: "child", status: "completed", text: "saved", startedAt: Date.now(), finishedAt: Date.now() });
      const envelope = id => ({ format: 1, spawner, result: result(id) });
      const fifo = file => execFileSync("mkfifo", [file], { timeout: 1_000 });
      let warnings = 0, openFlags = [], readLengths = [], initialBytes = 0, resolved;
      console.warn = () => { warnings++; };
      const observed = new Promise(resolve => { resolved = resolve; });
      const watch = new ActorChildCompletionWatch(store.directory, () => {
        // Scoped preflight and locked pending both inspect unsafe entries, alongside
        // a legitimate envelope. No acknowledgements or product removal are called.
        const pending = store.pending({ actorId: spawner.id });
        if (pending.some(item => item.result.id === good)) resolved(pending.map(item => item.result.id));
      });
      if (kind === "symlink-zero") fs.symlinkSync("/dev/zero", unsafe);
      else if (kind === "oversized") fs.writeFileSync(unsafe, JSON.stringify({ ...envelope(bad), result: { ...result(bad), text: "x".repeat(1024 * 1024) } }));
      else if (kind === "open-race") {
        fs.writeFileSync(unsafe, JSON.stringify(envelope(bad)));
        const open = fs.openSync; let swapped = false;
        fs.openSync = (file, flags, ...args) => {
          if (String(file) === unsafe && !swapped) {
            swapped = true; openFlags.push(flags);
            fs.renameSync(unsafe, unsafe + ".saved"); fifo(unsafe);
          }
          return open(file, flags, ...args);
        };
      } else if (kind === "read-growth") {
        fs.writeFileSync(unsafe, JSON.stringify(envelope(bad)));
        initialBytes = fs.statSync(unsafe).size;
        const open = fs.openSync, read = fs.readSync, close = fs.closeSync; let targetFd, grown = false;
        fs.openSync = (file, ...args) => { const fd = open(file, ...args); if (String(file) === unsafe) targetFd = fd; return fd; };
        fs.closeSync = fd => { if (fd === targetFd) targetFd = undefined; return close(fd); };
        fs.readSync = (fd, buffer, offset, length, position) => {
          if (fd === targetFd) {
            readLengths.push(length);
            if (!grown) { grown = true; fs.appendFileSync(unsafe, " ".repeat(1024 * 1024)); }
          }
          return read(fd, buffer, offset, length, position);
        };
      } else fifo(unsafe);
      if (kind === "receipt-fifo") fs.writeFileSync(path.join(store.directory, bad + ".json"), JSON.stringify(envelope(bad)));
      store.enqueue(result(good), spawner);
      const ids = await observed;
      const unscoped = store.pending().map(item => item.result.id);
      const retained = fs.lstatSync(unsafe);
      // Complete the normal mailbox handoff, recording rather than executing its
      // envelope removal. The durable receipt proves logical consumption; result
      // and unsafe bytes remain protected, and the recorded call earns CI's delete test.
      const removals = [], remove = fs.rmSync;
      fs.rmSync = (file, ...args) => {
        if (String(file) === path.join(store.directory, good + ".json")) { removals.push(String(file)); return; }
        return remove(file, ...args);
      };
      store.acknowledge(good, { handoff: true });
      const mailbox = store.mailboxClaimed(good);
      const afterHandoff = store.pending({ actorId: spawner.id }).map(item => item.result.id);
      const custodyVisible = store.hasPendingReply();
      await watch.close();
      console.log(JSON.stringify({ ids, unscoped, warnings, unsafeRetained: !!retained,
        resultRetained: fs.existsSync(store.resultFile(good)), openFlags, removals: removals.length,
        mailbox, afterHandoff, custodyVisible, readLengths, initialBytes,
        noFollow: fs.constants.O_NOFOLLOW, nonblock: fs.constants.O_NONBLOCK }));
    `;
    const started = performance.now();
    const output = JSON.parse((await run(script)).trim());
    expect(performance.now() - started).toBeLessThan(5_000);
    expect(output.ids).toEqual(["b".repeat(32)]);
    expect(output.unscoped).toEqual(["b".repeat(32)]);
    expect(output.warnings).toBeGreaterThan(0);
    expect(output.unsafeRetained).toBe(true);
    expect(output.resultRetained).toBe(true);
    expect(output.removals).toBeGreaterThan(0);
    expect(output.mailbox).toBe(true);
    expect(output.afterHandoff).toEqual([]);
    expect(output.custodyVisible).toBe(true);
    if (kind === "read-growth") {
      expect(output.readLengths.length).toBeGreaterThan(0);
      expect(output.readLengths.every((length: number) => length <= output.initialBytes + 1)).toBe(true);
    }
    if (kind === "open-race") {
      expect(output.openFlags).toHaveLength(1);
      expect(output.openFlags[0] & output.noFollow).toBe(output.noFollow);
      expect(output.openFlags[0] & output.nonblock).toBe(output.nonblock);
    }
  } finally {
    // Only this test's fixtures, including the refused special entries; no store cleanup/prune.
    fs.rmSync(root, { recursive: true, force: true });
  }
}, 10_000);
