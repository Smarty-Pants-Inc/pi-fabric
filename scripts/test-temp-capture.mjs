// Temporary opt-in diagnostics; never establish MSYS mappings before a real test shell.
import fs from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import { randomUUID, createHash } from "node:crypto";
import { spawnSync } from "node:child_process";

export const captureEnabled = () => process.platform === "win32" && process.env.PI_TEST_TEMP_CAPTURE === "1";
const LIMIT = 64 * 1024;
const TRUNCATED = "...[UTF-8 truncated]";
const text = value => {
  const bytes = Buffer.from(String(value ?? ""));
  if (bytes.length <= 2048) return bytes.toString();
  let end = 2048 - Buffer.byteLength(TRUNCATED);
  while (end > 0 && (bytes[end] & 0xc0) === 0x80) end--;
  return bytes.subarray(0, end).toString() + TRUNCATED;
};
const errorInfo = error => ({ code: text(error?.code), message: text(error?.message) });
export function rootState(root) {
  try { return { root, exists: true, directory: fs.statSync(root).isDirectory() }; }
  catch (error) { return { root, exists: error?.code === "ENOENT" ? false : null, error: errorInfo(error) }; }
}
export function captureRecord(phase, data = {}) {
  if (!captureEnabled()) return;
  try {
    const record = { phase, pid: process.pid, ppid: process.ppid,
      selection: text(process.env.PI_TEST_TEMP_PHASE || "standalone-config"),
      source: text(process.env.GITHUB_SHA), run: text(process.env.GITHUB_RUN_ID),
      attempt: text(process.env.GITHUB_RUN_ATTEMPT), image: text(process.env.ImageOS),
      imageVersion: text(process.env.ImageVersion), ...data };
    if ((phase === "allocated" || phase === "borrowed") && process.env.PI_TEST_TEMP_LEDGER)
      record.ledgerAppend = appendLedger({pid: process.pid, selection: record.selection, root: data.root,
        ownership:phase === "allocated" ? "owner" : "borrower"});
    let truncated = 0;
    let line = JSON.stringify(record, (_key, value) => {
      if (typeof value !== "string") return value;
      const bounded = text(value);
      if (bounded !== value || bounded.endsWith(TRUNCATED)) truncated++;
      return bounded;
    });
    if (truncated) line = JSON.stringify({...JSON.parse(line), incomplete:true, truncatedStrings:truncated});
    let framed = `FABRIC_TEMP_CAPTURE ${line}\n`;
    if (Buffer.byteLength(framed) > LIMIT)
      framed = `FABRIC_TEMP_CAPTURE ${JSON.stringify({phase:text(phase), incomplete:true, error:"record exceeds 64KiB UTF-8 including framing"})}\n`;
    // Exit listeners cannot wait for an asynchronous stdout flush.
    fs.writeSync(process.stdout.fd, framed);
  } catch { /* Diagnostics must never change test or cleanup outcomes. */ }
}
export function captureAllocation(root, borrowed = false) {
  if (!captureEnabled()) return;
  captureRecord(borrowed ? "borrowed" : "allocated", { root, inherited: Object.fromEntries(
    ["TMP", "TEMP", "TMPDIR"].map(key => [key, text(process.env[key])])), state: rootState(root) });
}
export function captureCleanup(phase, root, error) {
  if (captureEnabled()) captureRecord(phase, {state:rootState(root), ...(error ? {error:errorInfo(error)} : {})});
}
function readBounded(file, limit) {
  const fd = fs.openSync(file, "r");
  try {
    const before = fs.fstatSync(fd);
    if (!before.isFile() || before.size > limit) throw new Error(`file exceeds ${limit}-byte bound or is not regular`);
    const chunks = [];
    let total = 0;
    while (total <= limit) {
      const chunk = Buffer.alloc(Math.min(64 * 1024, limit + 1 - total));
      const count = fs.readSync(fd, chunk, 0, chunk.length, null);
      if (!count) break;
      chunks.push(chunk.subarray(0, count));
      total += count;
    }
    if (total > limit) throw new Error(`file grew beyond ${limit}-byte bound`);
    const after = fs.fstatSync(fd);
    if (["dev", "ino", "size", "mtimeMs", "ctimeMs"].some(key => before[key] !== after[key]))
      throw new Error("file changed during diagnostic read");
    return Buffer.concat(chunks, total);
  } finally { fs.closeSync(fd); }
}
function parseRows(bytes) {
  const lines = bytes.toString().trim().split("\n").filter(Boolean);
  if (lines.length > 64) throw new Error("ledger exceeds 64-row bound; roster incomplete");
  return lines.map(line => JSON.parse(line));
}
function appendLedger(row) {
  const ledger = process.env.PI_TEST_TEMP_LEDGER;
  let lock;
  try {
    // No wait/retry: concurrent allocation loses this diagnostic row explicitly.
    lock = fs.openSync(ledger + ".lock", "wx", 0o600);
    const before = readBounded(ledger, LIMIT);
    const line = JSON.stringify(row) + "\n";
    if (parseRows(before).length >= 64 || before.length + Buffer.byteLength(line) > LIMIT)
      throw new Error("ledger byte/row capacity reached; allocation omitted");
    fs.appendFileSync(ledger, line);
    return {accepted:true};
  } catch (error) {
    const failure = {accepted:false, incomplete:true, omitted:"at least one; total unknown", error:errorInfo(error)};
    try { fs.writeFileSync(ledger + ".incomplete", JSON.stringify(failure), {flag:"wx", mode:0o600}); }
    catch (markerError) { if (markerError?.code !== "EEXIST") failure.markerError = errorInfo(markerError); }
    return failure;
  } finally {
    if (lock !== undefined) {
      try { fs.closeSync(lock); fs.unlinkSync(ledger + ".lock"); } catch { /* owner cleanup below */ }
    }
  }
}
function ledgerRoots() {
  const ledger = process.env.PI_TEST_TEMP_LEDGER;
  if (!ledger) return {roots:[], incomplete:true, reason:"no driver ledger; standalone roots require log correlation"};
  const roots = parseRows(readBounded(ledger, LIMIT));
  try { return {roots, incomplete:true, omission:JSON.parse(readBounded(ledger + ".incomplete", LIMIT).toString())}; }
  catch (error) {
    if (error?.code !== "ENOENT") throw error;
    return {roots, incomplete:false};
  }
}
export function captureBoundary(phase) {
  if (!captureEnabled()) return;
  try { const roster = ledgerRoots(); captureRecord(phase, {...roster, roots:roster.roots.map(row => ({...row, state:rootState(row.root)}))}); }
  catch (error) { captureRecord(phase, { incomplete:true, error:errorInfo(error) }); }
}
export function beginDriverCapture() {
  if (!captureEnabled()) return () => {};
  const old = process.env.PI_TEST_TEMP_LEDGER;
  try {
    const ledger = path.join(tmpdir(), `pi-fabric-temp-capture-${process.pid}-${randomUUID()}.jsonl`);
    fs.writeFileSync(ledger, "", {flag:"wx", mode:0o600});
    process.env.PI_TEST_TEMP_LEDGER = ledger;
    return () => {
      for (const file of [ledger, ledger + ".lock", ledger + ".incomplete"]) {
        try { fs.unlinkSync(file); }
        catch (error) { if (error?.code !== "ENOENT") captureRecord("ledger-cleanup-error", {error:errorInfo(error)}); }
      }
      if (old === undefined) delete process.env.PI_TEST_TEMP_LEDGER;
      else process.env.PI_TEST_TEMP_LEDGER = old;
    };
  } catch (error) { captureRecord("ledger-setup-error", {error:errorInfo(error)}); return () => {}; }
}
function fileIdentity(file) {
  try {
    const bytes = readBounded(file, 32 * 1024 * 1024);
    return {file, size:bytes.length, sha256:createHash("sha256").update(bytes).digest("hex")};
  } catch (error) { return {file, error:errorInfo(error)}; }
}
function fstab(file) {
  try {
    const lines = readBounded(file, LIMIT).toString().split(/\r?\n/).filter(line => !/^\s*(#|$)/.test(line));
    // All /tmp ancestors and cygdrive entries can affect resolution. Bound the readback.
    const relevant = lines.filter(line => { const columns = line.trim().split(/\s+/); return ["/", "/tmp"].includes(columns[1]) || columns[2] === "cygdrive"; });
    return {file, entries:relevant.slice(0, 20), omitted:Math.max(0,relevant.length-20)};
  } catch (error) { return {file, error:errorInfo(error)}; }
}
let observed = false;
export function captureAfterShell(shell, completedOutput) {
  if (!captureEnabled() || observed) return;
  observed = true;
  try {
    // One post-existing-shell probe per shell-hang module. This can change shared
    // MSYS state: first-command output is pre-probe evidence, later tests are not.
    const beforeRoster = ledgerRoots();
    captureRecord("post-shell-before-probe", {shell, outputPrefix:text(completedOutput),
      temps:Object.fromEntries(["TMP", "TEMP", "TMPDIR"].map(key => [key,text(process.env[key])])),
      ...beforeRoster, roots:beforeRoster.roots.map(row => ({...row,state:rootState(row.root)})),
      perturbation:"No earlier capture shell. This new process may initialize/change MSYS mappings; subsequent tests are instrumented."});
    const probe = spawnSync(shell, ["-c", "printf 'BASH_VERSION=%s\\n' \"$BASH_VERSION\"; printf 'EXE='; readlink /proc/$$/exe; printf 'TMP_NATIVE='; cygpath -w /tmp; printf 'RUNTIME='; uname -r; printf 'USER='; id -un; test -d /tmp; printf 'TMP_IS_DIR_STATUS=%s\\n' \"$?\""],
      {encoding:"utf8", timeout:5000, maxBuffer:LIMIT, windowsHide:true});
    const out = probe.stdout || "";
    const field = key => out.split(/\r?\n/).find(line => line.startsWith(key + "="))?.slice(key.length+1);
    const mapping = field("TMP_NATIVE");
    const user = field("USER");
    const roster = ledgerRoots();
    const dirs = [...new Set([path.dirname(shell), path.resolve(path.dirname(shell), "../usr/bin")])];
    captureRecord("post-shell-probe", {shell, status:probe.status, signal:probe.signal,
      error:probe.error ? errorInfo(probe.error) : null, stdout:text(out), stderr:text(probe.stderr),
      mapping:mapping ? rootState(mapping) : null,
      rosterIncomplete:roster.incomplete, rosterOmission:roster.omission ?? roster.reason ?? null,
      matchingAllocatedRoots:mapping ? roster.roots.filter(row => path.resolve(row.root).toLowerCase() === path.resolve(mapping).toLowerCase()) : [],
      binaries:[fileIdentity(shell), ...dirs.flatMap(dir => [fileIdentity(path.join(dir,"bash.exe")),fileIdentity(path.join(dir,"msys-2.0.dll"))])],
      runtimeIdentityLimit:"DLL paths are adjacent installed candidates, not a loaded-module trace; uname is from the post-shell probe.",
      mounts:dirs.flatMap(dir => { const etc=path.resolve(dir, path.basename(path.dirname(dir)).toLowerCase() === "usr" ? "../../etc" : "../etc"); return [fstab(path.join(etc,"fstab")),
        ...(user && /^[A-Za-z0-9._-]+$/.test(user) ? [fstab(path.join(etc,"fstab.d",user))] : [])]; })});
  } catch (error) { captureRecord("post-shell-capture-error", {error:errorInfo(error)}); }
}
