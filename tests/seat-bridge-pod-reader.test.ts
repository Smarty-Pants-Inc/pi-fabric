import { createHash } from "node:crypto";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { podJournalArgv, podOwnerArgv } from "../src/seat-bridge/pod-journal.js";
import { procStartTime } from "../src/main-drain/binding.js";

// Trust boundary: execute the exact fixed Python argv, not a mock of Python, against real files and /proc.
// The independent image harness replays these same argv on the pinned seat image; this suite needs only Linux/Python.
const roots: string[] = [];
const children: ChildProcess[] = [];
afterEach(async () => {
  await Promise.all(children.splice(0).map(child => new Promise<void>(resolve => {
    child.once("exit", () => resolve()); child.kill();
  })));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const sha = (bytes: string | Buffer) => createHash("sha256").update(bytes).digest("hex");
const temp = () => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "b5-reader-")); roots.push(root); return root; };
const exec = (argv: string[]) => {
  const result = spawnSync(argv[0]!, argv.slice(1), { encoding: "utf8" });
  return { code: result.status, stderr: result.stderr, value: result.stdout ? JSON.parse(result.stdout) : null };
};
const sessionId = "00000000-0000-4000-8000-00000000d3d1";
const rootId = `session:${sessionId}`;
const incarnation = "00000000-0000-4000-8000-0000000001d1";

const owner = async (title = "pi") => {
  const root = temp();
  const child = spawn(process.execPath, ["-e", `process.title=${JSON.stringify(title)}; process.stdout.write('ready\\n'); setInterval(()=>{},1000)`],
    { stdio: ["ignore", "pipe", "pipe"] });
  children.push(child);
  await new Promise<void>((resolve, reject) => { child.stdout!.once("data", () => resolve()); child.once("error", reject); });
  const hash = sha(rootId);
  const entry = { format: 1, key: `topology/participants/${hash}`, value: { format: 1, id: rootId, kind: "root", rootId,
    sessionId, ownerHostId: rootId, ownerIdentityId: rootId, ownerIncarnation: incarnation,
    mainProcess: { pid: child.pid!, host: os.hostname(), startTime: procStartTime(child.pid!) } },
    natsProjection: { format: 1, machine: "sp-org-d3synth", kvKey: `sp-org-d3synth.p.${hash.slice(0, 32)}`, owner: `${rootId}#${incarnation}`,
      incarnation, revision: 1 } };
  const file = path.join(root, "participants", `${hash}.json`);
  fs.mkdirSync(path.dirname(file));
  const save = () => fs.writeFileSync(file, JSON.stringify(entry));
  save();
  return { root, entry, save };
};

describe.skipIf(process.platform !== "linux")("b5 fixed in-image read-only argv", () => {
  it("reads exact prefix/whole hashes, canonical suffix and stat mtime from a path with shell characters", () => {
    const root = temp();
    const file = path.join(root, "journal ';$(touch NOT_EXECUTED).jsonl");
    const prefix = '{"type":"session","version":3,"id":"synthetic"}\n';
    const suffix = '{"type":"thinking_level_change","id":"e2","parentId":"e1","thinkingLevel":"high"}\n';
    fs.writeFileSync(file, prefix + suffix);
    expect(exec(podJournalArgv(file, Buffer.byteLength(prefix)))).toEqual({ code: 0, stderr: "", value: {
      size: Buffer.byteLength(prefix + suffix), sha256: sha(prefix + suffix), prefixSha256: sha(prefix),
      suffixBase64: Buffer.from(suffix).toString("base64"), mtimeNs: fs.statSync(file, { bigint: true }).mtimeNs.toString() } });
    expect(fs.readdirSync(root)).toEqual([path.basename(file)]);
  });

  it.each([65536, 65537])("suffix length %i enforces the 64 KiB byte limit before base64", size => {
    const file = path.join(temp(), "journal.jsonl");
    fs.writeFileSync(file, "x" + "y".repeat(size));
    const result = exec(podJournalArgv(file, 1));
    if (size === 65536) {
      expect(result).toMatchObject({ code: 0, stderr: "" });
      expect(Buffer.from(result.value.suffixBase64, "base64")).toHaveLength(size);
    } else expect(result).toEqual({ code: 1, stderr: "", value: { error: "journal-suffix-too-large" } });
  });

  it("refuses a shortened file and a symlink, and never writes to the file", () => {
    const root = temp();
    const file = path.join(root, "journal"); const link = path.join(root, "link");
    fs.writeFileSync(file, "sealed"); fs.symlinkSync(file, link);
    const before = fs.statSync(file);
    expect(exec(podJournalArgv(file, 7))).toMatchObject({ code: 1, value: { error: "journal-prefix-mismatch" } });
    expect(exec(podJournalArgv(link, 1))).toMatchObject({ code: 1, value: { error: "journal-unreadable" } });
    expect(fs.readFileSync(file, "utf8")).toBe("sealed"); expect(fs.statSync(file).mtimeMs).toBe(before.mtimeMs);
  });

  it("binds the image's projected NATS incarnation to the Pod-local live pi PID/start time without cmdline", async () => {
    const w = await owner();
    expect(exec(podOwnerArgv(w.root, sessionId))).toEqual({ code: 0, stderr: "", value: {
      rootId, ownerHostId: rootId, ownerIdentityId: rootId, ownerIncarnation: incarnation, machine: "sp-org-d3synth",
      pid: w.entry.value.mainProcess.pid, processStartedAt: w.entry.value.mainProcess.startTime,
      projectionKey: w.entry.natsProjection.kvKey, projectionRevision: 1 } });
  });

  it.each(["start time", "host", "marker incarnation", "identity", "root"])("refuses a local projection with wrong %s", async fault => {
    const w = await owner();
    if (fault === "start time") w.entry.value.mainProcess.startTime = "1";
    if (fault === "host") w.entry.value.mainProcess.host = "other-pod";
    if (fault === "marker incarnation") w.entry.natsProjection.incarnation = "other";
    if (fault === "identity") w.entry.value.ownerIdentityId = "other";
    if (fault === "root") w.entry.value.rootId = "other";
    w.save();
    expect(exec(podOwnerArgv(w.root, sessionId))).toMatchObject({ code: 1, stderr: "", value: { error: expect.any(String) } });
  });

  it("refuses a live non-pi process even with a matching PID/start time", async () => {
    const w = await owner("not-pi");
    expect(exec(podOwnerArgv(w.root, sessionId))).toEqual({ code: 1, stderr: "", value: { error: "pod-owner-process-mismatch" } });
  });
});
