import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { execFileSync, spawnSync } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CompletionJournal, completionConsumed, consumeCompletion, pendingCompletions, sameArchiveInode, saveCompletion, type CompletionRecipient } from "../src/agents/completion-journal.js";
import { syncPathNamespace, syncPathNamespaceAsync } from "../src/core/atomic-write.js";
import type { AgentRunResult } from "../src/agents/types.js";
import type { MeshStore } from "../src/mesh/store.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

const roots: string[] = [];
const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
afterEach(() => {
  Object.defineProperty(process, "platform", platform);
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "completion-archive-")); roots.push(root);
  const meshRoot = path.join(root, "mesh");
  const recipient: CompletionRecipient = { rootId: "session:main", sessionId: "main", projectRoot: root, cwd: root, name: "main", startedAt: 1 };
  const directory = path.join(meshRoot, "agent-completions");
  const name = (id: string) => `${createHash("sha256").update(id).digest("hex")}.json`;
  const file = (id: string) => path.join(directory, name(id));
  const archive = (id: string) => path.join(directory, "archive", name(id));
  const receipt = (id: string) => path.join(directory, "receipts", name(id));
  const result = (index: number): AgentRunResult => ({ id: index.toString(16).padStart(32, "0"), name: "task", task: "synthetic", status: "completed", runner: "pi", transport: "process", cwd: root, text: "retained result", startedAt: 1, updatedAt: 2, finishedAt: 2, turns: 1, toolCalls: 0, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } });
  const seed = (index: number) => { const value = result(index); saveCompletion(meshRoot, recipient, value); return value; };
  const entries = new Map<string, any>();
  const mesh = { listAll: () => [...entries.values()], get: (key: string) => entries.get(key), put: async (args: any) => { entries.set(args.key, { key: args.key, value: args.value, updatedBy: args.identity, version: 1 }); }, delete: async (args: any) => { entries.delete(args.key); } } as unknown as MeshStore;
  const journal = (enqueue = vi.fn(), address = recipient) => new CompletionJournal(meshRoot, address, { list: () => [] } as unknown as FabricParticipantSource, mesh, enqueue);
  return { root, meshRoot, recipient, directory, file, archive, receipt, result, seed, mesh, journal };
};
const crashBeforeArchive = (source: string) => {
  const method = process.platform === "win32" ? "linkSync" : "renameSync";
  const publish = fs[method];
  const target = path.join(path.dirname(source), "archive", path.basename(source));
  return vi.spyOn(fs, method).mockImplementation((from, to) => {
    if (String(from) === source && String(to) === target) throw Object.assign(new Error("crash before archive"), { code: "EIO" });
    publish(from, to);
  });
};
const noDrainSync = () => {
  const sync = vi.spyOn(fs, "fsyncSync").mockImplementation(() => { throw new Error("drain must not fsync"); });
  const open = fs.promises.open;
  const handles: ReturnType<typeof vi.spyOn>[] = [];
  vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
    const handle = await open(...args);
    handles.push(vi.spyOn(handle, "sync").mockRejectedValue(new Error("drain must not sync")));
    return handle;
  });
  return { sync, handles };
};

describe("completion archive stable identity", () => {
  it("uses exact bigint IDs above 2^53 despite NTFS ctime and nlink changes", () => {
    Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    const source = { dev: 9n, ino: 2n ** 53n, size: 4096n, ctimeMs: 1n, birthtimeMs: 1n, nlink: 1n };
    const linked = { ...source, ctimeMs: 2n, birthtimeMs: 3n, nlink: 3n };
    expect(sameArchiveInode(source, linked)).toBe(true);
    const different = { ...linked, ino: source.ino + 1n };
    expect(Number(source.ino)).toBe(Number(different.ino)); // Rounded IDs would alias.
    expect(sameArchiveInode(source, different)).toBe(false);
    expect(sameArchiveInode(source, { ...linked, dev: 10n })).toBe(false);
    expect(sameArchiveInode(source, { ...linked, size: 4097n })).toBe(false);
  });

  it.each(["sync", "async"] as const)("binds bigint archive identities during %s namespace confirmation", async mode => {
    const h = setup(); const result = h.seed(1); const file = h.file(result.id);
    const identity = fs.statSync(file, { bigint: true });
    Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    const replacement = { ...identity, ino: identity.ino + 1n };
    if (mode === "sync") {
      expect(() => syncPathNamespace(file, identity)).not.toThrow();
      expect(() => syncPathNamespace(file, replacement)).toThrow("receipt inode changed");
    } else {
      await expect(syncPathNamespaceAsync(file, identity)).resolves.toBeUndefined();
      await expect(syncPathNamespaceAsync(file, replacement)).rejects.toThrow("receipt inode changed");
    }
  });

  it("keeps POSIX numeric dev/ino identity independent of size and link metadata", () => {
    Object.defineProperty(process, "platform", { ...platform, value: "linux" });
    const source = { dev: 9, ino: 10, size: 4096, ctimeMs: 1, nlink: 1 };
    const linked = { ...source, size: 4097, ctimeMs: 2, nlink: 3 };
    expect(sameArchiveInode(source, linked)).toBe(true);
    expect(sameArchiveInode(source, { ...source, ino: 11 })).toBe(false);
    expect(sameArchiveInode(source, { ...source, dev: 10 })).toBe(false);
  });
});

describe("completion receipt-time archive", () => {
  it("archives on receipt, preserving body and inode, only after receipt barriers", () => {
    const h = setup(); const result = h.seed(1); const body = fs.readFileSync(h.file(result.id), "utf8"); const before = fs.statSync(h.file(result.id));
    const rename = fs.renameSync; const sync = fs.fsyncSync; let receiptRenamed = false; let receiptSynced = false;
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => { sync(fd); if (receiptRenamed && fs.fstatSync(fd).isDirectory()) receiptSynced = true; });
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(to) === h.receipt(result.id)) receiptRenamed = true;
      if (String(from) === h.file(result.id)) { expect(receiptSynced || process.platform === "win32").toBe(true); expect(completionConsumed(h.meshRoot, result.id)).toBe(true); }
      rename(from, to);
    });
    if (process.platform === "win32") {
      const link = fs.linkSync;
      vi.spyOn(fs, "linkSync").mockImplementation((from, to) => {
        if (String(to) === h.archive(result.id)) {
          expect(receiptRenamed).toBe(true); expect(completionConsumed(h.meshRoot, result.id)).toBe(true);
        }
        link(from, to);
      });
    }
    consumeCompletion(h.meshRoot, result.id, "main");
    expect(fs.existsSync(h.file(result.id))).toBe(false);
    expect(fs.readFileSync(h.archive(result.id), "utf8")).toBe(body);
    expect(fs.statSync(h.archive(result.id))).toMatchObject({ dev: before.dev, ino: before.ino });
    expect(JSON.parse(fs.readFileSync(h.receipt(result.id), "utf8"))).toMatchObject({ id: result.id, sessionId: "main" });
    expect(pendingCompletions(h.meshRoot, h.root)).toEqual([]);
  });

  it("does not archive when receipt durability fails", () => {
    const h = setup(); const result = h.seed(1);
    vi.spyOn(fs, "fsyncSync").mockImplementation(() => { throw new Error("receipt barrier failed"); });
    expect(() => consumeCompletion(h.meshRoot, result.id, "main")).toThrow("receipt barrier failed");
    expect(fs.existsSync(h.file(result.id))).toBe(true); expect(fs.existsSync(h.archive(result.id))).toBe(false);
  });

  it.skipIf(process.platform === "win32").each(["archive", "source"] as const)("confirms the synchronous archive namespace at the %s directory before returning", barrier => {
    const h = setup(); const result = h.seed(1); const before = fs.statSync(h.file(result.id));
    const body = fs.readFileSync(h.file(result.id), "utf8");
    const rename = fs.renameSync; const open = fs.openSync; const sync = fs.fsyncSync;
    const descriptors = new Map<number, string>(); let archived = false;
    vi.spyOn(fs, "openSync").mockImplementation((...args) => { const fd = open(...args); descriptors.set(fd, String(args[0])); return fd; });
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => { rename(from, to); if (String(from) === h.file(result.id)) archived = true; });
    const failure = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (archived && descriptors.get(fd) === (barrier === "archive" ? path.dirname(h.archive(result.id)) : h.directory)) {
        throw new Error(`${barrier} archive barrier failed`);
      }
      sync(fd);
    });
    expect(() => consumeCompletion(h.meshRoot, result.id, "main")).toThrow(`${barrier} archive barrier failed`);
    expect(archived).toBe(true); expect(completionConsumed(h.meshRoot, result.id)).toBe(true);
    const receipt = fs.readFileSync(h.receipt(result.id), "utf8"); failure.mockRestore();
    const confirmed = new Set<string>();
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => { sync(fd); confirmed.add(descriptors.get(fd)!); });
    consumeCompletion(h.meshRoot, result.id, "main");
    expect(confirmed.has(path.dirname(h.archive(result.id)))).toBe(true); expect(confirmed.has(h.directory)).toBe(true);
    expect(fs.statSync(h.archive(result.id))).toMatchObject({ dev: before.dev, ino: before.ino });
    expect(fs.readFileSync(h.archive(result.id), "utf8")).toBe(body); expect(fs.readFileSync(h.receipt(result.id), "utf8")).toBe(receipt);
  });

  it.skipIf(process.platform === "win32").each(
    [false, true].flatMap(claim => ["archive", "source"].flatMap(barrier => [false, true].map(restored => ({ claim, barrier, restored })))),
  )("retains recovery evidence until the archive namespace barrier ($barrier, claim=$claim, restored=$restored)", async ({ claim, barrier, restored }) => {
    const h = setup(); const result = h.seed(1); const enqueue = vi.fn();
    const attempt = path.join(h.directory, "attempts", path.basename(h.file(result.id)));
    fs.mkdirSync(path.dirname(attempt)); fs.copyFileSync(h.file(result.id), attempt);
    if (claim) await h.journal().drain(false);
    const crash = crashBeforeArchive(h.file(result.id));
    expect(() => consumeCompletion(h.meshRoot, result.id, "main")).toThrow("crash before archive"); crash.mockRestore();
    const receipt = fs.readFileSync(h.receipt(result.id), "utf8"); const body = fs.readFileSync(h.file(result.id), "utf8");
    const before = fs.statSync(h.file(result.id)); const open = fs.promises.open; const rename = fs.promises.rename;
    let blocked = true; let moves = 0; let archiveSynced = false; let sourceSynced = false;
    vi.spyOn(fs.promises, "rename").mockImplementation(async (from, to) => { await rename(from, to); if (String(from) === h.file(result.id)) moves++; });
    vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
      const handle = await open(...args); const sync = handle.sync.bind(handle); const directory = String(args[0]);
      vi.spyOn(handle, "sync").mockImplementation(async () => {
        if (moves && blocked && directory === (barrier === "archive" ? path.dirname(h.archive(result.id)) : h.directory)) {
          throw new Error(`${barrier} archive barrier failed`);
        }
        await sync();
        if (directory === path.dirname(h.archive(result.id))) archiveSynced = true;
        if (directory === h.directory && archiveSynced) sourceSynced = true;
      });
      return handle;
    });
    const remove = h.mesh.delete.bind(h.mesh); const deletion = vi.spyOn(h.mesh, "delete");
    await expect(h.journal(enqueue).drain()).rejects.toThrow(`${barrier} archive barrier failed`);
    expect(moves).toBe(1); expect(deletion).not.toHaveBeenCalled(); expect(fs.existsSync(attempt)).toBe(true);
    expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(claim ? 1 : 0);
    if (restored) fs.renameSync(h.archive(result.id), h.file(result.id)); // Crash rolls back the unsynced rename.
    blocked = false; archiveSynced = false; sourceSynced = false;
    deletion.mockImplementation(async args => {
      expect(archiveSynced && sourceSynced).toBe(true); return await remove(args);
    });
    const rm = fs.promises.rm;
    vi.spyOn(fs.promises, "rm").mockImplementation(async (...args) => {
      if (String(args[0]) === attempt) expect(archiveSynced && sourceSynced).toBe(true);
      await rm(...args);
    });
    const recovered = h.journal(enqueue); await recovered.drain(); await recovered.drain();
    expect(archiveSynced && sourceSynced).toBe(true); expect(moves).toBe(restored ? 2 : 1);
    expect(deletion).toHaveBeenCalledTimes(claim ? 1 : 0); expect(enqueue).not.toHaveBeenCalled();
    expect(fs.existsSync(attempt)).toBe(false); expect(fs.existsSync(h.file(result.id))).toBe(false);
    expect(fs.statSync(h.archive(result.id))).toMatchObject({ dev: before.dev, ino: before.ino });
    expect(fs.readFileSync(h.archive(result.id), "utf8")).toBe(body); expect(fs.readFileSync(h.receipt(result.id), "utf8")).toBe(receipt);
    expect(recovered.result(result.id)).toMatchObject({ id: result.id, text: result.text }); expect(recovered.pending()).toEqual([]);
    expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(0);
  });

  it.skipIf(process.platform === "win32").each([false, true])("a fresh process crashes between archive rename and fsync, then recovers exactly once (restored=%s)", async restored => {
    const h = setup(); const result = h.seed(1); await h.journal().drain(false);
    const attempt = path.join(h.directory, "attempts", path.basename(h.file(result.id)));
    fs.mkdirSync(path.dirname(attempt)); fs.copyFileSync(h.file(result.id), attempt);
    const crash = crashBeforeArchive(h.file(result.id));
    expect(() => consumeCompletion(h.meshRoot, result.id, "main")).toThrow("crash before archive"); crash.mockRestore();
    const receipt = fs.readFileSync(h.receipt(result.id), "utf8");
    const claims = path.join(h.root, "claims.json"); fs.writeFileSync(claims, JSON.stringify(h.mesh.listAll("residency/completion-claims/")));
    const script = (crash: boolean) => `import fs from 'node:fs'; import {CompletionJournal} from ${JSON.stringify(path.resolve("src/agents/completion-journal.ts"))};
      const claims=${JSON.stringify(claims)}, source=${JSON.stringify(h.file(result.id))}, archive=${JSON.stringify(path.dirname(h.archive(result.id)))}, parent=${JSON.stringify(h.directory)}, attempt=${JSON.stringify(attempt)};
      const entries=new Map(JSON.parse(fs.readFileSync(claims,'utf8')).map(entry=>[entry.key,entry]));
      let archiveSynced=false,sourceSynced=false,moves=0,deletes=0;
      const open=fs.promises.open,rename=fs.promises.rename,rm=fs.promises.rm;
      fs.promises.open=async (...args)=>{const handle=await open(...args),sync=handle.sync.bind(handle);
        handle.sync=async()=>{await sync();if(String(args[0])===archive) archiveSynced=true;if(String(args[0])===parent&&archiveSynced) sourceSynced=true;};return handle;};
      fs.promises.rename=async(from,to)=>{await rename(from,to);if(String(from)===source){moves++;if(${crash}) process.exit(86);}};
      fs.promises.rm=async(...args)=>{if(String(args[0])===attempt&&!(archiveSynced&&sourceSynced)) throw new Error('early attempt removal');await rm(...args);};
      const mesh={listAll:()=>[...entries.values()],get:key=>entries.get(key),delete:async args=>{
        if(!${crash}&&!(archiveSynced&&sourceSynced)) throw new Error('early claim retirement');
        entries.delete(args.key);deletes++;fs.writeFileSync(claims,JSON.stringify([...entries.values()]));}};
      const journal=new CompletionJournal(${JSON.stringify(h.meshRoot)},${JSON.stringify(h.recipient)},{list:()=>[]},mesh,()=>{throw new Error('redelivered');});
      await journal.drain();await journal.drain();console.log(JSON.stringify({moves,deletes,archiveSynced,sourceSynced,text:journal.result(${JSON.stringify(result.id)})?.text,pending:journal.pending().length}));`;
    const interrupted = spawnSync("bun", ["--eval", script(true)], { encoding: "utf8", timeout: 15_000 });
    expect(interrupted.error).toBeUndefined(); expect(interrupted.status, interrupted.stderr).toBe(86);
    expect(JSON.parse(fs.readFileSync(claims, "utf8"))).toHaveLength(1); expect(fs.existsSync(attempt)).toBe(true);
    if (restored) fs.renameSync(h.archive(result.id), h.file(result.id));
    const recovered = JSON.parse(execFileSync("bun", ["--eval", script(false)], { encoding: "utf8", timeout: 15_000 }));
    expect(recovered).toEqual({ moves: restored ? 1 : 0, deletes: 1, archiveSynced: true, sourceSynced: true, text: result.text, pending: 0 });
    expect(JSON.parse(fs.readFileSync(claims, "utf8"))).toEqual([]); expect(fs.existsSync(attempt)).toBe(false);
    expect(fs.existsSync(h.file(result.id))).toBe(false); expect(fs.existsSync(h.archive(result.id))).toBe(true);
    expect(fs.readFileSync(h.receipt(result.id), "utf8")).toBe(receipt);
  });

  it.skipIf(process.platform === "win32").each(["visible archive", "restored source", "lost rename names", "duplicate names"] as const)("recovers claimless archive evidence in a fresh process: %s", layout => {
    const h = setup(); const result = h.seed(1); const rename = fs.renameSync; const sync = fs.fsyncSync;
    const before = fs.statSync(h.file(result.id)); const body = fs.readFileSync(h.file(result.id), "utf8");
    const evidence = path.join(h.directory, "archive-pending", path.basename(h.file(result.id))); let moved = false;
    const renames = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => { rename(from, to); if (String(from) === h.file(result.id)) moved = true; });
    const failure = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (moved && fs.fstatSync(fd).isDirectory()) throw new Error("crash after archive rename"); sync(fd);
    });
    expect(() => consumeCompletion(h.meshRoot, result.id, "main")).toThrow("crash after archive rename");
    renames.mockRestore(); failure.mockRestore();
    expect(fs.statSync(evidence)).toMatchObject({ dev: before.dev, ino: before.ino });
    expect(fs.readFileSync(evidence, "utf8")).toBe(body); expect(h.mesh.listAll("residency/completion-claims/")).toEqual([]);
    if (layout === "restored source") fs.renameSync(h.archive(result.id), h.file(result.id));
    if (layout === "lost rename names") fs.rmSync(h.archive(result.id));
    if (layout === "duplicate names") fs.linkSync(h.archive(result.id), h.file(result.id));
    const script = `import fs from 'node:fs'; import {CompletionJournal} from ${JSON.stringify(path.resolve("src/agents/completion-journal.ts"))};
      const open=fs.promises.open,rename=fs.promises.rename,rm=fs.promises.rm; let moves=0,archiveSynced=false,sourceSynced=false;
      fs.promises.open=async(...args)=>{const handle=await open(...args),sync=handle.sync.bind(handle);handle.sync=async()=>{
        await sync();if(String(args[0])===${JSON.stringify(path.dirname(h.archive(result.id)))}) archiveSynced=true;
        if(String(args[0])===${JSON.stringify(h.directory)}&&archiveSynced) sourceSynced=true;};return handle;};
      fs.promises.rename=async(from,to)=>{await rename(from,to);if(String(from)===${JSON.stringify(h.file(result.id))}) moves++;};
      fs.promises.rm=async(...args)=>{if(String(args[0])===${JSON.stringify(evidence)}&&!(archiveSynced&&sourceSynced)) throw new Error('early evidence removal');await rm(...args);};
      const journal=new CompletionJournal(${JSON.stringify(h.meshRoot)},${JSON.stringify(h.recipient)},{list:()=>[]},{listAll:()=>[],get:()=>undefined},()=>{throw new Error('redelivered');});
      await journal.drain();await journal.drain();console.log(JSON.stringify({moves,archiveSynced,sourceSynced,text:journal.result(${JSON.stringify(result.id)})?.text,pending:journal.pending().length}));`;
    expect(JSON.parse(execFileSync("bun", ["--eval", script], { encoding: "utf8", timeout: 15_000 }))).toEqual({
      moves: layout === "visible archive" ? 0 : 1, archiveSynced: true, sourceSynced: true, text: result.text, pending: 0,
    });
    expect(fs.existsSync(evidence)).toBe(false); expect(fs.existsSync(h.file(result.id))).toBe(false);
    expect(fs.statSync(h.archive(result.id))).toMatchObject({ dev: before.dev, ino: before.ino });
    expect(fs.readFileSync(h.archive(result.id), "utf8")).toBe(body);
  });

  it.skipIf(process.platform === "win32").each([false, true])("re-syncs a post-rename receipt before recovery cleanup, claim=%s", async claim => {
    const h = setup(); const result = h.seed(1); const enqueue = vi.fn();
    const attempt = path.join(h.directory, "attempts", path.basename(h.file(result.id)));
    fs.mkdirSync(path.dirname(attempt)); fs.copyFileSync(h.file(result.id), attempt);
    if (claim) await h.journal(enqueue).drain();
    const rename = fs.renameSync; const sync = fs.fsyncSync; let receiptRenamed = false;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      rename(from, to); if (String(to) === h.receipt(result.id)) receiptRenamed = true;
    });
    const failure = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (receiptRenamed && fs.fstatSync(fd).isDirectory()) throw new Error("receipt directory sync failed");
      sync(fd);
    });
    expect(() => consumeCompletion(h.meshRoot, result.id, "main")).toThrow("receipt directory sync failed");
    expect(receiptRenamed).toBe(true); expect(fs.existsSync(h.file(result.id))).toBe(true);
    expect(fs.existsSync(h.archive(result.id))).toBe(false);
    const before = fs.readFileSync(h.receipt(result.id), "utf8"); const stat = fs.statSync(h.receipt(result.id));
    failure.mockRestore();
    const recovered = h.journal(enqueue); const open = fs.promises.open;
    let blocked = true; let receiptDirectorySynced = false; let archives = 0;
    vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
      const handle = await open(...args); const syncHandle = handle.sync.bind(handle);
      if (String(args[0]) === path.dirname(h.receipt(result.id))) vi.spyOn(handle, "sync").mockImplementation(async () => {
        if (blocked) throw new Error("recovery directory sync failed");
        await syncHandle(); receiptDirectorySynced = true;
      });
      return handle;
    });
    const renameAsync = fs.promises.rename;
    vi.spyOn(fs.promises, "rename").mockImplementation(async (from, to) => {
      if (String(from) === h.file(result.id)) { expect(receiptDirectorySynced).toBe(true); archives++; }
      await renameAsync(from, to);
    });
    const removeClaim = vi.spyOn(h.mesh, "delete");
    await expect(recovered.drain()).rejects.toThrow("recovery directory sync failed");
    expect(removeClaim).not.toHaveBeenCalled(); expect(fs.existsSync(h.file(result.id))).toBe(true);
    expect(fs.existsSync(attempt)).toBe(true);
    expect(fs.existsSync(h.archive(result.id))).toBe(false);
    expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(claim ? 1 : 0);
    blocked = false; await recovered.drain(); await recovered.drain();
    expect(archives).toBe(1); expect(enqueue).toHaveBeenCalledTimes(claim ? 1 : 0);
    expect(fs.existsSync(attempt)).toBe(false);
    expect(fs.existsSync(h.file(result.id))).toBe(false); expect(fs.existsSync(h.archive(result.id))).toBe(true);
    expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(0);
    expect(fs.readFileSync(h.receipt(result.id), "utf8")).toBe(before);
    expect(fs.statSync(h.receipt(result.id))).toMatchObject({ ino: stat.ino, mtimeMs: stat.mtimeMs });
  });

  it.skipIf(process.platform === "win32")("a fresh process re-syncs a failed receipt rename before exactly one archive", () => {
    const h = setup(); const result = h.seed(1); const rename = fs.renameSync; const sync = fs.fsyncSync; let renamed = false;
    const renames = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      rename(from, to); if (String(to) === h.receipt(result.id)) renamed = true;
    });
    const failure = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (renamed && fs.fstatSync(fd).isDirectory()) throw new Error("post-rename sync failed");
      sync(fd);
    });
    expect(() => consumeCompletion(h.meshRoot, result.id, "main")).toThrow("post-rename sync failed");
    failure.mockRestore(); renames.mockRestore();
    const receipt = fs.readFileSync(h.receipt(result.id), "utf8");
    const script = `import fs from 'node:fs'; import {CompletionJournal} from ${JSON.stringify(path.resolve("src/agents/completion-journal.ts"))};
      const open=fs.promises.open, rename=fs.promises.rename; let synced=false, archives=0;
      fs.promises.open=async (...args)=>{const handle=await open(...args); const sync=handle.sync.bind(handle);
        if(String(args[0])===${JSON.stringify(path.dirname(h.receipt(result.id)))}) handle.sync=async()=>{await sync(); synced=true;}; return handle;};
      fs.promises.rename=async (from,to)=>{if(String(from)===${JSON.stringify(h.file(result.id))}){if(!synced) throw new Error('unconfirmed receipt'); archives++;} await rename(from,to);};
      const journal=new CompletionJournal(${JSON.stringify(h.meshRoot)},${JSON.stringify(h.recipient)},{list:()=>[]},{listAll:()=>[],get:()=>undefined},()=>{throw new Error('redelivered');});
      await journal.drain(); await journal.drain(); console.log(JSON.stringify({synced,archives}));`;
    expect(JSON.parse(execFileSync("bun", ["--eval", script], { encoding: "utf8", timeout: 15_000 }))).toEqual({ synced: true, archives: 1 });
    expect(fs.existsSync(h.file(result.id))).toBe(false); expect(fs.existsSync(h.archive(result.id))).toBe(true);
    expect(fs.readFileSync(h.receipt(result.id), "utf8")).toBe(receipt);
  });

  it.each([false, true])("recovers receipt-before-rename crash with async confirmation, claim=%s", async claim => {
    const h = setup(); const result = h.seed(1); const journal = h.journal();
    if (claim) await journal.drain(false);
    const crash = crashBeforeArchive(h.file(result.id));
    expect(() => consumeCompletion(h.meshRoot, result.id, "main")).toThrow("crash before archive"); crash.mockRestore();
    const before = fs.readFileSync(h.receipt(result.id), "utf8"); const mtime = fs.statSync(h.receipt(result.id)).mtimeMs;
    const enqueue = vi.fn(); const sync = vi.spyOn(fs, "fsyncSync"); const open = vi.spyOn(fs.promises, "open");
    await h.journal(enqueue).drain(); await h.journal(enqueue).drain();
    expect(sync).not.toHaveBeenCalled(); expect(open.mock.calls.filter(([file]) => String(file) === h.receipt(result.id))).toHaveLength(1);
    expect(enqueue).not.toHaveBeenCalled(); expect(fs.existsSync(h.file(result.id))).toBe(false); expect(fs.existsSync(h.archive(result.id))).toBe(true);
    expect(fs.readFileSync(h.receipt(result.id), "utf8")).toBe(before); expect(fs.statSync(h.receipt(result.id)).mtimeMs).toBe(mtime);
    expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(0);
  });

  it("synchronous pending discovery skips crash-left receipts without replay or fsync", () => {
    const h = setup(); const result = h.seed(1); const crash = crashBeforeArchive(h.file(result.id));
    expect(() => consumeCompletion(h.meshRoot, result.id, "main")).toThrow(); crash.mockRestore();
    const sync = vi.spyOn(fs, "fsyncSync");
    expect(pendingCompletions(h.meshRoot, h.root)).toEqual([]); expect(sync).not.toHaveBeenCalled(); expect(fs.existsSync(h.file(result.id))).toBe(true);
  });

  it.each(["native", "Windows-exclusive"] as const)("concurrent recovery has exactly one winning publication (%s)", async mode => {
    const h = setup(); const result = h.seed(1); const crash = crashBeforeArchive(h.file(result.id));
    const body = fs.readFileSync(h.file(result.id), "utf8"); const before = fs.statSync(h.file(result.id));
    expect(() => consumeCompletion(h.meshRoot, result.id, "main")).toThrow(); crash.mockRestore();
    const receipt = fs.readFileSync(h.receipt(result.id), "utf8"); const receiptStat = fs.statSync(h.receipt(result.id));
    // Exercise Windows' exclusive branch on POSIX too; the native filesystem is unchanged.
    if (mode === "Windows-exclusive") Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    const method = process.platform === "win32" ? "link" : "rename";
    const publish = fs.promises[method]; let arrivals = 0; let wins = 0; let losses = 0; let release!: () => void;
    const barrier = new Promise<void>(resolve => { release = resolve; });
    vi.spyOn(fs.promises, method).mockImplementation(async (from, to) => {
      if (String(from) !== h.file(result.id) || String(to) !== h.archive(result.id)) return publish(from, to);
      if (++arrivals === 2) release(); await barrier;
      try { await publish(from, to); wins++; }
      catch (error) {
        // A link loser may observe the winner's target or its already-removed source.
        if ((method === "link" ? ["EEXIST", "ENOENT"] : ["ENOENT"]).includes((error as NodeJS.ErrnoException).code ?? "")) losses++;
        throw error;
      }
    });
    const enqueue = vi.fn(); const journals = [h.journal(enqueue), h.journal(enqueue)];
    await Promise.all(journals.map(journal => journal.drain()));
    expect({ wins, losses }).toEqual({ wins: 1, losses: 1 }); expect(enqueue).not.toHaveBeenCalled();
    expect(fs.readdirSync(path.dirname(h.archive(result.id)))).toEqual([path.basename(h.file(result.id))]);
    expect(fs.existsSync(h.file(result.id))).toBe(false);
    expect(fs.existsSync(path.join(h.directory, "archive-pending", path.basename(h.file(result.id))))).toBe(false);
    expect(fs.readFileSync(h.archive(result.id), "utf8")).toBe(body);
    expect(fs.statSync(h.archive(result.id))).toMatchObject({ dev: before.dev, ino: before.ino });
    expect(fs.readFileSync(h.receipt(result.id), "utf8")).toBe(receipt);
    expect(fs.statSync(h.receipt(result.id))).toMatchObject({ ino: receiptStat.ino, mtimeMs: receiptStat.mtimeMs });
    for (const journal of journals) { expect(journal.pending()).toEqual([]); expect(journal.result(result.id)).toMatchObject({ text: result.text }); }
  });

  it.each(["sync", "async"] as const)("Windows resumes a crash after exclusive publication without replacing the archive (%s)", async mode => {
    const h = setup(); const result = h.seed(1); const body = fs.readFileSync(h.file(result.id), "utf8");
    Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    const remove = fs.rmSync;
    const crash = vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
      if (String(file) === h.file(result.id)) throw new Error("crash after exclusive publication");
      remove(file, options);
    });
    expect(() => consumeCompletion(h.meshRoot, result.id, "main")).toThrow("crash after exclusive publication"); crash.mockRestore();
    const before = fs.statSync(h.archive(result.id)); const receipt = fs.readFileSync(h.receipt(result.id), "utf8");
    expect(fs.statSync(h.file(result.id))).toMatchObject({ dev: before.dev, ino: before.ino });
    const enqueue = vi.fn(); const journal = h.journal(enqueue);
    if (mode === "sync") consumeCompletion(h.meshRoot, result.id, "other");
    else await journal.drain();
    expect(fs.readFileSync(h.archive(result.id), "utf8")).toBe(body);
    expect(fs.statSync(h.archive(result.id))).toMatchObject({ dev: before.dev, ino: before.ino, mtimeMs: before.mtimeMs });
    expect(fs.readFileSync(h.receipt(result.id), "utf8")).toBe(receipt);
    expect(fs.existsSync(h.file(result.id))).toBe(false);
    expect(fs.existsSync(path.join(h.directory, "archive-pending", path.basename(h.file(result.id))))).toBe(false);
    expect(enqueue).not.toHaveBeenCalled(); expect(journal.result(result.id)).toMatchObject({ text: result.text });
  });

  it.each(["sync", "async"] as const)("Windows refuses to replace a conflicting archive inode (%s)", async mode => {
    const h = setup(); const result = h.seed(1); const crash = crashBeforeArchive(h.file(result.id));
    expect(() => consumeCompletion(h.meshRoot, result.id, "main")).toThrow(); crash.mockRestore();
    fs.mkdirSync(path.dirname(h.archive(result.id)), { recursive: true });
    fs.writeFileSync(h.archive(result.id), "conflicting archive");
    Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    if (mode === "sync") expect(() => consumeCompletion(h.meshRoot, result.id, "main")).toThrow("Completion archive evidence changed");
    else await expect(h.journal().drain()).rejects.toThrow("Completion archive evidence changed");
    expect(fs.readFileSync(h.archive(result.id), "utf8")).toBe("conflicting archive");
    expect(fs.existsSync(h.file(result.id))).toBe(true);
    expect(fs.existsSync(path.join(h.directory, "archive-pending", path.basename(h.file(result.id))))).toBe(true);
    expect(completionConsumed(h.meshRoot, result.id)).toBe(true);
  });

  it("tolerates a consumer racing synchronous publication and keeps the first receipt", () => {
    const h = setup(); const result = h.seed(1); const method = process.platform === "win32" ? "linkSync" : "renameSync";
    const publish = fs[method]; let wins = 0;
    vi.spyOn(fs, method).mockImplementation((from, to) => {
      if (String(from) === h.file(result.id) && String(to) === h.archive(result.id)) {
        publish(from, to); wins++;
        // A sibling won after our path check but before our publication syscall.
        throw Object.assign(new Error("already archived"), { code: method === "linkSync" ? "EEXIST" : "ENOENT" });
      }
      publish(from, to);
    });
    expect(() => consumeCompletion(h.meshRoot, result.id, "main")).not.toThrow();
    const receipt = fs.readFileSync(h.receipt(result.id), "utf8");
    consumeCompletion(h.meshRoot, result.id, "other");
    expect(wins).toBe(1); expect(fs.readFileSync(h.receipt(result.id), "utf8")).toBe(receipt); expect(fs.existsSync(h.archive(result.id))).toBe(true);
  });

  it("does not touch any of 10,000 archived envelopes or receipts during repeated drains", async () => {
    const h = setup(); const archive = path.join(h.directory, "archive"); const receipts = path.join(h.directory, "receipts");
    fs.mkdirSync(archive, { recursive: true }); fs.mkdirSync(receipts);
    for (let index = 1; index <= 10_000; index++) {
      const result = h.result(index); fs.writeFileSync(h.archive(result.id), JSON.stringify({ format: 1, recipient: h.recipient, result }));
      fs.writeFileSync(h.receipt(result.id), JSON.stringify({ id: result.id, sessionId: "main", consumedAt: 1 }));
    }
    const historical = (value: unknown) => String(value) === archive || String(value).startsWith(archive + path.sep) || String(value) === receipts || String(value).startsWith(receipts + path.sep);
    const spies = [vi.spyOn(fs.promises, "readdir"), vi.spyOn(fs.promises, "open"), vi.spyOn(fs.promises, "readFile"), vi.spyOn(fs.promises, "stat"), vi.spyOn(fs, "readdirSync"), vi.spyOn(fs, "openSync"), vi.spyOn(fs, "readFileSync"), vi.spyOn(fs, "statSync")];
    const probes = noDrainSync(); const enqueue = vi.fn(); const journal = h.journal(enqueue);
    for (let index = 0; index < 5; index++) await journal.drain();
    for (const spy of spies) expect(spy.mock.calls.filter(args => historical(args[0]))).toHaveLength(0);
    expect(probes.sync).not.toHaveBeenCalled(); expect(enqueue).not.toHaveBeenCalled();
  }, 15_000);

  it("leaves unconsumed envelopes unchanged even alongside archived history", async () => {
    const h = setup(); const consumed = h.seed(1); consumeCompletion(h.meshRoot, consumed.id, "main"); const pending = h.seed(2);
    const before = fs.statSync(h.file(pending.id)); const body = fs.readFileSync(h.file(pending.id), "utf8"); const enqueue = vi.fn();
    await h.journal(enqueue).drain(false);
    expect(fs.readFileSync(h.file(pending.id), "utf8")).toBe(body); expect(fs.statSync(h.file(pending.id))).toMatchObject({ ino: before.ino, mtimeMs: before.mtimeMs });
    expect(fs.existsSync(h.archive(pending.id))).toBe(false); expect(completionConsumed(h.meshRoot, pending.id)).toBe(false); expect(enqueue).not.toHaveBeenCalled();
  });

  it("keeps targeted results after restart without widening exact-owner access", () => {
    const h = setup(); const result = h.seed(1); consumeCompletion(h.meshRoot, result.id, "main");
    expect(h.journal().result(result.id)).toMatchObject({ id: result.id, text: result.text });
    const foreign = h.journal(vi.fn(), { ...h.recipient, rootId: "session:other", sessionId: "other" });
    expect(foreign.result(result.id)).not.toHaveProperty("text"); expect(foreign.acknowledge(result.id)).toBe(false);
    saveCompletion(h.meshRoot, h.recipient, result); expect(fs.existsSync(h.file(result.id))).toBe(false);
  });

  it("retains archive and receipt while an exact-owner claim deletion is refused", async () => {
    const h = setup(); const result = h.seed(1); const journal = h.journal(); await journal.drain(false);
    consumeCompletion(h.meshRoot, result.id, "main"); vi.spyOn(h.mesh, "delete").mockRejectedValue(new Error("CAS refused"));
    const sync = vi.spyOn(fs, "fsyncSync"); const open = vi.spyOn(fs.promises, "open"); await journal.drain(false);
    expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(1); expect(sync).not.toHaveBeenCalled();
    expect(open.mock.calls.filter(([file]) => String(file) === h.receipt(result.id))).toHaveLength(1);
    expect(fs.existsSync(h.archive(result.id))).toBe(true); expect(completionConsumed(h.meshRoot, result.id)).toBe(true);
  });
});
