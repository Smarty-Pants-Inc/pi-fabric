import { execFileSync, spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtempSync, rmSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { build } from "esbuild";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

let root: string;
let entry: string;
const node = process.versions.bun ? "node" : process.execPath;
const prelude = () => `
  import assert from 'node:assert/strict';
  import { openDurableWorkerStorage as acquire } from ${JSON.stringify(entry)};
  import { BACKGROUND_CONTEXT as context } from '@earendil-works/chord/context';
  import { chmod, stat, readFile, writeFile, appendFile, unlink, mkdir, symlink, link } from 'node:fs/promises';
  import { join } from 'node:path';
`;
function run(body: string) {
  return execFileSync(node, ["--input-type=module", "-e", prelude() + body], {
    cwd: root, encoding: "utf8", timeout: 15_000,
  });
}

beforeAll(async () => {
  root = mkdtempSync(join(tmpdir(), "fabric-worker-storage-"));
  symlinkSync(resolve("node_modules"), join(root, "node_modules"), "junction");
  await build({ entryPoints: ["src/durable/storage.ts"], outfile: join(root, "storage.mjs"),
    bundle: true, packages: "external", platform: "node", format: "esm", target: "node24" });
  entry = pathToFileURL(join(root, "storage.mjs")).href;
});
afterAll(() => { if (root) rmSync(root, { recursive: true, force: true }); });

describe("durable worker exclusive storage (real Node SQLite)", () => {
  it("rejects same-process and symlink aliases; persists and reopens privately", () => {
    run(`
      const directory = join(process.cwd(), 'persist');
      const owner = await acquire(directory);
      await symlink(directory, directory + '-alias', 'junction');
      await assert.rejects(acquire(directory), /locked/);
      await assert.rejects(acquire(directory + '-alias'), /locked/);
      // A failed same-process open must not accidentally drop POSIX file locks.
      const { execFileSync } = await import('node:child_process');
      execFileSync(process.execPath, ['--input-type=module', '-e',
        'import assert from "node:assert/strict"; import { openDurableWorkerStorage as acquire } from ' +
        JSON.stringify(${JSON.stringify(entry)}) + '; await assert.rejects(acquire(' + JSON.stringify(directory) + '), /locked/);']);
      const id = await owner.storage.mintId();
      await owner.storage.commit([{ type: 'conversation', value: { id } }], context);
      assert.ok((await readFile(join(directory, 'store/main.jsonl'), 'utf8')).includes('conversation'));
      if (process.platform !== 'win32') {
        for (const [path, mode] of [[directory, 0o700], [join(directory, 'store'), 0o700],
          [join(directory, 'lease.sqlite'), 0o600], [join(directory, 'store/main.jsonl'), 0o600]]) {
          assert.equal((await stat(path)).mode & 0o777, mode);
        }
      }
      await owner.storage.close(context);
      await assert.rejects(acquire(directory), /locked/); // close alone must not release
      await Promise.all([owner.release(), owner.release()]);
      const reopened = await acquire(directory + '-alias');
      assert.deepEqual(await reopened.storage.conversation(id, context), { id });
      await owner.release(); // old releases cannot unlock the new owner
      await assert.rejects(acquire(directory), /locked/);
      await reopened.storage.close(context);
      await reopened.release();
    `);
  });

  it("flushes the main JSONL commit marker before resolving commit", () => {
    run(`
      const fs = (await import('node:fs/promises')).default;
      const { syncBuiltinESMExports } = await import('node:module');
      const original = fs.open;
      const flushed = [];
      fs.open = async (...args) => {
        const file = await original(...args);
        const sync = file.sync.bind(file);
        file.sync = async () => { await sync(); flushed.push(String(args[0])); };
        return file;
      };
      syncBuiltinESMExports();
      const owner = await acquire(join(process.cwd(), 'flush'));
      await owner.storage.commit([{ type: 'conversation', value: { id: await owner.storage.mintId() } }], context);
      assert.ok(flushed.some(path => path.endsWith('main.jsonl')));
      flushed.length = 0;
      const id = await owner.storage.mintId();
      await owner.storage.commit([{ type: 'document.create', record: { id, kind: 'flush-test', scope: { kind: 'session' } },
        content: { kind: 'base', version: 1, value: { secret: 'payload' } } }], context);
      const payloadFlush = flushed.findIndex(path => path.endsWith('doc-' + id + '.jsonl'));
      const markerFlush = flushed.findIndex(path => path.endsWith('main.jsonl'));
      assert.ok(payloadFlush >= 0 && markerFlush > payloadFlush, JSON.stringify(flushed));
      if (process.platform !== 'win32') assert.equal((await stat(join(process.cwd(), 'flush/store/doc-' + id + '.jsonl'))).mode & 0o777, 0o600);
      await owner.storage.close(context);
      await owner.release();
    `);
  });

  it("fails commits closed when the main marker cannot be flushed", () => {
    run(`
      const fs = (await import('node:fs/promises')).default;
      const { syncBuiltinESMExports } = await import('node:module');
      const original = fs.open;
      fs.open = async (...args) => {
        const file = await original(...args);
        if (String(args[0]).endsWith('main.jsonl')) file.sync = async () => { throw new Error('flush denied'); };
        return file;
      };
      syncBuiltinESMExports();
      const owner = await acquire(join(process.cwd(), 'flush-failure'));
      const writes = [{ type: 'conversation', value: { id: await owner.storage.mintId() } }];
      await assert.rejects(owner.storage.commit(writes, context), /poisoned/);
      await assert.rejects(owner.storage.commit(writes, context), /poisoned/);
      await owner.storage.close(context);
      await owner.release();
    `);
  });

  it("refuses symlink and hardlink files without changing their targets", () => {
    run(`
      const target = join(process.cwd(), 'outside');
      await writeFile(target, 'private target', { mode: 0o640 });
      const originalMode = (await stat(target)).mode;
      for (const name of ['lease.sqlite', 'store/main.jsonl', 'store/doc-1.jsonl']) {
        for (const kind of ['symlink', 'hardlink']) {
          const directory = join(process.cwd(), 'linked-' + kind + '-' + name.replaceAll('/', '-'));
          await mkdir(join(directory, 'store'), { recursive: true });
          if (kind === 'symlink') await symlink(target, join(directory, name));
          else await link(target, join(directory, name));
          await assert.rejects(acquire(directory), /regular/);
          assert.equal(await readFile(target, 'utf8'), 'private target');
          assert.equal((await stat(target)).mode, originalMode);
        }
      }
      const directory = join(process.cwd(), 'linked-directory');
      const outside = join(process.cwd(), 'outside-directory');
      await mkdir(outside);
      await mkdir(directory);
      await symlink(outside, join(directory, 'store'), 'junction');
      await assert.rejects(acquire(directory), /not a symlink/);
    `);
  });

  it("does not resolve optional engines or SQLite until acquisition", () => {
    execFileSync(node, ["--input-type=module", "-e", `
      import { registerHooks } from 'node:module';
      registerHooks({ resolve(specifier, context, next) {
        if (specifier.includes('@earendil-works/') || specifier === 'node:sqlite') throw new Error('eager: ' + specifier);
        return next(specifier, context);
      }});
      const module = await import(${JSON.stringify(entry)});
      if (typeof module.openDurableWorkerStorage !== 'function') throw new Error('missing export');
    `], { cwd: root, timeout: 15_000 });
  });

  it("release only closes the lease, not caller-owned storage", () => {
    run(`
      const owner = await acquire(join(process.cwd(), 'release-only'));
      const close = owner.storage.close.bind(owner.storage);
      owner.storage.close = () => { throw new Error('release called storage.close'); };
      await owner.release();
      await owner.release();
      await close(context);
    `);
  });

  it("closes the acquired lease when opening JSONL fails", () => {
    run(`
      const directory = join(process.cwd(), 'failure');
      await mkdir(join(directory, 'store'), { recursive: true });
      const main = join(directory, 'store/main.jsonl');
      await writeFile(main, 'not json\\n');
      await assert.rejects(acquire(directory), /Malformed/);
      await unlink(main);
      const owner = await acquire(directory);
      await owner.storage.close(context);
      await owner.release();
    `);
  });

  it("rejects another process then reacquires after confirmed SIGKILL without deleting the lease", async () => {
    const directory = join(root, "crash");
    const child = spawn(node, ["--input-type=module", "-e", prelude() + `
      const owner = await acquire(${JSON.stringify(directory)});
      await owner.storage.commit([{ type: 'conversation', value: { id: await owner.storage.mintId() } }], context);
      const docId = await owner.storage.mintId();
      await owner.storage.commit([{ type: 'document.create', record: { id: docId, kind: 'crash-test', scope: { kind: 'session' } },
        content: { kind: 'base', version: 1, value: { recovered: true } } }], context);
      // Simulate an interrupted next marker after a fully persisted commit.
      await appendFile(join(${JSON.stringify(directory)}, 'store/main.jsonl'), '{"format":');
      console.log('READY');
      setInterval(() => {}, 1000);
    `], { cwd: root, stdio: ["ignore", "pipe", "pipe"] });
    const exited = once(child, "exit");
    try {
      await new Promise<void>((resolveReady, reject) => {
        const timer = setTimeout(() => reject(new Error('holder readiness timeout')), 10_000);
        child.once("error", error => { clearTimeout(timer); reject(error); });
        child.once("exit", () => { clearTimeout(timer); reject(new Error('holder exited before readiness')); });
        child.stdout.on("data", chunk => {
          if (String(chunk).includes("READY")) { clearTimeout(timer); resolveReady(); }
        });
      });
      run(`await assert.rejects(acquire(${JSON.stringify(directory)}), /locked/);`);
      expect(child.kill("SIGKILL")).toBe(true);
      const [code, signal] = await exited;
      expect(signal === "SIGKILL" || (process.platform === "win32" && code !== 0)).toBe(true);
      run(`
        const directory = ${JSON.stringify(directory)};
        const before = await stat(join(directory, 'lease.sqlite'));
        const owner = await acquire(directory);
        assert.equal((await stat(join(directory, 'lease.sqlite'))).ino, before.ino);
        assert.equal((await owner.storage.scanConversations({}, 10, undefined, context)).items.length, 1);
        assert.ok(!(await readFile(join(directory, 'store/main.jsonl'), 'utf8')).endsWith('{"format":'));
        const docs = await owner.storage.scanDocuments({ scope: { kind: 'session' }, at: 'current' }, 10, undefined, context);
        assert.equal(docs.items.length, 1);
        assert.equal(docs.items[0].kind, 'crash-test');
        assert.deepEqual((await owner.storage.document(docs.items[0].id, 'current', context)).value, { recovered: true });
        assert.ok((await readFile(join(directory, 'store/doc-' + docs.items[0].id + '.jsonl'), 'utf8')).includes('recovered'));
        await owner.storage.close(context);
        await owner.release();
      `);
    } finally {
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      await exited;
    }
  }, 30_000);
});
