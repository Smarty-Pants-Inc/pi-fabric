// Loads a candidate Fabric release's own code from its dist/ entry points (no mocks, no source
// imports). Classes that no entry re-exports are resolved through the exact chunk the entry imports
// them from, so the shadow test runs the same module instances the release's CLIs run.
import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { pathToFileURL } from 'node:url';

const importMap = (entryFile) => {
  const text = fs.readFileSync(entryFile, 'utf8');
  const map = new Map();
  for (const match of text.matchAll(/import\s*\{([^}]*)\}\s*from\s*"([^"]+)"/g)) {
    for (const part of match[1].split(',').map(item => item.trim()).filter(Boolean)) {
      const [exported, local = exported] = part.split(/\s+as\s+/).map(item => item.trim());
      map.set(local, { exported, file: path.resolve(path.dirname(entryFile), match[2]) });
    }
  }
  return map;
};

// The chunk among an entry's static imports (named or bare) whose export list names `name`.
const exportingChunk = (entryFile, name) => {
  const text = fs.readFileSync(entryFile, 'utf8');
  for (const match of text.matchAll(/(?:from|import)\s*"(\.{1,2}\/[^"]+\.js)"/g)) {
    const file = path.resolve(path.dirname(entryFile), match[1]);
    const exports = fs.readFileSync(file, 'utf8').match(/export\s*\{([^}]*)\}\s*;?\s*(?:\/\/.*)?\s*$/);
    for (const part of exports?.[1].split(',').map(item => item.trim()).filter(Boolean) ?? []) {
      const [local, exported = local] = part.split(/\s+as\s+/).map(item => item.trim());
      if (exported === name) return { exported, file };
    }
  }
  return undefined;
};

/** The binding an entry point imports (or loads) under a name, from the chunk it really uses. */
export const entryImport = async (release, entry, name) => {
  const file = path.join(release, 'dist', entry);
  const found = importMap(file).get(name) ?? exportingChunk(file, name);
  if (!found) throw new Error(`${entry} does not import ${name}; is ${release} a Fabric release?`);
  const module = await import(pathToFileURL(found.file).href);
  if (!(found.exported in module)) throw new Error(`${found.file} does not export ${found.exported}`);
  return module[found.exported];
};

export const entryModule = (release, entry) => import(pathToFileURL(path.join(release, 'dist', entry)).href);

export const loadCandidate = async (release, wants = {}) => {
  const mesh = await entryModule(release, 'mesh.js');
  const out = { release, MeshStore: mesh.MeshStore, MeshLockTimeoutError: mesh.MeshLockTimeoutError };
  if (wants.directory) out.ParticipantDirectory = await entryImport(release, 'participants-cli.js', 'ParticipantDirectory');
  if (wants.registry) out.ActorRegistryStore = await entryImport(release, 'residency/host.js', 'ActorRegistryStore');
  if (wants.actorClient) out.ResidentActorClient = (await entryModule(release, 'residency/actor-client.js')).ResidentActorClient;
  return out;
};

// Fabric's defaults for mesh.maxEventBytes and mesh.maxReadEvents (src/config.ts), as the CLIs use.
export const MAX_EVENT_BYTES = 256 * 1024;
export const MAX_READ_EVENTS = 500;

export const openStore = (candidate, root, options = {}) =>
  new candidate.MeshStore(root, MAX_EVENT_BYTES, MAX_READ_EVENTS, { lockProtocol: 1, ...options });

/** A read-only participant reader, as fabric-participants lists (it never starts a heartbeat). */
export const openReader = (candidate, root, id) => new candidate.ParticipantDirectory(openStore(candidate, root), {
  enabled: true, hostId: id, rootId: id, identity: { id, name: 'shadow-observer', kind: 'main' }, reapDeadHosts: false,
});

/** A Main's participant directory: the real 5 s heartbeat, lease files and confirmWritable. */
export const startMain = async (candidate, root, main, cwd) => {
  const store = openStore(candidate, root, { backgroundReadCacheMs: 5_000 });
  const identity = { id: main.id, name: main.name, kind: 'main', sessionId: main.sessionId };
  const startedAt = Date.now();
  const directory = new candidate.ParticipantDirectory(store, {
    enabled: true, hostId: main.id, rootId: main.id, identity,
    onRootCollision: collision => process.stderr.write(`root collision ${JSON.stringify(collision)}\n`),
  });
  directory.registerSource(() => [directory.root({ id: main.id, cwd, sessionId: main.sessionId, status: 'idle',
    startedAt, updatedAt: startedAt, pendingMessages: 0 }, true, main.name, { role: undefined })]);
  await directory.resumeLineage();
  await directory.start();
  return { store, directory, identity };
};

export const sha256 = value => createHash('sha256').update(value).digest('hex');
/** host-leases/<sha256(hostId)[0:32]>.json, as src/topology/host-leases.ts names it. */
export const hostLeaseFile = (root, hostId) => path.join(root, 'host-leases', `${sha256(hostId).slice(0, 32)}.json`);
/** participants/<sha256(id)>.json, the per-participant record file. */
export const participantFile = (root, id) => path.join(root, 'participants', `${sha256(id)}.json`);
/** <mesh>/residency/<sha256(rootId)>, as src/residency/protocol.ts residentRoot. */
export const residentRoot = (root, rootId) => path.join(root, 'residency', sha256(rootId));
export const residentHostId = rootId => `resident:${sha256(rootId).slice(0, 24)}`;

export const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
export const readJson = file => { try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return undefined; } };
export const writeJson = (file, value) => {
  const temporary = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(temporary, JSON.stringify(value, null, 2));
  fs.renameSync(temporary, file);
};
export const argMap = (argv) => {
  const out = {};
  for (let index = 0; index < argv.length; index++) {
    const flag = argv[index];
    if (!flag.startsWith('--')) throw new Error(`Bad argument ${flag}`);
    const next = argv[index + 1];
    if (next === undefined || next.startsWith('--')) out[flag.slice(2)] = true;
    else { out[flag.slice(2)] = next; index++; }
  }
  return out;
};
export const isLockTimeout = error => error?.code === 'FABRIC_MESH_LOCK_TIMEOUT' || error?.name === 'MeshLockTimeoutError';
