import fs from 'node:fs';
import path from 'node:path';

const within = (parent, child) => child === parent || child.startsWith(parent + path.sep);

/** Create invocation-owned scratch/output paths without adopting old state. */
export function prepareProofPaths(scratch, out) {
  if (within(scratch, out) || within(out, scratch)) throw new Error('Proof scratch and output paths must not overlap');
  if (fs.existsSync(scratch)) throw new Error(`Proof scratch path already exists; refusing shared custody: ${scratch}`);
  fs.mkdirSync(scratch, { mode: 0o700 });
  if (fs.existsSync(out)) {
    const stat = fs.lstatSync(out);
    if (!stat.isDirectory() || stat.isSymbolicLink() || fs.readdirSync(out).length !== 0) {
      throw new Error(`Proof output path must be a fresh directory: ${out}`);
    }
  } else fs.mkdirSync(out, { recursive: true, mode: 0o700 });
}

/** Linux /proc start ticks (field 22), which distinguish reused PIDs. */
export function processStartTime(pid) {
  if (!Number.isSafeInteger(pid) || pid <= 0 || process.platform !== 'linux') return undefined;
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, 'utf8');
    return stat.slice(stat.lastIndexOf(')') + 2).trim().split(/\s+/)[19];
  } catch { return undefined; }
}

function processState(pid) {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, 'utf8');
    const fields = stat.slice(stat.lastIndexOf(')') + 2).trim().split(/\s+/);
    return { state: fields[0], startTime: fields[19] };
  } catch { return undefined; }
}

export function registerOwnedProcess(claims, pid, startTime) {
  if (Number.isSafeInteger(pid) && pid > 0 && typeof startTime === 'string' && startTime.length > 0) claims.set(pid, startTime);
}

export function ownedProcessAlive(claims, pid) {
  const expected = claims.get(pid);
  if (!expected) return false;
  const current = processState(pid);
  return !!current && !['Z', 'X'].includes(current.state) && current.startTime === expected;
}

/** Signal only the exact recorded process incarnation; an unknown identity is never kill authority. */
export function signalOwnedProcess(claims, pid, signal, signaler = process.kill.bind(process)) {
  if (!ownedProcessAlive(claims, pid)) return false;
  try { signaler(pid, signal); return true; } catch { return false; }
}
