// Old-release contest: no advisory queue awareness; retains the three-line owner wire.
import fs from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
export class MeshStore {
  constructor(root, _bytes, _read, options) { this.root = root; this.budget = options.lockTimeoutMs; }
  async exclusive(operation) {
    const lock = path.join(this.root, '.lock'), owner = path.join(lock, 'owner');
    const record = `${randomUUID()}\n${process.pid}\n${Date.now()}\n`;
    const deadline = Date.now() + this.budget;
    for (;;) {
      try { fs.mkdirSync(lock); fs.writeFileSync(owner, record, { flag: 'wx' }); break; }
      catch (error) {
        if (error.code !== 'EEXIST') throw error;
        if (Date.now() >= deadline) throw Object.assign(new Error('legacy timeout'), { code: 'FABRIC_MESH_LOCK_TIMEOUT' });
        await new Promise(resolve => setTimeout(resolve, 1 + Math.floor(Math.random() * Math.min(50, deadline - Date.now()))));
      }
    }
    try { return operation(); }
    finally { if (fs.readFileSync(owner, 'utf8') === record) fs.rmSync(lock, { recursive: true }); }
  }
}
