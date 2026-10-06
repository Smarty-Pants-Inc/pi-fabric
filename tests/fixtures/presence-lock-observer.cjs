// Passive syscall receipts only: never replace a lock decision, timeout, writer or timer.
const fs = require('node:fs');
const original = fs.mkdtempSync;
const append = fs.appendFileSync;
fs.mkdtempSync = function(prefix, ...args) {
  const text = String(prefix);
  if (process.env.PRESENCE_LOCK_TRACE && text.startsWith(process.env.PI_FABRIC_MESH_ROOT + '/.lock.pending.')) {
    append(process.env.PRESENCE_LOCK_TRACE, JSON.stringify({ at: Date.now(), pid: process.pid,
      token: text.slice(text.indexOf('.lock.pending.') + 14, -1) }) + '\n');
  }
  return original.call(this, prefix, ...args);
};
