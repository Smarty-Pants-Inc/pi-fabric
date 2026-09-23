// Preload probe for the SWE coordinator's write-ahead ordering. It logs every
// fsync/rename as it happens (the real calls still run) and can abort the
// process exactly before the *finished* checkpoint replaces checkpoints.json,
// which is the instant a crash would otherwise lose a paid attempt.
const fs = require("node:fs");
const path = require("node:path");

const realFsync = fs.fsyncSync;
const realRename = fs.renameSync;
const realFstat = fs.fstatSync;

const log = (line) => {
  if (process.env.FAKE_SWE_SYNC_LOG) fs.appendFileSync(process.env.FAKE_SWE_SYNC_LOG, `${line}\n`);
};

fs.fsyncSync = function (fd) {
  const isDirectory = (() => {
    try {
      return realFstat(fd).isDirectory();
    } catch {
      return false;
    }
  })();
  log(`fsync dir=${isDirectory}`);
  return realFsync.call(fs, fd);
};

fs.renameSync = function (from, to) {
  log(`rename to=${path.basename(String(to))}`);
  if (process.env.FAKE_SWE_ABORT_BEFORE_FINISHED_CHECKPOINT && String(to).endsWith("checkpoints.json")) {
    let body = "";
    try {
      body = fs.readFileSync(from, "utf8");
    } catch {
      body = "";
    }
    if (body.includes('"finished"')) process.exit(7);
  }
  return realRename.call(fs, from, to);
};
