// Independent protocol-1 holder: acquisition, timed hold, and exact-owner release.
import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
const [root, holdText = "3000", gapText = "0", durationText = holdText] = process.argv.slice(2);
const holdMs = Number(holdText), gapMs = Number(gapText), durationMs = Number(durationText);
const lock = path.join(root, ".lock"), owner = path.join(lock, "owner"), token = randomUUID();
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
const until = Date.now() + durationMs;
let ready = false;
do {
  try { fs.mkdirSync(lock, { mode: 0o700 }); }
  catch (error) { if (error.code !== "EEXIST") throw error; await pause(5); continue; }
  const receipt = `${token}\n${process.pid}\n${Date.now()}\n`;
  fs.writeFileSync(owner, receipt, { mode: 0o600 });
  if (!ready) { process.stdout.write("held\n"); ready = true; }
  try { await pause(holdMs); }
  finally {
    if (fs.readFileSync(owner, "utf8") !== receipt) throw new Error("Holder lost lock custody");
    const released = `${lock}.released.${token}`;
    fs.renameSync(lock, released); fs.rmSync(released, { recursive: true });
  }
  if (gapMs) await pause(gapMs);
} while (Date.now() < until);
