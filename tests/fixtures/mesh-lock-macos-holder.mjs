import fs from "node:fs";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";

const [root, domain, protocolText, ready] = process.argv.slice(2);
if (!root || (domain !== "mesh" && domain !== "custody") || !protocolText || !ready) {
  process.stderr.write("usage: mesh-lock-macos-holder.mjs ROOT mesh|custody PROTOCOL READY\n");
  process.exit(2);
}
const protocol = Number(protocolText);
if (protocol !== 1 && protocol !== 2) {
  process.stderr.write("protocol must be 1 or 2\n");
  process.exit(2);
}
const lock = path.join(root, domain === "mesh" ? ".lock" : "custody.lock");
fs.mkdirSync(root, { recursive: true, mode: 0o700 });
fs.mkdirSync(lock, { mode: 0o700 });
const start = protocol === 2
  ? `darwin:${execFileSync("/bin/ps", ["-p", String(process.pid), "-o", "lstart="], {
      encoding: "utf8",
      env: { ...process.env, LC_ALL: "C", TZ: "UTC" },
    }).trim()}`
  : undefined;
const record = `${randomUUID()}\n${process.pid}\n${Date.now()}\n${start ? `${start}\n` : ""}`;
fs.writeFileSync(path.join(lock, "owner"), record, { encoding: "utf8", flag: "wx", mode: 0o600 });
fs.writeFileSync(ready, JSON.stringify({ pid: process.pid, record }), { encoding: "utf8", flag: "wx", mode: 0o600 });
setInterval(() => undefined, 1_000);
