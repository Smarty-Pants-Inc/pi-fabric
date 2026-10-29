import { createHash } from "node:crypto";
import fs from "node:fs";
import { install, main } from "./seam.js";

// fabric-seat-bridge: the D3 bridge seam bundle's entry (dist/seat-bridge/fabric-seat-bridge.cjs).
// Only D3_SEAT_LOCK_FD is read from the environment; A fixes the rest. `--install` writes the pinned copy: its
// shebang names this node, and the three tokens below become that node's sha256, the ONE nats-link config file the
// seam may change and the link's systemd --user unit. An uninstalled bundle refuses to run a phase.
const PINNED_INTERPRETER_SHA256 = "__FABRIC_SEAT_BRIDGE_INTERPRETER_SHA256__";
const PINNED_LINK_CONFIG = "__FABRIC_SEAT_BRIDGE_LINK_CONFIG__";
const PINNED_LINK_UNIT = "__FABRIC_SEAT_BRIDGE_LINK_UNIT__";
const write = (fd: number, text: string): void => { fs.writeSync(fd, text); };
const io = {
  stdin: () => { try { return fs.readFileSync(0); } catch { return Buffer.alloc(0); } },
  stdout: (text: string) => write(1, text),
  stderr: (text: string) => write(2, text),
  env: { D3_SEAT_LOCK_FD: process.env.D3_SEAT_LOCK_FD },
};
const run = async (): Promise<number> => {
  const argv = process.argv.slice(2);
  if (argv[0] === "--install") return install(argv, io, __filename);
  if (!/^[0-9a-f]{64}$/.test(PINNED_INTERPRETER_SHA256) || !PINNED_LINK_CONFIG.startsWith("/")) {
    io.stderr("GAP: seam-not-installed [owner: fabric-v2]\n");
    return 2;
  }
  let actual: string;
  try { actual = createHash("sha256").update(fs.readFileSync("/proc/self/exe")).digest("hex"); } catch { actual = ""; }
  if (actual !== PINNED_INTERPRETER_SHA256) { io.stderr("GAP: interpreter-not-pinned [owner: fabric-v2]\n"); return 2; }
  return main(argv, io, { linkConfig: PINNED_LINK_CONFIG, linkUnit: PINNED_LINK_UNIT });
};
run().then(code => { process.exitCode = code; }, () => { write(2, "GAP: seam-internal [owner: fabric-v2]\n"); process.exitCode = 2; });
