import fs from "node:fs";
import { main } from "./seam.js";

// fabric-seat-drain: the self-contained seam bundle's entry (dist/seat-drain/fabric-seat-drain.cjs).
// Only D3_SEAT_LOCK_FD is read from the environment; A fixes the rest (SEAT.md §3).
main(process.argv.slice(2), {
  stdin: () => { try { return fs.readFileSync(0); } catch { return Buffer.alloc(0); } },
  stdout: text => { fs.writeSync(1, text); },
  stderr: text => { fs.writeSync(2, text); },
  env: { D3_SEAT_LOCK_FD: process.env.D3_SEAT_LOCK_FD },
}).then(code => { process.exitCode = code; }, () => { fs.writeSync(2, "GAP: seam-internal [owner: fabric-v2]\n"); process.exitCode = 2; });
