// TEST ONLY. Thin bounded custody fixture, NOT N6's product pidfd launcher ABI.
// This launcher alone owns the real Main's stdin write end. Its death delivers
// ordinary EOF; neither EOF nor launcher exit is proof of actual Main death.
import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { registerHooks } from "node:module";
import { pathToFileURL } from "node:url";
import { assertApprovedNativeSdk } from "../helpers/native-sdk-no-model.ts";

if (new URL(import.meta.url).search === "?observe-sdk") {
  // Observation ONLY: never replace source, exports, resolution or SDK success.
  // The approved real SDK still executes via nextLoad. Refused contenders must
  // have no records here, a stronger boundary check than extension callbacks.
  const directory = process.env.FABRIC_NATIVE_TEST_SDK_DIRECTORY;
  const log = process.env.FABRIC_NATIVE_TEST_SDK_IMPORTS;
  if (!directory || !log || !path.isAbsolute(directory) || !path.isAbsolute(log)) throw new Error("Native SDK observation pins required");
  if (fs.realpathSync(process.execPath) !== process.env.PI_FABRIC_NODE_BINARY) throw new Error("Native observer requires the explicitly pinned Node");
  assertApprovedNativeSdk(directory); // Existing 031c-only approval, never learned from this environment.
  const prefix = pathToFileURL(`${directory}${path.sep}`).href;
  registerHooks({ load(url, context, nextLoad) {
    if (url.startsWith(prefix)) fs.appendFileSync(log, JSON.stringify({ pid: process.pid, event: "sdk-load", url }) + "\n", { mode: 0o600 });
    return nextLoad(url, context);
  } });
} else if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  const [pinsFile, receipt, separator, ...args] = process.argv.slice(2);
  if (separator !== "--" || !pinsFile || !receipt || !path.isAbsolute(receipt)) throw new Error("Test launcher requires pinned product argv and custody receipt");
  const pins = JSON.parse(fs.readFileSync(pinsFile, "utf8"));
  const hash = file => createHash("sha256").update(fs.readFileSync(file)).digest("hex");
  if (fs.realpathSync(process.execPath) !== pins.nodeBinary || args[0] !== pins.nativeOwnerEntry ||
      args[1] !== pins.sdkDirectory || args[2] !== "1" || hash(args[0]) !== pins.nativeOwnerEntrySha256) throw new Error("Test launcher product generation mismatch");
  const birth = pid => {
    const text = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    const value = text.slice(text.lastIndexOf(")") + 2).trim().split(/\s+/)[19];
    if (!/^\d+$/.test(value ?? "")) throw new Error("Unknown test child birth");
    return value;
  };
  const child = spawn(pins.nodeBinary, args, { cwd: process.cwd(), env: process.env, stdio: ["pipe", "inherit", "inherit"] });
  child.once("error", error => { throw error; });
  child.stdin.on("error", error => { if (error.code !== "EPIPE") throw error; });
  process.stdin.pipe(child.stdin);
  child.once("spawn", () => {
    fs.writeFileSync(receipt, JSON.stringify({ version: 1, launcherPid: process.pid, launcherStarted: birth(process.pid),
      mainPid: child.pid, mainStarted: birth(child.pid), entry: args[0], entrySha256: pins.nativeOwnerEntrySha256,
      stdinCustody: "launcher-owned-pipe" }) + "\n", { mode: 0o600, flag: "wx" });
  });
  // One failure deadline only. Normal work is driven by native spawn/close events.
  // Signals affect ONLY this fixture's actual ChildProcess, never a pid lookup.
  const deadline = setTimeout(() => child.kill("SIGKILL"), 120_000);
  child.once("close", (code, signal) => {
    clearTimeout(deadline);
    process.stdin.unpipe(child.stdin); process.stdin.pause();
    fs.writeFileSync(`${receipt}.main-close.json`, JSON.stringify({ pid: child.pid, code, signal }) + "\n", { mode: 0o600, flag: "wx" });
    process.exitCode = code === 0 && signal === null ? 0 : 1;
  });
}
