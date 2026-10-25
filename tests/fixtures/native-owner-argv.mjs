#!/usr/bin/env node
// Thin argv adapter ONLY: the existing compiled worker/task-entry owns the entire Pi runtime.
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { pathToFileURL } from "node:url";
import { assertApprovedNativeSdk } from "../helpers/native-sdk-no-model.ts";
import { takeNativeOwnerSessionFence, nativeOwnerSession } from "./native-owner-session.mjs";
const file = process.env.FABRIC_NATIVE_OWNER_PINS;
if (!file || !path.isAbsolute(file)) throw new Error("Native fixture generation pins required");
const pins = JSON.parse(fs.readFileSync(file, "utf8"));
const hash = file => createHash("sha256").update(fs.readFileSync(file)).digest("hex");
if (fs.realpathSync(process.execPath) !== pins.nodeBinary || !path.isAbsolute(pins.taskEntry) ||
    !path.isAbsolute(pins.sdkDirectory) || hash(pins.taskEntry) !== pins.taskEntrySha256 ||
    hash(path.join(pins.sdkDirectory, "index.js")) !== pins.sdkIndexSha256) throw new Error("Native fixture generation changed");
process.umask(0o077);
const args = process.argv.slice(2);
if (!args.includes("--mode") || args[args.indexOf("--mode") + 1] !== "rpc") throw new Error("Native owner fixture is RPC-only");
assertApprovedNativeSdk(pins.sdkDirectory);
if (!process.env.PI_FABRIC_RESIDENT_CONFIG) {
  if (args[args.indexOf("--session") + 1] !== pins.sessionFile) throw new Error("Native Main must use its pinned session");
  // This is the ACTUAL Main, not its parent launcher. Fence before SessionManager
  // verify/open and retain until native process death (including hard crash).
  const fd = takeNativeOwnerSessionFence(pins.sessionFile);
  // Keep it until KERNEL process death: an early exit listener could unlock
  // before a later SDK exit listener performs its final session-file write.
  fs.appendFileSync(pins.observations, JSON.stringify({ role: "main", pid: process.pid, event: "main-fence-acquired" }) + "\n", { mode: 0o600 });
  await nativeOwnerSession("verify", pins.sdkDirectory, pins.sessionFile, process.cwd(), pins.sessionPin, fd);
  fs.appendFileSync(pins.observations, JSON.stringify({ role: "main", pid: process.pid, event: "main-session-verified" }) + "\n", { mode: 0o600 });
}
// An observation/input guard, NOT the registration/bootstrap extension. The resident
// loads this guard and its real pi-entry, never the originating Main bootstrap.
process.argv = [process.execPath, pins.taskEntry, pins.sdkDirectory, "1", "--extension", pins.observer, ...args];
await import(pathToFileURL(pins.taskEntry).href);
