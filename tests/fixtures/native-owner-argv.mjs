#!/usr/bin/env node
// Thin argv adapter ONLY: the existing compiled worker/task-entry owns the entire Pi runtime.
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { pathToFileURL } from "node:url";
import { assertApprovedNativeSdk } from "../helpers/native-sdk-no-model.ts";
import { nativeOwnerSession } from "./native-owner-session.mjs";
const file = process.env.FABRIC_NATIVE_OWNER_PINS;
if (!file || !path.isAbsolute(file)) throw new Error("Native fixture generation pins required");
const pins = JSON.parse(fs.readFileSync(file, "utf8"));
const hash = file => createHash("sha256").update(fs.readFileSync(file)).digest("hex");
if (fs.realpathSync(process.execPath) !== pins.nodeBinary || !path.isAbsolute(pins.taskEntry) ||
    !path.isAbsolute(pins.sdkDirectory) || hash(pins.taskEntry) !== pins.taskEntrySha256 ||
    !path.isAbsolute(pins.nativeOwnerEntry) || fs.realpathSync(pins.nativeOwnerEntry) !== pins.nativeOwnerEntry ||
    hash(pins.nativeOwnerEntry) !== pins.nativeOwnerEntrySha256 ||
    hash(path.join(pins.sdkDirectory, "index.js")) !== pins.sdkIndexSha256) throw new Error("Native fixture generation changed");
process.umask(0o077);
const args = process.argv.slice(2);
if (!args.includes("--mode") || args[args.indexOf("--mode") + 1] !== "rpc") throw new Error("Native owner fixture is RPC-only");
assertApprovedNativeSdk(pins.sdkDirectory);
// This fixture does not implement a fence or a Main. Both roles execute the
// hash-read-back PRODUCT adapter; only its post-acquisition callback can emit the
// existing N7 observations. The callback verifies the actual SDK header/pin under
// that returned product descriptor, never asserts success from a marker alone.
const { runNativeOwnerEntry } = await import(pathToFileURL(pins.nativeOwnerEntry).href);
process.argv = [process.execPath, pins.nativeOwnerEntry, pins.sdkDirectory, "1", "--extension", pins.observer, ...args];
await runNativeOwnerEntry({ onMainFence: async (sessionFile, fd) => {
  if (sessionFile !== pins.sessionFile) throw new Error("Native Main must use its pinned session");
  fs.appendFileSync(pins.observations, JSON.stringify({ role: "main", pid: process.pid, event: "main-fence-acquired",
    entry: pins.nativeOwnerEntry, entrySha256: pins.nativeOwnerEntrySha256 }) + "\n", { mode: 0o600 });
  await nativeOwnerSession("verify", pins.sdkDirectory, sessionFile, process.cwd(), pins.sessionPin, fd, pins.nativeOwnerEntry);
  fs.appendFileSync(pins.observations, JSON.stringify({ role: "main", pid: process.pid, event: "main-session-verified" }) + "\n", { mode: 0o600 });
} });
