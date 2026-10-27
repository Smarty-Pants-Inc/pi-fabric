// Test-only native session provisioning. The pinned SDK, never this fixture, writes the header.
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { createHash } from "node:crypto";
import { assertApprovedNativeSdk } from "../helpers/native-sdk-no-model.ts";

// Fence implementation lives ONLY in the actual compiled product entry. Import
// is side-effect-free; the standalone CLI below acquires BEFORE calling any SDK.
const productFence = async entry => {
  if (!entry || !path.isAbsolute(entry) || fs.realpathSync(entry) !== entry) throw new Error("Compiled native owner entry required");
  return import(pathToFileURL(entry).href);
};

export async function nativeOwnerSession(mode, sdkDirectory, file, cwd, pinFile, fenceFd, entry) {
if (!["provision", "verify"].includes(mode) || ![sdkDirectory, file, cwd, pinFile].every(value => value && path.isAbsolute(value))) {
  throw new Error("Explicit absolute native session inputs required");
}
const { assertNativeOwnerSessionFence } = await productFence(entry);
assertNativeOwnerSessionFence(file, fenceFd);
assertApprovedNativeSdk(sdkDirectory);
const pinned = fs.existsSync(pinFile) ? JSON.parse(fs.readFileSync(pinFile, "utf8")) : undefined;
if (mode === "verify" && !pinned) throw new Error("Native owner session pin missing");
let stat = fs.lstatSync(file, { throwIfNoEntry: false });
if (pinned && (!stat?.isFile() || stat.isSymbolicLink() || stat.size === 0)) throw new Error("Pinned native session missing or empty");
if (!stat) {
  if (mode !== "provision" || pinned) throw new Error("Native owner session is unavailable");
  const fd = fs.openSync(file, "wx", 0o600); fs.closeSync(fd);
  stat = fs.lstatSync(file);
}
if (!stat.isFile() || stat.isSymbolicLink() || stat.nlink !== 1 || stat.uid !== process.geteuid() || (stat.mode & 0o7777) !== 0o600) {
  throw new Error("Unsafe native session file");
}
// Existing empty file is the inspected native SDK _setSessionFile initialization branch.
const { SessionManager } = await import(pathToFileURL(path.join(sdkDirectory, "core/session-manager.js")).href);
const manager = SessionManager.open(file, path.dirname(file), cwd);
const header = manager.getHeader();
const sessionId = manager.getSessionId();
if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(sessionId) ||
    header?.type !== "session" || header.id !== sessionId || header.cwd !== cwd || manager.getSessionFile() !== file) {
  throw new Error("Native session header readback failed");
}
const fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
try { if (!fs.fstatSync(fd).size) throw new Error("Native header was not persisted"); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
const directoryFd = fs.openSync(path.dirname(file), fs.constants.O_RDONLY | fs.constants.O_DIRECTORY);
try { fs.fsyncSync(directoryFd); } finally { fs.closeSync(directoryFd); }
const reopened = SessionManager.open(file, path.dirname(file), cwd);
const value = { sessionFile: file, rootId: `session:${sessionId}`, sessionId };
if (reopened.getSessionId() !== sessionId || (pinned && JSON.stringify(pinned) !== JSON.stringify(value))) {
  throw new Error("Pinned native session identity changed");
}
if (!pinned) {
  const temporary = `${pinFile}.new`;
  const output = fs.openSync(temporary, "wx", 0o600);
  try { fs.writeFileSync(output, JSON.stringify(value)); fs.fsyncSync(output); } finally { fs.closeSync(output); }
  fs.linkSync(temporary, pinFile); fs.unlinkSync(temporary);
  const parent = fs.openSync(path.dirname(pinFile), fs.constants.O_RDONLY | fs.constants.O_DIRECTORY);
  try { fs.fsyncSync(parent); } finally { fs.closeSync(parent); }
}
return value;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  process.umask(0o077);
  const [mode, sdkDirectory, file, cwd, pinFile, entry, expectedHash] = process.argv.slice(2);
  if (!/^[0-9a-f]{64}$/.test(expectedHash ?? "") || createHash("sha256").update(fs.readFileSync(entry)).digest("hex") !== expectedHash) {
    throw new Error("Native session fixture compiled product generation changed");
  }
  const { takeNativeOwnerSessionFence, assertNativeOwnerSessionFence, guardNativeOwnerSessionIo } = await productFence(entry);
  if (["discovery", "discovery-bare-native", "discovery-resume"].includes(mode)) {
    assertApprovedNativeSdk(sdkDirectory);
    if (process.env.PI_CODING_AGENT_SESSION_DIR !== undefined) throw new Error("Ordinary discovery must not inherit native --session-dir/env pins");
    const { SessionManager, getDefaultSessionDir } = await import(pathToFileURL(path.join(sdkDirectory, "core/session-manager.js")).href);
    const { SettingsManager } = await import(pathToFileURL(path.join(sdkDirectory, "core/settings-manager.js")).href);
    const { createSessionManager } = await import(pathToFileURL(path.join(sdkDirectory, "main.js")).href);
    const { getAgentDir, getSessionsDir } = await import(pathToFileURL(path.join(sdkDirectory, "config.js")).href);
    const settings = SettingsManager.create(cwd, getAgentDir(), { projectTrusted: false });
    const sessionDir = settings.getSessionDir();
    if (sessionDir !== undefined) throw new Error("Ordinary profile unexpectedly configures a sessionDir override");
    const pinned = JSON.parse(fs.readFileSync(pinFile, "utf8"));
    if (mode === "discovery-bare-native") {
      await createSessionManager({ session: pinned.sessionId }, cwd, sessionDir, settings);
      throw new Error("Ordinary bare native ID unexpectedly resolved");
    }
    if (mode === "discovery-resume") {
      const selected = await createSessionManager({ resume: true }, cwd, sessionDir, settings);
      process.stdout.write(`NATIVE_DISCOVERY_RESUME_RESULT:${JSON.stringify({ sessionId: selected.getSessionId(), sessionFile: selected.getSessionFile() })}\n`);
      process.exit(0);
    }
    const defaultDirectory = getDefaultSessionDir(cwd), globalRoot = getSessionsDir();
    const relative = path.relative(globalRoot, file);
    if (relative === "" || (!path.isAbsolute(relative) && relative !== ".." && !relative.startsWith(`..${path.sep}`))) throw new Error("Native file lies in ordinary discovery root");
    const ordinaryFile = path.join(defaultDirectory, "ordinary-positive.jsonl");
    fs.closeSync(fs.openSync(ordinaryFile, "wx", 0o600));
    const ordinary = SessionManager.open(ordinaryFile, defaultDirectory, cwd);
    ordinary.appendCustomMessageEntry("ordinary-discovery-positive-control", "Ordinary positive control; no model turn.", false);
    ordinary.appendSessionInfo("ordinary-discovery-positive-control");
    const samples = [];
    for (let pass = 0; pass < 2; pass++) {
      const listed = await SessionManager.list(cwd, sessionDir), all = await SessionManager.listAll(sessionDir);
      const continued = await createSessionManager({ continue: true }, cwd, sessionDir, settings);
      const byId = await createSessionManager({ session: ordinary.getSessionId() }, cwd, sessionDir, settings);
      samples.push({ pass, listed: listed.map(value => ({ id: value.id, path: value.path })), all: all.map(value => ({ id: value.id, path: value.path })),
        nativeExact: SessionManager.findById(cwd, pinned.sessionId, sessionDir) ?? null,
        ordinaryExact: SessionManager.findById(cwd, ordinary.getSessionId(), sessionDir),
        continued: continued.getSessionFile(), bareOrdinary: byId.getSessionFile() });
    }
    process.stdout.write(JSON.stringify({ agentDir: getAgentDir(), globalRoot, defaultDirectory, sessionDir: sessionDir ?? null,
      envSessionDir: process.env.PI_CODING_AGENT_SESSION_DIR ?? null, ordinaryFile, ordinaryId: ordinary.getSessionId(), native: pinned,
      samples, resolver: "approved SDK main.createSessionManager + SessionManager.list/listAll/findById; repeated warm scan" }) + "\n");
    process.exit(0);
  }
  if (mode === "unlocked-proof") {
    const fd = fs.openSync(`${file}.main.lock`, fs.constants.O_RDWR | fs.constants.O_NOFOLLOW);
    // Same public inode, correct owner/mode, but NOT the locked open description.
    assertNativeOwnerSessionFence(file, fd);
    throw new Error("Unlocked same-inode descriptor unexpectedly admitted");
  }
  if (mode === "replaced-holder") {
    const fd = takeNativeOwnerSessionFence(file);
    assertNativeOwnerSessionFence(file, fd); guardNativeOwnerSessionIo(file);
    const { SessionManager } = await import(pathToFileURL(path.join(sdkDirectory, "core/session-manager.js")).href);
    const manager = SessionManager.open(file, path.dirname(file), cwd);
    fs.renameSync(`${file}.main.lock`, `${file}.main.lock.retained-holder`);
    fs.closeSync(fs.openSync(`${file}.main.lock`, "wx", 0o600));
    const refusals = [];
    for (const [operation, attempt] of [["open", () => SessionManager.open(file, path.dirname(file), cwd)],
      ["append", () => manager.appendThinkingLevelChange("off")]]) {
      try { attempt(); throw new Error(`Old holder ${operation} unexpectedly admitted`); }
      catch (error) {
        if (error.code !== "NATIVE_OWNER_FENCE_REQUIRED") throw error;
        refusals.push({ operation, name: error.name, code: error.code, message: error.message });
      }
    }
    process.stdout.write(JSON.stringify({ pid: process.pid, refusals }) + "\n");
    process.exit(0);
  }
  if (mode === "ordinary-custom-session") {
    const directory = path.join(cwd, "ordinary-custom-sessions");
    fs.mkdirSync(directory, { mode: 0o700 });
    const ordinaryFile = path.join(directory, "ordinary.jsonl");
    fs.closeSync(fs.openSync(ordinaryFile, "wx", 0o600));
    const { guardNativeOwnerSessionSelection } = await productFence(entry);
    guardNativeOwnerSessionSelection(ordinaryFile, cwd);
    const { SessionManager } = await import(pathToFileURL(path.join(sdkDirectory, "core/session-manager.js")).href);
    const manager = SessionManager.open(ordinaryFile, directory, cwd);
    process.stdout.write(JSON.stringify({ sessionFile: manager.getSessionFile(), sessionId: manager.getSessionId() }) + "\n");
    process.exit(0);
  }
  if (mode === "clean-cli-session" || mode === "inspect-cli-session") {
    const fd = takeNativeOwnerSessionFence(file);
    assertNativeOwnerSessionFence(file, fd); guardNativeOwnerSessionIo(file);
    const { SessionManager } = await import(pathToFileURL(path.join(sdkDirectory, "core/session-manager.js")).href);
    const manager = SessionManager.open(file, path.dirname(file), cwd);
    // Legitimate SDK history with no user/model/provider/tool turn: this inert
    // custom-message entry supplies resumable context. Unlike a header-only
    // N7 session, plain CLI need not initialize thinking metadata before hooks.
    // The unseeded counterexample only inspects the REAL native starting state
    // under custody. Never inject fake context to suppress SDK pre-hook writes.
    if (mode === "clean-cli-session") {
      manager.appendThinkingLevelChange("off");
      manager.appendCustomMessageEntry("native-owner-clean-tail-control", "No user or model turn.", false);
    }
    process.stdout.write(JSON.stringify({ sessionId: manager.getSessionId(), sessionFile: manager.getSessionFile(),
      messages: manager.buildSessionContext().messages.length }) + "\n");
    process.exit(0);
  }
  // Product fence FIRST, including before nativeOwnerSession's SDK import and
  // session/pin access. No duplicate fixture-only locking implementation.
  const fd = takeNativeOwnerSessionFence(file);
  try { process.stdout.write(JSON.stringify(await nativeOwnerSession(mode, sdkDirectory, file, cwd, pinFile, fd, entry)) + "\n"); }
  finally { fs.closeSync(fd); }
}
