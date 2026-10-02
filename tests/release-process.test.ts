import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { mainReleaseRecordDir, processStart, recordMainRelease } from "../src/lifecycle/release-process.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-release-process-"));
  roots.push(root);
  vi.stubEnv("PI_CODING_AGENT_DIR", root);
  vi.stubEnv("PI_FABRIC_PARENT_RUN", undefined);
  vi.stubEnv("PI_FABRIC_ACTOR_ID", undefined);
  return root;
};
const stat = (start: string) => `${process.pid} (pi (with spaces)) ${["S", "1", ...Array<string>(17).fill("0"), start, "0"].join(" ")}`;
const birth = (start?: string) => {
  const read = fs.readFileSync.bind(fs);
  vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, options?: unknown) => {
    if (String(file) === `/proc/${process.pid}/stat`) {
      if (start === undefined) throw new Error("unavailable birth identity");
      return stat(start);
    }
    return read(file, options as never);
  }) as typeof fs.readFileSync);
};

describe("observational Main release records", () => {
  it("parses birth ticks despite spaces and parentheses in the process name", () => {
    const root = fixture();
    fs.mkdirSync(path.join(root, "123"));
    fs.writeFileSync(path.join(root, "123", "stat"), stat("9876"));
    expect(processStart(123, root)).toBe("9876");
    expect(processStart(456, root)).toBeUndefined();
  });

  it("atomically records the loaded root, session and birth identity in the selected profile", () => {
    const root = fixture();
    birth("9876");
    const loadedRoot = path.join(root, "releases", "loaded");
    recordMainRelease("first-session", loadedRoot);
    const directory = mainReleaseRecordDir();
    const file = path.join(directory, `${process.pid}.json`);
    expect(directory).toBe(mainReleaseRecordDir(path.join(root, "settings.json")));
    expect(JSON.parse(fs.readFileSync(file, "utf8"))).toEqual({ pid: process.pid, start: "9876", sessionId: "first-session", loadedRoot });
    expect(fs.readdirSync(directory)).toEqual([`${process.pid}.json`]);
    recordMainRelease("reloaded-session", path.join(root, "releases", "next"));
    expect(JSON.parse(fs.readFileSync(file, "utf8"))).toMatchObject({ start: "9876", sessionId: "reloaded-session", loadedRoot: path.join(root, "releases", "next") });
    if (process.platform !== "win32") expect(fs.statSync(file).mode & 0o777).toBe(0o600);
  });

  it.each(["missing-root", "task-worker", "actor", "unknown-birth"])("does not write an unqualified Main record: %s", kind => {
    const root = fixture();
    birth(kind === "unknown-birth" ? undefined : "9876");
    if (kind === "task-worker") vi.stubEnv("PI_FABRIC_PARENT_RUN", "parent-run");
    if (kind === "actor") vi.stubEnv("PI_FABRIC_ACTOR_ID", "actor-one");
    recordMainRelease("session", kind === "missing-root" ? undefined : path.join(root, "loaded"));
    expect(fs.existsSync(mainReleaseRecordDir())).toBe(false);
  });

  it("treats an unwritable record directory as best-effort observational failure", () => {
    const root = fixture();
    birth("9876");
    fs.writeFileSync(path.join(root, "fabric"), "obstructed");
    expect(() => recordMainRelease("session", path.join(root, "loaded"))).not.toThrow();
    expect(fs.readFileSync(path.join(root, "fabric"), "utf8")).toBe("obstructed");
  });
});
