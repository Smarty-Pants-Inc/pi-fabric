import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { bashProcessEnvironment, childProcessEnvironment } from "../src/core/child-process-environment.js";
import { executeFile, spawnDetached } from "../src/agents/transports/process-utils.js";
import { taskAgentEnvironment } from "../src/agents/task-environment.js";

const roots: string[] = [];
afterEach(() => {
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const preserved = { FABRIC_ENV_MARKER: "preserved", SMARTY_ALPHA: "0",
  PI_FABRIC_WRITE_POLICY: JSON.stringify({ readOnly: true, writableRoots: [], shell: "deny" }) };
const report = "console.log(JSON.stringify(Object.fromEntries(Object.entries(process.env).filter(([key]) => key.startsWith('PI_FABRIC_LANDLOCK_') || ['FABRIC_ENV_MARKER', 'SMARTY_ALPHA', 'PI_FABRIC_WRITE_POLICY'].includes(key)))))";
const ambient = () => {
  vi.stubEnv("PI_FABRIC_LANDLOCK_ESCAPE", "1");
  vi.stubEnv("PI_FABRIC_LANDLOCK_ESCAPE_ONCE", "1");
  vi.stubEnv("PI_FABRIC_LANDLOCK_FUTURE_ESCAPE", "1");
  vi.stubEnv("FABRIC_ENV_MARKER", "preserved");
  vi.stubEnv("SMARTY_ALPHA", preserved.SMARTY_ALPHA);
  vi.stubEnv("PI_FABRIC_WRITE_POLICY", preserved.PI_FABRIC_WRITE_POLICY);
};

describe("reserved child launch environment", () => {
  it("copies ordinary values without mutating the caller; Bash preserves only native controls", () => {
    const environment = { ...preserved, PI_FABRIC_LANDLOCK_ESCAPE: "1",
      PI_FABRIC_LANDLOCK_FUTURE_ESCAPE: "1", PI_FABRIC_LANDLOCK_SHELL: "/bin/sh", PI_FABRIC_LANDLOCK_WRITES: "trusted" };
    expect(childProcessEnvironment(environment)).toEqual(preserved);
    expect(bashProcessEnvironment(environment)).toEqual({ ...preserved,
      PI_FABRIC_LANDLOCK_SHELL: "/bin/sh", PI_FABRIC_LANDLOCK_WRITES: "trusted" });
    expect(environment.PI_FABRIC_LANDLOCK_ESCAPE).toBe("1");
    expect(environment.SMARTY_ALPHA).toBe("0");
    expect(environment.PI_FABRIC_WRITE_POLICY).toBe(preserved.PI_FABRIC_WRITE_POLICY);
  });

  it.each(["0", "1", ""])("preserves alpha selection and serialized policy across task environment copies (%j)", alpha => {
    const environment = { ...preserved, SMARTY_ALPHA: alpha };
    for (const copy of [taskAgentEnvironment(environment), childProcessEnvironment(environment), bashProcessEnvironment(environment)]) {
      expect(copy.SMARTY_ALPHA).toBe(alpha);
      expect(copy.PI_FABRIC_WRITE_POLICY).toBe(preserved.PI_FABRIC_WRITE_POLICY);
    }
    expect(environment).toEqual({ ...preserved, SMARTY_ALPHA: alpha });
  });

  it.each([false, true])("executeFile scrubs actual child env (explicit=%s)", async explicit => {
    ambient();
    const result = await executeFile(process.execPath, ["-e", report], explicit ? { env: { ...process.env } } : {});
    expect(JSON.parse(result.stdout)).toEqual(preserved);
    expect(process.env.PI_FABRIC_LANDLOCK_ESCAPE).toBe("1");
  });

  it.each([false, true])("spawnDetached scrubs actual worker env (explicit=%s)", async explicit => {
    ambient();
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-child-env-"));
    roots.push(root);
    const worker = path.join(root, "worker.cjs");
    const receipt = path.join(root, "environment.json");
    fs.writeFileSync(worker, `require('node:fs').writeFileSync(${JSON.stringify(receipt)}, JSON.stringify(Object.fromEntries(Object.entries(process.env).filter(([key]) => key.startsWith('PI_FABRIC_LANDLOCK_') || ['FABRIC_ENV_MARKER', 'SMARTY_ALPHA', 'PI_FABRIC_WRITE_POLICY'].includes(key)))));`);
    const child = await spawnDetached(worker, [], root, undefined, explicit ? { ...process.env } : undefined);
    try {
      await child.closed;
      expect(JSON.parse(fs.readFileSync(receipt, "utf8"))).toEqual(preserved);
      expect(process.env.PI_FABRIC_LANDLOCK_ESCAPE).toBe("1");
    } finally { await child.stop(); }
  });
});
