import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { expect, it } from "vitest";
import type { writePolicyDispositionHeapProbe } from "./fixtures/write-policy-disposition-heap-probe.js";

// e22d68dc: the launch-held onWritePolicyDisposition closure shared run()'s
// lexical Context, retaining request.task (100 x 4 MiB) via manager.#runs ->
// launch.onWritePolicyDisposition -> context.previous.previous -> request.task.
const execute = promisify(execFile);
const FIVE_MIB = 5 * 1024 * 1024;
const FIXED = "        const onWritePolicyDisposition = writePolicyDispositionAudit(logFile, id, runner, writePolicy);\n";
// The exact original e22d68dc callback (unsafe lexical capture); only this boundary is substituted.
const UNSAFE = [
  "        let dispositionLogged = false;",
  "        const onWritePolicyDisposition = (disposition: \"local\" | \"remote\"): void => {",
  "          if (dispositionLogged) return;",
  "          dispositionLogged = true;",
  "          if (disposition === \"local\") auditWritePolicy(\"write_policy.default\",",
  "            \"Shell writes are unconfined by the task write policy (native bash/powershell and nested pi.bash); any Fabric Landlock settings are independent and may grant wider roots.\", writePolicy);",
  "          else auditWritePolicy(\"write_policy.remote_unconfined\",",
  "            \"Remote one-shot placement cannot forward task write roots or scratch yet; remote task remains unrestricted.\");",
  "        };",
  "",
].join("\n");

const childProgram = `
  import path from "node:path";
  import { createHash } from "node:crypto";
  import { createJiti } from "jiti";
  import { pathToFileURL } from "node:url";
  const [root, variant, fixed, unsafe] = process.argv.slice(1);
  const base = pathToFileURL(process.cwd() + "/index.js").href;
  const transformer = createJiti(base, { fsCache: false, tryNative: false });
  const managerPath = path.resolve("src/agents/manager.ts");
  let substituted = 0, sourceSha256 = "";
  const jiti = createJiti(base, { fsCache: false, tryNative: false, transform(options) {
    if (options.filename !== managerPath) return { code: transformer.transform(options) };
    let source = options.source;
    if (variant === "negative") {
      if (source.split(fixed).length !== 2) throw new Error("fixed callback boundary not found exactly once");
      source = source.replace(fixed, unsafe);
      substituted++;
    }
    sourceSha256 = createHash("sha256").update(source).digest("hex");
    return { code: transformer.transform({ ...options, source }) };
  } });
  const { writePolicyDispositionHeapProbe } = await jiti.import("./tests/fixtures/write-policy-disposition-heap-probe.ts");
  const receipt = await writePolicyDispositionHeapProbe(root);
  console.log(JSON.stringify({ completedReceipt: { ...receipt, variant, substituted, sourceSha256 } }));
`;

type Receipt = Awaited<ReturnType<typeof writePolicyDispositionHeapProbe>> & {
  variant: "negative" | "fixed"; substituted: number; sourceSha256: string;
};
const probe = async (variant: "negative" | "fixed"): Promise<Receipt> => {
  // Private root stays for its environment owner; nothing here deletes.
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "disposition-heap-"));
  try {
    const result = await execute("node", ["--expose-gc", "--max-old-space-size=4096", "--input-type=module", "-e", childProgram,
      root, variant, FIXED, UNSAFE], { cwd: process.cwd(), timeout: 90_000, killSignal: "SIGKILL", maxBuffer: 1024 * 1024 });
    // Receipts precede assertions, including the intentionally leaking control.
    process.stdout.write(result.stdout);
    if (result.stderr) process.stderr.write(result.stderr);
    const rows = result.stdout.trim().split("\n").map(line => JSON.parse(line) as { completedReceipt?: Receipt });
    const receipt = rows.find(row => row.completedReceipt)?.completedReceipt;
    if (!receipt) throw new Error("child did not return a completion receipt");
    return receipt;
  } catch (error) {
    const output = error as { stdout?: string; stderr?: string };
    if (output.stdout) process.stdout.write(output.stdout);
    if (output.stderr) process.stderr.write(output.stderr);
    throw error;
  }
};

it("retained onWritePolicyDisposition callbacks do not keep closed runs' tasks; the restored lexical callback does",
  async () => {
    // Sequential fresh children bound peak memory.
    const negative = await probe("negative");
    const fixed = await probe("fixed");
    for (const receipt of [negative, fixed]) {
      expect(receipt).toMatchObject({ runs: 100, taskUnits: 4 * 1024 * 1024, callbacks: 100,
        localRuns: 50, remoteRuns: 50, checks: [] });
      expect(receipt.sourceSha256).toMatch(/^[a-f0-9]{64}$/);
    }
    expect(negative.substituted).toBe(1);
    expect(fixed.substituted).toBe(0);
    expect(negative.sourceSha256).not.toBe(fixed.sourceSha256);
    expect(negative.retainedBytes).toBeGreaterThan(100 * FIVE_MIB / 2);
    expect(fixed.retainedBytes).toBeLessThan(FIVE_MIB);
  },
  190_000,
);
