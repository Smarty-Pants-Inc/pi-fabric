// N7's authenticated 19:42Z exception is ONLY this SDK-owned UNCONFIGURED
// placeholder, never an invented provider/model. A pin bump requires fresh review.
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { types } from "node:util";

export const NATIVE_SDK_RELEASE = "031c749962972b0cb966e443e921d64654627b20";
// Private proof resources are caller-supplied, never host path allow-lists.
// Approval remains the reviewed release and ALL ten source/package hashes below.
export const NATIVE_SDK_DIRECTORY = process.env.FABRIC_NATIVE_TEST_PI_SDK_DIST ?? "";
export const NATIVE_SDK_SOURCE_PINS: Readonly<Record<string, string>> = Object.freeze({
  "../package.json": "a87892a2985221d6c80a921a5016341117ebf8223f5f49eac12c79b3ac43719c",
  "../../pi-agent-core/package.json": "541f131cd43832f75a9c64ee886c367c219c0772da7047c66053d7cc80db98b9",
  "../../pi-agent-core/dist/agent.js": "0e73cc09ae5563b42e34272d5f3257766923b4086461944785c24701f7d0d04d",
  "index.js": "b2285dc6a9c15d8b577a6dbc287ec6babec2703cf8637afb733ad824156a8d6a",
  "core/sdk.js": "3f4859912cb8465239924468690fb8bf1f3d3a843e79273aa234c2923b6ecede",
  "core/model-runtime.js": "58a8afd627708c0e43fde668234c70be883134b4c8428846691e503928cb067f",
  "core/model-policy.js": "fdb5dc9c090e4b77e084e2bb3a40022eedd537d356767357b15301d9907de88f",
  "core/agent-session.js": "8dbef0a2e0f00b99ba176808442840201f29e1b4e80ffa1fd2bf7cc332a65f28",
  "core/settings-manager.js": "3ce7458e9c41ccb2bb83f11249148e8d5a97e9bbb6fd68f51dda4665827ffcbe",
  "core/session-manager.js": "ca138e988f07b62d403218007a95cf9e9a70d0f194c1eaeb7a4b796f5311d5a9",
});

/** Do not learn approval by hashing the environment and trusting those new hashes. */
export function assertApprovedNativeSdk(sdkDirectory: string): void {
  if (!path.isAbsolute(sdkDirectory) || path.resolve(sdkDirectory) !== sdkDirectory || fs.realpathSync(sdkDirectory) !== sdkDirectory) {
    throw new Error(`N7 BLOCKED: unapproved SDK release; required ${NATIVE_SDK_RELEASE}`);
  }
  for (const [relative, expected] of Object.entries(NATIVE_SDK_SOURCE_PINS)) {
    const actual = createHash("sha256").update(fs.readFileSync(path.join(sdkDirectory, relative))).digest("hex");
    if (actual !== expected) throw new Error(`N7 BLOCKED: approved SDK source changed: ${relative}`);
  }
}

/** Ordinary CI declares missing native coverage; the explicit native gate may never skip.
 * Validate any supplied SDK first, even if another prerequisite is absent: an
 * unapproved path or changed readable source is a failure, not an expected gap.
 */
export function nativeSdkTestPrerequisite(): string | undefined {
  const sdkDirectory = process.env.FABRIC_NATIVE_TEST_PI_SDK_DIST;
  const nodeBinary = process.env.FABRIC_NATIVE_TEST_NODE;
  let missingSdk = false;
  if (sdkDirectory) {
    if (!path.isAbsolute(sdkDirectory) || path.resolve(sdkDirectory) !== sdkDirectory) throw new Error("N7 BLOCKED: unapproved SDK path");
    try {
      if (fs.realpathSync(sdkDirectory) !== sdkDirectory) throw new Error("N7 BLOCKED: unapproved SDK realpath");
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      missingSdk = true;
    }
    // Check every existing pinned source before declaring absent files skippable.
    for (const [relative, expected] of Object.entries(NATIVE_SDK_SOURCE_PINS)) {
      try {
        const actual = createHash("sha256").update(fs.readFileSync(path.join(sdkDirectory, relative))).digest("hex");
        if (actual !== expected) throw new Error(`N7 BLOCKED: approved SDK source changed: ${relative}`);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
        missingSdk = true;
      }
    }
    if (!missingSdk) assertApprovedNativeSdk(sdkDirectory);
  }
  let missingNode = !nodeBinary;
  if (nodeBinary) {
    if (!path.isAbsolute(nodeBinary)) throw new Error("N7 BLOCKED: native test Node must be an explicit absolute path");
    try { fs.accessSync(nodeBinary, fs.constants.X_OK); missingNode = !fs.statSync(nodeBinary).isFile(); }
    catch (error) {
      if (!["ENOENT", "EACCES"].includes(String((error as NodeJS.ErrnoException).code))) throw error;
      missingNode = true;
    }
  }
  const gaps = [process.platform !== "linux" && "Linux kernel coverage", !sdkDirectory && "FABRIC_NATIVE_TEST_PI_SDK_DIST",
    missingSdk && "approved SDK files", missingNode && "executable FABRIC_NATIVE_TEST_NODE"].filter(Boolean);
  if (!gaps.length) return undefined;
  const reason = `native prerequisites absent: ${gaps.join(", ")}; run the approved Linux native gate for coverage`;
  if (process.env.FABRIC_NATIVE_TEST_REQUIRED === "1") throw new Error(`NATIVE_TEST_REQUIRED: ${reason}`);
  return reason;
}

// Literal copied from the APPROVED pi-agent-core/dist/agent.js:18-30. Kept
// private and mutable-descriptor-shaped, matching both live SDK and RPC JSON.
const unconfigured = {
  id: "unknown", name: "unknown", api: "unknown", provider: "unknown", baseUrl: "",
  reasoning: false, input: [], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  contextWindow: 0, maxTokens: 0,
};
const exact = (actual: unknown, expected: unknown): boolean => {
  if (expected === null || typeof expected !== "object") return Object.is(actual, expected);
  if (actual === null || typeof actual !== "object" || types.isProxy(actual) ||
      Object.getPrototypeOf(actual) !== Object.getPrototypeOf(expected)) return false;
  const keys = Reflect.ownKeys(expected);
  if (Reflect.ownKeys(actual).length !== keys.length) return false;
  return keys.every(key => {
    const a = Object.getOwnPropertyDescriptor(actual, key), e = Object.getOwnPropertyDescriptor(expected, key)!;
    // No accessor evaluation, hidden/symbol extras, array holes, foreign/null
    // prototypes, coercion, or incomplete nested-cost comparisons.
    return !!a && "value" in a && "value" in e && a.enumerable === e.enumerable &&
      a.configurable === e.configurable && a.writable === e.writable && exact(a.value, e.value);
  });
};

/** Shared by the real Main/resident hooks and the parent's RPC readback. */
export function isNativeSdkUnconfigured(model: unknown, sdkDirectory: string): boolean {
  try { assertApprovedNativeSdk(sdkDirectory); return exact(model, unconfigured); }
  catch { return false; }
}
