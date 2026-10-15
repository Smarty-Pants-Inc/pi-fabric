import { readFileSync } from "node:fs";
import { afterEach, describe, expect, it } from "vitest";
import { windowsHelperDigest } from "../src/storage/verified-remove.js";

const nativeGlobal = globalThis as typeof globalThis & { __FABRIC_NATIVE_DIGESTS__?: string };
const previous = nativeGlobal.__FABRIC_NATIVE_DIGESTS__;

afterEach(() => {
  if (previous === undefined) delete nativeGlobal.__FABRIC_NATIVE_DIGESTS__;
  else nativeGlobal.__FABRIC_NATIVE_DIGESTS__ = previous;
});

describe("source-run native helper trust", () => {
  it("uses the pinned Windows helper digest when the build define is absent", () => {
    const toolchain = JSON.parse(readFileSync(new URL("../native/toolchain.json", import.meta.url), "utf8")) as {
      expected?: { windows?: Record<string, string> };
    };
    delete nativeGlobal.__FABRIC_NATIVE_DIGESTS__;
    expect(windowsHelperDigest()).toBe(toolchain.expected?.windows?.["fabric-win-remove.exe"]);
  });
});
