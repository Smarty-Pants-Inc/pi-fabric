import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

// smarty-dev#8121 / pi-fabric#879 review: a cached pinned compiler is trusted only from a private cache, and
// only as the sha256-verified tarball, unpacked fresh for each build. No network: the tarball bytes are planted.
const temps: string[] = [];
const privateDir = (): string => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "native-toolchain-"));
  fs.chmodSync(dir, 0o700);
  temps.push(dir);
  return dir;
};
afterEach(() => {
  vi.unstubAllEnvs();
  for (const dir of temps.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});
const toolchain = async (cache: string) => {
  vi.stubEnv("FABRIC_TOOLCHAIN_CACHE", cache);
  return import("../scripts/native-toolchain.mjs");
};
const tarballName = (m: Awaited<ReturnType<typeof toolchain>>): string => {
  const arch = m.nativeArch();
  return `zig-${m.TOOLCHAIN.zig.version}-${arch}-${m.TOOLCHAIN.zig[arch]!.sha256}.tar.xz`;
};

describe.runIf(process.platform === "linux" && ["x64", "arm64"].includes(process.arch))("pinned native toolchain cache", () => {
  it("accepts a private cache chain", async () => {
    const cache = path.join(privateDir(), "cache");
    fs.mkdirSync(cache, { mode: 0o700 });
    const m = await toolchain(cache);
    expect(m.untrustedCache()).toBeUndefined();
    expect(m.pinnedTarball().reason).toContain("not fetched");
  });

  it("refuses a group-writable cache directory", async () => {
    const cache = path.join(privateDir(), "cache");
    fs.mkdirSync(cache);
    fs.chmodSync(cache, 0o770);
    const m = await toolchain(cache);
    expect(m.untrustedCache()).toContain("group- or other-writable");
    fs.writeFileSync(path.join(cache, tarballName(m)), "planted");
    expect(m.pinnedTarball()).toEqual({ reason: expect.stringContaining("untrusted toolchain cache") });
  });

  it("refuses a group-writable ancestor of ours above a private cache", async () => {
    const base = privateDir();
    fs.mkdirSync(path.join(base, "shared"));
    fs.chmodSync(path.join(base, "shared"), 0o770);
    fs.mkdirSync(path.join(base, "shared", "cache"), { mode: 0o700 });
    const m = await toolchain(path.join(base, "shared", "cache"));
    expect(m.untrustedCache()).toContain(`${path.join(base, "shared")} is group- or other-writable`);
  });

  it("accepts a root-owned ancestor only when it is not group/other-writable, or is sticky", async () => {
    const m = await toolchain("/stub/cache");
    const uid = process.getuid!();
    // Root ownership cannot be faked on disk without root: inject the stat for the root-owned levels only.
    const chain = (rootMode: number) => (dir: string) => {
      const own = dir === "/stub/cache";
      return { uid: own ? uid : 0, mode: own ? 0o40700 : rootMode, isDirectory: () => true, isSymbolicLink: () => false };
    };
    expect(m.untrustedCache("/stub/cache", chain(0o40755))).toBeUndefined();
    expect(m.untrustedCache("/stub/cache", chain(0o41777))).toBeUndefined(); // sticky, like /tmp
    expect(m.untrustedCache("/stub/cache", chain(0o40775))).toContain("/stub is group- or other-writable (mode 775)");
    expect(m.untrustedCache("/stub/cache", chain(0o40777))).toContain("/stub is group- or other-writable (mode 777)");
  });

  it("refuses a sticky world-writable ancestor that is ours: only a root-owned sticky one is exempt", async () => {
    const base = privateDir();
    fs.mkdirSync(path.join(base, "shared"));
    fs.chmodSync(path.join(base, "shared"), 0o1777);
    fs.mkdirSync(path.join(base, "shared", "cache"), { mode: 0o700 });
    const m = await toolchain(path.join(base, "shared", "cache"));
    expect(m.untrustedCache()).toContain(`${path.join(base, "shared")} is group- or other-writable (mode 1777)`);
  });

  it("refuses a symlinked toolchain directory", async () => {
    const base = privateDir();
    fs.mkdirSync(path.join(base, "real"), { mode: 0o700 });
    fs.symlinkSync(path.join(base, "real"), path.join(base, "cache"));
    const m = await toolchain(path.join(base, "cache"));
    expect(m.untrustedCache()).toContain("is not a real directory");
    expect(m.pinnedTarball().reason).toContain("untrusted toolchain cache");
  });

  it("refuses a planted compiler: a cached tarball must hash to the pin, and nothing else in the cache is read", async () => {
    const cache = path.join(privateDir(), "cache");
    fs.mkdirSync(cache, { mode: 0o700 });
    const m = await toolchain(cache);
    const tarball = path.join(cache, tarballName(m));
    fs.writeFileSync(tarball, "a planted toolchain");
    // A forged receipt and an unpacked zig beside it change nothing.
    fs.writeFileSync(path.join(cache, ".verified-sha256"), `${m.TOOLCHAIN.zig[m.nativeArch()]!.sha256}\n`);
    fs.writeFileSync(path.join(cache, "zig"), "#!/bin/sh\nexit 0\n", { mode: 0o700 });
    expect(m.pinnedTarball()).toEqual({ path: tarball });
    const use = vi.fn();
    expect(() => m.withPinnedZig(tarball, use)).toThrow(/is not the pinned/);
    expect(use).not.toHaveBeenCalled();
  });
});
