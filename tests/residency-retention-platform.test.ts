import fs from "node:fs";
import fsp from "node:fs/promises";
import { expect, it, vi } from "vitest";
import { retentionV2Enabled } from "../src/storage/retention-platform.js";
import { ResidentLegacyRunArchive } from "../src/residency/legacy-run-archive.js";

it("one platform gate enables POSIX V2 and keeps Windows retirement silent and inactive (smarty-dev#5132)", async () => {
  const nativePlatform = Object.getOwnPropertyDescriptor(process, "platform")!;
  const archive = new ResidentLegacyRunArchive("unused-retention-platform-root", {}, { isRetained: () => { throw new Error("must not inspect custody"); } });
  try {
    Object.defineProperty(process, "platform", { ...nativePlatform, value: "linux" });
    expect(retentionV2Enabled()).toBe(true);
    Object.defineProperty(process, "platform", { ...nativePlatform, value: "win32" });
    expect(retentionV2Enabled()).toBe(false);
    const timer = vi.spyOn(globalThis, "setInterval");
    const audit = vi.spyOn(console, "info");
    const open = vi.spyOn(fsp, "opendir");
    const syncOpen = vi.spyOn(fs, "opendirSync");
    const move = vi.spyOn(fsp, "rename");
    const mkdir = vi.spyOn(fsp, "mkdir");
    const remove = vi.spyOn(fsp, "rm");
    archive.start(); await archive.sweep(); await archive.sweep();
    for (const probe of [timer, audit, open, syncOpen, move, mkdir, remove]) expect(probe).not.toHaveBeenCalled();
    expect(archive.health).toEqual({ checked: 0, archived: 0, skipped: 0, error: "" });
  } finally {
    await archive.close(); vi.restoreAllMocks(); Object.defineProperty(process, "platform", nativePlatform);
  }
});
