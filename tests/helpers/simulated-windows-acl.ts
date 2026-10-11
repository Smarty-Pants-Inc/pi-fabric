import { vi } from "vitest";
import { windowsRequestsAclAdapters } from "../../src/residency/windows-acl.js";

const nativePlatform = process.platform;
const userSid = "S-1-5-21-1000000001-1000000002-1000000003-1001";

/**
 * Tests that inject process.platform = "win32" on another OS have no Get-Acl or
 * icacls. Report the private DACL the real create path sets (this user, SYSTEM,
 * Administrators). On real Windows the product adapters stay in place. Spies are
 * reset by vitest's restoreMocks after each test.
 */
export const installSimulatedWindowsRequestsAcl = (): void => {
  if (nativePlatform === "win32" || vi.isMockFunction(windowsRequestsAclAdapters.readAcl)) return;
  const privateAcl = () => ({
    user: userSid, owner: userSid, dacl: "present" as const,
    aces: [userSid, "S-1-5-18", "S-1-5-32-544"].map(sid => ({ type: "Allow" as const, sid, rights: 2032127 })),
  });
  vi.spyOn(windowsRequestsAclAdapters, "readAcl").mockImplementation(async () => privateAcl());
  vi.spyOn(windowsRequestsAclAdapters, "restrict").mockResolvedValue(undefined);
  // Request files the host itself wrote: FILE_ATTRIBUTE_ARCHIVE, inherited private DACL.
  vi.spyOn(windowsRequestsAclAdapters, "readFileAcls").mockImplementation(async files => files.map(() => ({ attributes: 0x20, acl: privateAcl() })));
};
