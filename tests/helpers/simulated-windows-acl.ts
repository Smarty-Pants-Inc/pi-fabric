import { vi } from "vitest";
import { windowsRequestsAclAdapters, type WindowsDirectoryAcl, type WindowsRequestsAclDeps } from "../../src/residency/windows-acl.js";

const nativePlatform = process.platform;
const userSid = "S-1-5-21-1000000001-1000000002-1000000003-1001";

const privateAcl = (user = userSid): WindowsDirectoryAcl => ({
  user, owner: user, dacl: "present",
  aces: [user, "S-1-5-18", "S-1-5-32-544"].map(sid => ({ type: "Allow", sid, rights: 2032127 })),
});

/** Simulate Set-Acl replacement so tests must actually secure inherited writer grants. */
export const createSimulatedWindowsRequestsAcl = (
  initialAcl: (directory: string) => WindowsDirectoryAcl = () => privateAcl(),
): Required<WindowsRequestsAclDeps> => {
  const secured = new Map<string, WindowsDirectoryAcl>();
  return {
    readAcl: async directory => secured.get(directory) ?? initialAcl(directory),
    restrict: async (directory, user) => {
      const acl = secured.get(directory) ?? initialAcl(directory);
      if (acl.owner !== user || acl.user !== user) throw new Error("foreign directory owner");
      secured.set(directory, privateAcl(user));
    },
    // Request files the host itself wrote: FILE_ATTRIBUTE_ARCHIVE, inherited private DACL.
    readFileAcls: async files => files.map(() => ({ attributes: 0x20, acl: privateAcl() })),
  };
};

/**
 * Tests injecting win32 on another OS have no Get-Acl or Set-Acl. Real Windows
 * keeps the product adapters; vitest restores these spies after each test.
 */
export const installSimulatedWindowsRequestsAcl = (): void => {
  if (nativePlatform === "win32" || vi.isMockFunction(windowsRequestsAclAdapters.readAcl)) return;
  const deps = createSimulatedWindowsRequestsAcl();
  vi.spyOn(windowsRequestsAclAdapters, "readAcl").mockImplementation(deps.readAcl);
  vi.spyOn(windowsRequestsAclAdapters, "restrict").mockImplementation(deps.restrict);
  vi.spyOn(windowsRequestsAclAdapters, "readFileAcls").mockImplementation(deps.readFileAcls);
};
