import { describe, it } from "vitest";

// Windows trace archival intentionally fails closed until owner/ACL-verified support lands (smarty-dev#15838).
export const itPosixArchive = it.skipIf(process.platform === "win32");
export const describePosixArchive = describe.skipIf(process.platform === "win32");
