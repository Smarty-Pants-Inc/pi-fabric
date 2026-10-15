// Stable import path for the one Windows PowerShell 5.1 ACL command builder, which lives
// in windows-temp-root.ts. Layout only (startup graph): the scratch import gives every
// importer, lazy ones included, the whole eager storage closure, so esbuild keeps the
// builder in that existing chunk instead of splitting it into one more startup file.
import "./scratch.js";
export { windowsSecurityPowerShell } from "./windows-temp-root.js";
