// Stable source API; profile-scoped implementation shares the existing eager core entry.
export {
  DIAGNOSTICS_MAX_BYTES,
  isPrincipalDiagnostics,
  setPrewalkStatus,
  fabricDiagnosticsFile,
  appendDiagnosticSync,
  configureFabricDiagnostics,
  flushFabricDiagnostics,
  diagnostics,
  notifyDiagnostic,
} from "./agent-dir.js";
