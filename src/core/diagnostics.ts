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
  notifyPrincipalDiagnosticOnce,
  resetPrincipalDiagnosticTurn,
} from "./agent-dir.js";
