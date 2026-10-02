/** Legacy/pre-protocol launchers cannot acquire retrofitted release custody.
 * Idle participant or /proc samples are not whole-attempt exit receipts.
 * No suspend, signal, or automatic replacement path is permitted here.
 * Automatic retirement remains a follow-up to complete attempt containment.
 */
export const legacyRetirementDiagnostic =
  "Legacy resident host/launcher has no release custody protocol; installer drain required";
