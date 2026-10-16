import type { FabricExecutionTraceOperationV1, FabricExecutionTraceV1 } from "./audit/trace.js";

const MAX_COMPLETED_CALLS = 8;
const MAX_PATH_CHARS = 100;
const RECEIPT_REFS = new Set([
  "agents.followUp",
  "agents.steer",
  "mesh.publish",
  "agents.create",
  "agents.spawn",
]);

const compactPath = (value: string): string => {
  const singleLine = value.replaceAll(/\s+/g, " ").trim();
  if (singleLine.length <= MAX_PATH_CHARS) return singleLine;
  return `…${singleLine.slice(-(MAX_PATH_CHARS - 1))}`;
};

const isStructuralIdentifier = (value: unknown): value is string =>
  typeof value === "string" &&
  value.length > 0 &&
  value.length <= 1_024 &&
  /^[A-Za-z0-9][A-Za-z0-9._:/-]*$/.test(value);

const effectiveRef = (operation: FabricExecutionTraceOperationV1): string =>
  operation.provider && operation.action
    ? `${operation.provider}.${operation.action}`
    : operation.ref;

const receiptParts = (operation: FabricExecutionTraceOperationV1): string[] => {
  const result = operation.result;
  if (typeof result !== "object" || result === null || Array.isArray(result)) return [];
  const record = result as Record<string, unknown>;
  const parts: string[] = [];
  const addIdentifier = (key: string): void => {
    if (isStructuralIdentifier(record[key])) parts.push(`${key}=${record[key]}`);
  };
  const addNumber = (key: string): void => {
    const value = record[key];
    if (typeof value === "number" && Number.isFinite(value)) parts.push(`${key}=${value}`);
  };
  switch (effectiveRef(operation)) {
    case "agents.followUp":
    case "agents.steer":
      addIdentifier("messageId");
      addIdentifier("deliveryId");
      break;
    case "mesh.publish":
      addIdentifier("id");
      addNumber("sequence");
      break;
    case "agents.create":
    case "agents.spawn":
      addIdentifier("id");
      break;
  }
  return parts;
};

const hasRequiredReceiptId = (operation: FabricExecutionTraceOperationV1): boolean => {
  const result = operation.result;
  if (typeof result !== "object" || result === null || Array.isArray(result)) return false;
  const record = result as Record<string, unknown>;
  switch (effectiveRef(operation)) {
    case "agents.followUp":
    case "agents.steer":
      return isStructuralIdentifier(record.messageId);
    case "mesh.publish":
    case "agents.create":
    case "agents.spawn":
      return isStructuralIdentifier(record.id);
    default:
      return true;
  }
};

const operationSummary = (operation: FabricExecutionTraceOperationV1): string => {
  const ref = effectiveRef(operation);
  if (RECEIPT_REFS.has(ref)) {
    const receipt = receiptParts(operation);
    return `${ref}[${receipt.length > 0 ? receipt.join(", ") : "receipt unavailable"}]`;
  }
  const path = operation.args.path;
  return typeof path === "string"
    ? `${ref}(${compactPath(path)})`
    : ref;
};

export const formatFailureProgress = (
  trace: FabricExecutionTraceV1,
): string | undefined => {
  if (trace.outcome === "succeeded") return undefined;
  const completed = trace.operations.filter(
    (operation) => operation.outcome === "succeeded",
  );
  const ordinaryCompleted = completed.filter(
    (operation) => !RECEIPT_REFS.has(effectiveRef(operation)),
  );
  const shownOrdinary = new Set(ordinaryCompleted.slice(0, MAX_COMPLETED_CALLS));
  const shown = completed.filter(
    (operation) => RECEIPT_REFS.has(effectiveRef(operation)) || shownOrdinary.has(operation),
  );
  const omittedOrdinary = ordinaryCompleted.length - shownOrdinary.size;
  const uncertain = trace.operations.filter(
    (operation) =>
      operation.outcome !== "succeeded" && operation.failureStage === "invoke",
  );
  const incomplete: string[] = [];
  if (trace.counts.droppedOperations > 0) {
    incomplete.push(
      `receipt list incomplete: ${trace.counts.droppedOperations} trace entries dropped; do not assume unlisted calls did not run.`,
    );
  }
  if (completed.some(
    (operation) => RECEIPT_REFS.has(effectiveRef(operation)) && !hasRequiredReceiptId(operation),
  )) {
    incomplete.push(
      "receipt list incomplete: a completed side-effect receipt id was not retained; do not assume the call did not run.",
    );
  }
  if (shown.length === 0 && uncertain.length === 0 && incomplete.length === 0) return undefined;

  const sections: string[] = [];
  if (incomplete.length > 0) sections.push(...incomplete);
  if (shown.length > 0) {
    sections.push(
      `Completed before the outer failure (outputs not returned; side-effect receipts shown): ${shown
        .map(operationSummary)
        .join("; ")}${omittedOrdinary > 0 ? `; +${omittedOrdinary} more` : ""}.`,
    );
  }
  if (uncertain.length > 0) {
    sections.push(
      `Started but not confirmed (delivery outcome unknown): ${uncertain
        .map((operation) => receiptParts(operation).length > 0 ? operationSummary(operation) : effectiveRef(operation))
        .join("; ")}.`,
    );
  }
  if (shown.length > 0) {
    sections.push(
      "Successful calls may already have changed the workspace; inspect before repeating mutations.",
    );
  }
  return sections.join("\n");
};
