import fs from "node:fs/promises";
import { closeScratch, createScratch } from "./storage/scratch.js";
import path from "node:path";
import { truncateMiddle } from "./util.js";
import type { FabricResidentOutcomeReceipt } from "./runtime/kernel.js";

export const MAX_FAILURE_MODEL_OUTPUT_CHARS = 20_000;

export const modelOutputBudget = (
  configuredMaxChars: number,
  success: boolean,
): number => success
  ? configuredMaxChars
  : Math.min(configuredMaxChars, MAX_FAILURE_MODEL_OUTPUT_CHARS);

export interface BoundedModelOutput {
  text: string;
  artifactPath?: string;
  originalChars: number;
  omittedChars: number;
}

type ArtifactWriter = (content: string) => Promise<string>;

export interface PriorityModelOutput {
  /** Safety-critical host facts. This block is FIRST and is never truncated. */
  text: string;
  /** Independently budgeted prose (guest logs, runtime cause, progress, etc.). */
  sections: readonly string[];
}

export const formatResidentOutcomePriority = (receipts: readonly FabricResidentOutcomeReceipt[]): string => [
  "ResidentOutcomeUnknownError: resident mutation outcome unknown; reconcile the receipts below.",
  "Do not retry or reassign this work. Check agents.actorStatus / agents.status / agents.list; publication may still be pending. Use agents.stop with the known ID once registered to cancel.",
  `Resident receipts (${receipts.length}):`,
  ...receipts.map(receipt => `- state=${receipt.state}, operation=${receipt.operation}, requestId=${receipt.requestId}, ` +
    `${receipt.entityKind}Id=${receipt.id ?? "not yet known"}, ownerHostId=${receipt.ownerHostId ?? "not yet known"}`),
].join("\n");

/** Fairly share only the remaining prose budget, redistributing short sections' unused shares. */
const boundPrioritySections = (sections: readonly string[], budget: number): string[] => {
  const values = sections.filter(Boolean);
  const budgets = values.map(() => 0);
  let remaining = Math.floor(Math.max(0, budget));
  let pending = values.map((_, index) => index);
  while (remaining > 0 && pending.length > 0) {
    const share = Math.max(1, Math.floor(remaining / pending.length));
    for (const index of pending) {
      const take = Math.min(remaining, share, values[index]!.length - budgets[index]!);
      budgets[index]! += take;
      remaining -= take;
    }
    pending = pending.filter(index => budgets[index]! < values[index]!.length);
  }
  return values.map((value, index) => {
    const limit = budgets[index]!;
    if (limit === 0) return "";
    const bounded = truncateMiddle(value, limit);
    // A truncation marker can exceed a tiny allocation. Only prose is cut here.
    return bounded.length <= limit ? bounded : value.slice(0, limit);
  }).filter(Boolean);
};

const writeOutputArtifact: ArtifactWriter = async (content) => {
  const directory = createScratch("output");
  try {
    const artifactPath = path.join(directory, "output.txt");
    await fs.writeFile(artifactPath, content, { encoding: "utf8", mode: 0o600, flag: "wx" });
    closeScratch(directory);
    return artifactPath;
  } catch (error) {
    await fs.rm(directory, { recursive: true, force: true }).catch(() => undefined);
    throw error;
  }
};

export const boundModelOutput = async (
  visible: string,
  maxChars: number,
  fullOutput = visible,
  writeArtifact: ArtifactWriter = writeOutputArtifact,
  priority?: PriorityModelOutput,
): Promise<BoundedModelOutput> => {
  if (priority) visible = [priority.text, ...priority.sections.filter(Boolean)].join("\n\n");
  if (visible.length <= maxChars && fullOutput.length <= maxChars) {
    return { text: visible, originalChars: fullOutput.length, omittedChars: 0 };
  }

  let artifactPath: string | undefined;
  try {
    artifactPath = await writeArtifact(fullOutput);
  } catch {
    artifactPath = undefined;
  }
  const suffix = artifactPath
    ? `\n\n[Full output (${fullOutput.length} chars) saved to: ${artifactPath}]`
    : "";
  if (priority) {
    // IDs and the no-reassignment warning are an irreducible safety floor.
    // If they alone exceed a configured soft budget, keep ALL receipts rather
    // than silently dropping an entity. Only supplementary prose is bounded.
    const prioritySuffix = priority.text.length + suffix.length <= maxChars ? suffix : "";
    const sectionCount = priority.sections.filter(Boolean).length;
    const proseBudget = Math.max(0, maxChars - priority.text.length - prioritySuffix.length - sectionCount * 2);
    const sections = boundPrioritySections(priority.sections, proseBudget);
    const text = [priority.text, ...sections].join("\n\n") + prioritySuffix;
    return {
      text,
      ...(artifactPath ? { artifactPath } : {}),
      originalChars: fullOutput.length,
      omittedChars: Math.max(0, fullOutput.length - priority.text.length - proseBudget),
    };
  }
  const bodyBudget = Math.max(1, maxChars - suffix.length);
  const body = truncateMiddle(visible, bodyBudget);
  let text = `${body}${suffix}`;
  if (text.length > maxChars) {
    // The suffix carries the artifact path; shrink the body again instead of
    // cutting into the path. A truncation marker can itself exceed a tiny
    // rebudget, so fall back to the bare suffix (or its tail), which always
    // fits and still ends with the path.
    const rebudget = maxChars - suffix.length;
    if (rebudget <= 0) {
      text = suffix.slice(-Math.max(1, maxChars));
    } else {
      const shrunk = truncateMiddle(body, rebudget);
      text = shrunk.length + suffix.length <= maxChars ? `${shrunk}${suffix}` : suffix;
    }
  }
  return {
    text,
    ...(artifactPath ? { artifactPath } : {}),
    originalChars: fullOutput.length,
    omittedChars: Math.max(0, fullOutput.length - Math.min(fullOutput.length, bodyBudget)),
  };
};
