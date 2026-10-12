import type { FabricProgramKernel } from "./store.js";

// Guest source for saved programs. A program reads its input from the
// `input` global, declared by a prefix in the program's own kernel language.

/** JSON value as a Python literal (JSON string escapes are valid Python). */
export const pythonLiteral = (value: unknown): string => {
  if (value === null || value === undefined) return "None";
  if (value === true) return "True";
  if (value === false) return "False";
  if (typeof value === "number") return Number.isFinite(value) ? JSON.stringify(value) : "None";
  if (typeof value === "string") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(pythonLiteral).join(", ")}]`;
  return `{${Object.entries(value as Record<string, unknown>)
    .filter(([, item]) => item !== undefined)
    .map(([key, item]) => `${JSON.stringify(key)}: ${pythonLiteral(item)}`)
    .join(", ")}}`;
};

/**
 * TypeScript keeps line numbers by declaring `input` on the program's first
 * line; Python cannot join a compound statement after `;`, so it gets its own
 * line.
 */
export const programSourceWithInput = (code: string, kernel: FabricProgramKernel, input: unknown): string =>
  kernel === "python"
    ? `input = ${pythonLiteral(input ?? null)}\n${code}`
    : `const input: any = ${JSON.stringify(input ?? null)}; ${code}`;

/** A one-call host program that runs a saved program through `programs.run`. */
export const hostProgramRunSource = (
  kernel: FabricProgramKernel,
  args: { ref: string; input?: unknown; requirePromoted?: boolean },
): string => {
  if (kernel === "python") {
    const parts = [`ref=${pythonLiteral(args.ref)}`];
    if (args.input !== undefined) parts.push(`input=${pythonLiteral(args.input)}`);
    if (args.requirePromoted) parts.push("requirePromoted=True");
    return `return await programs.run(${parts.join(", ")})`;
  }
  return `return await programs.run(${JSON.stringify({
    ref: args.ref,
    ...(args.input !== undefined ? { input: args.input } : {}),
    ...(args.requirePromoted ? { requirePromoted: true } : {}),
  })});`;
};
