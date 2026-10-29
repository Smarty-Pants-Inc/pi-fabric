import { describe, vi } from "vitest";
import type { RecoveryJournal, RecoveryJournalOptions } from "../../src/recovery/durability-adapter-journal.js";
import { runConformance } from "../contracts/l7-b6-durability-v1/conformance.js";
import { createRealDurabilityHarness } from "./harness.js";

// Keep: the real journal/adapter trust boundary, committed-work data safety and lower-layer
// faults. This runs EVERY frozen shared case; no fake implementation or outcome is imported.
const decorators = vi.hoisted(() => new Map<string, (journal: RecoveryJournal) => RecoveryJournal>());
vi.mock("../../src/recovery/durability-adapter-journal.js", async () => {
  const actual = await vi.importActual<typeof import("../../src/recovery/durability-adapter-journal.js")>(
    "../../src/recovery/durability-adapter-journal.js",
  );
  return {
    ...actual,
    async openRecoveryJournal(options: RecoveryJournalOptions): Promise<RecoveryJournal> {
      const journal = await actual.openRecoveryJournal(options);
      try {
        return decorators.get(options.directory)?.(journal) ?? journal;
      } catch (error) {
        await journal.close();
        throw error;
      }
    },
  };
});

// Expected platform skip ONLY: the existing syncPathNamespaceDurable primitive explicitly
// refuses Windows directory durability. Skipped coverage is not a fake green durability
// claim. Unix-like runners must execute the complete unchanged shared suite below.
describe.skipIf(process.platform === "win32")("real file-backed B6 namespace durability", () => {
  runConformance({
    name: "openRecoveryDurabilityStore + real file journal / injected native fixture catalogue",
    create: () => createRealDurabilityHarness((directory, decorate) => {
      if (decorators.has(directory)) throw new Error("Duplicate real-journal fixture custody");
      decorators.set(directory, decorate);
      // The partial mock becomes an untouched importActual pass-through after each close.
      return () => { decorators.delete(directory); };
    }),
  });
});
