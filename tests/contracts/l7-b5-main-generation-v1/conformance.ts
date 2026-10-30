import { describe, expect, it } from "vitest";
import { GENERATION, intent, type Scenario } from "../l7-b4-effect-recovery-provider-v1/fixtures.js";

/** Moved C21/C22, not a B4 provider duty. Main's factory must drive its composed
 * B5 admission/recovery route (ensureRecoveryGeneration before B4 settlement),
 * with retained prior evidence when scenario.pin is null. recover(g) attempts
 * continuation/repair under g; it is a test driver, not a new production API.
 * Owner: fv2-l7-main + fv2-l7-b5fix; real binding due 2026-10-30 18:00Z.
 * No standalone registration or fake B5 implementation: importing this suite
 * does not claim it has run against B5. See README.md. */
export interface GenerationPinHarness {
  recover(generation: string): Promise<unknown>;
  reopen(): Promise<void>;
  ledgerDigest(): Promise<string>;
  generations(): readonly string[];
  destination(): readonly string[];
  dispose(): Promise<void>;
}
export interface GenerationPinScenario extends Scenario {
  readonly pin?: string | null;
}
export type GenerationPinFactory = (scenario: GenerationPinScenario) => Promise<GenerationPinHarness>;

export function runGenerationPinConformance(factory: GenerationPinFactory): void {
  const test = (name: string, scenario: GenerationPinScenario, check: (h: GenerationPinHarness) => Promise<void>) => {
    it(name, async () => {
      const h = await factory(scenario);
      try { await check(h); } finally { await h.dispose(); }
    });
  };
  const value = intent();
  describe("B5/Main generation ownership — moved from B4", () => {
    test("C21 missing B5 pin with prior evidence never starts settlement", { intents: [value], pin: null }, async h => {
      const before = await h.ledgerDigest();
      await expect(h.recover(GENERATION)).rejects.toMatchObject({ code: "RECOVERY_HISTORY_INCOMPLETE" });
      expect(await h.ledgerDigest()).toBe(before);
      expect(h.generations()).toEqual([]); expect(h.destination()).toEqual([]);
    });
    test("C22 second recovery cannot replace the retained generation", { intents: [value], lostAck: true }, async h => {
      await h.recover(GENERATION); await h.reopen(); const before = await h.ledgerDigest();
      await expect(h.recover("caller-random-g2")).rejects.toMatchObject({ code: "RECOVERY_CONFLICT" });
      expect(await h.ledgerDigest()).toBe(before); expect(h.generations()).toEqual([GENERATION]);
    });
  });
}
