import { describe } from "vitest";
import { runConformance } from "./conformance.js";
import { createFake } from "./fake.js";

// K5 regression: a correct code with a human message must pass the SAME C25/C33,
// as B5 RecoveryMaintenanceError does not include its code in message text.
describe("fake resolver human-message errors / COMPONENT ONLY", () => runConformance(async scenario => {
  const h = await createFake(scenario);
  const resolve = h.provider.resolveRepair;
  h.provider.resolveRepair = ref => {
    try { return resolve(ref); } catch (error) {
      throw Object.assign(new Error("The retained repair evidence does not match this request"), {
        code: (error as { code: string }).code,
      });
    }
  };
  return h;
}));
