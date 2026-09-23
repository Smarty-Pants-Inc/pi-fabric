// One bounded artifact shared by the readiness action, controller and transcript.
export interface PrewalkPlan {
  outcome: string;
  steps: string[];
  verification: string[];
  risks: string;
}

const textSchema = { type: "string", minLength: 1, maxLength: 4000 };
const listSchema = { type: "array", minItems: 1, maxItems: 32, items: textSchema };
export const prewalkPlanSchema = {
  type: "object",
  properties: { outcome: textSchema, steps: listSchema, verification: listSchema, risks: textSchema },
  required: ["outcome", "steps", "verification", "risks"],
  additionalProperties: false,
};

export const checkedPrewalkPlan = (input: Record<string, unknown>): PrewalkPlan => {
  const text = (value: unknown): string => {
    if (typeof value !== "string" || !value.trim() || value.length > 4000) {
      throw new Error("Prewalk plan fields must contain 1–4000 characters of nonblank text");
    }
    return value.trim();
  };
  const list = (value: unknown): string[] => {
    if (!Array.isArray(value) || value.length < 1 || value.length > 32) {
      throw new Error("Prewalk plan steps and verification need 1–32 entries");
    }
    return value.map(text);
  };
  if (Object.keys(input).some((key) => !Object.hasOwn(prewalkPlanSchema.properties, key))) {
    throw new Error("Unknown prewalk plan field");
  }
  const plan = { outcome: text(input.outcome), steps: list(input.steps), verification: list(input.verification), risks: text(input.risks) };
  if (JSON.stringify(plan).length > 20000) throw new Error("Prewalk plan exceeds 20000 characters");
  return plan;
};

export const prewalkPlanText = (plan: PrewalkPlan): string => [
  "Prewalk plan captured",
  plan.outcome,
  "Steps:", ...plan.steps.map((step, index) => `${index + 1}. ${step}`),
  "Verification:", ...plan.verification.map((check) => `- ${check}`),
  `Risks: ${plan.risks}`,
].join("\n");
