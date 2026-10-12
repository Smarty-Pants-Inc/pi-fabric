import { Type } from "typebox";
import { DECISION_APIS } from "./decision-profiles.js";
const description = Type.Union([Type.String(), Type.Array(Type.Unknown()), Type.Record(Type.String(), Type.Unknown())]);
const question = Type.Union([
  Type.Object({ type: Type.Union(["boolean", "bool", "noul", "predicate"].map(v => Type.Literal(v))), instructions: description, criteria: Type.Optional(Type.Object({ true: Type.Optional(description), false: Type.Optional(description) }, { additionalProperties: false })) }, { additionalProperties: false }),
  Type.Object({ type: Type.Literal("choice"), instructions: description, criteria: Type.Record(Type.String(), Type.Union([description, Type.Null()]), { minProperties: 1, maxProperties: 255 }) }, { additionalProperties: false }),
  Type.Object({ type: Type.Literal("score"), instructions: description, criteria: Type.Array(description, { minItems: 2, maxItems: 10 }) }, { additionalProperties: false }),
]);
export const decisionRequestSchema = Type.Object({
  state: description,
  questions: Type.Record(Type.String({ minLength: 1, maxLength: 128 }), question, { minProperties: 1, maxProperties: 128 }),
  profile: Type.Optional(Type.String({ pattern: "^[A-Za-z0-9_.-]{1,128}$" })),
  provider: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })),
  api: Type.Optional(Type.Union(DECISION_APIS.map(v => Type.Literal(v)))),
  model: Type.Optional(Type.String({ minLength: 1 })),
  endpoint: Type.Optional(Type.String({ minLength: 1 })),
  allowLocal: Type.Optional(Type.Boolean()),
  allowGenerated: Type.Optional(Type.Boolean()),
  providerOptions: Type.Optional(Type.Record(Type.String(), Type.Unknown())),
  temperature: Type.Optional(Type.Number()),
  images: Type.Optional(Type.Array(Type.Object({ type: Type.Optional(Type.Literal("image")), data: Type.String({ minLength: 1 }), mimeType: Type.Union(["image/png", "image/jpeg", "image/webp", "image/gif"].map(v => Type.Literal(v))) }, { additionalProperties: false }), { maxItems: 128 })),
}, { additionalProperties: false });
