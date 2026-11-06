import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "@earendil-works/pi-ai";
import assert from "node:assert/strict";
import { pathToFileURL } from "node:url";

export type RunLogSecretCase = "text" | "structuredContent" | "details" | "percent-encoded";

/**
 * Offline worker regression fixture, NOT a production secret detector.
 * The portable middleware replaces only the exact configured FAKE text literal.
 * Supplemental fields deliberately remain unguarded: the worker must omit them.
 * An optional externally supplied module can install the real guard instead;
 * its default export/fleetSecrets are NEVER called, nor are live source roots used.
 */
export default async function (pi: ExtensionAPI) {
  const secret = process.env.PI_FABRIC_TEST_RUN_LOG_FAKE_SECRET;
  const auth = process.env.PI_FABRIC_TEST_RUN_LOG_EMPTY_AUTH;
  const variant = process.env.PI_FABRIC_TEST_RUN_LOG_CASE as RunLogSecretCase | undefined;
  if (!secret?.startsWith("FAKE-RUN-LOG-") || !auth ||
      !["text", "structuredContent", "details", "percent-encoded"].includes(variant ?? "")) {
    throw new Error("Run-log regression requires an explicit FAKE inventory and owned empty auth directory");
  }
  // Positive control in the native Pi child: actor metadata must actually cross
  // the manager -> worker -> Pi boundary, not just be present in the request.
  const expectedActor = JSON.parse(process.env.PI_FABRIC_TEST_RUN_LOG_ACTOR_SELF ?? "null") as {
    actorId: string; name: string;
  } | null;
  const actualActor = JSON.parse(process.env.PI_FABRIC_ACTOR_SELF ?? "null");
  assert.deepEqual(actualActor, expectedActor, "native child activation identity differs from the requested mode");
  assert.equal(process.env.PI_FABRIC_ACTOR_ID, expectedActor?.actorId);
  assert.equal(process.env.PI_FABRIC_ACTOR_NAME, expectedActor?.name);
  const marker = "[REDACTED:run-log-fixture]";
  // Non-PI_FABRIC prefix deliberately survives the test fleet environment scrub.
  const guardSource = process.env.FABRIC_RUN_LOG_TEST_SECRET_GUARD;
  if (guardSource) {
    const guard = await import(pathToFileURL(guardSource).href) as {
      install: (api: ExtensionAPI, load: () => Array<{ value: string; kind: string }>, auth: string) => void;
    };
    if (typeof guard.install !== "function") throw new Error("Supplied secret guard has no install export");
    guard.install(pi, () => [{ value: secret, kind: "run-log-fixture" }], auth);
  } else {
    pi.on("tool_result", event => {
      if (event.toolName !== "run_log_secret_fixture") return undefined;
      return {
        content: event.content.map(part => part.type === "text"
          ? { ...part, text: part.text.split(secret).join(marker) } : part),
        // Preserve the untrusted supplemental values even when text changes.
        // This fixture tests the persistence boundary, not detector coverage.
        details: event.details,
        ...(event.structuredContent === undefined ? {} : { structuredContent: event.structuredContent }),
      };
    });
  }

  pi.registerTool({
    name: "run_log_secret_fixture",
    label: "Offline run-log secret fixture",
    description: "Return the configured FAKE regression payload without accessing any secret source",
    parameters: Type.Object({}),
    async execute(_id, _args, _signal, onUpdate) {
      const encoded = encodeURIComponent(secret);
      const result = {
        content: [{ type: "text" as const, text: variant === "text"
          ? `fixture text: ${secret}` : `clean fixture text: ${variant}` }],
        structuredContent: {
          fixtureSupplemental: "structured-content-must-not-be-persisted",
          value: variant === "structuredContent" ? secret : variant === "percent-encoded" ? encoded : "benign",
        },
        details: {
          fixtureSupplemental: "nested-details-must-not-be-persisted",
          nested: [{ payload: { value: variant === "details" ? secret : variant === "percent-encoded" ? encoded : "benign" } }],
        },
      };
      // Both frames are produced by Pi's real execution path, not fake RPC JSON.
      // tool_result middleware applies only to the final canonical message;
      // raw execution events are not evidence that a result passed that hook.
      onUpdate?.(result);
      return result;
    },
  });
}
