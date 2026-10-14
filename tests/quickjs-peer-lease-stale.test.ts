// smarty-dev#11703: the typed peer-lease-stale refusal keeps its retryable classification
// across the QuickJS guest boundary, so a guest program can distinguish it from Unknown and retry.
import { describe, expect, it } from "vitest";
import { QuickJsRuntime } from "../src/runtime/quickjs-runtime.js";

const options = { timeoutMs: 5_000, memoryLimitBytes: 32 * 1024 * 1024 };

describe("FABRIC_PEER_LEASE_STALE guest metadata (smarty-dev#11703)", () => {
  it.each(["followUp", "steer"] as const)("carries name, code and retryable through agents.%s, but no host-only fields", async action => {
    const result = await new QuickJsRuntime().execute(
      `try { await agents.${action}({ id: "session:remote-00000000", message: "unchanged" }); return { ok: true }; }
       catch (error) { return { name: error.name, code: error.code, retryable: error.retryable,
         secret: error.secret ?? null, cause: error.cause ?? null, unknown: String(error.message).startsWith("Unknown "),
         keys: Object.keys(error).sort() }; }`,
      async () => {
        throw Object.assign(new Error("Fabric peer session:remote-00000000 on remote host forge may still be live: its mirrored lease lapsed; retry"), {
          name: "FabricPeerLeaseStaleError", code: "FABRIC_PEER_LEASE_STALE", retryable: true,
          targetId: "session:remote-00000000", remoteHost: "forge", lapsedMs: 16_000,
          secret: "never bridge", cause: { secret: "never bridge" },
        });
      }, options,
    );
    expect(result.error).toBeUndefined();
    expect(result.value).toMatchObject({ name: "FabricPeerLeaseStaleError", code: "FABRIC_PEER_LEASE_STALE", retryable: true,
      secret: null, cause: null, unknown: false });
    const keys = (result.value as { keys: string[] }).keys;
    expect(keys).toEqual(expect.arrayContaining(["code", "message", "name", "retryable"]));
    expect(keys).not.toContain("secret");
    expect(keys).not.toContain("cause");
  });
});
