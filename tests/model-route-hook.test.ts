import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { decideModelRoute, routeHeader } from "../src/agents/model-route.js";
import modelRouteHook from "../src/guards/model-route-hook.js";

const header = "bounded-lookup/test%2Fluna-medium/shadow-choice:0123456789abcdef0123456789abcdef";
afterEach(() => vi.unstubAllEnvs());
describe("child routing header hook", () => {
  it("mutates the real before_provider_headers header map without changing other headers", () => {
    vi.stubEnv("PI_FABRIC_ROUTE_HEADER", header);
    let hook: ((event: { headers: Record<string, string | null> }) => unknown) | undefined;
    const on = vi.fn((name, handler) => { expect(name).toBe("before_provider_headers"); hook = handler; });
    modelRouteHook({ on } as unknown as ExtensionAPI);
    const headers = { Authorization: "unmodified", "X-Session-Id": "unmodified" };
    expect(hook?.({ headers })).toBeUndefined();
    expect(headers).toEqual({ Authorization: "unmodified", "X-Session-Id": "unmodified", "X-Smarty-Route": header });
  });
  it.each(["status-groom", "task:merge-additive", "task:ci-test-fixture", "task:exact-checks"])("preserves the live decision join for %s", routeClass => {
    for (const reason of ["live-choice", "admission-blocked", "admission-state-error"]) {
      const value = `${routeClass}/test%2Fluna-max/${reason}:0123456789abcdef0123456789abcdef`;
      vi.stubEnv("PI_FABRIC_ROUTE_HEADER", value);
      let hook: ((event: { headers: Record<string, string> }) => void) | undefined;
      modelRouteHook({ on: (_name: string, handler: typeof hook) => { hook = handler; } } as unknown as ExtensionAPI);
      const headers: Record<string, string> = {}; hook?.({ headers });
      expect(headers["X-Smarty-Route"]).toBe(value);
    }
  });
  it("percent-encodes model punctuation into the bounded ASCII envelope", async () => {
    const decision = await decideModelRoute({ routeClass: "bounded-lookup", protected: true,
      pin: { model: "test/model!(rev)'*", effort: "high" }, candidates: [], parentSessionId: "parent" }, async () => { throw new Error("excluded"); });
    const value = routeHeader(decision);
    expect(value).toContain("test%2Fmodel%21%28rev%29%27%2A-high");
    vi.stubEnv("PI_FABRIC_ROUTE_HEADER", value);
    const on = vi.fn(); modelRouteHook({ on } as unknown as ExtensionAPI);
    expect(on).toHaveBeenCalledOnce();
  });
  it.each([undefined, "", header + "\r\nX-Evil: yes", header.replace("shadow-choice", "free text"), header.replace("test%2Fluna", "test/luna"), "x".repeat(513)])("rejects missing/free-text/unbounded metadata: %s", value => {
    vi.stubEnv("PI_FABRIC_ROUTE_HEADER", value);
    const on = vi.fn();
    modelRouteHook({ on } as unknown as ExtensionAPI);
    expect(on).not.toHaveBeenCalled();
  });
});
