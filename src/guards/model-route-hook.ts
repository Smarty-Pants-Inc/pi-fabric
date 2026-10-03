import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

/** Header-only hook. No Fabric graph, no task text, no identity or credential rewrite. */
export default function modelRouteHook(pi: ExtensionAPI): void {
  const value = process.env.PI_FABRIC_ROUTE_HEADER;
  // A bounded ASCII envelope; reject CRLF, raw slashes in models and arbitrary/free-text reasons.
  if (!value || value.length > 512 || !/^[a-z][a-z0-9:-]{0,63}\/[A-Za-z0-9._~%+-]+-(?:off|minimal|low|medium|high|xhigh|max)\/(?:judgment-agent|live-choice|class-reverted|revert-state-error|shadow-choice|excluded-protected|excluded-unknown|excluded-class|low-confidence|jev-error|jev-timeout|malformed|invalid-candidates|record-failed):[a-f0-9]{32}$/.test(value)) return;
  pi.on("before_provider_headers", event => {
    // Pi ignores returns from this hook: mutate the supplied header map in place.
    event.headers["X-Smarty-Route"] = value;
  });
}
