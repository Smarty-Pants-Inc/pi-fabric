import { execFileSync } from "node:child_process";
import { expect, it } from "vitest";
import {
  createNativeCleanupAttempt,
  type NativeCleanupEvent,
  withDiagnosticProbe,
  withProductCleanupProtection,
  withTestOnlyAclReadback,
} from "./helpers/native-cleanup-diagnostics.js";

const realChild = (value: string): string => execFileSync(process.execPath,
  ["-e", `process.stdout.write(${JSON.stringify(value)})`], { encoding: "utf8", timeout: 2_000 });
const originalExecFileSync = execFileSync;

it("diagnostic pass-through preserves real child results/errors and isolates overlapping retry attribution", async () => {
  const firstEvents: NativeCleanupEvent[] = [], retryEvents: NativeCleanupEvent[] = [];
  const first = createNativeCleanupAttempt({ fixture: "fixture-live-cleanup", runId: "attempt-1", emit: event => firstEvents.push(event) });
  const retry = createNativeCleanupAttempt({ fixture: "fixture-live-cleanup", runId: "attempt-2", emit: event => retryEvents.push(event) });
  try {
    const [firstOutput, retryOutput] = await Promise.all([
      first.measure("provider-cleanup", async () => {
        await new Promise(resolve => setTimeout(resolve, 5));
        return withDiagnosticProbe("attempt-1", "diagnostic-probe", () => realChild("first"));
      }),
      retry.measure("provider-cleanup", async () => {
        await new Promise(resolve => setTimeout(resolve, 1));
        return withDiagnosticProbe("attempt-2", "diagnostic-probe", () => realChild("retry"));
      }),
    ]);
    expect(firstOutput).toBe("first");
    expect(retryOutput).toBe("retry");

    let childError: unknown;
    try {
      withDiagnosticProbe("attempt-1", "real-child-error", () => execFileSync(process.execPath,
        ["-e", "process.exit(23)"], { stdio: "pipe", timeout: 2_000 }));
    } catch (error) { childError = error; }
    expect(childError).toMatchObject({ status: 23 });

    expect(firstEvents.some(event => event.event === "phase-end" && event.phase === "provider-cleanup" && event.outcome === "ok")).toBe(true);
    expect(retryEvents.some(event => event.event === "phase-end" && event.phase === "provider-cleanup" && event.outcome === "ok")).toBe(true);
    expect(firstEvents.some(event => event.event === "native-end" && event.phase === "real-child-error" && event.outcome === "error" && event.status === 23)).toBe(true);
    expect(firstEvents.every(event => event.fixture === "fixture-live-cleanup" && event.runId === "attempt-1")).toBe(true);
    expect(retryEvents.every(event => event.fixture === "fixture-live-cleanup" && event.runId === "attempt-2")).toBe(true);
  } finally {
    first.close();
    retry.close();
  }
}, 20_000);

it("restores the global child observer without attributing unrelated calls", () => {
  const productEvents: NativeCleanupEvent[] = [];
  const aclEvents: NativeCleanupEvent[] = [];
  expect(execFileSync).toBe(originalExecFileSync);
  expect(realChild("before-registration")).toBe("before-registration");

  const product = createNativeCleanupAttempt({ fixture: "fixture-observer", runId: "observer-product", emit: event => productEvents.push(event) });
  const acl = createNativeCleanupAttempt({ fixture: "fixture-observer", runId: "observer-acl", emit: event => aclEvents.push(event) });
  try {
    expect(withProductCleanupProtection("observer-product", "product-root", () => realChild("product"))).toBe("product");
    expect(withTestOnlyAclReadback("observer-acl", "acl-readback", () => realChild("acl"))).toBe("acl");

    const productCount = productEvents.length;
    const aclCount = aclEvents.length;
    expect(realChild("unattributed-while-registered")).toBe("unattributed-while-registered");
    expect(productEvents).toHaveLength(productCount);
    expect(aclEvents).toHaveLength(aclCount);

    product.close();
    expect(withTestOnlyAclReadback("observer-acl", "acl-readback-after-first-close", () => realChild("acl-still-observed"))).toBe("acl-still-observed");
    expect(productEvents.map(({ event, kind, phase }) => ({ event, kind, phase }))).toEqual([
      { event: "native-start", kind: "product-custody-protection", phase: "product-root" },
      { event: "native-end", kind: "product-custody-protection", phase: "product-root" },
    ]);
    expect(productEvents.filter(event => event.event === "native-end").every(event => event.outcome === "ok")).toBe(true);
    expect(aclEvents.map(({ event, kind, phase }) => ({ event, kind, phase }))).toEqual([
      { event: "native-start", kind: "test-only-acl-readback", phase: "acl-readback" },
      { event: "native-end", kind: "test-only-acl-readback", phase: "acl-readback" },
      { event: "native-start", kind: "test-only-acl-readback", phase: "acl-readback-after-first-close" },
      { event: "native-end", kind: "test-only-acl-readback", phase: "acl-readback-after-first-close" },
    ]);
    expect(aclEvents.filter(event => event.event === "native-end").every(event => event.outcome === "ok")).toBe(true);

    acl.close();
    expect(execFileSync).toBe(originalExecFileSync);
    const productCountAfterClose = productEvents.length;
    const aclCountAfterClose = aclEvents.length;
    expect(realChild("after-last-close")).toBe("after-last-close");
    expect(productEvents).toHaveLength(productCountAfterClose);
    expect(aclEvents).toHaveLength(aclCountAfterClose);
  } finally {
    product.close();
    acl.close();
  }
}, 20_000);
