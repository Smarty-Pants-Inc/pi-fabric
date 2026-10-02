import { describe, expect, it, vi } from "vitest";
import { retainRootRegistration, forgetRetainedRootRegistration, releaseRetainedRootRegistrations } from "../src/main-agent.js";

describe("lightweight retained root retirement", () => {
  it("releases only the exact native session, once, without engine loading", async () => {
    const first = vi.fn(async () => {}), other = vi.fn(async () => {});
    retainRootRegistration("synthetic-retained-a", "private-mesh-a:owner", first);
    retainRootRegistration("synthetic-retained-b", "private-mesh-b:owner", other);
    await releaseRetainedRootRegistrations("synthetic-retained-a");
    await releaseRetainedRootRegistrations("synthetic-retained-a");
    expect(first).toHaveBeenCalledOnce(); expect(other).not.toHaveBeenCalled();
    await releaseRetainedRootRegistrations("synthetic-retained-b");
    expect(other).toHaveBeenCalledOnce();
  });

  it("a resumed owner forgets the old release callback before later retirement", async () => {
    const old = vi.fn(async () => {});
    retainRootRegistration("synthetic-retained-resume", "private-mesh:owner", old);
    forgetRetainedRootRegistration("synthetic-retained-resume", "private-mesh:owner");
    await releaseRetainedRootRegistrations("synthetic-retained-resume");
    expect(old).not.toHaveBeenCalled();
  });

  it("keeps a failed cleanup available for an explicit later retirement retry", async () => {
    const release = vi.fn().mockRejectedValueOnce(new Error("synthetic contention")).mockResolvedValueOnce(undefined);
    retainRootRegistration("synthetic-retained-retry", "private-mesh:owner", release);
    await expect(releaseRetainedRootRegistrations("synthetic-retained-retry")).rejects.toThrow("synthetic contention");
    await releaseRetainedRootRegistrations("synthetic-retained-retry");
    expect(release).toHaveBeenCalledTimes(2);
  });
});
