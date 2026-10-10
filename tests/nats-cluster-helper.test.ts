import path from "node:path";
import os from "node:os";
import { afterEach, describe, expect, it, vi } from "vitest";

const missingBinary = path.join(os.tmpdir(), "absent-nats-server", "nats-server");

afterEach(() => {
  vi.unstubAllEnvs();
  vi.resetModules();
});

describe("NATS integration prerequisite", () => {
  it("allows optional local suites to skip an unavailable explicit binary", async () => {
    vi.stubEnv("NATS_SERVER", missingBinary);
    vi.stubEnv("NATS_SERVER_REQUIRED", undefined);
    const helper = await import("./helpers/nats-cluster.js");
    expect(helper.natsServerBinary).toBe(missingBinary);
    expect(helper.natsAvailable).toBe(false);
  });

  it("fails suite loading when the required binary is unavailable", async () => {
    vi.stubEnv("NATS_SERVER", missingBinary);
    vi.stubEnv("NATS_SERVER_REQUIRED", "1");
    await expect(import("./helpers/nats-cluster.js")).rejects.toThrow(
      `NATS_SERVER_REQUIRED=1 but nats-server binary is missing: ${missingBinary}`,
    );
  });
});
