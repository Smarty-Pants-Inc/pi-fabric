import { describe, expect, it } from "vitest";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig } from "../src/config.js";
import { normalizeRecordsConfig } from "../src/records/config.js";

describe("records configuration", () => {
  it("is off by default: absent, empty, or anything but enabled: true", () => {
    expect(DEFAULT_FABRIC_CONFIG.records.enabled).toBe(false);
    expect(normalizeFabricConfig({}).records.enabled).toBe(false);
    for (const input of [undefined, {}, { enabled: "true" }, { enabled: 1 }, { org: "smarty-pants", connection: { host: "/run/pg" } }]) {
      expect(normalizeRecordsConfig(input).enabled).toBe(false);
    }
    expect(normalizeRecordsConfig({ enabled: true }).enabled).toBe(true);
  });

  it("keeps admission off without targets and the mirror off unless enabled", () => {
    const config = normalizeRecordsConfig({ enabled: true, admission: { targets: [{ name: "m4max" }] } });
    expect(config.admission.targets).toEqual([]);
    expect(config.mirror.enabled).toBe(false);
    expect(config.admission).toMatchObject({ alarmSeconds: 120, refuseSeconds: 300 });
  });
});
