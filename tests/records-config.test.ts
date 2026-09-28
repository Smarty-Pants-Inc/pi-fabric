import { describe, expect, it } from "vitest";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig } from "../src/config.js";
import { normalizeRecordsConfig, RECORDS_DISABLED_HINT } from "../src/records/config.js";

describe("records configuration", () => {
  it("is off by default: absent, empty, or anything but enabled: true", () => {
    expect(DEFAULT_FABRIC_CONFIG.records.enabled).toBe(false);
    expect(normalizeFabricConfig({}).records.enabled).toBe(false);
    for (const input of [undefined, {}, { enabled: "true" }, { enabled: 1 }, { socket: "/run/smarty-pants-records/records.sock" }]) {
      expect(normalizeRecordsConfig(input).enabled).toBe(false);
    }
    expect(normalizeRecordsConfig({ enabled: true }).enabled).toBe(true);
  });

  it("takes no database access or role policy from a caller's configuration (C10)", () => {
    const config = normalizeRecordsConfig({
      enabled: true, socket: "/run/org-records/records.sock",
      connection: { host: "/run/org-records-pg", user: "postgres" }, importers: ["session:me"], mirrors: ["session:me"],
      roles: { importer: ["session:me"] }, admission: { targets: [{ name: "x", command: ["true"] }] },
    });
    expect(Object.keys(config).sort()).toEqual(["consumerLagSeconds", "enabled", "socket", "watchdogMs"]);
  });

  it("the disabled provider's guidance is the socket-only activation the installer prints (#1720 item 4)", () => {
    const snippet = /set "records": (\{.*?\}) in \.pi\/fabric\.json/.exec(RECORDS_DISABLED_HINT)?.[1];
    expect(snippet).toBeDefined();
    const config = normalizeRecordsConfig(JSON.parse(snippet!));
    expect(config).toMatchObject({ enabled: true, socket: "/run/<org>-records/records.sock" });
    expect(Object.keys(config).sort()).toEqual(["consumerLagSeconds", "enabled", "socket", "watchdogMs"]);
    expect(RECORDS_DISABLED_HINT).not.toMatch(/connection|"org"/);
  });
});
