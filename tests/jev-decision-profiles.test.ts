import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { normalizeDecisionProfiles } from "../src/jev/decision-profiles.js";
import { loadFabricConfig, normalizeFabricConfig, saveFabricConfig } from "../src/config.js";
const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const document = { version: 1, defaultProfile: "a", profiles: { a: { provider: "openai" } } };
describe("trusted decision profiles", () => {
  it("clones a bounded routing-only document and validates selectors", () => {
    const normalized = normalizeDecisionProfiles(document);
    expect(normalized).toEqual(document); expect(normalized).not.toBe(document);
    expect(normalizeFabricConfig({ jev: { decisionProfiles: document, decisionProfile: "a" } }).jev.decisionProfile).toBe("a");
    expect(normalizeFabricConfig({ jev: { decisionProfiles: document, decisionProfile: null } }).jev.decisionProfile).toBeNull();
    for (const selector of ["", "missing", "constructor", 1]) expect(() => normalizeFabricConfig({ jev: { decisionProfiles: document, decisionProfile: selector } })).toThrow("known decision profile");
  });
  it.each(["__proto__", "prototype", "constructor", "", "a b", "café", "x".repeat(129)])("rejects unsafe profile name %s", name => {
    expect(() => normalizeDecisionProfiles({ version: 1, profiles: { [name]: { provider: "openai" } } })).toThrow("Invalid");
  });
  it.each(["credential", "apiKey", "state", "questions", "images", "headers", "secret"])("rejects %s even inside options", key => {
    expect(() => normalizeDecisionProfiles({ version: 1, profiles: { a: { provider: "openai", providerOptions: { [key]: "bad" } } } })).toThrow("Invalid");
  });
  it("enforces byte/count bounds, own defaults and strict routing fields", () => {
    for (const value of [
      { version: 2, profiles: {} }, { ...document, defaultProfile: "constructor" },
      { version: 1, profiles: Object.fromEntries(Array.from({ length: 129 }, (_, i) => [`p${i}`, { provider: "openai" }])) },
      { version: 1, profiles: { a: { provider: "openai", model: "😀".repeat(17_000) } } },
      { version: 1, profiles: { a: { provider: "openai", extra: true } } },
      { version: 1, profiles: { a: { provider: "openai", endpoint: "https://secret@host/v1" } } },
    ]) expect(() => normalizeDecisionProfiles(value)).toThrow("Invalid");
  });
  it("enforces portable API, scalar model, temperature and raw endpoint constraints even on unused profiles", () => {
    const wrap = (fields: object) => ({ ...document, profiles: { ...document.profiles, unused: { provider: "typesafe", ...fields } } });
    for (const fields of [
      { api: "not-an-api" }, { api: null }, { model: "a".repeat(257) }, { model: "😀".repeat(257) }, { model: "\ud800" },
      { temperature: -0.01 }, { temperature: 2.01 }, { temperature: NaN }, { temperature: Infinity },
      ...["https://example.com/?safe=1", "https://example.com/?", "https://example.com/#", "https://@example.com/",
        "https://user@example.com/", "https://example.com/\\path", "http://example.com/", "ftp://example.com/",
        "http://localhost/", "http://127.1/", "http://2130706433/", "http://127.0.0.1.evil/", "https://example.com/café"].map(endpoint => ({ endpoint })),
    ]) expect(() => normalizeDecisionProfiles(wrap(fields))).toThrow("Invalid");
    for (const api of ["system-one", "cloudflare", "openai-decisions", "vercel-evaluate", "llama-cpp", "llama-system-one", "anthropic", "google"]) {
      expect(normalizeDecisionProfiles(wrap({ api, model: "😀".repeat(256) })).profiles.unused!.api).toBe(api);
    }
    for (const temperature of [0, 2]) expect(normalizeDecisionProfiles(wrap({ temperature })).profiles.unused!.temperature).toBe(temperature);
    for (const endpoint of ["https://example.com/v1", "http://127.0.0.1:8080/v1", "http://[::1]:8080/v1"]) {
      expect(normalizeDecisionProfiles(wrap({ endpoint, allowLocal: false })).profiles.unused!.endpoint).toBe(endpoint);
    }
  });
  it("copies strict JSON without invoking getters/toJSON or losing source fields", () => {
    let invoked = 0;
    const accessor = Object.defineProperty({}, "x", { enumerable: true, get() { invoked++; return 1; } });
    const serializer = { toJSON() { invoked++; return {}; } };
    const cycle: any = {}; cycle.self = cycle;
    const sparse = new Array(1); Object.defineProperty(sparse, "4294967295", { enumerable: true, value: 1 });
    for (const options of [accessor, serializer, cycle, { x: undefined }, { x: BigInt(1) }, { x: new Date() },
      { x: new Array(1) }, { x: sparse }, { [Symbol("x")]: 1 }, Object.defineProperty({}, "x", { value: 1 })]) {
      expect(() => normalizeDecisionProfiles({ ...document, profiles: { a: { provider: "openai", providerOptions: options } } })).toThrow("Invalid");
    }
    expect(invoked).toBe(0);
    const providerOptions = { futureOption: { values: [null, true, 1, "safe"] } };
    const input = { ...document, profiles: { a: { provider: "openai", providerOptions } } };
    const output = normalizeDecisionProfiles(input);
    expect(output).toEqual(input);
    expect(output.profiles.a!.providerOptions).not.toBe(providerOptions);
  });
  it("replaces documents atomically across layers and allows project null to reset a global selector", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "decision-profiles-")); roots.push(root);
    const location = { cwd: path.join(root, "project"), agentDir: path.join(root, "agent"), projectTrusted: true };
    fs.mkdirSync(location.cwd, { recursive: true }); fs.mkdirSync(location.agentDir, { recursive: true });
    saveFabricConfig({ ...location, scope: "global" }, { jev: { decisionProfile: "a", decisionProfiles: { ...document, profiles: { a: { provider: "custom", api: "system-one", endpoint: "https://old.example/v1", model: "old" } } } } });
    const next = { version: 1, defaultProfile: "a", profiles: { a: { provider: "openai" } } };
    saveFabricConfig({ ...location, scope: "project" }, { jev: { decisionProfile: null, decisionProfiles: next } });
    const effective = loadFabricConfig(location).jev;
    expect(effective.decisionProfiles).toEqual(next); expect(effective.decisionProfile).toBeNull();
    saveFabricConfig({ ...location, scope: "project" }, { jev: { decisionProfiles: { version: 1, profiles: { b: { provider: "typesafe" } } } } });
    expect(loadFabricConfig(location).jev.decisionProfiles).toEqual({ version: 1, profiles: { b: { provider: "typesafe" } } });
  });
});
