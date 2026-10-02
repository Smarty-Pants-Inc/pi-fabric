import { afterEach, describe, expect, it, vi } from "vitest";
import { JevClient, JevCredentialCommandUnsupportedError, JevCredentials } from "../src/jev/client.js";
import { DEFAULT_JEV_CONFIG } from "../src/jev/config.js";
const { spawn } = vi.hoisted(() => ({ spawn: vi.fn() }));
vi.mock("node:child_process", () => ({ spawn }));
const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
const signal = () => new AbortController().signal;
const request = { state: { text: "offline" }, questions: { yes: { type: "noul" as const, instructions: "Offline?" } } };
afterEach(() => {
  Object.defineProperty(process, "platform", platform);
  vi.restoreAllMocks(); vi.clearAllMocks();
});

// SR-7 scope cut: Windows has no persistent owned-tree identity for a command
// tree (ponytail: Job Object follow-up), so the command is refused pre-spawn.
describe("SR-7 Windows refuses command-backed credentials before spawning", () => {
  it("rejects with a typed reason, spawns nothing and owes no retirement", async () => {
    Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    const credentials = new JevCredentials(["offline-credential-fixture", "FAKE_SECRET"], {});
    const client = new JevClient(DEFAULT_JEV_CONFIG, undefined, credentials);
    const error = await credentials.resolve(signal()).then(() => undefined, (e: unknown) => e);
    expect(error).toBeInstanceOf(JevCredentialCommandUnsupportedError);
    expect(error).toMatchObject({ code: "JEV_CREDENTIAL_COMMAND_UNSUPPORTED", message: "Jev credential command unsupported on Windows; use the environment or Pi credential" });
    expect(String(error)).not.toContain("FAKE_SECRET");
    expect(spawn).not.toHaveBeenCalled();
    await client.drainCredentials(); // no obligation was ever created
  });
  it("evaluate makes zero Jev HTTP calls when only a command is configured", async () => {
    Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    const fetcher = vi.fn(async () => new Response("{}")) as unknown as typeof fetch;
    const client = new JevClient(DEFAULT_JEV_CONFIG, fetcher, new JevCredentials(["offline-credential-fixture"], {}));
    await expect(client.evaluate(request, signal())).rejects.toBeInstanceOf(JevCredentialCommandUnsupportedError);
    expect(fetcher).not.toHaveBeenCalled();
    expect(spawn).not.toHaveBeenCalled();
  });
  it("environment and Pi-stored credentials still resolve on Windows without a spawn", async () => {
    Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    expect(await new JevCredentials(["offline-credential-fixture"], { TYPESAFE_API_KEY: " offline-env-key " }).resolve(signal())).toBe("offline-env-key");
    const pi = { configured: () => true, resolve: vi.fn(async () => "offline-pi-key") };
    expect(await new JevCredentials(["offline-credential-fixture"], {}, pi).resolve(signal())).toBe("offline-pi-key");
    expect(spawn).not.toHaveBeenCalled();
  });
  it("POSIX still spawns the command in its own detached group", async () => {
    Object.defineProperty(process, "platform", { ...platform, value: "linux" });
    spawn.mockImplementationOnce(() => { throw new Error("FAKE_SECRET"); });
    await expect(new JevCredentials(["offline-credential-fixture", "--flag"], {}).resolve(signal())).rejects.toThrow(/^Jev credential resolver failed$/);
    expect(spawn).toHaveBeenCalledWith("offline-credential-fixture", ["--flag"], { detached: true, windowsHide: true, stdio: ["ignore", "pipe", "pipe"] });
  });
});
