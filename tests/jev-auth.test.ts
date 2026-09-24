import { describe, expect, it, vi } from "vitest";
import { createModels, InMemoryCredentialStore, type AuthInteraction } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { createJevAuthProvider, registerJevAuth } from "../src/jev/auth.js";
import { JevCredentials } from "../src/jev/client.js";
describe("Jev native login", () => {
  it("registers for login without publishing a text-generating model", () => {
    const registerProvider = vi.fn(); registerJevAuth({registerProvider} as unknown as ExtensionAPI);
    const provider = registerProvider.mock.calls[0]![0];
    expect(provider.id).toBe("jev"); expect(provider.getModels()).toEqual([]);
    expect(provider.auth.apiKey.login).toBeTypeOf("function");
  });
  // Newer Pi's createProvider rejects a provider without any api/images/classifiers
  // implementation, so the login-only provider carries a stream that only ever errors.
  it("carries a concrete stream implementation that ends with an error", async () => {
    const provider = createJevAuthProvider();
    const model = { id: "none", name: "none", api: "openai-completions", provider: "jev" } as unknown as Parameters<typeof provider.stream>[0];
    for (const stream of [provider.stream(model, { messages: [] } as never), provider.streamSimple(model, { messages: [] } as never)]) {
      const events = [];
      for await (const event of stream) events.push(event);
      expect(events.map((event) => event.type)).toEqual(["error"]);
      await expect(stream.result()).resolves.toMatchObject({ stopReason: "error", errorMessage: expect.stringContaining("login-only") });
    }
  });
  it("persists api_key credentials through Pi storage and resolves login/logout", async () => {
    const storage = new InMemoryCredentialStore();
    const models = createModels({ credentials: storage }); models.setProvider(createJevAuthProvider());
    const prompt = vi.fn(async () => "fake-login-key");
    await models.login("jev","api_key",{prompt} as unknown as AuthInteraction);
    expect(prompt).toHaveBeenCalledWith(expect.objectContaining({type:"secret"}));
    expect(await storage.read("jev")).toEqual({type:"api_key",key:"fake-login-key"});
    expect((await models.getAuth("jev"))?.auth.apiKey).toBe("fake-login-key");
    expect(models.getModels("jev")).toEqual([]);
    expect(models.getProviders().some(p=>p.id==="jev")).toBe(true);
    await models.logout("jev"); expect(await storage.read("jev")).toBeUndefined();
  });
  it("prefers Pi auth, does not resolve during status, and sees credential changes", async () => {
    let key: string | undefined = "fake-stored";
    const resolve = vi.fn(async () => key);
    const credentials = new JevCredentials([], {TYPESAFE_API_KEY:"fake-env"},{configured:()=>Boolean(key),resolve});
    expect(credentials.status().source).toBe("pi"); expect(resolve).not.toHaveBeenCalled();
    expect(await credentials.resolve(new AbortController().signal)).toBe("fake-stored");
    key = undefined; expect(await credentials.resolve(new AbortController().signal)).toBe("fake-env");
  });
});
