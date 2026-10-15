import { describe, expect, it, vi } from "vitest";
import { JevClient, JevCredentials } from "../src/jev/client.js";
import { DEFAULT_JEV_CONFIG, normalizeJevConfig } from "../src/jev/config.js";
import { JEV_OPENROUTER_ROUTE, JEV_SUBQ_ROUTE, JEV_TYPESAFE_ROUTE, JEV_VERCEL_ROUTE } from "../src/jev/routes.js";
import { checkRequest, checkResponse, checkSchema, checkValue, JevBilledError, JevInvalidResponseError, JevTransientHttpError } from "../src/jev/validation.js";
import type { JevRequest } from "../src/jev/types.js";
const request: JevRequest = { state:{text:"Please refund this charge"}, questions:{route:{type:"choice",instructions:"Which team handles this?",criteria:{billing:null,other:null}}, yes:{type:"noul",instructions:"Is a refund requested?"}, score:{type:"score",instructions:"How urgent is the request?",criteria:["No urgency", "Explicit immediate deadline"]}} };
const response = { model:"jev-latest", answers:{route:{type:"choice",choice:"billing",confidence:1,probabilities:{billing:1,other:0}},yes:{type:"noul",noul:0.98},score:{type:"score",score:0.1,confidence:0.9,probabilities:{"0":0.9,"1":0.1},legend:{"0":"No urgency","1":"Explicit immediate deadline"}}},usage:{input_tokens:30,output_tokens:10} };
const signal = () => new AbortController().signal;
describe("Jev transport and credentials", () => {
  it("detects credentials without executing the resolver; environment has precedence", async () => {
    const credentials = new JevCredentials(["does-not-exist"], { TYPESAFE_API_KEY:"test-key" });
    expect(credentials.status()).toEqual({ configured:true,source:"environment",verified:false });
    expect(await credentials.resolve(signal())).toBe("test-key");
    expect(new JevCredentials(["does-not-exist"], {}).status()).toEqual(process.platform === "win32"
      ? { configured: false, source: "command-unsupported", verified: false }
      : { configured: true, source: "command", verified: false });
    await expect(new JevCredentials([], {}).resolve(signal())).rejects.toThrow("unavailable");
  });
  it.skipIf(process.platform === "win32")("never exposes credential command stdout/stderr on failure", async () => {
    const c = new JevCredentials([process.execPath,"-e","console.error('FAKE_SECRET');process.exit(1)"], {});
    await expect(c.resolve(signal())).rejects.toThrow(/^Jev credential resolver failed$/);
  });
  it("uses host-only bearer auth, fixed origin, no redirects, and preserves typed answers", async () => {
    const fetcher = vi.fn(async () => new Response(JSON.stringify(response))) as unknown as typeof fetch;
    const client = new JevClient(DEFAULT_JEV_CONFIG, fetcher, new JevCredentials([], { TYPESAFE_API_KEY:"test-key" }));
    expect(await client.evaluate(request, signal())).toEqual(response);
    const [url, args] = vi.mocked(fetcher).mock.calls[0]!;
    expect(url).toBe("https://api.typesafe.ai/v1/systemone");
    expect(args?.redirect).toBe("error");
    expect(args?.headers).toEqual({ "Content-Type":"application/json", Authorization:"Bearer test-key" });
    expect(args?.body).not.toContain("test-key");
  });
  it("redacts service/network errors and does not automatically retry", async () => {
    const fetcher = vi.fn(async () => new Response("FAKE_SECRET",{status:429})) as unknown as typeof fetch;
    const client = new JevClient(DEFAULT_JEV_CONFIG, fetcher, new JevCredentials([], { TYPESAFE_API_KEY:"test-key" }));
    await expect(client.evaluate(request, signal())).rejects.toThrow(/^TypeSafe HTTP 429: rate limited; back off before retrying$/);
    expect(fetcher).toHaveBeenCalledTimes(1);
    const fail = new JevClient(DEFAULT_JEV_CONFIG, (async () => { throw new Error("FAKE_SECRET"); }) as typeof fetch, new JevCredentials([], { TYPESAFE_API_KEY:"test-key" }));
    await expect(fail.evaluate(request, signal())).rejects.toThrow(/^Jev network request failed$/);
  });
  it("classifies only received transient HTTP statuses with validated usage as billed retries", async () => {
    const rejection = async (reply: Response) => {
      let sent = "";
      const client = new JevClient(DEFAULT_JEV_CONFIG, (async (_url: string, init: { body: string }) => { sent = init.body; return reply; }) as unknown as typeof fetch, new JevCredentials([], { TYPESAFE_API_KEY:"test-key" }));
      return { error: await client.evaluate(request, signal()).catch((error: unknown) => error), bytes: Buffer.byteLength(sent) };
    };
    const unknownCost = (error: unknown) => {
      expect(error).toBeInstanceOf(Error); expect(error).not.toBeInstanceOf(JevBilledError);
      expect(error).not.toHaveProperty("usage");
    };
    for (const status of [408, 429, 500, 502, 503, 504, 529]) {
      const { error } = await rejection(new Response(JSON.stringify({ secret: "FAKE_SECRET", usage: { input_tokens: 5, output_tokens: 2 } }), { status }));
      expect(error).toBeInstanceOf(JevTransientHttpError);
      expect(error).toMatchObject({ status, usage: { input_tokens: 5, output_tokens: 2 }, guestFields: { status } });
      expect((error as Error).message).not.toContain("FAKE_SECRET");
      // Missing usage is unknown cost: never invented from request bytes.
      const missing = await rejection(new Response("FAKE_SECRET", { status }));
      unknownCost(missing.error);
      expect((missing.error as Error).message).not.toContain("FAKE_SECRET");
    }
    for (const status of [400, 401, 403, 404, 409, 422]) {
      const { error } = await rejection(new Response(JSON.stringify({ usage: { input_tokens: 1, output_tokens: 1 } }), { status }));
      expect(error).toBeInstanceOf(Error); expect(error).not.toBeInstanceOf(JevTransientHttpError);
    }
    const reported = await rejection(new Response(JSON.stringify({ usage: { input_tokens: 4, output_tokens: 1 } }), { status: 502, headers: { "retry-after-ms": "1500.2", "retry-after": "9" } }));
    expect(reported.error).toMatchObject({ usage: { input_tokens: 4, output_tokens: 1 }, retryAfterMs: 1501, guestFields: { status: 502, retryAfterMs: 1501 } });
    const invalid = await rejection(new Response(JSON.stringify({ usage: { input_tokens: -4, output_tokens: 1 } }), { status: 503, headers: { "retry-after": "soon" } }));
    unknownCost(invalid.error);
    expect((invalid.error as Error).message).toBe("TypeSafe HTTP 503");
    for (const usage of [{ input_tokens: 1 }, { input_tokens: 1.5, output_tokens: 0 }, { input_tokens: "1", output_tokens: 0 }, null])
      unknownCost((await rejection(new Response(JSON.stringify({ usage }), { status: 500 }))).error);
    // An oversized body is not read past the bound and its usage is not trusted.
    unknownCost((await rejection(new Response(JSON.stringify({ usage: { input_tokens: 1, output_tokens: 1 }, pad: "x".repeat(70_000) }), { status: 502 }))).error);
    const noRetryAfter = await rejection(new Response(JSON.stringify({ usage: { input_tokens: 2, output_tokens: 0 } }), { status: 503, headers: { "retry-after": "soon" } }));
    expect(noRetryAfter.error).toMatchObject({ usage: { input_tokens: 2, output_tokens: 0 }, retryAfterMs: undefined, guestFields: { status: 503 } });
    const dated = await rejection(new Response(JSON.stringify({ usage: { input_tokens: 1, output_tokens: 0 } }), { status: 429, headers: { "retry-after": new Date(Date.now() + 60_000).toUTCString() } }));
    expect((dated.error as JevTransientHttpError).retryAfterMs).toBeGreaterThan(50_000);
  });
  it("bounds a fetch that does not cooperate with cancellation", async () => {
    const client=new JevClient({...DEFAULT_JEV_CONFIG,requestTimeoutMs:20},(()=>new Promise<Response>(()=>{})) as typeof fetch,new JevCredentials([],{TYPESAFE_API_KEY:"fake"}));
    await expect(client.evaluate(request,signal())).rejects.toThrow("timed out");
  });
  it("routes OpenRouter decisions through the openrouter credential and endpoint", async () => {
    const fetched = vi.fn(async () => new Response(JSON.stringify({ ...response, model: "typesafe/jev-1.13" }))) as unknown as typeof fetch;
    const credentials = new JevCredentials([], { TYPESAFE_OPENROUTER_API_KEY: "or-key" }, undefined, JEV_OPENROUTER_ROUTE.envKeys);
    expect(credentials.status()).toEqual({ configured:true, source:"environment", verified:false });
    expect(new JevCredentials([], { TYPESAFE_API_KEY: "wrong-route" }, undefined, JEV_OPENROUTER_ROUTE.envKeys).status().source).toBe("missing");
    const client = new JevClient({ ...DEFAULT_JEV_CONFIG, model: "typesafe/jev-1.13" }, fetched, credentials, JEV_OPENROUTER_ROUTE);
    expect(await client.evaluate(request, signal())).toMatchObject({ model:"typesafe/jev-1.13" });
    const [url, args] = vi.mocked(fetched).mock.calls[0]!;
    expect(url).toBe("https://openrouter.ai/api/alpha/decisions");
    expect(args?.headers).toEqual({ "Content-Type":"application/json", Authorization:"Bearer or-key" });
    expect(JSON.parse(args!.body as string).model).toBe("typesafe/jev-1.13");
  });
  it("maps OpenRouter aliases and rejects direct-route models on an OpenRouter client", async () => {
    const fetched = vi.fn(async () => new Response(JSON.stringify({ ...response, model: "typesafe/jev-1.13" }))) as unknown as typeof fetch;
    const client = new JevClient({ ...DEFAULT_JEV_CONFIG, model: "jev-latest" }, fetched, new JevCredentials([], { OPENROUTER_API_KEY:"or-key" }, undefined, JEV_OPENROUTER_ROUTE.envKeys), JEV_OPENROUTER_ROUTE);
    await client.evaluate(request, signal());
    expect(JSON.parse(vi.mocked(fetched).mock.calls[0]![1]!.body as string).model).toBe("~typesafe/jev-latest");
    const pinned = new JevClient({ ...DEFAULT_JEV_CONFIG, model: "jev-preview" }, fetched, new JevCredentials([], { OPENROUTER_API_KEY:"or-key" }, undefined, JEV_OPENROUTER_ROUTE.envKeys), JEV_OPENROUTER_ROUTE);
    await expect(pinned.evaluate(request, signal())).rejects.toThrow("not available on the OpenRouter route");
    expect(fetched).toHaveBeenCalledTimes(1);
  });
  it("routes Vercel AI Gateway ids through the vercel-ai-gateway credential and endpoint", async () => {
    const fetched = vi.fn(async () => new Response(JSON.stringify({ ...response, model: "typesafe-ai/jev" }))) as unknown as typeof fetch;
    const credentials = new JevCredentials([], { AI_GATEWAY_API_KEY:"vg-key" }, undefined, JEV_VERCEL_ROUTE.envKeys);
    expect(credentials.status()).toEqual({ configured:true, source:"environment", verified:false });
    expect(new JevCredentials([], { TYPESAFE_API_KEY:"wrong-route" }, undefined, JEV_VERCEL_ROUTE.envKeys).status().source).toBe("missing");
    const client = new JevClient({ ...DEFAULT_JEV_CONFIG, model: "typesafe-ai/jev" }, fetched, credentials, JEV_VERCEL_ROUTE);
    expect(await client.evaluate(request, signal())).toMatchObject({ model:"typesafe-ai/jev" });
    const [url, args] = vi.mocked(fetched).mock.calls[0]!;
    expect(url).toBe("https://ai-gateway.vercel.sh/typesafe/v1/systemone");
    expect(args?.redirect).toBe("error");
    expect(args?.headers).toEqual({ "Content-Type":"application/json", Authorization:"Bearer vg-key" });
    expect(JSON.parse(args!.body as string).model).toBe("typesafe-ai/jev");
  });
  it("routes SubQ ids through the subq credential and endpoint, unchanged", async () => {
    // SubQ reports no output tokens: its decisions are billed on input alone.
    const fetched = vi.fn(async () => new Response(JSON.stringify({ ...response, model: "subq-snap-preview", usage: { input_tokens: 197, output_tokens: 0 } }))) as unknown as typeof fetch;
    const credentials = () => new JevCredentials([], { SUBQ_API_KEY:"sq-key" }, undefined, JEV_SUBQ_ROUTE.envKeys);
    expect(credentials().status()).toEqual({ configured:true, source:"environment", verified:false });
    expect(new JevCredentials([], { TYPESAFE_API_KEY:"wrong-route" }, undefined, JEV_SUBQ_ROUTE.envKeys).status().source).toBe("missing");
    const client = new JevClient({ ...DEFAULT_JEV_CONFIG, model: "subq-snap-preview" }, fetched, credentials(), JEV_SUBQ_ROUTE);
    expect(await client.evaluate(request, signal())).toMatchObject({ model:"subq-snap-preview", usage:{ input_tokens:197, output_tokens:0 } });
    const [url, args] = vi.mocked(fetched).mock.calls[0]!;
    expect(url).toBe("https://api.subq.ai/v1/systemone");
    expect(args?.redirect).toBe("error");
    expect(args?.headers).toEqual({ "Content-Type":"application/json", Authorization:"Bearer sq-key" });
    expect(JSON.parse(args!.body as string).model).toBe("subq-snap-preview");
    const jev = new JevClient({ ...DEFAULT_JEV_CONFIG, model: "jev-latest" }, fetched, credentials(), JEV_SUBQ_ROUTE);
    await expect(jev.evaluate(request, signal())).rejects.toThrow("not available on the SubQ route");
    expect(fetched).toHaveBeenCalledTimes(1);
  });
  it("maps Vercel AI Gateway aliases and rejects direct-route models on a gateway client", async () => {
    const fetched = vi.fn(async () => new Response(JSON.stringify({ ...response, model: "typesafe-ai/jev" }))) as unknown as typeof fetch;
    const credentials = () => new JevCredentials([], { AI_GATEWAY_API_KEY:"vg-key" }, undefined, JEV_VERCEL_ROUTE.envKeys);
    const client = new JevClient({ ...DEFAULT_JEV_CONFIG, model: "jev-latest" }, fetched, credentials(), JEV_VERCEL_ROUTE);
    await client.evaluate(request, signal());
    expect(JSON.parse(vi.mocked(fetched).mock.calls[0]![1]!.body as string).model).toBe("typesafe-ai/jev");
    const pinned = new JevClient({ ...DEFAULT_JEV_CONFIG, model: "jev-preview" }, fetched, credentials(), JEV_VERCEL_ROUTE);
    await expect(pinned.evaluate(request, signal())).rejects.toThrow("not available on the Vercel AI Gateway route");
    expect(fetched).toHaveBeenCalledTimes(1);
  });
  it("honors abort before fetching and rejects malformed/oversized responses", async () => {
    const fetcher = vi.fn(async () => new Response("x".repeat(1_048_577))) as unknown as typeof fetch;
    const client = new JevClient(DEFAULT_JEV_CONFIG, fetcher, new JevCredentials([], { TYPESAFE_API_KEY:"test-key" }));
    const controller = new AbortController(); controller.abort();
    await expect(client.evaluate(request,controller.signal)).rejects.toThrow(); expect(fetcher).not.toHaveBeenCalled();
    await expect(client.evaluate(request,signal())).rejects.toThrow("oversized");
    await expect(client.evaluate(request,signal())).rejects.not.toBeInstanceOf(JevInvalidResponseError);
  });
  it("keeps only well-formed reported usage when rejecting a malformed answer", async () => {
    const malformed = { ...response, answers: { ...response.answers, yes: { type: "noul", noul: 2 } } };
    const client = new JevClient(DEFAULT_JEV_CONFIG, (async () => new Response(JSON.stringify(malformed))) as unknown as typeof fetch, new JevCredentials([], { TYPESAFE_API_KEY:"test-key" }));
    const rejected = await client.evaluate(request, signal()).catch((error: unknown) => error);
    expect(rejected).toBeInstanceOf(JevInvalidResponseError);
    expect(rejected).toMatchObject({ message: "TypeSafe returned an invalid or oversized typed response", usage: { input_tokens: 30, output_tokens: 10 } });
    expect(rejected).not.toHaveProperty("answers");
    for (const usage of [{ input_tokens: -1, output_tokens: 10 }, undefined]) {
      const unknown = (() => { try { return checkResponse({ ...malformed, usage }, request); } catch (error) { return error; } })();
      expect(unknown).toBeInstanceOf(Error); expect(unknown).not.toBeInstanceOf(JevInvalidResponseError);
    }
  });
});
describe("Jev validation", () => {
  it("accepts structured question rubrics and rejects generated-text requests", () => {
    expect(() => checkRequest(request,131072)).not.toThrow();
    expect(() => checkRequest({state:"x",questions:{x:{type:"text",instructions:"Write prose"}}},131072)).toThrow("not free-text");
    expect(() => checkRequest({state:"x",questions:{x:{type:"score",instructions:"How much?",criteria:["one"]}}},131072)).toThrow("2–10");
    expect(() => checkRequest({...request,apiKey:"no"},131072)).toThrow();
  });
  it("checks answer coverage, option membership, finite probabilities, and usage", () => {
    expect(checkResponse(response,request)).toEqual(response);
    for (const mutate of [
      (v: any) => delete v.answers.yes,
      (v: any) => v.answers.route.choice = "invented",
      (v: any) => v.answers.route.probabilities.billing = NaN,
      (v: any) => v.answers.yes.noul = 2,
      (v: any) => v.usage.input_tokens = -1,
    ]) { const value = structuredClone(response); mutate(value); expect(() => checkResponse(value,request)).toThrow(); }
  });
  it("validates the bounded program schema subset rather than accepting unknown keywords", () => {
    for (const schema of [{type:"strng"},{pattern:"(x+)+"},{ $ref:"#" },{items:4},{maximum:"5"}]) expect(() => checkSchema(schema)).toThrow();
    const schema = { type:"array",items:{type:"integer"},maxItems:3 }; checkSchema(schema);
    expect(() => checkValue(schema,[1,2],"output")).not.toThrow();
    expect(() => checkValue(schema,[1,"2"],"output")).toThrow();
  });
  it.each(["typesafe/jev-1.13", "~typesafe/jev-latest", "typesafe-ai/jev", "jev-1.13.0", "jev-preview"])("accepts routed model %s and rejects unknown shapes", model => {
    expect(normalizeJevConfig({ model }).model).toBe(model);
    expect(normalizeJevConfig({ model: "typesafe/../../etc" }).model).toBe(DEFAULT_JEV_CONFIG.model);
    expect(() => checkRequest({ ...request, model }, 131072)).not.toThrow();
    expect(() => checkRequest({ ...request, model: "typesafe/../bad" }, 131072)).toThrow();
  });
  it.each([0, 0.5, 0.975, 1])("preserves finite approval threshold %s without rounding", autoApprovalThreshold => {
    expect(normalizeJevConfig({ autoApprovalThreshold }).autoApprovalThreshold).toBe(autoApprovalThreshold);
  });
  it.each([undefined, null, "0.75", true, -0.01, 1.01, NaN, Infinity])("defaults invalid approval threshold %s to 0.50", autoApprovalThreshold => {
    expect(normalizeJevConfig({ autoApprovalThreshold }).autoApprovalThreshold).toBe(0.5);
  });
  it("normalizes host ceilings and never interprets credential commands as a shell string", () => {
    const c = normalizeJevConfig({maxDurationMs:Infinity,maxConcurrentRuns:1000,credentialCommand:"echo key"});
    expect(c.maxDurationMs).toBe(DEFAULT_JEV_CONFIG.maxDurationMs); expect(c.maxConcurrentRuns).toBe(16); expect(c.credentialCommand).toEqual([]);
  });
});
