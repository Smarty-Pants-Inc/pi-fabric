import http from "node:http";

export interface FakeOpenAIRequest {
  model: string;
  messages: unknown[];
}

export function parseFakeOpenAIRequest(body: string): FakeOpenAIRequest | undefined {
  let value: unknown;
  try {
    value = JSON.parse(body) as unknown;
  } catch {
    return undefined;
  }
  if (value === null || typeof value !== "object" || Array.isArray(value)) return undefined;
  const payload = value as { model?: unknown; messages?: unknown };
  if (typeof payload.model !== "string" || !Array.isArray(payload.messages)) return undefined;
  return { model: payload.model, messages: payload.messages };
}

export async function probeFakeOpenAIServer(port: number): Promise<void> {
  for (const body of ["", "not-json", "{}"]) {
    const status = await new Promise<number>((resolve, reject) => {
      const request = http.request({
        hostname: "127.0.0.1",
        port,
        path: "/v1/chat/completions",
        method: "POST",
        headers: { "content-type": "application/json", "content-length": Buffer.byteLength(body) },
      }, response => {
        response.resume();
        response.once("end", () => resolve(response.statusCode ?? 0));
      });
      request.once("error", reject);
      request.end(body);
    });
    if (status !== 400) throw new Error(`fake OpenAI server accepted invalid request ${JSON.stringify(body)} with HTTP ${status}`);
  }
}
