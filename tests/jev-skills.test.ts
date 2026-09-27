import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { QuickJsRuntime } from "../src/runtime/quickjs-runtime.js";
import { MontyRuntime } from "../src/runtime/monty-runtime.js";
import type { JevLaunch, JevRequest, JevRunInfo } from "../src/jev/types.js";
import { jevContext, setupJev } from "./jev-test-helpers.js";

const montyAvailable = await import("@pydantic/monty/node").then(() => true, () => false);
type Kernel = "typescript" | "python";
const markdown = (kernel: Kernel) => readFileSync(`skillsets/${kernel}/fabric-jev/SKILL.md`, "utf8");
const example = (kernel: Kernel, background = false) => {
  const fence = kernel === "typescript" ? /```ts\r?\n([\s\S]*?)\r?\n```/ : /```python\r?\n([\s\S]*?)\r?\n```/;
  const code = markdown(kernel).match(fence)?.[1];
  if (!code) throw new Error(`Missing ${kernel} Jev starter`);
  return background ? code.replace("const background = false;", "const background = true;").replace("background = False", "background = True") : code;
};
const execute = async (kernel: Kernel, host: (ref: string, args: Record<string, unknown>, signal: AbortSignal) => Promise<unknown>, background = false) => {
  const runtime = kernel === "typescript" ? new QuickJsRuntime() : new MontyRuntime();
  const result = await runtime.execute(example(kernel, background), async (ref, args, signal) => {
    if (ref === "fabric.$call") return host(args.ref as string, (args.args ?? {}) as Record<string, unknown>, signal);
    return host(ref, args, signal);
  }, { timeoutMs: 10000, memoryLimitBytes: 64 * 1024 * 1024 });
  expect(result.terminationReason, result.error).toBe("completed");
  return result.value as Pick<JevRunInfo, "id" | "state" | "result" | "error" | "evaluations">;
};

it.each(["typescript", "python"] as const)("keeps %s Jev auth, policy, loop, and shell-first guidance discoverable", (kernel) => {
  const skill = markdown(kernel);
  for (const required of ["disable-model-invocation: true", "<skill-dir>/../../../docs/jev.md", "/login jev", "auth.json", "verified: false", "requires", "Schema enforce", "managed hosts", "session-owned, not restart-durable", "tasks.wait", "tasks.watch", "maxEvaluations: 0", "browser-harness-js", "macos-harness serve", "10 Hz", "64 events", "429/529", "## Completion criterion", "program.nextEvent()", "program.advise", "status.observation", "Do not wait/join an active observer"]) {
    expect(skill, `${kernel}: ${required}`).toContain(required);
  }
  for (const obsolete of ["components.apply", "interactionModulePath", "allowedMethods", "browser.cdp", "macos.connect"]) expect(skill).not.toContain(obsolete);
  for (const name of ["fabric-guide", "fabric-exec"]) {
    expect(readFileSync(`skillsets/${kernel}/${name}/SKILL.md`, "utf8")).toContain("/skill:fabric-jev");
  }
});

for (const kernel of ["typescript", "python"] as const) {
  describe.skipIf(kernel === "python" && !montyAvailable)(`${kernel} Jev skill through real runtimes and mocked transport`, () => {
    const fixture = (mode: "certain" | "uncertain" | "no-match" | "failure" = "certain") => {
      const requests: JevRequest[] = [];
      const fetcher = (async (_url, init) => {
        const request = JSON.parse(String(init?.body)) as JevRequest;
        requests.push(request);
        if (mode === "failure" && requests.length === 2) return new Response("private upstream detail", { status: 429 });
        const first = requests.length === 1;
        const choice = first ? "billing" : mode === "no-match" ? "other" : "technical";
        return new Response(JSON.stringify({
          model: "jev-latest",
          answers: {
            route: { type: "choice", choice, confidence: !first && mode === "uncertain" ? 0.5 : 0.99,
              probabilities: { billing: choice === "billing" ? 0.98 : 0.01, technical: choice === "technical" ? 0.98 : 0.01, other: choice === "other" ? 0.98 : 0.01 } },
            refund: { type: "noul", noul: first ? 0.98 : 0.01 },
            urgency: { type: "score", score: 0.25, confidence: 0.6, probabilities: { "0": 0.8, "1": 0.15, "2": 0.05 } },
          },
          usage: { input_tokens: 10, output_tokens: 3 },
        }));
      }) as typeof fetch;
      const { provider } = setupJev({ executor: { kernel } }, fetcher);
      const host = (ref: string, args: Record<string, unknown>, signal: AbortSignal) => {
        expect(["jev.run", "jev.spawn"]).toContain(ref);
        return provider.invoke(ref.slice(4), args, jevContext(signal));
      };
      return { provider, requests, host };
    };

    it.each(["certain", "uncertain", "no-match"] as const)("batches all primitives and handles %s routing", async (mode) => {
      const { provider, requests, host } = fixture(mode);
      try {
        const result = await execute(kernel, host);
        expect(result.state, result.error).toBe("completed");
        expect(result.evaluations).toBe(2);
        expect(result.result).toEqual([
          { route: "billing", refundProbability: 0.98, urgency: 0.25 },
          { route: mode === "certain" ? "technical" : "review", refundProbability: 0.01, urgency: 0.25 },
        ]);
        expect(requests.map(r => Object.values(r.questions).map(q => q.type))).toEqual([
          ["choice", "noul", "score"], ["choice", "noul", "score"],
        ]);
        const run = provider.manager.status(result.id);
        expect(run.events.map(e => e.value)).toEqual([{ processed: 1 }, { processed: 2 }]);
        expect(run.usage).toEqual({ input_tokens: 20, output_tokens: 6 });
      } finally { await provider.close(); }
    });

    it("detaches the same artifact and joins it without another model turn", async () => {
      const { provider, host } = fixture();
      try {
        const started = await execute(kernel, host, true);
        expect(started.state).toBe("running");
        const terminal = await provider.manager.join(started.id);
        expect(terminal.state, terminal.error).toBe("completed");
        expect(terminal.evaluations).toBe(2);
      } finally { await provider.close(); }
    });

    it("reports failure and retains partial progress without retrying", async () => {
      const { provider, host, requests } = fixture("failure");
      try {
        const result = await execute(kernel, host);
        expect(result.state).toBe("failed");
        expect(result.error).toContain("429");
        expect(JSON.stringify(result)).not.toContain("private upstream detail");
        expect(requests).toHaveLength(2);
        expect(provider.manager.status(result.id).events.map(e => e.value)).toEqual([{ processed: 1 }]);
      } finally { await provider.close(); }
    });
  });
}

it.skipIf(!montyAvailable)("submits the same serializable artifact from both kernel variants", async () => {
  const launches: JevLaunch[] = [];
  for (const kernel of ["typescript", "python"] as const) {
    await execute(kernel, async (ref, args) => {
      expect(ref).toBe("jev.run");
      launches.push(args as unknown as JevLaunch);
      return { id: "fixture", state: "completed", result: [], evaluations: 0 };
    });
  }
  expect(launches[1]).toEqual(launches[0]);
});
