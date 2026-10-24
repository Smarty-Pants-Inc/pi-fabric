import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { findExecutable } from "../src/agents/transports/process-utils.js";
import { ActorManager } from "../src/actors/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";

const cleanups: Array<() => Promise<unknown> | void> = [];
afterEach(async () => {
  try { for (const close of cleanups.splice(0).reverse()) await close(); }
  finally { vi.unstubAllEnvs(); }
});

type Probe = {
  id: string;
  tool: "write" | "edit";
  target: string;
  denied: boolean;
  args: Record<string, unknown>;
};
type Activation = {
  number: number;
  runId?: string;
  scratch?: string;
  probes: Probe[];
  cursor: number;
  responses: Map<string, string>;
  advertisedTools?: string[];
  discoveryResponse?: string;
  temporary?: { id: string; sent: boolean; workerEnvironment: string[]; response?: string };
};
const temporaryArgs = {
  command: String.raw`d=$(mktemp -d); printf x > "$d/claim.json"; printf 'claim=%s\n' "$d"; printf 'TMPDIR=%s\nTMP=%s\nTEMP=%s\n' "$TMPDIR" "$TMP" "$TEMP"`,
  timeout: 10,
};
const textOf = (content: unknown): string => typeof content === "string" ? content :
  Array.isArray(content) ? content.map((part) => part?.text ?? "").join("\n") : JSON.stringify(content) ?? "";

// Keep reason: actor state-file overwrite incident (smarty-dev#14404), and the
// host-to-actor temp environment trust boundary (smarty-dev#4605, Linux bash).
// ActorManager -> AgentManager -> freshly built dist/worker.js -> native Pi CLI.
// Only inference is substituted. Extensions=false uses advertised native tools;
// extensions=true keeps fullCodeMode and calls the real pi.* tools via fabric_exec.
// No mocked runner/worker/guard, no direct call to writePolicyDenial, no actor-policy
// fields, and no successful silent native skip.
describe("native actor write/edit confinement", () => {
  it.each([false, true])("default scratch-only actor refuses outside writes/edits across restart (extensions=%s)", async (extensions) => {
    const worker = path.resolve("dist/worker.js");
    const extension = path.resolve("dist/index.js");
    expect(fs.existsSync(worker), "run bun run build on the candidate first").toBe(true);
    expect(fs.existsSync(extension), "compiled production extension required").toBe(true);
    const piBinary = process.env.PI_FABRIC_ACTIVATION_TEST_PI_BINARY ?? findExecutable("pi");
    expect(piBinary, "native Pi CLI required: set PI_FABRIC_ACTIVATION_TEST_PI_BINARY; never skip this proof").toBeTruthy();
    fs.accessSync(piBinary!, fs.constants.X_OK);

    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-actor-write-native-")));
    cleanups.push(() => fs.promises.rm(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }));
    const cwd = path.join(root, "work");
    const agentDir = path.join(root, "agent");
    const meshRoot = path.join(root, "mesh");
    const runRoot = path.join(root, "runs");
    const otherState = path.join(root, "other-state");
    for (const directory of [cwd, agentDir, otherState]) fs.mkdirSync(directory);
    const before = "native sentinel before\n";
    const written = "native scratch written\n";
    const edited = "native scratch edited\n";
    const forbidden = "NATIVE ACTOR MUST NOT WRITE THIS\n";
    const errors: unknown[] = [];
    const activations: Activation[] = [];
    let current: Activation;
    let owner: ActorManager;
    let agents: AgentManager;
    let actorId: string;
    let sessionFile: string;

    const server = http.createServer((request, response) => {
      if (request.method === "GET" && request.url === "/v1/models") {
        response.writeHead(200, { "Content-Type": "application/json" });
        response.end(JSON.stringify({ object: "list", data: [{ id: "actor", object: "model" }] }));
        return;
      }
      if (request.method !== "POST" || request.url !== "/v1/chat/completions") {
        response.writeHead(404); response.end("offline fixture route not found"); return;
      }
      let body = "";
      request.on("data", (data) => { body += data; });
      request.on("end", () => {
        try {
          const payload = JSON.parse(body);
          const discoveryId = `activation-${current.number}-discovery`;
          current.advertisedTools = payload.tools.map((tool: any) => tool.function?.name);
          for (const message of payload.messages) {
            if (message.role !== "tool") continue;
            if (message.tool_call_id === discoveryId) current.discoveryResponse = textOf(message.content);
            if (current.temporary && message.tool_call_id === current.temporary.id) {
              current.temporary.response = textOf(message.content);
            }
            if (current.probes.some((probe) => probe.id === message.tool_call_id)) {
              current.responses.set(message.tool_call_id, textOf(message.content));
            }
          }
          const reply = (id: string, name: string, args: unknown) => {
            response.writeHead(200, { "Content-Type": "text/event-stream" });
            response.end(`data: ${JSON.stringify({
              id: "actor-write-offline", object: "chat.completion.chunk", created: 1, model: "actor",
              choices: [{ index: 0, delta: { role: "assistant", tool_calls: [{ index: 0, id, type: "function",
                function: { name, arguments: JSON.stringify(args) } }] }, finish_reason: null }],
            })}\n\ndata: ${JSON.stringify({
              id: "actor-write-offline", object: "chat.completion.chunk", created: 1, model: "actor",
              choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }],
            })}\n\ndata: [DONE]\n\n`);
          };
          if (current.probes.length === 0) {
            // Observe the real admitted run, never intercept or replace launch.
            const runId = owner.status(actorId).inFlightRun?.id;
            expect(runId, "native inference must belong to an ActorManager activation").toBeTruthy();
            current.runId = runId!;
            const runDirectory = agents.runDirectory(runId!)!;
            expect(runDirectory).toBeTruthy();
            // smarty-dev#15878: a short private TMPDIR when one fits the host's temp path, else the run's own tmp.
            const recorded = (JSON.parse(fs.readFileSync(path.join(runDirectory, "status.json"), "utf8")) as { temporaryDirectory?: string }).temporaryDirectory;
            const scratch = recorded ?? path.join(runDirectory, "tmp");
            current.scratch = scratch;
            if (process.platform === "linux") {
              // Observe the live worker's exec environment as well as bash's:
              // worker.ts can mask a transport leak when it exports --tmpdir to Pi.
              const workerPid = agents.status(runId!).sessionId;
              expect(workerPid, "native process worker PID required").toMatch(/^\d+$/);
              current.temporary = { id: `activation-${current.number}-temporary`, sent: false,
                workerEnvironment: fs.readFileSync(`/proc/${workerPid}/environ`, "utf8").split("\0")
                  .filter((entry) => /^(TMPDIR|TMP|TEMP)=/.test(entry)).sort() };
            }
            let editSchema;
            if (extensions) {
              expect(current.advertisedTools, "full-code actor must expose its real execution tool").toContain("fabric_exec");
              expect(current.advertisedTools).not.toContain("write");
              expect(current.advertisedTools).not.toContain("edit");
              if (current.discoveryResponse === undefined) {
                reply(discoveryId, "fabric_exec", { code: 'return await Promise.all([tools.describe({ref:"pi.write"}), tools.describe({ref:"pi.edit"})]);',
                  resultFormat: "json" });
                return;
              }
              const discovered = JSON.parse(current.discoveryResponse);
              expect(discovered, `actual in-actor discovery: ${current.discoveryResponse}`).toEqual([
                expect.objectContaining({ ref: "pi.write", inputSchema: expect.any(Object) }),
                expect.objectContaining({ ref: "pi.edit", inputSchema: expect.any(Object) }),
              ]);
              editSchema = discovered[1].inputSchema;
            } else {
              editSchema = payload.tools.find((tool: any) => tool.function?.name === "edit")?.function?.parameters;
              expect(editSchema, "actual native edit tool must be advertised").toBeTruthy();
              expect(current.advertisedTools).toContain("write");
            }
            const add = (tool: Probe["tool"], target: string, denied: boolean, oldText = before, newText = forbidden) => {
              const args = tool === "write" ? { path: target, content: denied ? forbidden : written } :
                editSchema.properties?.edits ? { path: target, edits: [{ oldText, newText }] } : { path: target, oldText, newText };
              current.probes.push({ id: `activation-${current.number}-probe-${current.probes.length}`, tool, target, denied, args });
            };
            // Run metadata, actor state, unrelated state and cwd must never be
            // granted just because they are needed by the host or contain tmp/.
            for (const [label, directory] of [["cwd", cwd], ["run-metadata", runDirectory],
              ["actor-state", path.dirname(sessionFile)], ["other-state", otherState]] as const) {
              const writeTarget = path.join(directory, `${current.number}-${label}-write.txt`);
              const editTarget = path.join(directory, `${current.number}-${label}-edit.txt`);
              fs.writeFileSync(editTarget, before);
              // Relative cwd paths additionally prove actual Pi path resolution.
              add("write", label === "cwd" ? path.basename(writeTarget) : writeTarget, true);
              add("edit", label === "cwd" ? path.basename(editTarget) : editTarget, true);
            }
            const inside = path.join(scratch, "inside.txt");
            add("write", inside, false);
            add("edit", inside, false, written, edited);
            // Created only after the native inside write/edit, below. Both real
            // tools must refuse a lexical scratch path resolving outside it.
            add("write", path.join(scratch, "escape", `${current.number}-symlink-write.txt`), true);
            fs.writeFileSync(path.join(cwd, `${current.number}-symlink-edit.txt`), before);
            add("edit", path.join(scratch, "escape", `${current.number}-symlink-edit.txt`), true);
          }
          const probe = current.probes[current.cursor++];
          if (probe?.target.includes(`${path.sep}escape${path.sep}`)) {
            const link = path.join(current.scratch!, "escape");
            if (!fs.existsSync(link)) fs.symlinkSync(cwd, link, "junction");
          }
          if (probe) {
            // Do not catch nested failures: the real refusal must make the outer
            // fabric_exec result isError, with the exact pi.* invocation in its audit.
            reply(probe.id, extensions ? "fabric_exec" : probe.tool, extensions ? {
              code: `return await tools.call({ref:"pi.${probe.tool}",args:JSON.parse(π.args)});`,
              payloads: { args: JSON.stringify(probe.args) }, resultFormat: "json",
            } : probe.args);
          } else if (current.temporary && !current.temporary.sent) {
            current.temporary.sent = true;
            reply(current.temporary.id, extensions ? "fabric_exec" : "bash", extensions ? {
              code: 'return await tools.call({ref:"pi.bash",args:JSON.parse(π.args)});',
              payloads: { args: JSON.stringify(temporaryArgs) }, resultFormat: "json",
            } : temporaryArgs);
          } else {
            response.writeHead(200, { "Content-Type": "text/event-stream" });
            response.end(`data: ${JSON.stringify({
              id: "actor-write-offline", object: "chat.completion.chunk", created: 1, model: "actor",
              choices: [{ index: 0, delta: { role: "assistant", content: `native actor write probe ${current.number} complete` },
                finish_reason: "stop" }],
            })}\n\ndata: [DONE]\n\n`);
          }
        } catch (error) {
          errors.push(error);
          if (!response.headersSent) response.writeHead(500, { "Content-Type": "application/json" });
          response.end(JSON.stringify({ error: { message: String(error), type: "fixture_error" } }));
        }
      });
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    cleanups.push(() => new Promise<void>((resolve) => { server.closeAllConnections(); server.close(() => resolve()); }));
    fs.writeFileSync(path.join(agentDir, "models.json"), JSON.stringify({ providers: { "actor-write-offline": {
      baseUrl: `http://127.0.0.1:${(server.address() as { port: number }).port}/v1`, apiKey: "offline-only", api: "openai-completions",
      models: [{ id: "actor", name: "actor", reasoning: false, input: ["text"], contextWindow: 200000, maxTokens: 65536,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
    } } }));
    fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ enableInstallTelemetry: false,
      compaction: { enabled: false }, retry: { enabled: false } }));
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ fullCodeMode: true,
      agents: { notifyOnComplete: false, retainRuns: true, nice: 19, sessionExport: false },
      mesh: { enabled: true, actorPollMs: 20 }, mcp: { enabled: false }, memory: { enabled: false },
      jev: { enabled: false }, residency: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false },
    }));
    vi.stubEnv("TMPDIR", root);
    vi.stubEnv("TMP", root);
    vi.stubEnv("TEMP", root);
    vi.stubEnv("XDG_CACHE_HOME", path.join(root, "cache"));
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
    vi.stubEnv("PI_OFFLINE", "1");
    vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined);
    vi.stubEnv("PI_FABRIC_MESH_ROOT", meshRoot);
    vi.stubEnv("PI_FABRIC_WRITE_POLICY", undefined);
    agents = new AgentManager(cwd, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 45000, sessionExport: false, nice: 19 }, {
      workerPath: worker, fabricExtensionPath: extension, piBinary: piBinary!, runRoot,
      mainAgentId: "session:actor-write-native", fabricSessionId: "actor-write-native", fullCodeMode: true,
    });
    cleanups.push(() => agents.close());
    const makeOwner = () => new ActorManager("actor-write-native", {
      id: "session:actor-write-native", name: "owner", kind: "main", sessionId: "actor-write-native",
    }, new MeshStore(meshRoot, 65536, 100), { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
      persistent: true, actorRoot: path.join(meshRoot, "actors", "actor-write-native"), claimResidency: "session",
      rootId: "session:actor-write-native", resolvePiModel: async (model) => model,
    });
    owner = makeOwner();
    cleanups.push(() => owner.close());
    const actor = await owner.create({ name: "native-write-policy", instructions: "Perform the supplied native write/edit probes.",
      delivery: "mailbox", responseMode: "text", runner: "pi", transport: "process", residency: "session",
      model: "actor-write-offline/actor", tools: ["write", "edit", ...(process.platform === "linux" ? ["bash"] : [])],
      extensions, nice: 19, timeoutMs: 45000 });
    actorId = actor.id;
    sessionFile = actor.sessionFile!;

    for (const number of [1, 2]) {
      if (number === 2) {
        await owner.close();
        owner = makeOwner(); // Restore the persistent actor, not a hand-made AgentRunRequest.
        cleanups.push(() => owner.close());
        expect(owner.status(actorId).sessionFile).toBe(sessionFile);
      }
      current = { number, probes: [], cursor: 0, responses: new Map() };
      activations.push(current);
      let message;
      try {
        message = await owner.ask(actorId, `Execute native write/edit activation ${number}.`);
      } catch (error) {
        // The provider may hide the fixture's HTTP 500 body. Preserve the exact
        // assertion/parse error and real tool output instead of reporting only 500.
        throw new Error(`Native actor activation ${number} (extensions=${extensions}) failed: ${String(error)}\n` +
          `Fixture errors:\n${errors.map((entry) => entry instanceof Error ? entry.stack : String(entry)).join("\n")}\n` +
          `Tool surface: ${JSON.stringify(current.advertisedTools)}\nDiscovery: ${current.discoveryResponse}\n` +
          `Last tool result: ${JSON.stringify([...current.responses].at(-1))}\nTemporary tool result: ${current.temporary?.response}`, { cause: error });
      }
      expect(errors).toEqual([]);
      expect(message.error).toBeUndefined();
      expect(message.text).toBe(`native actor write probe ${number} complete`);
      expect(current.probes).toHaveLength(12);
      expect(current.responses.size, "each real tool result must return to native inference").toBe(12);
      expect(agents.status(current.runId!).status).toBe("completed");
      const entries = fs.readFileSync(sessionFile, "utf8").trim().split("\n").map((line) => JSON.parse(line));
      for (const probe of current.probes) {
        const result = entries.find((entry) => entry.type === "message" && entry.message?.role === "toolResult" &&
          entry.message.toolCallId === probe.id)?.message;
        expect(result, `${probe.id}: native session must contain the actual tool result`).toBeTruthy();
        expect(result.toolName).toBe(extensions ? "fabric_exec" : probe.tool);
        expect(result.isError === true, `${probe.id}: ${textOf(result.content)}`).toBe(probe.denied);
        const returned = current.responses.get(probe.id)!;
        expect(returned, `${probe.id}: inference must receive the actual persisted result`).toBe(textOf(result.content));
        const audit = extensions ? result.details?.audits?.find((entry: any) => entry.ref === `pi.${probe.tool}`) : undefined;
        if (extensions) {
          expect(result.details?.success).toBe(!probe.denied);
          expect(audit, `${probe.id}: real nested pi.* call must be audited`).toMatchObject({
            ref: `pi.${probe.tool}`, args: probe.args, success: !probe.denied,
          });
          if (!probe.denied) {
            expect(audit.result).toMatchObject({ ok: true });
            expect(JSON.parse(returned)).toMatchObject({ ok: true });
          }
        }
        if (probe.denied) {
          const denial = `Fabric write policy refuses ${probe.tool} outside writable roots; ` +
            `refused path: ${JSON.stringify(probe.target)}; allowed roots: ${JSON.stringify([current.scratch])}` +
            "; write outside actor scratch refused; use the shell or a trusted helper for factory state";
          if (extensions) {
            // QuickJS transports a thrown host Error as JSON in the runtime
            // error line; compare its actual message, not JSON-escaped display bytes.
            const runtimeError = returned.match(/^Runtime error: (.+)$/m)?.[1];
            expect(runtimeError, `${probe.id}: actual runtime error must return to inference`).toBeTruthy();
            expect(JSON.parse(runtimeError!).message).toBe(denial);
            expect(audit.error).toBe(denial);
          } else expect(returned).toContain(denial);
          const actual = path.resolve(cwd, probe.target);
          if (probe.tool === "write") expect(fs.existsSync(actual), `${probe.id}: outside file must not be created`).toBe(false);
          else expect(fs.readFileSync(actual, "utf8"), `${probe.id}: outside bytes must be unchanged`).toBe(before);
        } else expect(returned).not.toContain("Fabric write policy refuses");
      }
      if (process.platform === "linux") {
        // Linux /proc proves the worker's launch environment without replacing
        // transport/worker/CLI; mktemp and claim creation run in the real bash tool.
        const temporary = current.temporary!;
        expect(temporary?.sent, "native bash temporary-directory probe must run").toBe(true);
        const result = entries.find((entry) => entry.type === "message" && entry.message?.role === "toolResult" &&
          entry.message.toolCallId === temporary.id)?.message;
        expect(result, "native session must retain the real bash result").toBeTruthy();
        expect(result.toolName).toBe(extensions ? "fabric_exec" : "bash");
        expect(result.isError === true, textOf(result.content)).toBe(false);
        expect(temporary.response, "inference must receive the actual bash result").toBe(textOf(result.content));
        let output = temporary.response!;
        if (extensions) {
          expect(result.details?.success).toBe(true);
          expect(result.details?.audits?.find((entry: any) => entry.ref === "pi.bash"))
            .toMatchObject({ ref: "pi.bash", success: true });
          const value = JSON.parse(output);
          expect(value).toMatchObject({ ok: true, output: expect.any(String) });
          output = value.output;
        }
        const lines = output.trimEnd().split("\n");
        const directory = lines.find((line) => line.startsWith("claim="))?.slice("claim=".length);
        const scratch = current.scratch!;
        const environment = ["TMPDIR", "TMP", "TEMP"].map((key) =>
          lines.find((line) => line.startsWith(`${key}=`))?.slice(key.length + 1));
        expect(directory, `mktemp output: ${output}`).toBeTruthy();
        expect(environment, "TMPDIR/TMP/TEMP must all name this run's scratch").toEqual([scratch, scratch, scratch]);
        expect(temporary.workerEnvironment, "worker must not inherit Main's temp environment")
          .toEqual([`TMPDIR=${scratch}`, `TMP=${scratch}`, `TEMP=${scratch}`].sort());
        expect(path.dirname(directory!)).toBe(scratch);
        expect(path.dirname(fs.realpathSync(directory!))).toBe(fs.realpathSync(scratch));
        expect(fs.statSync(scratch).mode & 0o777).toBe(0o700);
        expect(fs.statSync(directory!).isDirectory()).toBe(true);
        expect(fs.statSync(directory!).mode & 0o777).toBe(0o700);
        const claim = path.join(directory!, "claim.json");
        expect(fs.realpathSync(claim)).toBe(path.join(fs.realpathSync(directory!), "claim.json"));
        expect(fs.statSync(claim).isFile()).toBe(true);
        expect(fs.readFileSync(claim, "utf8")).toBe("x");
      }
      expect(fs.readFileSync(path.join(current.scratch!, "inside.txt"), "utf8")).toBe(edited);
      expect(fs.existsSync(path.join(cwd, `${number}-symlink-write.txt`))).toBe(false);
      expect(fs.readFileSync(path.join(cwd, `${number}-symlink-edit.txt`), "utf8")).toBe(before);
    }
    expect(activations[0]!.runId).not.toBe(activations[1]!.runId);
    expect(activations[0]!.scratch).not.toBe(activations[1]!.scratch);
    console.info("offline native actor write-policy proof", JSON.stringify({ piBinary: fs.realpathSync(piBinary!), worker,
      extension, actorId, extensions, route: extensions ? "fabric_exec -> tools.call(pi.*) -> real tool lifecycle" : "direct native tools",
      sessionFile, activations: activations.map(({ runId, scratch, probes, advertisedTools, discoveryResponse, temporary }) => ({
        runId, scratch, advertisedTools, discoveredTools: discoveryResponse === undefined ? undefined :
          JSON.parse(discoveryResponse).map(({ ref, provider, namespace }: any) => ({ ref, provider, namespace })),
        toolResults: probes.length + (temporary?.response === undefined ? 0 : 1), denied: probes.filter((probe) => probe.denied).length,
        temporary,
      })) }));
  }, 120000);
});
