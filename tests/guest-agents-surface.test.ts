import { describe, expect, it } from "vitest";
import { AGENTS_ACTION_DESCRIPTORS } from "../src/providers/agents-actions.js";
import { messageTargetArgs } from "../src/providers/agents-provider.js";
import { CPYTHON_CHILD_SOURCE } from "../src/runtime/cpython-child-source.js";
import { GUEST_TYPE_DECLARATIONS, guestTypeDeclarations } from "../src/runtime/guest-types.js";
import { GUEST_SETUP, QuickJsRuntime } from "../src/runtime/quickjs-runtime.js";
import { typeCheckFabricCode } from "../src/runtime/type-checker.js";

// AgentsProvider.invoke implements every descriptor, and the audit projection,
// docs, and arg repair all spell the same refs. Only the TypeScript prelude
// curates a literal binding table, so a missed entry degrades into
// "agents.x is not a function" at runtime instead of a type error — and the
// Python kernel's dynamic proxy hides the asymmetry from parity checks.
const slice = (source: string, start: string, end: string): string => {
  const from = source.indexOf(start);
  const to = source.indexOf(end, from + start.length);
  if (from === -1 || to === -1) throw new Error(`Missing block: ${start}`);
  return source.slice(from, to);
};

const names = (block: string, pattern: RegExp): Set<string> =>
  new Set([...block.matchAll(pattern)].map((match) => (match[1] ?? "").replaceAll('"', "")));

const IMPLEMENTED = AGENTS_ACTION_DESCRIPTORS.map((descriptor) => descriptor.name);

describe("guest agents surface", () => {
  it.each(["send", "steer"])("repairs agents.%s targets without relaxing the existing guard", method => {
    expect(messageTargetArgs(method, { to: "session:peer", message: "HOLD", priority: "interrupt" }))
      .toEqual({ id: "session:peer", message: "HOLD", priority: "interrupt" });
    expect(() => messageTargetArgs(method, { id: "session:peer", to: "session:other", message: "HOLD" })).toThrow("two targets");
    expect(() => messageTargetArgs(method, { sessionId: "peer", message: "HOLD" })).toThrow("has no sessionId field");
  });
  it.each([false, true])("types and registers interrupt-priority send/steer (fullCodeMode=%s)", fullCodeMode => {
    const declarations = guestTypeDeclarations(fullCodeMode);
    for (const method of ["send", "steer"]) {
      const code = `return await agents.${method}({ id: "session:peer", message: "HOLD", priority: "interrupt" });`;
      expect(typeCheckFabricCode(code, declarations, true).errors).toEqual([]);
      const bad = `return await agents.${method}({ id: "session:peer", message: "HOLD", priority: "urgent" });`;
      expect(typeCheckFabricCode(bad, declarations, true).errors.map(error => error.message)).toEqual([
        expect.stringContaining('not assignable to type')]);
      const descriptor = AGENTS_ACTION_DESCRIPTORS.find(action => action.name === method)!;
      expect(descriptor.inputSchema.properties).toHaveProperty("priority", { type: "string", enum: ["interrupt"],
        description: expect.any(String) });
    }
    expect(typeCheckFabricCode('await agents.followUp({ id: "peer", message: "later", priority: "interrupt" });', declarations, true).errors.length).toBeGreaterThan(0);
  });
  it("bridges agents.send priority to the public provider, without extra authority", async () => {
    const calls: Array<{ ref: string; args: unknown }> = [];
    const result = await new QuickJsRuntime().execute('return await agents.send({ id: "session:peer", message: "HOLD", priority: "interrupt" });',
      async (ref, args) => { calls.push({ ref, args }); return { queued: true }; },
      { timeoutMs: 5000, memoryLimitBytes: 32 * 1024 * 1024 });
    expect(result.terminationReason).toBe("completed");
    expect(calls).toEqual([{ ref: "agents.send", args: { id: "session:peer", message: "HOLD", priority: "interrupt" } }]);
  });

  it.each([false, true])("types and registers spawn complexity hints (fullCodeMode=%s)", fullCodeMode => {
    for (const complexity of ["simple", "normal", "complex", "delicate"]) {
      expect(typeCheckFabricCode(`return await agents.spawn({ task: "work", complexity: "${complexity}" });`, guestTypeDeclarations(fullCodeMode), true).errors).toEqual([]);
    }
    expect(typeCheckFabricCode('return await agents.spawn({ task: "work", complexity: "unknown" });', guestTypeDeclarations(fullCodeMode), true).errors).not.toEqual([]);
    const schema = AGENTS_ACTION_DESCRIPTORS.find(d => d.name === "spawn")!.inputSchema as { properties: Record<string, unknown> };
    expect(schema.properties.complexity).toMatchObject({ type: "string", enum: ["simple", "normal", "complex", "delicate"] });
  });

  it.each([false, true])("types opt-in actor occurrence dedupe (fullCodeMode=%s)", fullCodeMode => {
    const code = `const actor = await agents.create({ name: "alarm", instructions: "Handle alarms", residency: "durable", topics: ["ops.owner"], dedupeKey: "data.key", coalesceKey: "payload.number", activation: { minIntervalMs: 1000 } }); return { dedupeKey: actor.dedupeKey, activation: actor.activation };`;
    expect(typeCheckFabricCode(code, guestTypeDeclarations(fullCodeMode), true).errors).toEqual([]);
    expect(typeCheckFabricCode(`await agents.create({ name: "bad", instructions: "x", dedupeKey: 42 });`, guestTypeDeclarations(fullCodeMode), true).errors)
      .toEqual([expect.objectContaining({ message: expect.stringContaining("not assignable to type 'string'") })]);
  });

  it.each([false, true])("types literal public message retry keys (fullCodeMode=%s)", fullCodeMode => {
    const code = `if (false) {
      await agents.followUp({ id: "session:peer", message: "unchanged", idempotencyKey: "follow-up-key" });
      await agents.steer({ id: "actor:peer", message: "unchanged", idempotencyKey: "steer-key" });
    } return "typed";`;
    expect(typeCheckFabricCode(code, guestTypeDeclarations(fullCodeMode), true).errors).toEqual([]);
    for (const name of ["followUp", "steer"]) {
      const schema = AGENTS_ACTION_DESCRIPTORS.find(d => d.name === name)!.inputSchema as { properties: Record<string, unknown> };
      expect(schema.properties.idempotencyKey).toMatchObject({ type: "string", minLength: 1, maxLength: 200 });
      expect(typeCheckFabricCode(`await agents.${name}({ id: "peer", message: "unchanged", idempotencyKey: 7 });`, guestTypeDeclarations(fullCodeMode), true).errors)
        .toEqual([expect.objectContaining({ message: expect.stringContaining("not assignable to type 'string'") })]);
    }
  });
  it.each([false, true])("types explicit task-auto pins but removes quality reporting (fullCodeMode=%s)", fullCodeMode => {
    const code = `const run = await agents.run({ task: "exact checks", model: "auto", routeClass: "task:exact-checks", protected: false, pinModel: "test/sol", pinThinking: "max" }); return run.id;`;
    expect(typeCheckFabricCode(code, guestTypeDeclarations(fullCodeMode), true).errors).toEqual([]);
    for (const method of ["routeOutcome", "reportRouteQuality"]) {
      const invalid = `return await agents.${method}({ id: "run-1", routeQuality: "fail" });`;
      expect(typeCheckFabricCode(invalid, guestTypeDeclarations(fullCodeMode), true).errors.map(error => error.message))
        .toEqual([expect.stringContaining(`Property '${method}' does not exist`)]);
    }
  });
  it("does not register or bridge the removed quality API", async () => {
    expect(IMPLEMENTED).not.toContain("routeOutcome");
    expect(GUEST_SETUP).not.toContain('agents.routeOutcome');
    const calls: string[] = [];
    const result = await new QuickJsRuntime().execute(`return typeof agents.routeOutcome;`, async ref => {
      calls.push(ref); return null;
    }, { timeoutMs: 5000, memoryLimitBytes: 32 * 1024 * 1024 });
    expect(result.terminationReason).toBe("completed");
    expect(calls).toEqual([]);
  });
  it.each([false, true])("types per-filter telemetry and expiry (fullCodeMode=%s)", fullCodeMode => {
    const code = `await agents.setActivationFilter({ id: "reviewer", activationFilter: ["hold"], expiresAt: Date.now() + 60000 });
      const actor = await agents.actorStatus({ id: "reviewer" });
      const count: number = actor.filterSkipped.count;
      const key: string | null = actor.filterSkipped.lastKey;
      const topic: string | null = actor.filterSkipped.lastTopic;
      const at: number | null = actor.filterSkipped.lastAt;
      const expiry: number | undefined = actor.activationFilterExpiresAt;
      return { count, key, topic, at, expiry };`;
    expect(typeCheckFabricCode(code, guestTypeDeclarations(fullCodeMode), true).errors).toEqual([]);
    const descriptor = AGENTS_ACTION_DESCRIPTORS.find(d => d.name === "setActivationFilter")!;
    expect(descriptor.inputSchema.properties).toHaveProperty("expiresAt");
  });
  it.each(["spawn", "run", "wait", "join"])("types the optional observed Fabric release on agents.%s", method => {
    const args = method === "spawn" || method === "run" ? '{ task: "work" }' : '{ id: "child" }';
    for (const fullCodeMode of [false, true]) {
      const code = `const result = await agents.${method}(${args});
        const release: string | undefined = result.fabricRelease;
        const omitted: Pick<typeof result, "fabricRelease"> = {};
        return { release, omitted };`;
      expect(typeCheckFabricCode(code, guestTypeDeclarations(fullCodeMode), true).errors).toEqual([]);
    }
  });

  it.each([false, true])("types Main before/after readback without regressing literal actor targets (fullCodeMode=%s)", fullCodeMode => {
    const code = `const main = await agents.setThinking({ id: "session:root", thinking: "high" });
      const actor = await agents.setModel({ id: "actor", model: "probe/b" });
      const caller: string = main.caller;
      const previous: string | undefined = main.previous.model;
      const scope: "session" | "project" = actor.scope;
      const dynamic = await agents.setThinking({ id: (await agents.main()).id, thinking: "high" });
      if ("previous" in dynamic) return dynamic.previous.thinking;
      return { caller, previous, scope };`;
    const declarations = guestTypeDeclarations(fullCodeMode);
    expect(typeCheckFabricCode(code, declarations, true).errors).toEqual([]);
    const refused = `const model = await agents.setModel({ id: "session:root", model: "probe/b" }); return model.previous;`;
    expect(typeCheckFabricCode(refused, declarations, true).errors.map(error => error.message))
      .toEqual([expect.stringContaining("does not exist on type 'never'")]);
  });

  it.each([false, true])("#3819 types inline XOR verified file instructions (fullCodeMode=%s)", fullCodeMode => {
    const declarations = guestTypeDeclarations(fullCodeMode);
    for (const name of ["create", "createActor", "setInstructions"]) {
      const target = name === "setInstructions" ? 'id: "actor"' : 'name: "actor"';
      expect(typeCheckFabricCode(`await agents.${name}({ ${target}, instructionsFile: "/factory/role.md", sha256: "${"a".repeat(64)}" });`, declarations, true).errors).toEqual([]);
      expect(typeCheckFabricCode(`await agents.${name}({ ${target}, instructions: "inline" });`, declarations, true).errors).toEqual([]);
      for (const fields of ['instructionsFile: "/factory/role.md"', 'sha256: "digest"', 'instructions: "inline", instructionsFile: "/factory/role.md", sha256: "digest"']) {
        expect(typeCheckFabricCode(`await agents.${name}({ ${target}, ${fields} });`, declarations, true).errors.length).toBeGreaterThan(0);
      }
      const schema = AGENTS_ACTION_DESCRIPTORS.find(action => action.name === name)!.inputSchema as { properties: Record<string, unknown> };
      expect(schema.properties.instructionsFile).toMatchObject({ type: "string" });
      expect(schema.properties.sha256).toMatchObject({ type: "string", pattern: "^[a-f0-9]{64}$" });
    }
  });

  it.each([false, true])("generated reset guidance never requires destructive stop (fullCodeMode=%s)", fullCodeMode => {
    const declarations = guestTypeDeclarations(fullCodeMode);
    const guidance = declarations.slice(declarations.lastIndexOf("/**", declarations.indexOf("resetSession(args:")), declarations.indexOf("resetSession(args:"));
    expect(guidance).not.toMatch(/stop[ -]first|idle boundary/);
    expect(guidance).toMatch(/owning Main.*directly/);
    expect(guidance).toMatch(/activation.*settles.*fenced boundary/);
    expect(guidance).toMatch(/stop.*cancels work/);
  });
  it.each([false, true])("types and advertises modelReason on launch calls (fullCodeMode=%s)", fullCodeMode => {
    const result = typeCheckFabricCode(
      `const run = await agents.run({ task: "probe", model: "cliproxyapi/gpt-6-astra", modelReason: "Compatibility probe" });
       await agents.spawn({ task: "probe", modelReason: "Compatibility probe" });
       await agents.create({ name: "probe", instructions: "Work.", modelReason: "Compatibility probe" });
       await agents.createActor({ name: "alias-probe", instructions: "Work.", modelReason: "Compatibility probe" });
       const actor = await agents.setModel({ id: "actor", model: "cliproxyapi/gpt-6-astra", modelReason: "Named probe" });
       const actorReason: string | undefined = actor.modelReason;
       const reason: string | undefined = run.modelReason; return { reason, actorReason };`,
      guestTypeDeclarations(fullCodeMode), true,
    );
    expect(result.errors).toEqual([]);
    for (const name of ["run", "spawn", "create", "createActor", "setModel"]) {
      const schema = AGENTS_ACTION_DESCRIPTORS.find(descriptor => descriptor.name === name)!.inputSchema as { properties: Record<string, unknown> };
      expect(schema.properties.modelReason).toMatchObject({ type: "string", maxLength: 200 });
    }
  });

  it.each([false, true])("types model selection provenance without conflating effective models (fullCodeMode=%s)", fullCodeMode => {
    const declarations = guestTypeDeclarations(fullCodeMode);
    const code = `const run = await agents.run({ task: "work", model: "sol" });
      const actor = await agents.setModel({ id: "actor", model: "sol", scope: "project" });
      const imported = await agents.import({ id: "template" });
      const markers: Array<string | undefined> = [run.via, actor.via, imported.via];
      const selections: Array<string | undefined> = [run.selectedModel, actor.selectedModel, imported.selectedModel];
      return { markers, selections, effective: actor.model, observed: run.model };`;
    expect(typeCheckFabricCode(code, declarations, true).errors).toEqual([]);
    expect(typeCheckFabricCode(`${code}\nconst invalid: boolean = run.selectedModel;`, declarations, true).errors.map(error => error.message))
      .toEqual([expect.stringContaining("not assignable to type 'boolean'")]);
  });

  it.each([false, true])("types activation-blocked diagnostics on actor status (fullCodeMode=%s)", fullCodeMode => {
    const declarations = guestTypeDeclarations(fullCodeMode);
    const code = `const status = await agents.actorStatus({ id: "actor" });
      const code: string | undefined = status.activationBlocked?.code;
      const reason: string | undefined = status.activationBlocked?.reason;
      const since: number | undefined = status.activationBlocked?.since;
      const count: number | undefined = status.activationBlocked?.count;
      const omitted: Pick<FabricActorInfo, "activationBlocked"> = {};
      return { code, reason, since, count, omitted };`;
    expect(typeCheckFabricCode(code, declarations, true).errors).toEqual([]);
    expect(typeCheckFabricCode(`${code}\nconst invalid: boolean = status.activationBlocked?.code;`, declarations, true).errors.map(error => error.message))
      .toEqual([expect.stringContaining("not assignable to type 'boolean'")]);
  });

  it.each([false, true])("#3307 types preparation states and diagnostics (fullCodeMode=%s)", fullCodeMode => {
    const result = typeCheckFabricCode(
      `const actor = await agents.actorStatus({ id: "actor" });
       const preparing: FabricActorInfo["status"] = "preparing";
       const waiting: FabricActorInfo["status"] = "waiting";
       if (actor.status === preparing || actor.status === waiting) {
         const phase: string | undefined = actor.preparing?.phase;
         const startedAt: number | undefined = actor.preparing?.startedAt;
         const ageS: number | undefined = actor.preparing?.ageS;
         const attempts: number | undefined = actor.preparing?.attempts;
         const runId: string | undefined = actor.preparing?.runId;
         const queuePosition: number | undefined = actor.preparing?.queuePosition;
         return { phase, startedAt, ageS, attempts, runId, queuePosition };
       }
       return (await agents.actors())[0]?.preparing?.phase;`,
      guestTypeDeclarations(fullCodeMode), true,
    );
    expect(result.errors).toEqual([]);
  });
  it.each([false, true])("types retry keys on public durable create/spawn (fullCodeMode=%s)", fullCodeMode => {
    const result = typeCheckFabricCode(
      `await agents.create({ name: "actor", instructions: "work", residency: "durable", idempotencyKey: "actor-retry" });
       return await agents.spawn({ task: "work", residency: "durable", idempotencyKey: "spawn-retry" });`,
      guestTypeDeclarations(fullCodeMode), true,
    );
    expect(result.errors).toEqual([]);
    for (const name of ["create", "spawn"]) {
      const schema = AGENTS_ACTION_DESCRIPTORS.find(descriptor => descriptor.name === name)!.inputSchema as { properties: Record<string, unknown> };
      expect(schema.properties.idempotencyKey).toMatchObject({ type: "string", minLength: 1, maxLength: 256 });
    }
  });

  it("followUp advisory types the bounded warning on public message receipts", () => {
    const result = typeCheckFabricCode(
      `const receipt = await agents.followUp({ id: "task", message: "later" });
       const code: "FABRIC_FOLLOW_UP_RUNNING_TASK" | undefined = receipt.warning?.code;
       const kind: "agent" | undefined = receipt.warning?.kind;
       const status: "running" | undefined = receipt.warning?.status;
       const targetId: string | undefined = receipt.warning?.targetId;
       const message: string | undefined = receipt.warning?.message;
       const steerWarning = (await agents.steer({ id: "task", message: "now" })).warning;
       return { code, kind, status, targetId, message, steerWarning };`,
      GUEST_TYPE_DECLARATIONS, true,
    );
    expect(result.errors).toEqual([]);
  });

  it.each(["steer", "followUp", "tell"])("types public %s wake receipts for object and positional Main targets", action => {
    const code = `const object = await agents.${action}({ id: "main", message: "resume" });
      const positional = await agents.${action}("main", "resume");
      const triggered: Array<boolean | undefined> = [object.triggered, positional.triggered];
      const reasons: Array<string | undefined> = [object.reason, positional.reason];
      return { triggered, reasons };`;
    expect(typeCheckFabricCode(code, GUEST_TYPE_DECLARATIONS, true).errors).toEqual([]);
  });

  it("types the FIFO position on a queued spawn receipt", () => {
    const result = typeCheckFabricCode(
      `const handle = await agents.spawn({ task: "work" });
       const position: number | undefined = handle.queuePosition;
       return position;`,
      GUEST_TYPE_DECLARATIONS,
      true,
    );
    expect(result.errors).toEqual([]);
  });
  it.each(["run", "wait", "join", "spawn"] as const)("types the optional terminal-compaction diagnostic on agents.%s", (method) => {
    const args = method === "run" || method === "spawn" ? '{ task: "work" }' : '{ id: "child" }';
    for (const fullCodeMode of [false, true]) {
      const declarations = guestTypeDeclarations(fullCodeMode);
      const code = `const result = await agents.${method}(${args});
        const skipped: string | undefined = result.compactionSkipped;
        const omitted: Pick<typeof result, "compactionSkipped"> = {};
        return { skipped, omitted, status: result.status };`;
      // Include type-correctness diagnostics: a missing public property is TS2339.
      expect(typeCheckFabricCode(code, declarations, true).errors).toEqual([]);
      const wrongType = `${code}\nconst invalid: boolean = result.compactionSkipped;`;
      expect(typeCheckFabricCode(wrongType, declarations, true).errors.map((error) => error.message))
        .toEqual([expect.stringContaining("not assignable to type 'boolean'")]);
    }
  });

  it.each(["completed", "failed", "stopped", "timed_out"])("preserves %s status with a skipped-compaction diagnostic through public results", async (status) => {
    const snapshot = { status, compactionSkipped: "Terminal run-log compaction skipped; full log retained" };
    const result = await new QuickJsRuntime().execute(
      `const run = await agents.run({ task: "work" });
       const wait = await agents.wait({ id: "child" });
       const join = await agents.join({ id: "child" });
       return { run, wait, join };`,
      async () => snapshot,
      { timeoutMs: 5_000, memoryLimitBytes: 32 * 1024 * 1024 },
    );
    expect(result.terminationReason).toBe("completed");
    expect(result.value).toEqual({ run: snapshot, wait: snapshot, join: snapshot });
  });

  it("binds every implemented action in the TypeScript prelude", () => {
    const agents = slice(GUEST_SETUP, "globalThis.agents = Object.freeze({", "\n});");
    const bound = names(agents, /^ {2}"?([A-Za-z_$][\w$]*)"?:/gm);
    expect(IMPLEMENTED.filter((name) => !bound.has(name))).toEqual([]);
  });

  it("declares every implemented action in the guest types", () => {
    const api = slice(guestTypeDeclarations(true), "interface FabricAgentsApi {", "\n}");
    const declared = names(api, /^ {2}"?([A-Za-z_$][\w$]*)"?\(/gm);
    expect(IMPLEMENTED.filter((name) => !declared.has(name))).toEqual([]);
  });

  it("routes the template actions through the host bridge", async () => {
    const calls: Array<{ ref: string; args: unknown }> = [];
    const result = await new QuickJsRuntime().execute(
      `const imported = await agents.import({ name: "reviewer", as: "auditor" });
       const exported = await agents.export({ id: "actor-1", write: true, overwrite: true });
       const unbound = ["sessions", "compact", "setTools", "setDeliveryPolicy", "clearMessages", "import", "export"]
         .filter((name) => typeof agents[name] !== "function");
       return { imported, exported, unbound };`,
      async (ref, args) => {
        calls.push({ ref, args });
        return { ref };
      },
      { timeoutMs: 5_000, memoryLimitBytes: 32 * 1024 * 1024 },
    );

    expect(result.terminationReason).toBe("completed");
    expect(calls).toEqual([
      { ref: "agents.import", args: { name: "reviewer", as: "auditor" } },
      { ref: "agents.export", args: { id: "actor-1", write: true, overwrite: true } },
    ]);
    expect(result.value).toMatchObject({ unbound: [] });
  });

  it("forwards the actors scope so global templates and project actors stay distinct", async () => {
    const calls: Array<{ ref: string; args: unknown }> = [];
    const result = await new QuickJsRuntime().execute(
      `const project = await agents.actors();
       const explicit = await agents.actors({ scope: "project" });
       const templates = await agents.actors({ scope: "global" });
       return { project, explicit, templates };`,
      async (ref, args) => {
        calls.push({ ref, args });
        return (args as { scope?: string }).scope === "global" ? ["template"] : ["actor"];
      },
      { timeoutMs: 5_000, memoryLimitBytes: 32 * 1024 * 1024 },
    );

    expect(result.terminationReason).toBe("completed");
    expect(calls).toEqual([
      { ref: "agents.actors", args: {} },
      { ref: "agents.actors", args: { scope: "project" } },
      { ref: "agents.actors", args: { scope: "global" } },
    ]);
    expect(result.value).toEqual({ project: ["actor"], explicit: ["actor"], templates: ["template"] });
  });

  it("types a stored template's validWhile as serialized source, not a callable", () => {
    const read = typeCheckFabricCode(
      `const [template] = await agents.actors({ scope: "global" });
       const exported = await agents.export({ id: "actor-1", write: true });
       const sources: string[] = [template.validWhile?.source, exported.validWhile?.source];
       const actors = await agents.actors();
       return { sources, status: actors[0]?.status };`,
      GUEST_TYPE_DECLARATIONS,
    );
    expect(read.errors).toEqual([]);

    for (const call of [
      `const [template] = await agents.actors({ scope: "global" });
       return template.validWhile?.({} as never);`,
      `const exported = await agents.export({ id: "actor-1", write: true });
       return exported.validWhile?.({} as never);`,
    ]) {
      expect(typeCheckFabricCode(call, GUEST_TYPE_DECLARATIONS).errors.map((error) => error.message))
        .toEqual([expect.stringContaining("not callable")]);
    }
  });

  // review/astra F2 on #73: role and project are part of the guest contract, not only the host's.
  it("types role and project on peers, sessions and the project agent", () => {
    const read = typeCheckFabricCode(
      `const lead: Pick<Awaited<ReturnType<typeof agents.projectAgent>>, "id" | "role" | "project"> = await agents.projectAgent();
       const peers: Array<Pick<FabricPeerInfo, "id" | "role" | "project">> = await agents.peers();
       const sessions = await agents.sessions();
       const roles: Array<string | undefined> = sessions.map((session) => session.role);
       return { lead, peers, roles, project: sessions[0]?.project };`,
      GUEST_TYPE_DECLARATIONS,
    );
    expect(read.errors).toEqual([]);
  });

  // review/astra F1 on #125: the owner session is part of the guest contract, not only the host's.
  it("types ownerSessionId on actor status and actor listing", () => {
    const read = typeCheckFabricCode(
      `const id = "actor-1";
       const owner: string | undefined = (await agents.actorStatus({ id })).ownerSessionId;
       const listed: string | undefined = (await agents.actors())[0].ownerSessionId;
       const owners: string[] = [owner ?? "", listed ?? ""];
       return owners;`,
      GUEST_TYPE_DECLARATIONS,
      true, // report type-correctness errors too: a missing property is TS2339
    );
    expect(read.errors).toEqual([]);
  });

  it("types sender notices on steer/followUp and mesh publish receipts", () => {
    const read = typeCheckFabricCode(
      `const steer: string | undefined = (await agents.steer({ id: "main", message: "head abc1234" })).notice;
       const followUp: string | undefined = (await agents.followUp({ id: "main", message: "head abc1234" })).notice;
       const publish: string | undefined = (await mesh.publish({ topic: "team", text: "head abc1234" })).notice;
       return { steer, followUp, publish };`,
      GUEST_TYPE_DECLARATIONS,
      true,
    );
    expect(read.errors).toEqual([]);
  });

  it("keeps the Python kernel's dynamic agents proxy in place", () => {
    expect(CPYTHON_CHILD_SOURCE).toContain('"agents"');
  });
});
