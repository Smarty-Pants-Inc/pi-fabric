import { afterEach, describe, expect, it, vi } from "vitest";
import { setTimeout as delay } from "node:timers/promises";
import { callProgram, jevContext, launch, setupJev } from "./jev-test-helpers.js";
import type { JevProvider } from "../src/providers/jev-provider.js";

// Deterministic offline regression for the two realtime decision shapes this
// repository supports end to end:
//   * operation + speculative target heads (one request, one validated head);
//   * factorized control axes answered together, executed as a timed pulse.
// Neither depend on a live model or a real browser: the transport is a fixture
// and the observation source is an ordinary Fabric provider.

const providers: JevProvider[] = [];
afterEach(async () => { await Promise.all(providers.splice(0).map(provider => provider.close())); });
const setup = (config: Record<string, unknown> = {}, fetcher?: typeof fetch) => {
  const session = setupJev(config, fetcher);
  providers.push(session.provider);
  return session;
};

const RUN_LIMITS = { timeoutMs: 120000, maxEvaluations: 500, maxToolCalls: 50000, maxTokens: 200000 };

interface Call { bytes: number; questions: string[]; state: Record<string, unknown> }

// ---------------------------------------------------------------- arena shape

const GRID = [".........", ".##.##...", ".........", "..#..#...", ".........", "........."];
const wall = (x: number, y: number) => y < 0 || y >= GRID.length || x < 0 || x >= GRID[0]!.length || GRID[y]![x] === "#";

function makeArena() {
  const player = { x: 0, y: 0 };
  const exit = { x: 8, y: 5 };
  const enemy = { x: 4, y: 0 };
  let alive = true, hp = 3, revision = 1, acts = 0, observations = 0, rejected = false;
  const lineOfSight = () => {
    if (!alive) return false;
    const [from, to] = enemy.y === player.y ? [enemy.x, player.x] : [enemy.y, player.y];
    if (enemy.y !== player.y && enemy.x !== player.x) return false;
    for (let step = Math.min(from, to) + 1; step < Math.max(from, to); step++)
      if (wall(enemy.y === player.y ? step : enemy.x, enemy.y === player.y ? enemy.y : step)) return false;
    return Math.abs(from - to) <= 4;
  };
  const candidates = () => {
    const offered: Array<{ id: string; op: "move" | "fire"; dx: number; dy: number; label: string }> = [];
    for (const [dx, dy, id, label] of [[1, 0, "right", "Step right"], [-1, 0, "left", "Step left"], [0, 1, "down", "Step down"], [0, -1, "up", "Step up"]] as const)
      if (!wall(player.x + dx, player.y + dy)) offered.push({ id, op: "move", dx, dy, label });
    offered.push({ id: "fire", op: "fire", dx: 0, dy: 0, label: lineOfSight() ? "Fire at the aligned enemy" : "Fire into empty space" });
    return offered;
  };
  const observe = () => {
    observations++;
    return { revision, tick: acts, hp, pos: { ...player }, exit: { ...exit }, enemyAligned: lineOfSight(),
      exitReached: player.x === exit.x && player.y === exit.y, grid: GRID.join("\n"), candidates: candidates() };
  };
  const act = (id: string, rev: number) => {
    if (rev !== revision) throw new Error("stale revision: the observed frame changed");
    // One deterministic concurrent-mutation rejection, then the session must recover.
    if (id === "right" && acts === 3 && !rejected) { rejected = true; revision++; throw new Error("stale revision: the observed frame changed"); }
    const candidate = candidates().find(entry => entry.id === id);
    if (!candidate) throw new Error("candidate is not offered by the current observation");
    if (candidate.op === "fire") { if (lineOfSight()) alive = false; }
    else { player.x += candidate.dx; player.y += candidate.dy; }
    if (alive) {
      const stepX = Math.sign(player.x - enemy.x), stepY = Math.sign(player.y - enemy.y);
      if (stepX && !wall(enemy.x + stepX, enemy.y)) enemy.x += stepX;
      else if (stepY && !wall(enemy.x, enemy.y + stepY)) enemy.y += stepY;
      if (Math.abs(enemy.x - player.x) + Math.abs(enemy.y - player.y) <= 1) hp -= 1;
    }
    acts++; revision++;
    return observe();
  };
  return { observe, act, stats: () => ({ observations }) };
}

const ARENA_DESCRIPTORS = [
  { name: "observe", description: "Observe a simulated arena frame", inputSchema: { type: "object", properties: {}, additionalProperties: false }, risk: "read" as const },
  { name: "act", description: "Apply a simulated arena action", inputSchema: { type: "object", properties: { id: { type: "string" }, revision: { type: "integer" } }, required: ["id", "revision"], additionalProperties: false }, risk: "execute" as const },
];
function registerArena(registry: { register(provider: unknown): unknown }) {
  const arena = makeArena();
  registry.register({ name: "arena", description: "Deterministic arena fixture", list: async () => ARENA_DESCRIPTORS,
    describe: async (name: string) => ARENA_DESCRIPTORS.find(descriptor => descriptor.name === name),
    invoke: async (name: string, args: Record<string, unknown>) =>
      name === "observe" ? arena.observe() : arena.act(args.id as string, args.revision as number) });
  return arena;
}

function arenaFetcher(calls: Call[]) {
  return (async (_url: string, init: { body: string }) => {
    const body = JSON.parse(init.body);
    const frame = body.state.frame, questions = body.questions;
    calls.push({ bytes: init.body.length, questions: Object.keys(questions), state: body.state });
    const ids = (name: string) => Object.keys(questions[name].criteria) as string[];
    const answer = (name: string, choice: string) => ({ type: "choice", choice, confidence: 1,
      probabilities: Object.fromEntries(ids(name).map(id => [id, id === choice ? 1 : 0])) });
    let operation = frame.exitReached ? "done" : frame.enemyAligned ? "fire" : "move";
    if (!ids("operation").includes(operation)) operation = "blocked";
    const moves = ids("move_target");
    const ranked = (frame.candidates as Array<{ id: string; op: string; dx: number; dy: number }>)
      .filter(candidate => candidate.op === "move")
      .sort((a, b) => (Math.abs(frame.exit.x - frame.pos.x - a.dx) + Math.abs(frame.exit.y - frame.pos.y - a.dy))
        - (Math.abs(frame.exit.x - frame.pos.x - b.dx) + Math.abs(frame.exit.y - frame.pos.y - b.dy)));
    const move = ranked[0] && moves.includes(ranked[0].id) ? ranked[0].id : moves[0]!;
    return new Response(JSON.stringify({ model: "jev-latest", usage: { input_tokens: 150, output_tokens: 12 }, answers: {
      operation: answer("operation", operation), move_target: answer("move_target", move),
      fire_target: answer("fire_target", ids("fire_target")[0]!),
    } }), { status: 200 });
  }) as unknown as typeof fetch;
}

const ARENA_LOOP = String.raw`
interface ArenaFrame { revision:number; tick:number; hp:number; pos:{x:number;y:number}; exit:{x:number;y:number}; enemyAligned:boolean; exitReached:boolean; grid:string; candidates:Array<{id:string;op:"move"|"fire";dx:number;dy:number;label:string}> }
let executed = 0, staleRetries = 0, observations = 0, done = false;
for (let tick = 0; tick < input.ticks && !done; tick++) {
  let acted = false;
  for (let attempt = 0; attempt < 2 && !acted; attempt++) {
    const frame = await tools.call({ ref: "arena.observe" }) as ArenaFrame;
    observations++;
    if (frame.hp <= 0) throw new Error("the player died at tick " + tick);
    if (frame.exitReached) { done = true; acted = true; break; }
    const moveCriteria: Record<string, string | null> = {};
    const fireCriteria: Record<string, string | null> = {};
    for (const candidate of frame.candidates) (candidate.op === "move" ? moveCriteria : fireCriteria)[candidate.id] = candidate.label;
    const operationCriteria: Record<string, string | null> = { done: "The exit cell has been reached", blocked: "No offered action can make progress" };
    for (const candidate of frame.candidates) operationCriteria[candidate.op === "move" ? "move" : "fire"] = candidate.op === "move" ? "Walk to the exit" : "Fire at the aligned enemy";
    const decision = await jev.evaluate({
      state: { goal: input.goal, frame: { tick: frame.tick, hp: frame.hp, enemyAligned: frame.enemyAligned, pos: frame.pos, exit: frame.exit, grid: frame.grid, candidates: frame.candidates } },
      questions: {
        operation: { type: "choice", instructions: "Choose the arena operation that advances goal: move, fire, done, or blocked.", criteria: operationCriteria },
        move_target: { type: "choice", instructions: "If the operation is move, choose the offered destination closest to the exit.", criteria: moveCriteria },
        fire_target: { type: "choice", instructions: "If the operation is fire, choose the offered fire target.", criteria: fireCriteria },
      },
    });
    const operation = decision.answers.operation.choice;
    if (operation === "done") { done = true; acted = true; break; }
    if (operation === "blocked") throw new Error("no legal action at tick " + tick);
    const target = operation === "move" ? decision.answers.move_target.choice : decision.answers.fire_target.choice;
    try { await tools.call({ ref: "arena.act", args: { id: target, revision: frame.revision } }); executed++; acted = true; }
    catch (error) { staleRetries++; await program.emit({ stale: true, tick, message: String(error) }); }
  }
  await program.sleep(1);
}
return { executed, staleRetries, observations, done };
`;

// --------------------------------------------------------------- engine shape

const AXES = {
  movement: ["HOLD_POSITION", "EXPLORE_WORLD", "MOVE_TO_ENEMY", "RETREAT_FROM_ENEMY", "COLLECT_NEAREST_PICKUP", "MOVE_TO_USE"],
  view: ["KEEP_HEADING", "SCAN", "FACE_ENEMY"],
  trigger: ["HOLD_FIRE", "FIRE"],
  interaction: ["NO_USE", "USE"],
} as const;

function makeEngine() {
  let tick = 0, hp = 10, ammo = 8, epoch = 0;
  const pulses: Array<{ mask: string; epoch: number }> = [];
  const released: number[] = [];
  const observe = () => ({ tick, hp, ammo, enemies: hp > 0 ? [{ id: "imp_1", distance: hp <= 6 ? 1 : 3, aligned: hp % 2 === 0 }] : [],
    pickups: ["medkit_1"], motion: { stuck: tick % 7 === 3 }, exploration: { visited_cells: tick, novelty: 1 / (tick + 1) } });
  const control = (mask: string, nextEpoch: number) => {
    epoch = nextEpoch; pulses.push({ mask, epoch });
    if (mask === "FIRE" && ammo > 0) ammo--;
    hp = Math.max(0, hp - 1);
    tick++;
    return { applied: true, epoch };
  };
  const release = (requested: number) => { released.push(requested); return { released: requested === epoch }; };
  return { observe, control, release, stats: () => ({ pulses, released }) };
}

const ENGINE_DESCRIPTORS = [
  { name: "observe", description: "Read structured engine state", inputSchema: { type: "object", properties: {}, additionalProperties: false }, risk: "read" as const },
  { name: "control", description: "Apply a timed motor pulse", inputSchema: { type: "object", properties: { mask: { type: "string" }, epoch: { type: "integer" } }, required: ["mask", "epoch"], additionalProperties: false }, risk: "execute" as const },
  { name: "release", description: "Release a motor pulse by epoch", inputSchema: { type: "object", properties: { epoch: { type: "integer" } }, required: ["epoch"], additionalProperties: false }, risk: "execute" as const },
];
function registerEngine(registry: { register(provider: unknown): unknown }) {
  const engine = makeEngine();
  registry.register({ name: "doom", description: "Deterministic engine fixture", list: async () => ENGINE_DESCRIPTORS,
    describe: async (name: string) => ENGINE_DESCRIPTORS.find(descriptor => descriptor.name === name),
    invoke: async (name: string, args: Record<string, unknown>) => name === "observe" ? engine.observe()
      : name === "control" ? engine.control(args.mask as string, args.epoch as number) : engine.release(args.epoch as number) });
  return engine;
}

function engineFetcher(calls: Call[]) {
  return (async (_url: string, init: { body: string }) => {
    const body = JSON.parse(init.body);
    calls.push({ bytes: init.body.length, questions: Object.keys(body.questions), state: body.state });
    const answer = (choice: string, keys: readonly string[], confidence = 0.9) => ({ type: "choice", choice, confidence,
      probabilities: Object.fromEntries(keys.map(key => [key, key === choice ? confidence : (1 - confidence) / (keys.length - 1)])) });
    const aligned = (body.state?.visible_enemies ?? []).some((enemy: { aligned: boolean }) => enemy.aligned);
    const answers: Record<string, unknown> = {
      movement: answer(aligned ? "HOLD_POSITION" : "EXPLORE_WORLD", AXES.movement),
      view: answer(aligned ? "FACE_ENEMY" : "SCAN", AXES.view),
      trigger: answer(aligned && body.state.ammo > 0 ? "FIRE" : "HOLD_FIRE", AXES.trigger, calls.length === 6 ? 0.1 : 0.9),
      interaction: answer("NO_USE", AXES.interaction),
    };
    // First degraded decision: a malformed typed response. Second: a low-confidence answer.
    if (calls.length === 5) delete answers.view;
    return new Response(JSON.stringify({ model: "jev-latest", usage: { input_tokens: 300, output_tokens: 20 }, answers }), { status: 200 });
  }) as unknown as typeof fetch;
}

const ENGINE_LOOP = `
interface EngineState { tick:number; hp:number; ammo:number; enemies:Array<{id:string;distance:number;aligned:boolean}>; pickups:string[]; motion:{stuck:boolean}; exploration:{visited_cells:number;novelty:number} }
const CONTROLS = ["EXPLORE_WORLD","MOVE_TO_ENEMY","RETREAT_FROM_ENEMY","FACE_ENEMY","FIRE","COLLECT_NEAREST_PICKUP","USE_NEAREST_LINE","IDLE"];
const movementCriteria: Record<string,string> = { HOLD_POSITION:"Do not translate.", EXPLORE_WORLD:"Navigate toward under-visited space.", MOVE_TO_ENEMY:"Path toward the nearest living enemy.", RETREAT_FROM_ENEMY:"Create distance.", COLLECT_NEAREST_PICKUP:"Path toward the nearest pickup.", MOVE_TO_USE:"Path toward a usable line." };
const viewCriteria: Record<string,string> = { KEEP_HEADING:"Keep the current heading.", SCAN:"Turn to inspect the environment.", FACE_ENEMY:"Center the nearest visible enemy." };
const triggerCriteria: Record<string,string> = { HOLD_FIRE:"Do not fire.", FIRE:"Press the weapon trigger." };
const interactionCriteria: Record<string,string> = { NO_USE:"Do not activate anything.", USE:"Activate a nearby door or switch." };
const hold = { movement: "HOLD_POSITION", view: "KEEP_HEADING", trigger: "HOLD_FIRE", interaction: "NO_USE" };
let decisions = 0, degradedCount = 0, dead = false, previous = "EXPLORE_WORLD";
while (true) {
  const engine = await tools.call({ ref: "doom.observe" }) as EngineState;
  if (engine.hp <= 0) { dead = true; break; }
  const state = { game_context: { objective: "Survive, defeat hostile monsters, and explore the level.", combat: "Movement, view, trigger, and interaction are independent.", decision_model: "Choose a composable control frame each interval." },
    player: { health: engine.hp, ammo: engine.ammo }, visible_enemies: engine.enemies, visible_pickups: engine.pickups,
    exploration: engine.exploration, history: { previous_action: previous, stuck_probability: engine.motion.stuck ? 1 : 0 }, controls: CONTROLS };
  let frame = { ...hold }, confidence = 0, degraded = false;
  try {
    const decision = await jev.evaluate({ state, questions: {
      movement: { type: "choice", instructions: { task: "Choose navigation for this tick." }, criteria: movementCriteria },
      view: { type: "choice", instructions: { task: "Choose where to look." }, criteria: viewCriteria },
      trigger: { type: "choice", instructions: { task: "Choose whether to fire.", constraints: ["Fire only when a living enemy is visible, aligned, and in range."] }, criteria: triggerCriteria },
      interaction: { type: "choice", instructions: { task: "Choose whether to use a nearby line." }, criteria: interactionCriteria },
    } });
    confidence = Math.min(decision.answers.movement.confidence, decision.answers.view.confidence, decision.answers.trigger.confidence, decision.answers.interaction.confidence);
    frame = { movement: decision.answers.movement.choice, view: decision.answers.view.choice, trigger: decision.answers.trigger.choice, interaction: decision.answers.interaction.choice };
  } catch (error) {
    degraded = true;
    await program.emit({ fallback: "error", tick: engine.tick, message: String(error) });
  }
  if (confidence < input.minConfidence) {
    if (!degraded) await program.emit({ fallback: "low_confidence", tick: engine.tick, confidence });
    degraded = true;
    frame = { ...hold };
  }
  if (degraded) degradedCount++;
  const motor = frame.trigger === "FIRE" ? "FIRE" : frame.movement === "MOVE_TO_ENEMY" ? "APPROACH" : frame.movement === "HOLD_POSITION" ? "HOLD" : "EXPLORE";
  const pulse = await tools.call({ ref: "doom.control", args: { mask: motor, epoch: engine.tick } }) as { epoch: number };
  previous = frame.movement;
  decisions++;
  await program.sleep(input.pulseMs);
  await tools.call({ ref: "doom.release", args: { epoch: pulse.epoch } });
}
return { decisions, degraded: degradedCount, dead };
`;

describe("realtime decision shapes", () => {
  it("validates only the target head for the chosen operation and recovers from a stale frame", async () => {
    const calls: Call[] = [];
    const { provider, registry } = setup({ jev: { maxEvaluations: 1000, maxToolCalls: 100000, maxDurationMs: 600000 } }, arenaFetcher(calls));
    registerArena(registry);
    const run = await callProgram(provider, "run", launch(ARENA_LOOP, {
      requires: ["jev.evaluate", "arena.observe", "arena.act"], limits: RUN_LIMITS,
      inputSchema: { type: "object", properties: { ticks: { type: "integer" }, goal: { type: "string" } }, required: ["ticks", "goal"], additionalProperties: false },
      outputSchema: { type: "object", properties: { executed: { type: "integer" }, staleRetries: { type: "integer" }, observations: { type: "integer" }, done: { type: "boolean" } }, required: ["executed", "staleRetries", "observations", "done"], additionalProperties: false },
    }, { ticks: 40, goal: "Reach the arena exit, firing only when the enemy is aligned" }));
    expect(run.state, run.error).toBe("completed");
    const result = run.result as { executed: number; staleRetries: number; observations: number; done: boolean };
    expect(result).toMatchObject({ done: true, staleRetries: 1 });
    expect(result.executed).toBeGreaterThanOrEqual(14);
    expect(result.observations).toBe(result.executed + result.staleRetries + 1);
    // One System One request per tick, operation plus both speculative heads, even though only one head can execute.
    expect(calls.length).toBe(run.evaluations);
    expect(calls.every(call => call.questions.join(",") === "operation,move_target,fire_target")).toBe(true);
    expect(run.events.some(event => (event.value as { stale?: boolean }).stale === true)).toBe(true);
  }, 30000);

  it("runs four control axes as one batched request, pulses the motor with an epoch guard, and stops on death", async () => {
    const calls: Call[] = [];
    const { provider, registry } = setup({ jev: { maxEvaluations: 1000, maxToolCalls: 100000, maxDurationMs: 600000 } }, engineFetcher(calls));
    const engine = registerEngine(registry);
    const run = await callProgram(provider, "run", launch(ENGINE_LOOP, {
      requires: ["jev.evaluate", "doom.observe", "doom.control", "doom.release"], limits: RUN_LIMITS,
      inputSchema: { type: "object", properties: { pulseMs: { type: "integer" }, minConfidence: { type: "number" } }, required: ["pulseMs", "minConfidence"], additionalProperties: false },
      outputSchema: { type: "object", properties: { decisions: { type: "integer" }, degraded: { type: "integer" }, dead: { type: "boolean" } }, required: ["decisions", "degraded", "dead"], additionalProperties: false },
    }, { pulseMs: 40, minConfidence: 0.5 }));
    expect(run.state, run.error).toBe("completed");
    const result = run.result as { decisions: number; degraded: number; dead: boolean };
    expect(result).toMatchObject({ decisions: 10, degraded: 2, dead: true });
    expect(calls.length).toBe(result.decisions);
    expect(calls.every(call => call.questions.join(",") === "movement,view,trigger,interaction")).toBe(true);
    expect(calls[0]!.state).toMatchObject({ controls: expect.any(Array), game_context: expect.any(Object) });
    expect((calls[0]!.state.controls as unknown[]).length).toBe(8);
    // A timed pulse is superseded by epoch, never by a later release of an older pulse.
    expect(engine.stats().pulses.map(pulse => pulse.epoch)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    expect(engine.stats().released).toHaveLength(10);
    const degraded = run.events.map(event => (event.value as { fallback?: string }).fallback).filter(Boolean);
    expect(degraded).toEqual(["error", "low_confidence"]);
  }, 30000);

  it("keeps a paced loop inspectable, drainable, and stoppable", async () => {
    const calls: Call[] = [];
    const { provider, registry } = setup({ jev: { maxEvaluations: 100000, maxToolCalls: 100000, maxDurationMs: 600000 } }, arenaFetcher(calls));
    registerArena(registry);
    const run = await callProgram(provider, "spawn", launch(
      "while (true) { await tools.call({ ref: \"arena.observe\" }); await program.emit({ tick: 1 }); await program.sleep(20); }",
      { requires: ["arena.observe"], limits: { timeoutMs: 60000, maxToolCalls: 100000 } }), jevContext());
    expect(run.state).toBe("running");
    const first = provider.manager.status(run.id, 0);
    await vi.waitFor(() => expect(provider.manager.status(run.id, 0).nextSequence).toBeGreaterThan(first.nextSequence), { timeout: 5000, interval: 20 });
    const drained = provider.manager.status(run.id, first.nextSequence - 1);
    expect(drained.state).toBe("running");
    expect(drained.events.length).toBeGreaterThan(0);
    expect(drained.events.every(event => event.sequence >= first.nextSequence)).toBe(true);
    expect((await provider.manager.stop(run.id)).state).toBe("cancelled");
    expect((await provider.manager.stop(run.id)).state).toBe("cancelled");
  }, 30000);
});
