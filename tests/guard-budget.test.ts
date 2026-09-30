import { describe, expect, it, vi } from "vitest";
import { GUARD_BUDGET_REASON, GuardBudget, GuardBudgetExceeded } from "../src/core/guard-budget.js";
import { killsByPattern, scanCommand, wipesTmp } from "../src/core/pattern-kill.js";

// R5 owner scope cut: only these exact formerly-allowed complex commands migrate.
// IDs/commands and the historical 102 false positives are unchanged.
const round5IntentionalState = new Set<string>([
  "eval 'eval '\\''eval '\\''\\'\\'''\\''kill 4242; rm -f .local/own-file'\\''\\'\\'''\\'''\\'''"
]);
function expectRound5Guard(command: string, result: ReturnType<typeof scanCommand>, original: { blocked?: boolean; wipe?: boolean; exhausted?: boolean; overall?: boolean }): void {
  const intentional = round5IntentionalState.has(command);
  const originallyRefused = original.blocked === true || original.wipe === true || original.exhausted === true || original.overall === true;
  if (!intentional && !(originallyRefused && result.shellState === true)) expect("shellState" in result, command).toBe(false);
  if (intentional || (originallyRefused && result.shellState === true)) {
    expect(Object.prototype.hasOwnProperty.call(result, "shellState"), command).toBe(true);
    expect(result, command).toEqual({ blocked: false, wipe: false, exhausted: false, shellState: true });
  } else if (original.overall !== undefined) {
    expect(Object.keys(result).sort(), command).toEqual(["blocked", "exhausted", "wipe"]);
    expect(result.exhausted, command).toBe(original.exhausted ?? false);
    expect(typeof result.blocked, command).toBe("boolean");
    expect(typeof result.wipe, command).toBe("boolean");
    expect(result.blocked || result.wipe, command).toBe(original.overall);
  } else {
    expect(result, command).toEqual(original);
  }
  expect(killsByPattern(command), command).toBe(result.blocked || result.shellState === true);
  expect(wipesTmp(command), command).toBe(result.wipe || result.shellState === true);
}


const REFERENCE = /\$([A-Z]+)/g;
const quote = (text: string): string => `'${text.replaceAll("'", "'\\''")}'`;
const nestedEval = (text: string, depth: number): string => {
  for (let i = 0; i < depth; i++) text = `eval ${quote(text)}`;
  return text;
};

const replayTree = (commands: number): string => {
  let text = `pgrep worker; ${"echo safe; ".repeat(commands)}`;
  // Every level contains a lookup, so collect/replay revisits nested substitutions.
  // Depth three is below the script/reader depth limits; leaf words remain tiny.
  for (let i = 0; i < 3; i++) text = `pgrep worker; echo $(${text})`;
  return text;
};

// These commands are scanner DATA. Never execute them in a shell.
describe("guard structural budget", () => {
  it("explains a resource refusal without accusing an actual kill or tmp deletion", () => {
    expect(GUARD_BUDGET_REASON).toMatch(/structural analysis budget/);
    expect(GUARD_BUDGET_REASON).toMatch(/split it into smaller commands/);
    expect(GUARD_BUDGET_REASON).not.toMatch(/this deletes|kills other|kill by name/);
  });

  it("shares cumulative reservations and never refunds work", () => {
    const budget = new GuardBudget(5);
    budget.spend(2); // collection
    budget.spend(2); // replay
    budget.spend(); // nested work
    expect(() => budget.spend()).toThrow(GuardBudgetExceeded);
    expect(() => budget.spend(0)).toThrow(GuardBudgetExceeded);
  });

  it.each([-1, NaN, Infinity, 0.5, Number.MAX_SAFE_INTEGER + 1])("rejects invalid reservation %s", (units) => {
    expect(() => new GuardBudget(10).spend(units)).toThrow(GuardBudgetExceeded);
  });

  it("rejects an oversized replacement before its renderer can allocate", () => {
    const value = "x".repeat(4096);
    const render = vi.fn((text: string) => text);
    expect(() => new GuardBudget(100).replace("$A$A", REFERENCE, () => value, render)).toThrow(GuardBudgetExceeded);
    expect(render).not.toHaveBeenCalled();
  });

  it("does not reset the budget between successive expansions", () => {
    const budget = new GuardBudget(30);
    expect(budget.replace("$A", REFERENCE, () => "abc")).toBe("abc");
    const render = vi.fn((text: string) => text);
    expect(() => budget.replace("$A", REFERENCE, () => "abc", render)).toThrow(GuardBudgetExceeded);
    expect(render).not.toHaveBeenCalled();
  });

  it("preserves ordinary replacement semantics, masking, and regex state", () => {
    const references = /([$\u0002])\{?([A-Z]+)\}?/g;
    references.lastIndex = 3;
    const values = new Map([["A", "/tmp/*"]]);
    const output = new GuardBudget(1000).replace("$A \u0002{A} $B", references,
      (match) => values.get(match[2]!) ?? match[0],
      (value, match) => match[1] === "\u0002" ? value.replaceAll("*", "\uE000") : value);
    expect(output).toBe("/tmp/* /tmp/\uE000 $B");
    expect(references.lastIndex).toBe(3);
  });

  it("handles shorter, empty, adjacent, and absent replacements", () => {
    expect(new GuardBudget(1000).replace("a$A$Bz", REFERENCE,
      (match) => match[1] === "A" ? "" : "b")).toBe("abz");
    expect(new GuardBudget(1000).replace("literal", REFERENCE, () => "unused")).toBe("literal");
  });

  it("refuses zero-width or unsupported reference expressions", () => {
    expect(() => new GuardBudget(100).replace("x", /(?:)/g, () => "")).toThrow(GuardBudgetExceeded);
    expect(() => new GuardBudget(100).replace("$A", /\$A/, () => "a")).toThrow(GuardBudgetExceeded);
    expect(() => new GuardBudget(100).replace("$A", /\$A/gy, () => "a")).toThrow(GuardBudgetExceeded);
  });

  it("refuses a renderer that violates the length-preserving contract", () => {
    expect(() => new GuardBudget(100).replace("$A", REFERENCE, () => "a", () => "longer"))
      .toThrow(GuardBudgetExceeded);
  });

  it("limits reader recursion without refunding cumulative reservations", () => {
    const budget = new GuardBudget(1000);
    for (let i = 0; i < 32; i++) budget.enterReader();
    expect(() => budget.enterReader()).toThrow(GuardBudgetExceeded);
    for (let i = 0; i < 32; i++) budget.leaveReader();
    expect(() => budget.spend(0)).toThrow(GuardBudgetExceeded);
    const sequential = new GuardBudget(2);
    sequential.enterReader(); sequential.leaveReader();
    sequential.enterReader(); sequential.leaveReader();
    expect(() => sequential.enterReader()).toThrow(GuardBudgetExceeded);
  });
});

describe("real scanner budget integration (N1)", () => {
  it("distinguishes resource exhaustion from either destructive verdict", () => {
    expect(scanCommand(nestedEval("echo safe", 8))).toEqual({ blocked: true, wipe: true, exhausted: true });
    expect(scanCommand("kill 4242; rm -f .local/own-file")).toEqual({ blocked: false, wipe: false, exhausted: false });
    expect(scanCommand("pkill worker")).toEqual({ blocked: true, wipe: false, exhausted: false });
    expect(scanCommand("rm -rf /tmp/tmp.*")).toEqual({ blocked: false, wipe: true, exhausted: false });
  });

  it.each(["pkill worker", "rm -rf /tmp/tmp.*"])("fails both verdicts closed beyond script depth: %s", (payload) => {
    const command = nestedEval(payload, 8);
    expect(killsByPattern(command)).toBe(true);
    expect(wipesTmp(command)).toBe(true);
  });

  it("allows a small recorded-PID and own-path nested script", () => {
    const command = nestedEval("kill 4242; rm -f .local/own-file", 3);
    expectRound5Guard(command, scanCommand(command), { blocked: false, wipe: false, exhausted: false });
    expectRound5Guard(command, scanCommand(command), { wipe: false, blocked: false, exhausted: false });
  });

  it("fails both verdicts closed on aggregate expansion before the stored-value cap", () => {
    const command = `A=${"x".repeat(4096)}; B=${"$A".repeat(1024)}; :`;
    expect(killsByPattern(command)).toBe(true);
    expect(wipesTmp(command)).toBe(true);
  });

  it("fails both verdicts closed on cumulative nested collect/replay work", () => {
    const command = replayTree(3000);
    expect(killsByPattern(command)).toBe(true);
    expect(wipesTmp(command)).toBe(true);
  });

  it("allows the same nested collect/replay structure with ordinary work", () => {
    const command = replayTree(8);
    expect(killsByPattern(command)).toBe(false);
    expect(wipesTmp(command)).toBe(false);
  });

  it("allows ordinary stored-value and recorded-source expansion", () => {
    const command = "A=.local; B=$A/own-file; rm -f \"$B\"; kill $(cat .local/server.pid)";
    expect(killsByPattern(command)).toBe(false);
    expect(wipesTmp(command)).toBe(false);
  });
});
