import { spawnSync } from "node:child_process";
import fs from "node:fs";
import { expect, it } from "vitest";
import { availablePythonBackends } from "./fixtures/python-backends.js";

// Decode stdin bytes explicitly: Python's -I ignores PYTHON* encoding overrides.
const parser = `import ast, json, sys, textwrap
for example in json.loads(sys.stdin.buffer.read().decode("utf-8")):
    ast.parse("async def example():\\n" + textwrap.indent(example["code"], "    "), filename=example["label"])
`;
const syntaxCheck = (examples: Array<{ label: string; code: string }>) => spawnSync("python3", ["-I", "-B", "-c", parser], {
  encoding: "utf8", input: JSON.stringify(examples), timeout: 10000,
});

it.skipIf(!availablePythonBackends.cpython)("syntax-checks shipped Python decision examples without running their effects", () => {
  const examples: Array<{ label: string; code: string }> = [];
  for (const file of ["docs/decisions.md", "skillsets/python/fabric-exec/SKILL.md", "skillsets/python/fabric-graph/SKILL.md"]) {
    const text = fs.readFileSync(file, "utf8");
    for (const match of text.matchAll(/```python\r?\n([\s\S]*?)\r?\n```/g)) examples.push({ label: file, code: match[1]! });
    const decisions = [...text.matchAll(/`([^`\n]+)`/g)].map(match => match[1]!).filter(code =>
      code.includes("await decisions.raise(") || code.includes('await tools.call(ref="decisions.raise"'));
    if (!file.includes("fabric-graph")) expect(decisions.length, `${file}: decision example is exercised`).toBeGreaterThan(0);
    for (const code of decisions) examples.push({ label: file, code });
  }
  expect(examples.length).toBeGreaterThanOrEqual(8);
  const result = syntaxCheck(examples);
  expect(result.status, result.stderr).toBe(0);
  // A negative control ensures this check rejects the original keyword form.
  expect(syntaxCheck([{ label: "negative-control", code: 'await decisions.raise(title="Deploy?")' }]).status).not.toBe(0);
});
