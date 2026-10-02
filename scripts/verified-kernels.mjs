#!/usr/bin/env node
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, writeFileSync, mkdirSync, rmSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { build, transform } from "esbuild";
import ts from "typescript";

const root = fileURLToPath(new URL("../", import.meta.url));
const version = "2.0.34";
const targets = [
  { stem: "storage-kernel", source: "proofs/storage-kernel.bend", abi: "proofs/storage-abi.json", types: "proofs/storage-types.d.ts" },
  { stem: "authority-kernel", source: "proofs/authority-kernel.bend", abi: "proofs/authority-abi.json", types: "proofs/authority-types.d.ts" },
  { stem: "lifecycle-kernel", source: "proofs/lifecycle-kernel.bend", abi: "proofs/lifecycle-abi.json", types: "proofs/lifecycle-types.d.ts" },
  { stem: "kernel", source: "proofs/kernel.bend", abi: "proofs/abi.json", types: "proofs/types.d.ts" },
  { stem: "provider-kernel", source: "proofs/provider-kernel.bend", abi: "proofs/provider-abi.json", types: "proofs/provider-types.d.ts" },
];
const sources = ["LAWS.bend", "PROOF.bend", "proofs/kernel.bend", "proofs/resources.bend", "proofs/resource-spec.bend", "proofs/resource-proof.bend", "proofs/abi.json", "proofs/types.d.ts", "scripts/verified-kernels.mjs", "proofs/state-plans.bend", "proofs/state-spec.bend", "proofs/state-proof.bend", "proofs/state-kernel.bend", "proofs/provider-plans.bend", "proofs/provider-spec.bend", "proofs/provider-kernel.bend", "proofs/provider-abi.json", "proofs/provider-types.d.ts"];
sources.push("proofs/lifecycle.bend", "proofs/lifecycle-kernel.bend", "proofs/lifecycle-spec.bend", "proofs/lifecycle-proof.bend", "proofs/lifecycle-abi.json", "proofs/lifecycle-types.d.ts");
sources.push("proofs/authority-state.bend", "proofs/authority-spec.bend", "proofs/authority-proof.bend", "proofs/authority-kernel.bend", "proofs/authority-abi.json", "proofs/authority-types.d.ts");
sources.push("proofs/storage-plans.bend", "proofs/storage-spec.bend", "proofs/storage-proof.bend", "proofs/storage-kernel.bend", "proofs/storage-abi.json", "proofs/storage-types.d.ts");
const artifactPaths = targets.flatMap(({ stem }) => [`src/verified/generated/${stem}.js`, `src/verified/generated/${stem}.d.ts`]);
const receipt = "src/verified/generated/manifest.json";
const read = (path) => readFileSync(resolve(root, path), "utf8");
const hash = (text) => createHash("sha256").update(text).digest("hex");
const inputs = () => Object.fromEntries(sources.map((path) => [path, hash(read(path))]));
const args = process.argv.slice(2);
if (args.length !== 1 || !["--write", "--check", "--artifact"].includes(args[0])) {
  throw new Error("Usage: node scripts/verified-kernels.mjs --write|--check|--artifact");
}
const mode = args[0];
if (mode === "--artifact") {
  const stored = JSON.parse(read(receipt));
  if (stored.version !== 2 || stored.bend !== version ||
      JSON.stringify(stored.inputs) !== JSON.stringify(inputs()) ||
      JSON.stringify(stored.outputs) !== JSON.stringify(Object.fromEntries(artifactPaths.map(path => [path, hash(read(path))])))) {
    throw new Error("Verified kernel artifact is stale. Run bun run proof:generate with the pinned Bend compiler.");
  }
  console.log("Verified kernel artifact matches its proof sources and bridge.");
} else {
  const env = { ...process.env, BEND_NO_TELEMETRY: "1" };
  const bend = (...args) => execFileSync(process.env.BEND_BIN || "bend", args, {
    cwd: root, env, encoding: "utf8", maxBuffer: 16 * 1024 * 1024, timeout: 120_000,
  });
  if (bend("version").trim() !== `bend ${version}`) throw new Error(`Bend ${version} is required`);
  for (const path of sources.filter((path) => path.endsWith(".bend"))) {
    if (/@unsafe|\?\w|^\s*import\s+["']|^\s*import\s+0x/m.test(read(path))) {
      throw new Error(`Unsafe code, holes, foreign effects, and remote imports are forbidden in ${path}`);
    }
    for (const match of read(path).matchAll(/^import\s+(\S+)/gm)) {
      if (match[1] === "Base") continue;
      const imported = relative(root, resolve(root, dirname(path), match[1])).replaceAll("\\", "/");
      if (!sources.includes(imported) || !imported.endsWith(".bend")) {
        throw new Error(`Untracked proof dependency in ${path}: ${match[1]}`);
      }
    }
  }
  const checked = bend("PROOF.bend", "--check-only");
  if (!checked.includes("ALL PROOFS CHECK")) throw new Error(`Bend did not confirm closed proofs: ${checked}`);
  const temp = mkdtempSync(join(tmpdir(), "fabric-bend-"));
  try {
    const files = new Map();
    let exportsCount = 0;
    for (const target of targets) {
      const emitted = join(temp, `${target.stem}.mjs`);
      bend(target.source, "-o", emitted);
      const js = readFileSync(emitted, "utf8");
      const ast = ts.createSourceFile("kernel.js", js, ts.ScriptTarget.Latest, true, ts.ScriptKind.JS);
      const statements = [...ast.statements];
      const table = statements.at(-1);
      if (!table || !ts.isExportAssignment(table) || !ts.isObjectLiteralExpression(table.expression) ||
          statements.some((statement) => ts.isExpressionStatement(statement))) {
        throw new Error("Unrecognized Bend module footer; review the compiler bridge before upgrading");
      }
      const defs = new Map(statements.filter(ts.isFunctionDeclaration).map((node) => [node.name?.text, node]));
      const entries = new Map(table.expression.properties.filter(ts.isPropertyAssignment)
        .map((property) => [property.name.getText(ast).replace(/^"|"$/g, ""), property.initializer]));
      // Bend host tags carry the declaring module ("lifecycle.Life"); root and
      // Base constructors stay bare. The host ABI keeps bare tags, so every
      // namespaced tag must name exactly one constructor in this kernel.
      const literals = new Set();
      const collect = (node) => { if (ts.isStringLiteral(node)) literals.add(node.text); ts.forEachChild(node, collect); };
      collect(ast);
      const tags = new Map();
      for (const literal of literals) {
        const bare = /^[a-z][a-z0-9-]*\.([A-Z][A-Za-z0-9]*)$/.exec(literal)?.[1];
        if (!bare) continue;
        if (tags.has(bare) || literals.has(bare)) throw new Error(`Ambiguous host tag ${bare} in ${target.source}`);
        tags.set(bare, literal);
      }
      const abi = JSON.parse(read(target.abi));
      const exports = Object.entries(abi).map(([name, signature]) => {
        const symbol = `$${name}$`;
        const arity = signature.length - 1;
        const entry = entries.get(name);
        if (!/^[a-zA-Z][a-zA-Z0-9]*$/.test(name) || defs.get(symbol)?.parameters.length !== arity ||
            !entry || !ts.isCallExpression(entry) || entry.expression.getText(ast) !== "run_lib" ||
            entry.arguments[1]?.getText(ast) !== String(arity) || !ts.isArrowFunction(entry.arguments[0]) ||
            entry.arguments[0].parameters.length !== arity || !entry.arguments[0].getText(ast).includes(`${symbol}(`)) {
          throw new Error(`Missing or incompatible compiled definition: ${name}`);
        }
        const params = Array.from({ length: arity }, (_, index) => `a${index}`);
        return `const $lib$${name} = /* @__PURE__ */ ${entry.getText(ast)};\n` +
          `export const ${name} = (${params.join(", ")}) => $retag$($lib$${name}(${params.map((param) => `$retag$(${param}, $tag$)`).join(", ")}), $untag$);`;
      });
      // Keep only the ABI entries of Bend's own module table, which marshal host
      // naturals and drain the trampoline, and rename constructor tags at the
      // boundary. No algorithm is translated.
      const bridge = [
        `const $tag$ = new Map(${JSON.stringify([...tags])});`,
        `const $untag$ = new Map(${JSON.stringify([...tags].map(([bare, full]) => [full, bare]))});`,
        `function $retag$(value, names) {
  const top = [value];
  for (const stack = [[top, 0]]; stack.length > 0;) {
    const [parent, key] = stack.pop();
    const node = parent[key];
    if (node === null || typeof node !== "object") continue;
    const copy = Array.isArray(node) ? [...node] : { ...node };
    if (typeof copy.$ === "string") copy.$ = names.get(copy.$) ?? copy.$;
    parent[key] = copy;
    for (const field of Object.keys(copy)) stack.push([copy, field]);
  }
  return top[0];
}`,
      ];
      const library = js.slice(0, table.getFullStart()) + "\n" + bridge.join("\n") + "\n" + exports.join("\n");
      const result = await build({
        stdin: { contents: library, sourcefile: "bend-kernel.js", resolveDir: root },
        bundle: true, write: false, format: "esm", platform: "node", packages: "external", external: ["bun:ffi"], target: "es2022",
        minifySyntax: true,
        legalComments: "none", treeShaking: true,
        banner: { js: `// Generated by Bend ${version}; do not edit. See LAWS.bend and PROOF.bend.\n// Includes adapted Bend runtime/Base code, Copyright 2026 HigherOrderCO, Apache-2.0.\n// ABI exports selected and tags adapted by Pi Fabric; see THIRD_PARTY_NOTICES.md.` },
      });
      const pure = result.outputFiles[0].text;
      const generatedAst = ts.createSourceFile("generated.js", pure, ts.ScriptTarget.Latest, true, ts.ScriptKind.JS);
      if (generatedAst.statements.some((s) => ts.isImportDeclaration(s)) || /\b(?:process|globalThis|io_exit|io_push|require|__require)\b/.test(pure)) {
        throw new Error("Effectful runtime escaped into the pure generated library");
      }
      // Check unmangled names before compacting compiler locals. ABI exports stay stable.
      const generated = (await transform(pure, {
        format: "esm", target: "es2022", minifyIdentifiers: true, minifySyntax: true, legalComments: "none",
        banner: `// Generated by Bend ${version}; do not edit. See LAWS.bend and PROOF.bend.\n// Includes adapted Bend runtime/Base code, Copyright 2026 HigherOrderCO, Apache-2.0.\n// ABI exports selected and tags adapted by Pi Fabric; see THIRD_PARTY_NOTICES.md.`,
      })).code;
      const types = `// Generated ABI declarations; see ${target.abi}.\n` + read(target.types) + "\n" +
        Object.entries(abi).map(([name, signature]) => {
          const args = signature.slice(0, -1).map(([name, type]) => `${name}: ${type}`).join(", ");
          return `export declare function ${name}(${args}): ${signature.at(-1)};`;
        }).join("\n") + "\n";
      exportsCount += Object.keys(abi).length;
      files.set(`src/verified/generated/${target.stem}.js`, generated);
      files.set(`src/verified/generated/${target.stem}.d.ts`, types);
    }
    // Remove only previously generated outputs retired by this manifest.
    if (mode === "--write" && existsSync(resolve(root, receipt))) {
      const previous = JSON.parse(read(receipt));
      for (const path of Object.keys(previous.outputs ?? {})) {
        if (/^src\/verified\/generated\/[a-z-]+\.(?:js|d\.ts)$/.test(path) && !files.has(path)) rmSync(resolve(root, path), { force: true });
      }
    }
    const outputs = Object.fromEntries([...files].map(([path, contents]) => [path, hash(contents)]));
    files.set(receipt, JSON.stringify({ version: 2, bend: version, inputs: inputs(), outputs }, null, 2) + "\n");
    for (const [path, contents] of files) {
      if (mode === "--write") {
        mkdirSync(resolve(root, "src/verified/generated"), { recursive: true });
        writeFileSync(resolve(root, path), contents);
      } else if (read(path) !== contents) {
        throw new Error(`${path} differs from freshly proved/generated output; run bun run proof:generate`);
      }
    }
    console.log(`Bend ${version}: all laws checked; ${exportsCount} executable kernels ${mode === "--write" ? "generated" : "reproduced"} in ${targets.length} separately loadable artifacts.`);
  } finally {
    rmSync(temp, { recursive: true, force: true });
  }
}
