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
const version = "2.0.26";
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
  if (!checked.includes("All terms check.")) throw new Error(`Bend did not confirm closed proofs: ${checked}`);
  const temp = mkdtempSync(join(tmpdir(), "fabric-bend-"));
  try {
    const files = new Map();
    let exportsCount = 0;
    for (const target of targets) {
      const emitted = join(temp, `${target.stem}.js`);
      bend(target.source, "-o", emitted);
      const js = readFileSync(emitted, "utf8");
      const ast = ts.createSourceFile("kernel.js", js, ts.ScriptTarget.Latest, true, ts.ScriptKind.JS);
      const statements = [...ast.statements];
      const last = statements.slice(-2).map((statement) => statement.getText(ast).replace(/\s/g, ""));
      if (last[0] !== "cli(process.argv.slice(2));" || last[1] !== "io_exit($main$,null);") {
        throw new Error("Unrecognized Bend executable footer; review the compiler bridge before upgrading");
      }
      const defs = new Map(statements.filter(ts.isFunctionDeclaration).map((node) => [node.name?.text, node]));
      const abi = JSON.parse(read(target.abi));
      const exports = Object.entries(abi).map(([name, signature]) => {
        const symbol = `$${name}$`;
        const arity = signature.length - 1;
        if (!/^[a-zA-Z][a-zA-Z0-9]*$/.test(name) || defs.get(symbol)?.parameters.length !== arity) {
          throw new Error(`Missing or incompatible compiled definition: ${name}`);
        }
        return `export const ${name} = /* @__PURE__ */ run_lib(${symbol}, ${arity});`;
      });
      // Only remove the two checked CLI invocations and expose actual compiler
      // definitions through Bend's own trampoline. No algorithm is translated.
      const library = js.slice(0, statements.at(-2).getFullStart()) + "\n" + exports.join("\n");
      const result = await build({
        stdin: { contents: library, sourcefile: "bend-kernel.js", resolveDir: root },
        bundle: true, write: false, format: "esm", platform: "node", packages: "external", external: ["bun:ffi"], target: "es2022",
        minifySyntax: true,
        legalComments: "none", treeShaking: true,
        banner: { js: `// Generated by Bend ${version}; do not edit. See LAWS.bend and PROOF.bend.\n// Includes adapted Bend runtime/Base code, Copyright 2026 HigherOrderCO, Apache-2.0.\n// CLI removed and exports added by Pi Fabric; see THIRD_PARTY_NOTICES.md.` },
      });
      const pure = result.outputFiles[0].text;
      const generatedAst = ts.createSourceFile("generated.js", pure, ts.ScriptTarget.Latest, true, ts.ScriptKind.JS);
      if (generatedAst.statements.some((s) => ts.isImportDeclaration(s)) || /\b(?:process|globalThis|io_exit|io_push|require|__require)\b/.test(pure)) {
        throw new Error("Effectful runtime escaped into the pure generated library");
      }
      // Check unmangled names before compacting compiler locals. ABI exports stay stable.
      const generated = (await transform(pure, {
        format: "esm", target: "es2022", minifyIdentifiers: true, minifySyntax: true, legalComments: "none",
        banner: `// Generated by Bend ${version}; do not edit. See LAWS.bend and PROOF.bend.\n// Includes adapted Bend runtime/Base code, Copyright 2026 HigherOrderCO, Apache-2.0.\n// CLI removed and exports added by Pi Fabric; see THIRD_PARTY_NOTICES.md.`,
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
