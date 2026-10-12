import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, symlinkSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { build } from "esbuild";
import { describe, expect, it } from "vitest";

describe("lazy durable Pi startup", () => {
  it("imports ordinary Fabric and the adapter entry without resolving optional engines", async () => {
    const directory = mkdtempSync(path.join(os.tmpdir(), "fabric-durable-import-"));
    try {
      symlinkSync(path.resolve("node_modules"), path.join(directory, "node_modules"), "junction");
      await build({
        entryPoints: ["src/index.ts", "src/runners.ts", "src/durable.ts"],
        outdir: directory,
        bundle: true,
        packages: "external",
        platform: "node",
        format: "esm",
        target: "node24",
        splitting: true,
        logLevel: "silent",
      });
      const output = execFileSync(process.execPath, ["--input-type=module", "-e", `
        import assert from "node:assert/strict";
        import path from "node:path";
        import { pathToFileURL } from "node:url";
        import { registerHooks } from "node:module";
        // The real Pi host is already present before an extension is imported.
        await import("@earendil-works/pi-coding-agent");
        const optional = ["@earendil-works/pi-durable", "@earendil-works/chord", "pi-fabric-worker-sdk"];
        registerHooks({ resolve(specifier, context, next) {
          if (optional.some(name => specifier === name || specifier.startsWith(name + "/"))) {
            throw new Error("Optional durable dependency was resolved: " + specifier);
          }
          return next(specifier, context);
        }});
        await assert.rejects(import("@earendil-works/pi-durable"), /Optional durable dependency/);
        const directory = ${JSON.stringify(directory)};
        const load = name => import(pathToFileURL(path.join(directory, name + ".js")).href);
        const extension = await load("index");
        assert.equal(typeof extension.default, "function");
        const runners = await load("runners");
        const before = runners.listAgentRunners().map(runner => runner.id);
        const adapter = await load("durable");
        assert.equal(typeof adapter.createPiDurableRunner, "function");
        const runner = adapter.createPiDurableRunner({
          models: {}, registry: {}, env: () => undefined,
          allowedModels: [{provider: "test", modelId: "offline"}],
          defaultModel: {provider: "test", modelId: "offline"},
          storage: {kind: "jsonl", directory: path.join(directory, "unused")},
        });
        assert.equal(runner.defaultModel(), "test/offline");
        assert.equal(runner.capabilities.steer, true);
        assert.equal(runner.capabilities.imageInput, true);
        await runner.close();
        assert.deepEqual(runners.listAgentRunners().map(runner => runner.id), before);
        assert.deepEqual(before, ["pi", "pi-durable", "claude", "veda"]);
        console.log("durable engines stay lazy even with the built-in runner registered");
      `], { encoding: "utf8", timeout: 30_000 });
      expect(output).toContain("durable engines stay lazy even with the built-in runner registered");
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  }, 40_000);
});
