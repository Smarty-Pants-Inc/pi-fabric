import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const guardUrl = new URL("../test-deletion-guard.mjs", import.meta.url);
const inside = (root: string, target: string): boolean => {
  const relative = path.relative(root, target);
  return relative === "" || (!path.isAbsolute(relative) && relative !== ".." && !relative.startsWith(`..${path.sep}`));
};
type GuardState = { root?: string; report: boolean; count: number };

/** Fresh retained fixtures under main's private temp, never a caller-supplied recording root. */
export function residentDrainTemp(prefix: string) {
  const guard = (process as unknown as Record<symbol, GuardState>)[Symbol.for("pi-fabric.test-deletion-guard")];
  const temp = fs.realpathSync(os.tmpdir());
  if (!guard?.root || !inside(guard.root, temp)) throw new Error("Drain fixtures require main's private temp deletion guard");
  const outsideCount = guard.count;
  const container = fs.realpathSync(fs.mkdtempSync(path.join(temp, prefix)));
  const root = path.join(container, "work");
  const home = path.join(container, "home");
  for (const directory of [root, home]) fs.mkdirSync(directory, { mode: 0o700 });
  return {
    root, home,
    assertNoOutsideDeletion(): void {
      if (guard.count !== outsideCount) throw new Error("Drain fixture attempted deletion/mutation outside main's private temp");
      const audit = path.join(root, "deletion-guard-audit.jsonl");
      if (fs.existsSync(audit)) {
        const events = fs.readFileSync(audit, "utf8").split("\n").filter(Boolean)
          .map(line => JSON.parse(line) as { event: string; count?: number });
        if (events.some(event => event.event === "outside" || (event.event === "exit" && event.count !== 0))) {
          throw new Error(`Drain child attempted deletion/mutation outside private temp; see ${audit}`);
        }
      }
    },
    /**
     * Narrow the inherited guard ceiling, never remove the preload or widen it.
     * HOME is a private sibling: the guard deliberately rejects a deletion root
     * containing HOME. All product/temp state goes in work, not HOME. Descendants
     * inherit this narrower ceiling unchanged. The parent still guards both siblings.
     */
    childEnvironment(nodeOptions: string): NodeJS.ProcessEnv {
      let found = false;
      const narrowed = nodeOptions.replace(/--import=(file:[^\s"']+)/g, (token, href: string) => {
        const url = new URL(href);
        if (fileURLToPath(url) !== fileURLToPath(guardUrl)) return token;
        const ceiling = url.searchParams.get("root");
        if (!ceiling || fs.realpathSync(ceiling) !== guard.root || !inside(guard.root!, root)) {
          throw new Error("Drain child guard ceiling does not match main's pinned private temp");
        }
        url.searchParams.set("root", root);
        found = true;
        return `--import=${url.href}`;
      });
      if (!found) throw new Error("Drain children must inherit main's deletion guard preload");
      const mcporter = path.join(root, "mcporter.json");
      fs.writeFileSync(mcporter, '{"mcpServers":{},"imports":[]}\n', { mode: 0o600 });
      const audit = path.join(root, "deletion-guard-audit.jsonl");
      const preload = path.join(root, "drain-safety-preload.mjs");
      // Observe the unmodified standard guard. Reports are retained immediately,
      // even if a child is later killed; local caught refusals also count at exit.
      fs.writeFileSync(preload, `import fs from "node:fs";
import path from "node:path";
const root = ${JSON.stringify(root)};
const audit = ${JSON.stringify(audit)};
const guard = process[Symbol.for("pi-fabric.test-deletion-guard")];
const relative = guard?.root && path.relative(root, guard.root);
if (!guard?.root || path.isAbsolute(relative) || relative === ".." || relative.startsWith(".." + path.sep)) {
  throw new Error("Drain child lost or widened main's deletion guard");
}
const record = value => fs.appendFileSync(audit, JSON.stringify({ pid: process.pid, ...value }) + "\\n");
record({ event: "installed", root: guard.root, report: guard.report });
const write = process.stderr.write;
process.stderr.write = function(chunk, ...args) {
  if (String(chunk).includes("TEST_DELETION_REPORT")) record({ event: "outside", text: String(chunk) });
  return Reflect.apply(write, this, [chunk, ...args]);
};
process.once("exit", () => record({ event: "exit", count: guard.count, root: guard.root }));
`, { mode: 0o600 });
      return {
        NODE_OPTIONS: `${narrowed} --import=${pathToFileURL(preload).href}`, HOME: home, USERPROFILE: home,
        TMPDIR: root, TMP: root, TEMP: root,
        XDG_CACHE_HOME: path.join(root, "cache"), XDG_CONFIG_HOME: path.join(root, "config"),
        XDG_DATA_HOME: path.join(root, "data"), XDG_STATE_HOME: path.join(root, "state"),
        MCPORTER_CONFIG: mcporter, PI_FABRIC_LOCK_STATS: "0",
      };
    },
  };
}
