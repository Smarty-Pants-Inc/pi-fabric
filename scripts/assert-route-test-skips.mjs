import fs from "node:fs";
import path from "node:path";

// Pin the complete workflow glob, not just its three race/handoff suites (C-1).
// Adding/removing a selected suite requires a reviewed inventory change here.
export const REQUIRED_ROUTE_SUITES = [
  "model-route-hook.test.ts", "model-route-live.test.ts", "model-route-manual-revert.test.ts",
  "model-route-owner.test.ts", "model-route-recovery.test.ts", "model-route-resident.test.ts",
  "model-route-security-followups.test.ts", "model-route.test.ts", "model-route-worker.test.ts",
  "run-directory-mode.test.ts", "task-control-archive-legacy.test.ts", "task-control-archive-noanswer.test.ts",
  "task-control-archive-route.test.ts", "task-control-archive.test.ts", "task-control-legacy-read.test.ts",
  "task-control-legacy-retention.test.ts", "task-control-manager-root.test.ts", "task-control-reserved-alias.test.ts",
  "task-control-root.test.ts", "task-control-route-platform.test.ts", "task-control-route-race.test.ts",
  "task-control-route-residual.test.ts", "task-control-route-session-identity.test.ts", "task-control-security.test.ts",
  "task-control-snapshot.test.ts", "task-control-windows-allocation.test.ts",
];
const skipped = row => ["skipped", "pending", "todo"].includes(row.status);
const rule = (file, pattern, max, reason, platforms = ["darwin", "win32"]) => ({ file, pattern, max, reason, platforms });
// Whole POSIX-only suites still name EVERY existing row; no prefix/file waiver.
// Literal parameter sets pin the current names, and each rule is count-bounded.
const WINDOWS_POSIX_SUITES = [
  ["task-control-root.test.ts", "POSIX ownership/mode and fd-root fixtures", [
    "lives under the validated state home, outside temp, and refuses temp placement",
    "refuses a symlink alias and a group-writable state home",
    "derives a deterministic run-id plus path identity, so reused archive ids never collide",
    "refuses grants on the root, its rename-capable ancestors and parent controls (SEC01); admits normal run/workspace grants",
    "keys the lexical path, refusing a child-planted nested alias onto a victim run without touching its controls",
    "never recreates missing controls of a central-v1 run, while archive creation admits a fresh destination",
    "CUTOFF: raw sibling-v1 (single genuine-looking answer) refuses initialize/read/save and preserves bytes",
    "CUTOFF: inode-bound v1 tri-layout (in-run, sibling, both, empty-looking) is unknown history, never final",
    "CUTOFF: un-bootstrapped bound sibling plus in-run answer refuses before any bootstrap",
    "CUTOFF: unbound old sibling no longer ignores in-run rows and saves NEW; it refuses, bytes preserved (#12214)",
    "CUTOFF: in-run-only legacy record with no layout marker refuses (was IN_RUN)",
    "CUTOFF: raw history with v1 binding cannot be bootstrapped, and a central write cannot launder it",
    "single-source current-only answer stays final (host ctor writes the null-snapshot binding; initialize only validates)",
    "openLegacyTaskControlFiles: current-only yields no fds; raw layers refuse without leaking any fd",
    "after host bootstrap, raw old-layout forgery is ignored; host receipt and admission events stay authoritative",
    "creates a deeply nonexistent absolute XDG state home privately",
    "missing old sibling for a sibling-v1 run refuses fallback and recreation",
  ]],
  ["task-control-manager-root.test.ts", "POSIX symlinked-root fixtures", [
    "launches an ordinary spawn under a symlinked default tmpdir (macOS /var -> /private/var shape)",
    "refuses an explicit host root reached through a user-owned symlink before creating any paths",
    "still refuses a child-planted alias BELOW the trusted root (inherited nested root) and leaves the victim untouched",
    "ROUTED inherited root through a symlink fails before the route receipt, first write or launch (repeatable)",
    "ROUTED spawn under a legitimate trusted host root still launches and writes its route receipt there",
  ]],
  ["task-control-snapshot.test.ts", "POSIX inode-bound authority fixtures", [
    "exclusive host allocation publishes snapshot:null; reads/saves/finality use central only",
    "post-bootstrap raw forgery is ignored for finality/read/save",
    "public initialize never mints; unknown raw/v1/unbound central refuses preserving bytes",
    "reserves protected metadata names against public write/remove",
    "archive ctor writes its own null binding despite copied status",
    "immutable final name: a second save never replaces the first receipt",
    "non-null snapshot binding and stray .legacy-snapshot refuse, bytes preserved",
    "current-only producer: no legacy sources ever, sibling/in-run raw bytes never read",
    "stale stage is refused, preserved, never adopted",
  ]],
  ["task-control-windows-allocation.test.ts", "Windows-emulation seam intentionally runs on POSIX only", [
    "allocates, publishes central v2 controls, writes, reads and saves the final answer without opening a directory",
    "keeps exclusive allocation, non-directory and alias refusals on the checked path",
    "refuses a reparse alias that lstats as a directory",
    "refuses a parent swapped during allocation instead of recapturing it",
    "refuses a foreign central route/ without repairing it or writing any route file",
    "publishes all route controls centrally and records pending recovery unlink without touching the run",
  ]],
  ["task-control-legacy-read.test.ts", "POSIX link/FIFO/mode raw legacy fixtures", [
    "selects the pinned run itself and reads admission, delivered control and final receipt",
    "reads minimal stopped status and queued rows without inventing consumption",
    "refuses every write, lock and publication helper without creating central, stage or state",
    "sender outbox never routes, retains or proves refusal from raw legacy evidence",
    ...["central-v1", "sibling-v1", "unknown-layout", null].map(value => `taskControlLayout ${JSON.stringify(value)} without central refuses`),
    ...["binding", "snapshot", "sibling"].map(value => `raw protected metadata or .ctl sibling (${value}) refuses`),
    ...["missing", "malformed", "array", "linked", "oversized", "group-writable-run"].map(value => `unsafe status (${value}) never selects the run`),
    "an aliased run path refuses before any read",
    ...["symlink", "hardlink", "fifo", "malformed", "wrong-version"].map(value => `${value} final receipt never establishes finality`),
    ...["symlink", "hardlink", "fifo"].map(value => `${value} control row reads as uncertain, never consumed`),
    "oversized or malformed control rows fail closed", "a linked terminal-controls directory is never followed",
    ...["symlink", "hardlink", "fifo"].map(value => `${value} steering journal refuses admission reads`),
    "malformed steering rows grant nothing and an incomplete row truncates",
  ]],
].map(([file, reason, titles]) => ({ file, reason, titles: new Set(titles), max: titles.length, platforms: ["win32"] }));
const WINDOWS_PRIVATE_DIR_TITLES = new Set([
  "creates a run directory 0700 under umask 002 through the real AgentManager spawn",
  "creates a usable 0700 run directory under umask 0177 through the real AgentManager spawn",
  "mkdirPrivate makes every directory it creates 0700 under umask 0177, and leaves existing ones",
  "does not follow a symlink swapped in at a copied nested path",
  "does not archive below a group-writable ancestor above the anchor, and warns",
  "does not archive through a symlinked destination, and warns",
  "tightens our own 0775 container below the anchor to 0755 and creates the run 0700",
  "refuses the same 0775 container above the anchor",
  "refuses a foreign-owned group-writable container below the anchor (injected fstat)",
  "never writes through a symlink planted in a healed archive directory",
  "writes new archive entries 0600, and replaces an existing entry only when it is our own regular file",
  "on Linux, fails closed when /proc/self/fd is unusable, and creates nothing",
  "creates usable 0700 directories and 0600 archive files under umask 0400",
  "falls back to the validated walk: directories it creates are 0700, under umask 0177 too",
  "creates usable 0700 directories under umask 0400 too", "still refuses a symlinked ancestor",
  "archives 0700/0600 and never writes through a planted leaf symlink", "works through a /dev/fd base",
  "does not heal through a root-owned symlink below the anchor (injected lstat)",
  "mkdirPrivate refuses a symlinked existing ancestor and writes nothing through it",
  "mkdirPrivate refuses an existing group-writable ancestor",
  "mkdirPrivate refuses a competing creator that made a group-writable directory at the component it was creating",
  "mkdirPrivate refuses a competing creator that made a symlink at the component it was creating",
  "archives an actor run's nested tree with 0700 directories",
]);
const PLATFORM_SKIPS = [
  ...WINDOWS_POSIX_SUITES,
  rule("task-control-route-session-identity.test.ts", /post-verification leaf swap/, 1, "waiver W-1: Pi receives the plain central path off Linux"),
  rule("task-control-route-residual.test.ts", /expected skip: F[01].+requires enforced Linux Landlock$/, 3, "F0/F1 grant-confined probes require Linux Landlock"),
  rule("task-control-root.test.ts", /openLegacyTaskControlFiles: current-only yields no fds; raw layers refuse without leaking any fd$/, 1, "Linux fd enumeration fixture", ["darwin"]),
  rule("run-directory-mode.test.ts", /on Linux, fails closed when \/proc\/self\/fd is unusable, and creates nothing$/, 1, "Linux-only proc-fd refusal", ["darwin"]),
  rule("task-control-security.test.ts", /confines every control write\/FIFO\/rename and live lock change under (explicit request|inherited parent policy) while run\/workspace operations remain allowed$/, 2, "enforced Linux Landlock"),
  rule("task-control-security.test.ts", /SEC01 #12079: a parent run grant covering descendant R\/nested\/C refuses both central ctls with EACCES while nested artifacts stay writable$/, 1, "enforced Linux Landlock"),
  rule("task-control-security.test.ts", /FORGE-BEFORE-FIRST-SNAPSHOT: parent-written raw C\.ctl before host creation is refused, never adopted$/, 1, "enforced Linux Landlock"),
  rule("task-control-security.test.ts", /CURRENT-ONLY: parent-written raw C\.ctl and in-run controls after host bootstrap never replace host evidence \((no host answer|host answer)\)$/, 2, "enforced Linux Landlock"),
  rule("task-control-security.test.ts", /hosted-only: refuses actual control replacement\/removal and live-lock deletion$/, 1, "hosted Linux Landlock real-delete row"),
  rule("task-control-legacy-retention.test.ts", /retains (linked legacy control|hard-linked legacy control|linked sibling)$/, 3, "POSIX legacy link fixtures", ["win32"]),
  rule("model-route.test.ts", /R2 ignores workspace ledger (parent|leaf) links$/, 2, "POSIX ledger links", ["win32"]),
  rule("model-route.test.ts", /R2 refuses (link|fifo) ledger endpoints without blocking or changing targets$/, 2, "POSIX ledger links/FIFO", ["win32"]),
  rule("model-route-security-followups.test.ts", /control: on POSIX the same command fixture really runs and routes$/, 1, "POSIX command fixture", ["win32"]),
  rule("model-route-live.test.ts", /fences failed\/stopped terminal saves across restart and repairs the original joins exactly once$/, 1, "POSIX non-root permission-fault fixture", ["win32"]),
  rule("model-route-live.test.ts", /pins even one pending successful terminal save and resumes only after repair$/, 1, "POSIX non-root permission-fault fixture", ["win32"]),
  rule("model-route-live.test.ts", /orders repaired A-success before B-failure and C-stop from three pre-admitted runs$/, 1, "POSIX non-root permission-fault fixture", ["win32"]),
];

export function assertRouteTestReport(report, platform = process.platform) {
  if (!["linux", "darwin", "win32"].includes(platform)) throw new Error(`unsupported route-test platform: ${platform}`);
  const tests = (report.testResults ?? []).flatMap(file => (file.assertionResults ?? []).map(test => ({
    file: path.basename(file.name ?? ""), name: test.fullName ?? test.title ?? "",
    title: test.title ?? test.fullName ?? "", status: test.status,
  })));
  const requireFile = name => {
    const rows = tests.filter(test => test.file === name);
    if (!rows.length) throw new Error(`required route suite did not report tests: ${name}`);
    return rows;
  };
  REQUIRED_ROUTE_SUITES.forEach(requireFile);
  const counts = new Map();
  for (const test of tests) {
    if (!REQUIRED_ROUTE_SUITES.includes(test.file)) throw new Error(`unlisted route suite: ${test.file}`);
    if (test.status === "passed") continue;
    if (!skipped(test)) throw new Error(`route test did not pass: ${test.file}: ${test.name}`);
    const allowed = PLATFORM_SKIPS.find(item => item.file === test.file && item.platforms.includes(platform) && (item.titles?.has(test.title) || item.pattern?.test(test.name)));
    const privateDir = platform === "win32" && test.file === "run-directory-mode.test.ts" && WINDOWS_PRIVATE_DIR_TITLES.has(test.title);
    if (!allowed && !privateDir) throw new Error(`unlisted route skip: ${test.file}: ${test.name}`);
    const key = allowed ?? WINDOWS_PRIVATE_DIR_TITLES, count = (counts.get(key) ?? 0) + 1;
    counts.set(key, count);
    if (count > (allowed?.max ?? WINDOWS_PRIVATE_DIR_TITLES.size)) throw new Error(`route skip count exceeded: ${test.file}: ${allowed?.reason ?? "existing POSIX private-directory fixtures"}`);
  }
  // New archive-route/platform/race suites have NO skip exception on any platform.
  const residual = requireFile("task-control-route-residual.test.ts");
  if (platform === "linux") {
    const probes = residual.filter(test => /task\.txt must not land|victim file must remain unchanged/.test(test.name));
    if (probes.length !== 3 || probes.some(test => test.status !== "passed")) throw new Error("all three hosted F0/F1 expected-failure probes must execute on Linux");
  } else if (residual.filter(skipped).length !== 3) throw new Error("expected exactly three declared Linux-only residual skips");
  const session = requireFile("task-control-route-session-identity.test.ts");
  const handoff = session.filter(test => /post-verification leaf swap/i.test(test.name));
  if (handoff.length !== 1 || handoff[0].status !== (platform === "linux" ? "passed" : "skipped")) throw new Error("post-verification leaf swap must pass on Linux and be the declared W-1 skip elsewhere");
}

if (import.meta.main) {
  const reportPath = process.argv[2];
  if (!reportPath) throw new Error("usage: node scripts/assert-route-test-skips.mjs <vitest-json-report>");
  assertRouteTestReport(JSON.parse(fs.readFileSync(reportPath, "utf8")));
}
