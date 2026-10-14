import fs from "node:fs";
import path from "node:path";

const reportPath = process.argv[2];
if (!reportPath) throw new Error("usage: node scripts/assert-route-test-skips.mjs <vitest-json-report>");
const report = JSON.parse(fs.readFileSync(reportPath, "utf8"));
const tests = (report.testResults ?? []).flatMap((file) => (file.assertionResults ?? []).map((test) => ({
  file: path.basename(file.name ?? ""),
  name: test.fullName ?? test.title ?? "",
  status: test.status,
})));
const requireFile = (name) => {
  const rows = tests.filter((test) => test.file === name);
  if (rows.length === 0) throw new Error(`required route suite did not report tests: ${name}`);
  return rows;
};
const skipped = (rows) => rows.filter((test) => ["skipped", "pending", "todo"].includes(test.status));
const platform = process.platform;

const race = requireFile("task-control-route-race.test.ts");
if (race.some(test => test.status !== "passed")) throw new Error(`route race suite must execute every test: ${race.filter(test => test.status !== "passed").map(test => test.name).join("; ")}`);

const residual = requireFile("task-control-route-residual.test.ts");
if (platform === "linux") {
  if (skipped(residual).length) throw new Error(`Linux residual probes unexpectedly skipped: ${skipped(residual).map((test) => test.name).join("; ")}`);
  const probes = residual.filter((test) => /task\.txt must not land|victim file must remain unchanged/.test(test.name));
  if (probes.length !== 3 || probes.some((test) => test.status !== "passed")) throw new Error("all three hosted F0/F1 expected-failure probes must execute on Linux");
} else {
  const expected = skipped(residual);
  if (expected.length !== 3 || expected.some((test) => !/expected skip: F[01].+requires enforced Linux Landlock/.test(test.name))) {
    throw new Error(`expected exactly the three declared Linux-only residual skips, got: ${expected.map((test) => test.name).join("; ")}`);
  }
}

const session = requireFile("task-control-route-session-identity.test.ts");
const handoff = session.filter((test) => /post-verification leaf swap/i.test(test.name));
if (handoff.length !== 1) throw new Error("session suite must expose exactly one 'post-verification leaf swap' handoff row");
if (platform === "linux" && handoff[0].status !== "passed") throw new Error("post-verification leaf swap must pass on Linux");
if (platform !== "linux" && handoff[0].status !== "skipped") throw new Error("post-verification leaf swap must be the declared W-1 skip on darwin/win32");
const sessionSkips = skipped(session);
if (sessionSkips.length !== (platform === "linux" ? 0 : 1) || sessionSkips.some(test => test !== handoff[0])) {
  throw new Error("unexpected session skips: only the non-Linux W-1 handoff skip is allowed");
}
