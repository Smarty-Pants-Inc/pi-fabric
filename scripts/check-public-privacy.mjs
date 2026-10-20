#!/usr/bin/env node
// Public-tree privacy gate. smarty-dev#14658 tracks shrinking the named exceptions to zero.
// No dependencies, directory exclusions, network access, or Git writes.
import path from "node:path";
import { devNull } from "node:os";
import { spawnSync } from "node:child_process";
import { isIP } from "node:net";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";

const literalPins = "SHA-256 of exact matched UTF-8 token; identifies original literals without duplicating private values.";
const tokenHash = value => createHash("sha256").update(value, "utf8").digest("hex");
const validHash = value => typeof value === "string" && /^[a-f\d]{64}$/.test(value);
const ruleNames = new Set(["fleet-path", "org-fqdn", "ip-address", "fleet-alias"]);
const kinds = new Set(["synthetic-fixture", "historical-doc", "identity-boundary"]);
const aliasPattern = /(?<![a-z\d])(?:dev[1-3]|ryzen[1-7]|epyc[12]|qs3272[9]|intel[1]|m[45]|i[9])(?![a-z\d])/gi;
const pathPattern = /\/(?:home|srv\/scratch)\/paul(?=$|[^a-z\d_.-])/gi;
const domainPattern = /(?<![a-z\d-])(?:[a-z\d-]+\.)*(?:smartypants\.ai|tail[a-z\d-]+\.ts\.net)(?![a-z\d-]|\.[a-z\d-])/gi;
const ipv4Pattern = /(?<![\w.])(?:\d{1,3}\.){3}\d{1,3}(?![\w]|\.\d)/g;
const ipv6Pattern = /(?<![\w:.])[a-f\d:.]*:[a-f\d:.]*:[a-f\d:.]*(?:%[a-z\d]+)?(?![\w:.])/gi;
// Exact pre-existing synthetic RFC1918 fixture values, not a private-network exemption.
const syntheticV4 = new Set([[10, 0, 0, 1], [10, 0, 0, 2], [10, 0, 0, 5], [10, 0, 0, 6], [10, 1, 1, 1], [10, 9, 9, 9], [192, 168, 0, 1], [192, 168, 1, 2]].map(parts => parts.join(".")));

function allowedV4(value) {
  const [a, b, c, d] = value.split(".").map(Number);
  return syntheticV4.has(value) || a === 127 || (a === 0 && b === 0 && c === 0 && d === 0)
    || (a === 192 && b === 0 && c === 2) || (a === 198 && b === 51 && c === 100) || (a === 203 && b === 0 && c === 113);
}
function allowedIP(value) {
  if (isIP(value) === 4) return allowedV4(value);
  if (isIP(value) !== 6) return false;
  const host = new URL(`http://[${value.split("%")[0]}]/`).hostname.slice(1, -1);
  if (host === "::" || host === "::1" || /^2001:db8:/i.test(host)) return true;
  const mapped = /^::ffff:([a-f\d]+):([a-f\d]+)$/i.exec(host);
  if (!mapped) return false;
  const high = Number.parseInt(mapped[1], 16), low = Number.parseInt(mapped[2], 16);
  return allowedV4([high >> 8, high & 255, low >> 8, low & 255].join("."));
}

/** Findings contain the matched token, not entire potentially private source lines. */
export function scanText(text) {
  const findings = [];
  const add = (rule, match) => findings.push({ rule, value: match[0], line: text.slice(0, match.index).split("\n").length });
  for (const [rule, pattern] of [["fleet-path", pathPattern], ["org-fqdn", domainPattern], ["fleet-alias", aliasPattern]]) {
    for (const match of text.matchAll(pattern)) add(rule, match);
  }
  // Scan every tracked byte stream with the same policy; a NUL is not an escape hatch.
  for (const pattern of [ipv4Pattern, ipv6Pattern]) {
    for (const match of text.matchAll(pattern)) {
      if (pattern === ipv6Pattern) match[0] = match[0].replace(/\.$/, "");
      if (isIP(match[0]) && !allowedIP(match[0])) add("ip-address", match);
    }
  }
  return findings;
}

function exactKeys(value, required, optional = []) {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    && required.every(key => Object.hasOwn(value, key))
    && Object.keys(value).every(key => required.includes(key) || optional.includes(key));
}
function namedFile(file) {
  return typeof file === "string" && file.length > 0 && !/[\\*?\[\]{}!\x00-\x1f]/.test(file)
    && !path.posix.isAbsolute(file) && !/^[a-z]:/i.test(file)
    && file.split("/").every(part => part !== "" && part !== "." && part !== "..");
}
export function validateExceptions(document) {
  if (!exactKeys(document, ["version", "followup", "literalPins", "exceptions"]) || document.version !== 1
    || document.followup !== "smarty-dev#14658: shrink exceptions to zero" || document.literalPins !== literalPins || !Array.isArray(document.exceptions)) {
    throw new Error("Invalid privacy exceptions document");
  }
  const seen = new Set();
  for (const entry of document.exceptions) {
    if (!exactKeys(entry, ["file", "rules", "kind", "reason"], ["line", "literalSha256"]) || !namedFile(entry.file)
      || !Array.isArray(entry.rules) || entry.rules.length === 0 || new Set(entry.rules).size !== entry.rules.length
      || entry.rules.some(rule => !ruleNames.has(rule) || !["fleet-alias", "org-fqdn", "ip-address"].includes(rule))
      || !kinds.has(entry.kind) || typeof entry.reason !== "string" || entry.reason.trim().length < 12
      || (Object.hasOwn(entry, "line") && (typeof entry.line !== "string" || !entry.line.trim() || /[\r\n]/.test(entry.line)))) {
      throw new Error("Invalid privacy exception: expected one named file or exact line, rules, kind, and reason");
    }
    // Lead-approved identity contracts are scoped to exact file AND exact literal.
    if (entry.rules.includes("org-fqdn")) {
      if (!["scripts/lib/native-placement-proof.mjs", "tests/native-placement-proof.test.ts"].includes(entry.file)
        || entry.kind !== "identity-boundary" || entry.rules.length !== 1 || Object.hasOwn(entry, "line")
        || !validHash(entry.literalSha256) || !entry.reason.includes("#14658")) {
        throw new Error("Unapproved identity FQDN exception: exact file and literal required");
      }
    } else if (entry.rules.includes("ip-address")) {
      if (entry.rules.length !== 1 || !validHash(entry.literalSha256)
        || Object.hasOwn(entry, "line") || !["synthetic-fixture", "historical-doc"].includes(entry.kind)) {
        throw new Error("Address constructs require an exact file and valid exact literal with a fixture/history reason");
      }
    } else if (Object.hasOwn(entry, "literalSha256")) throw new Error("Literal pins require an identity or address-construct rule");
    const key = JSON.stringify([entry.file, entry.line ?? null, entry.literalSha256 ?? null, [...entry.rules].sort()]);
    if (seen.has(key)) throw new Error("Duplicate privacy exception");
    seen.add(key);
  }
  return document.exceptions;
}

function git(root, args, allowNoMatch = false) {
  // Bind every operation to this cwd, never an inherited repository or index.
  const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.toUpperCase().startsWith("GIT_")));
  Object.assign(env, { LC_ALL: "C", LANG: "C", GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: devNull,
    GIT_TERMINAL_PROMPT: "0", GIT_NO_LAZY_FETCH: "1", GIT_OPTIONAL_LOCKS: "0" });
  const result = spawnSync("git", ["--no-pager", "-c", "color.ui=false", ...args], {
    cwd: root, env, encoding: "utf8", maxBuffer: 32 * 1024 * 1024, timeout: 15_000, stdio: ["ignore", "pipe", "pipe"],
  });
  // Neither Git stderr nor spawn errors are safe to republish.
  if (result.error || (result.status !== 0 && !(allowNoMatch && result.status === 1))) throw new Error("Git operation failed");
  return result.stdout;
}

// Index-only contract: no worktree content, untracked overlays, or mixed-policy --ref mode.
// Replay historical trees in a private indexed fixture instead.
export function checkTree(root, exceptionsFile, ref) {
  if (ref !== undefined) throw new Error("Only the current index is supported");
  root = path.resolve(git(root, ["rev-parse", "--show-toplevel"]).replace(/\n$/, ""));
  const head = git(root, ["rev-parse", "HEAD"]).trim();
  if (!/^[a-f\d]{40,64}$/.test(head)) throw new Error("Invalid Git HEAD");
  const index = git(root, ["ls-files", "--stage", "-z"]);
  const entries = index.split("\0").filter(Boolean);
  if (!entries.length) throw new Error("No tracked files");
  const files = new Set();
  for (const entry of entries) {
    const match = /^(100644|100755) [a-f\d]+ 0\t([\s\S]+)$/.exec(entry);
    if (!match || !namedFile(match[2])) throw new Error("Unsupported, unmerged, or unsafe tracked entry");
    files.add(match[2]);
  }
  const relativeExceptions = exceptionsFile === undefined ? "scripts/public-privacy-exceptions.json"
    : path.relative(root, path.resolve(exceptionsFile)).split(path.sep).join("/");
  if (!namedFile(relativeExceptions) || !files.has(relativeExceptions)) throw new Error("Exceptions must be a named indexed file");
  const exceptions = validateExceptions(JSON.parse(git(root, ["show", `:${relativeExceptions}`])));
  for (const entry of exceptions) if (!files.has(entry.file)) throw new Error("Exception does not name an indexed file");
  const findings = [];
  let excepted = 0;
  const displayFile = file => scanText(file).length ? `file-${tokenHash(file)}` : file;
  for (const file of [...files].sort()) {
    // Content exceptions never hide private filenames; the identifier must not repeat them.
    for (const finding of scanText(file)) findings.push({ file: displayFile(file), rule: finding.rule, line: 0 });
  }
  // Deliberately broad candidate filter; scanText remains the sole rule/boundary authority.
  // -a is essential: a NUL must never suppress candidate lines in any byte stream.
  const matches = git(root, ["grep", "--cached", "-n", "-a", "-z", "-i", "-E", "--no-color", "--no-column",
    "--no-heading", "--no-break", "--full-name", "-e", "[0-9:]|paul|smartypants|tail", "--"], true);
  let offset = 0;
  while (offset < matches.length) {
    // -z separates filename and line number only. The matched line itself may contain NULs.
    const nameEnd = matches.indexOf("\0", offset), numberEnd = matches.indexOf("\0", nameEnd + 1);
    const lineEnd = matches.indexOf("\n", numberEnd + 1);
    if (nameEnd < offset || numberEnd < nameEnd || lineEnd < numberEnd) throw new Error("Invalid Git grep record");
    const file = matches.slice(offset, nameEnd), number = matches.slice(nameEnd + 1, numberEnd);
    if (!files.has(file) || !/^[1-9]\d*$/.test(number)) throw new Error("Invalid Git grep location");
    const line = Number(number), text = matches.slice(numberEnd + 1, lineEnd);
    if (!Number.isSafeInteger(line)) throw new Error("Invalid Git grep line");
    for (const finding of scanText(text)) {
      if (exceptions.some(entry => entry.file === file && entry.rules.includes(finding.rule)
        && (!Object.hasOwn(entry, "line") || entry.line === text)
        && (!Object.hasOwn(entry, "literalSha256") || entry.literalSha256 === tokenHash(finding.value)))) excepted++;
      else findings.push({ file: displayFile(file), rule: finding.rule, line });
    }
    offset = lineEnd + 1;
  }
  if (head !== git(root, ["rev-parse", "HEAD"]).trim() || index !== git(root, ["ls-files", "--stage", "-z"])) {
    throw new Error("Git snapshot changed during privacy scan");
  }
  return { head, files: files.size, excepted, findings };
}

function main() {
  try {
    if (process.argv.length !== 2) throw new Error("Only the current index is supported");
    const result = checkTree(process.cwd());
    for (const finding of result.findings) console.error(`${finding.file}:${finding.line}: ${finding.rule}`);
    const summary = `public-privacy: ${result.findings.length ? "FAIL" : "PASS"}; head=${result.head}; ref=index; files=${result.files}; findings=${result.findings.length}; excepted=${result.excepted}`;
    if (result.findings.length) { console.error(summary); process.exitCode = 1; }
    else console.log(summary);
  } catch {
    console.error("public-privacy: ERROR (fail closed): unable to validate indexed snapshot or policy");
    process.exitCode = 2;
  }
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) main();
