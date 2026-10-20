import { execFileSync, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
// @ts-expect-error The dependency-free JavaScript CLI intentionally ships no TypeScript declaration.
import { scanText } from "../scripts/check-public-privacy.mjs";

// Trust-boundary/data-safety coverage: execute the actual dependency-free Node CLI
// against a private Git fixture, including a newly tracked file after its first pass.
// Test data is assembled so this public test does not itself publish private tokens.
const guard = path.resolve("scripts/check-public-privacy.mjs");
const alias = ["ryzen", 3].join("");
const domain = ["smarty", "pants", "ai"].join(".").replace("smarty.pants", "smartypants");
const fqdn = [alias, domain].join(".");
const tail = ["machine", "tailabcdef", "ts", "net"].join(".");
const ip = (...parts: number[]) => parts.join(".");
const sha256 = (value: string) => createHash("sha256").update(value, "utf8").digest("hex");
const document = (exceptions: unknown[] = []) => ({ version: 1, followup: "smarty-dev#14658: shrink exceptions to zero", literalPins: "SHA-256 of exact matched UTF-8 token; identifies original literals without duplicating private values.", exceptions });
const entry = (file = "fixture.txt") => ({ file, rules: ["fleet-alias"], kind: "synthetic-fixture", reason: "Synthetic fixture identity for this exact named file." });
const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.toUpperCase().startsWith("GIT_")));

function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "public-privacy-guard-"));
  const git = (...args: string[]) => execFileSync("git", ["-C", root, ...args], { env, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
  const put = (file: string, text: string, tracked = true) => {
    fs.mkdirSync(path.dirname(path.join(root, file)), { recursive: true });
    fs.writeFileSync(path.join(root, file), text);
    if (tracked) git("add", "--", file);
  };
  git("init", "--quiet");
  put("scripts/public-privacy-exceptions.json", JSON.stringify(document()));
  put("fixture.txt", "public fixture\n");
  git("-c", "user.name=Privacy Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--quiet", "-m", "fixture");
  const runWithEnv = (overrides: Record<string, string> = {}, ...args: string[]) => {
    const result = spawnSync(process.execPath, [guard, ...args], { cwd: root, env: { ...env, ...overrides }, encoding: "utf8", timeout: 15_000 });
    expect(result.error).toBeUndefined();
    return { code: result.status, output: result.stdout + result.stderr };
  };
  const run = (...args: string[]) => runWithEnv({}, ...args);
  return { root, put, run, runWithEnv, git };
}

describe("public privacy guard actual CLI", () => {
  it("ignores untracked files and catches newly indexed bytes even after a clean worktree replacement", () => {
    const f = fixture();
    expect(f.run().code).toBe(0);
    f.put("new.txt", fqdn, false);
    expect(f.run().code).toBe(0);
    f.git("add", "--", "new.txt");
    expect(f.run()).toMatchObject({ code: 1, output: expect.stringContaining("new.txt:1: org-fqdn") });
    f.put("new.txt", "public replacement", false);
    expect(f.run().code).toBe(1);
  });

  it.each(["private", "missing", "directory"])("scans a clean index despite a %s worktree replacement", kind => {
    const f = fixture();
    const file = path.join(f.root, "fixture.txt");
    if (kind === "private") fs.writeFileSync(file, fqdn);
    else {
      fs.renameSync(file, path.join(f.root, "saved.txt"));
      if (kind === "directory") fs.mkdirSync(file);
    }
    expect(f.run().code).toBe(0);
  });

  it.each(["private", "missing"])("uses indexed policy despite %s worktree policy", kind => {
    const f = fixture();
    const file = path.join(f.root, "scripts/public-privacy-exceptions.json");
    if (kind === "private") fs.writeFileSync(file, fqdn);
    else fs.renameSync(file, file + ".saved");
    expect(f.run().code).toBe(0);
  });

  it.each(["scripts/check-public-privacy.mjs", "tests/public-privacy-guard.test.ts"])("never overlays untracked own file %s", file => {
    const f = fixture();
    f.put(file, fqdn, false);
    expect(f.run().code).toBe(0);
  });

  const redirects = ["GIT_INDEX_FILE", "GIT_DIR", "GIT_WORK_TREE", "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_CEILING_DIRECTORIES", "GIT_NAMESPACE", "GIT_CONFIG_COUNT", "GIT_CONFIG_PARAMETERS", "GIT_CONFIG_GLOBAL", "GIT_CONFIG_SYSTEM"];
  it.each(redirects)("cannot hide an actual indexed leak with inherited %s", variable => {
    const f = fixture(), foreign = fixture();
    f.put("leak.txt", fqdn);
    const values: Record<string, string> = {
      GIT_INDEX_FILE: path.join(foreign.root, ".git/index"), GIT_DIR: path.join(foreign.root, ".git"),
      GIT_WORK_TREE: foreign.root, GIT_OBJECT_DIRECTORY: path.join(foreign.root, ".git/objects"),
      GIT_ALTERNATE_OBJECT_DIRECTORIES: path.join(foreign.root, ".git/objects"), GIT_CEILING_DIRECTORIES: f.root,
      GIT_NAMESPACE: "foreign", GIT_CONFIG_COUNT: "1", GIT_CONFIG_PARAMETERS: "'core.bare=true'",
      GIT_CONFIG_GLOBAL: path.join(foreign.root, "hostile-config"), GIT_CONFIG_SYSTEM: path.join(foreign.root, "hostile-config"),
    };
    fs.writeFileSync(path.join(foreign.root, "hostile-config"), "[core]\n bare = true\n");
    expect(f.runWithEnv({ [variable]: values[variable], ...(variable === "GIT_CONFIG_COUNT" ? { GIT_CONFIG_KEY_0: "core.bare", GIT_CONFIG_VALUE_0: "true" } : {}) }).code).toBe(1);
  });

  it.each(redirects)("cannot reject an actual clean index with inherited %s", variable => {
    const f = fixture(), foreign = fixture();
    foreign.put("fixture.txt", fqdn);
    const hostile = path.join(foreign.root, "hostile-config");
    fs.writeFileSync(hostile, "[core]\n bare = true\n");
    const values: Record<string, string> = {
      GIT_INDEX_FILE: path.join(foreign.root, ".git/index"), GIT_DIR: path.join(foreign.root, ".git"),
      GIT_WORK_TREE: foreign.root, GIT_OBJECT_DIRECTORY: path.join(foreign.root, ".git/objects"),
      GIT_ALTERNATE_OBJECT_DIRECTORIES: path.join(foreign.root, ".git/objects"), GIT_CEILING_DIRECTORIES: f.root,
      GIT_NAMESPACE: "foreign", GIT_CONFIG_COUNT: "1", GIT_CONFIG_PARAMETERS: "'core.bare=true'",
      GIT_CONFIG_GLOBAL: hostile, GIT_CONFIG_SYSTEM: hostile,
    };
    expect(f.runWithEnv({ [variable]: values[variable], ...(variable === "GIT_CONFIG_COUNT" ? { GIT_CONFIG_KEY_0: "core.bare", GIT_CONFIG_VALUE_0: "true" } : {}) }).code).toBe(0);
  });

  it.each(["git_index_file", "gIt_InDeX_fIlE", "git_dir", "gIt_DiR", "git_config_count", "gIt_CoNfIg_CoUnT"])("scrubs case-insensitive Windows redirect %s for an indexed leak", variable => {
    const f = fixture(), foreign = fixture();
    f.put("leak.txt", fqdn);
    const value = variable.toUpperCase() === "GIT_INDEX_FILE" ? path.join(foreign.root, ".git/index")
      : variable.toUpperCase() === "GIT_DIR" ? path.join(foreign.root, ".git") : "1";
    expect(f.runWithEnv({ [variable]: value, git_config_key_0: "core.bare", git_config_value_0: "true" }).code).toBe(1);
  });

  it.each(["git_index_file", "gIt_InDeX_fIlE", "git_dir", "gIt_DiR", "git_config_count", "gIt_CoNfIg_CoUnT"])("scrubs case-insensitive Windows redirect %s for a clean index", variable => {
    const f = fixture(), foreign = fixture();
    foreign.put("fixture.txt", fqdn);
    const value = variable.toUpperCase() === "GIT_INDEX_FILE" ? path.join(foreign.root, ".git/index")
      : variable.toUpperCase() === "GIT_DIR" ? path.join(foreign.root, ".git") : "1";
    expect(f.runWithEnv({ [variable]: value, git_config_key_0: "core.bare", git_config_value_0: "true" }).code).toBe(0);
  });

  it.each(["symlink", "unmerged"])("fails closed for an indexed %s without filesystem symlink privileges", kind => {
    const f = fixture();
    const blob = f.git("rev-parse", ":fixture.txt").trim();
    if (kind === "symlink") f.git("update-index", "--cacheinfo", `120000,${blob},fixture.txt`);
    else execFileSync("git", ["-C", f.root, "update-index", "--index-info"], {
      env, input: `0 ${"0".repeat(40)}\tfixture.txt\n100644 ${blob} 1\tfixture.txt\n100644 ${blob} 2\tfixture.txt\n`, stdio: ["pipe", "pipe", "pipe"],
    });
    expect(f.run()).toMatchObject({ code: 2, output: expect.stringContaining("fail closed") });
  });

  it("never republishes private content but retains location and rule", () => {
    const f = fixture();
    const canary = ["content-canary", fqdn].join("-");
    f.put("new.txt", "safe\n" + canary);
    const result = f.run();
    expect(result).toMatchObject({ code: 1, output: expect.stringContaining("new.txt:2: org-fqdn") });
    for (const token of [canary, fqdn, alias, domain]) expect(result.output).not.toContain(token);
  });

  it("uses opaque diagnostics for private filenames", () => {
    const f = fixture();
    f.put(fqdn, "public");
    const result = f.run();
    expect(result).toMatchObject({ code: 1, output: expect.stringContaining(":0: org-fqdn") });
    for (const token of [fqdn, alias, domain]) expect(result.output).not.toContain(token);
  });

  it.each(["json", "exception", "git", "grep"])("redacts %s error canaries while failing closed", kind => {
    const f = fixture();
    const canary = ["error-canary", fqdn].join("-");
    if (kind === "grep") f.git("config", "grep.threads", canary);
    if (kind === "json") f.put("scripts/public-privacy-exceptions.json", canary);
    if (kind === "exception") f.put("scripts/public-privacy-exceptions.json", JSON.stringify(document([entry(canary)])));
    if (kind === "git") {
      fs.renameSync(path.join(f.root, ".git"), path.join(f.root, ".git-saved"));
      fs.writeFileSync(path.join(f.root, ".git"), "gitdir: " + canary);
    }
    const result = f.run();
    expect(result).toMatchObject({ code: 2, output: expect.stringContaining("fail closed") });
    for (const token of [canary, fqdn, alias, domain]) expect(result.output).not.toContain(token);
  });

  it("parses NUL-containing matched lines without losing addresses or later lines", () => {
    const f = fixture();
    f.put("new.txt", "safe\n\0" + alias + "\0" + fqdn + "\0" + ip(8, 8, 4, 4) + "\n" + tail);
    const result = f.run();
    expect(result.code).toBe(1);
    for (const location of ["new.txt:2: fleet-alias", "new.txt:2: org-fqdn", "new.txt:2: ip-address", "new.txt:3: org-fqdn"]) expect(result.output).toContain(location);
  });

  it("suppresses repository grep color, column, heading, and break settings", () => {
    const f = fixture();
    for (const [key, value] of [["color.grep", "always"], ["grep.column", "true"], ["grep.lineNumber", "false"], ["grep.heading", "true"], ["grep.break", "true"], ["grep.fullName", "false"]] as const) f.git("config", key, value);
    f.put("new.txt", "safe\n" + fqdn);
    expect(f.run()).toMatchObject({ code: 1, output: expect.stringContaining("new.txt:2: org-fqdn\n") });
  });

  it.each([
    ["principal home", ["", "home", "paul", "lane"].join("/")],
    ["principal scratch", ["", "srv", "scratch", "paul", "task"].join("/")],
    ["exact home boundary", ["", "home", "paul"].join("/")],
    ["organization FQDN", fqdn], ["organization domain", domain], ["tailnet FQDN", tail],
    ["absolute FQDN", fqdn.toUpperCase() + "."], ["IPv4 sentence punctuation", ip(8, 8, 4, 4) + "."],
    ["NUL-hidden alias", "\0" + alias],
    ["tailnet IPv4", ip(100, 78, 65, 112)], ["other private IPv4", ip(10, 42, 43, 44)],
    ["public IPv4", ip(8, 8, 4, 4)], ["public IPv6", ["2606", "4700", "4700", "", "1111"].join(":")],
    ["tailnet IPv6", ["fd7a", "115c", "a1e0", "", "1234"].join(":")],
    ["IPv6 followed by punctuation", ["2606", "4700", "4700", "", "1111"].join(":") + "."],
    ["IPv6 followed by opening bracket", ["2606", "4700", "4700", "", "1111"].join(":") + "["],
    ["bare alias", alias], ["case-insensitive alias", alias.toUpperCase()],
    ["alias hostname suffix", alias + "-agent.example.com"],
  ])("rejects %s in a newly tracked file", (_label, token) => {
    const f = fixture();
    f.put("new.txt", token);
    expect(f.run()).toMatchObject({ code: 1, output: expect.stringContaining("new.txt:1:") });
  });

  it("rejects the removed mixed-policy ref mode and unknown arguments", () => {
    const f = fixture();
    expect(f.run("--ref", f.git("rev-parse", "HEAD").trim()).code).toBe(2);
    expect(f.run("--ref", "not-a-sha").code).toBe(2);
    expect(f.run("--ignore", "anything").code).toBe(2);
  });

  it("covers every known fleet alias and hostname suffix, without a broad word wildcard", () => {
    const f = fixture();
    const aliases = [
      ...Array.from({ length: 3 }, (_, i) => "dev" + (i + 1)),
      ...Array.from({ length: 7 }, (_, i) => "ryzen" + (i + 1)),
      ...Array.from({ length: 2 }, (_, i) => "epyc" + (i + 1)),
      "qs" + 32729, "intel" + 1, "m" + 4, "m" + 5, "i" + 9,
    ];
    f.put("new.txt", aliases.map(name => name.toUpperCase() + "-agent").join("\n"));
    const result = f.run();
    expect(result.code).toBe(1);
    expect((result.output.match(/: fleet-alias\n/g) ?? []).length).toBe(aliases.length);
  });

  it("allows only intended placeholders, specific synthetic private values, and generic scratch", () => {
    const f = fixture();
    f.put("allowed.txt", [
      "example.com host.example.com host.invalid host.test",
      ["", "srv", "scratch"].join("/"), ["", "srv", "scratch", "generic-work"].join("/"),
      ["", "home", "paulina"].join("/"), ["", "srv", "scratch", "paul-backup"].join("/"),
      ip(192, 0, 2, 77), ip(198, 51, 100, 88), ip(203, 0, 113, 99),
      ip(127, 9, 8, 7), ip(0, 0, 0, 0), ip(10, 0, 0, 5), ip(192, 168, 1, 2),
      ["2001", "db8", "", "abcd"].join(":"), "::1", "::",
      "::ffff:" + ip(127, 0, 0, 1), "::ffff:7f00:1", "0:0:0:0:0:ffff:7fff:fffe",
      "ack.1.1.1.1.0", "identifier" + alias + "suffix",
    ].join("\n"));
    const result = f.run();
    expect(result.code, result.output).toBe(0);
  });

  it("permits a named-file baseline alias but never a new unlisted file or other rules", () => {
    const f = fixture();
    f.put("scripts/public-privacy-exceptions.json", JSON.stringify(document([entry()])));
    f.put("fixture.txt", alias);
    expect(f.run().code).toBe(0);
    f.put("new.txt", alias);
    expect(f.run().code).toBe(1);
    f.put("new.txt", "public");
    f.put("fixture.txt", fqdn);
    expect(f.run().code).toBe(1);
  });

  it("scopes address-shaped software versions to an exact named file and literal", () => {
    const f = fixture();
    const literal = ip(2, 14, 7, 1);
    const approval = { ...entry(), rules: ["ip-address"], literalSha256: sha256(literal) };
    f.put("scripts/public-privacy-exceptions.json", JSON.stringify(document([approval])));
    f.put("fixture.txt", literal);
    expect(f.run().code).toBe(0);
    f.put("new.txt", literal);
    expect(f.run().code).toBe(1);
    f.put("new.txt", "public");
    f.put("fixture.txt", ip(8, 8, 4, 4));
    expect(f.run().code).toBe(1);
  });

  it("matches optional exact-line exceptions without permitting different lines", () => {
    const f = fixture();
    const policy = JSON.stringify(document([{ ...entry(), line: `fixture: ${alias}` }]))
      .replaceAll(alias, [...alias].map(c => "\\u" + c.charCodeAt(0).toString(16).padStart(4, "0")).join(""));
    f.put("scripts/public-privacy-exceptions.json", policy);
    f.put("fixture.txt", `fixture: ${alias}\n`);
    expect(f.run().code).toBe(0);
    f.put("fixture.txt", `new: ${alias}\n`);
    expect(f.run().code).toBe(1);
  });

  it("allows approved FQDN identity literals only in their exact approved file", () => {
    const f = fixture();
    const file = "scripts/lib/native-placement-proof.mjs";
    f.put(file, fqdn);
    const approval = { file, rules: ["org-fqdn"], kind: "identity-boundary", reason: "Lead-approved exact identity recognition contract #14658.", literalSha256: sha256(fqdn) };
    // Hash exact tokens: the public policy does not copy private identity literals.
    const json = JSON.stringify(document([entry(file), approval]));
    f.put("scripts/public-privacy-exceptions.json", json);
    expect(f.run().code).toBe(0);
    f.put(file, ["other", domain].join("."));
    expect(f.run().code).toBe(1);
    f.put(file, fqdn);
    f.put("new.txt", fqdn);
    expect(f.run().code).toBe(1);
  });

  it.each([
    { ...entry(), file: "tests/*" }, { ...entry(), file: "tests/**" }, { ...entry(), file: "tests/" },
    { ...entry(), file: "../outside" }, { ...entry(), file: "/outside" }, { ...entry(), file: "tests/[ab].txt" },
    { ...entry(), file: "tests/?.txt" }, { ...entry(), file: "tests/{a,b}.txt" },
    { ...entry(), rules: ["ip-address"] }, { ...entry(), rules: ["unknown"] },
    { ...entry(), rules: [] }, { ...entry(), reason: "" }, { ...entry(), kind: "anything" },
    { ...entry(), glob: true }, { ...entry(), line: "" }, { ...entry(), line: "one\ntwo" },
    { ...entry(), rules: ["ip-address"], literalSha256: "a".repeat(63) },
    { ...entry(), rules: ["ip-address"], literalSha256: "G".repeat(64) },
    { ...entry(), rules: ["ip-address"], literal: "not-a-pin" },
    { ...entry("scripts/lib/native-placement-proof.mjs"), rules: ["org-fqdn"], kind: "identity-boundary" },
  ])("fails closed for malformed/wildcard exceptions %#", exception => {
    const f = fixture();
    f.put("scripts/public-privacy-exceptions.json", JSON.stringify(document([exception])));
    expect(f.run()).toMatchObject({ code: 2, output: expect.stringContaining("fail closed") });
  });

  it("fails closed when Git silently treats a missing index as an empty tracked tree", () => {
    const f = fixture();
    fs.renameSync(path.join(f.root, ".git", "index"), path.join(f.root, ".git", "saved-index"));
    expect(f.run()).toMatchObject({ code: 2, output: expect.stringContaining("fail closed") });
  });

  it("fails closed for malformed indexed JSON and Git errors, not missing worktree files", () => {
    const f = fixture();
    f.put("scripts/public-privacy-exceptions.json", "{");
    expect(f.run().code).toBe(2);
    f.put("scripts/public-privacy-exceptions.json", JSON.stringify(document()));
    fs.renameSync(path.join(f.root, "fixture.txt"), path.join(f.root, "saved.txt"));
    expect(f.run().code).toBe(0);
    fs.mkdirSync(path.join(f.root, "fixture.txt"));
    expect(f.run().code).toBe(0);
    fs.renameSync(path.join(f.root, ".git"), path.join(f.root, ".git-disabled"));
    expect(f.run().code).toBe(2);
  });
});

describe("complete address tokens and binary false positives", () => {
  it("keeps complete mapped-loopback tokens and leaves regex constructs to exact-file exceptions", () => {
    for (const token of ["::ffff:" + ip(127, 0, 0, 1), "::ffff:7f00:1", "::ffff:7fXX:XXXX"]) {
      expect(scanText(token)).toEqual([]);
    }
    const prefix = ["", "", "ffff", "7f"].join(":");
    expect(scanText("/^" + prefix + "[0-9a-f]{2}:/")).toMatchObject([{ rule: "ip-address", value: prefix }]);
    const publicMapped = ["", "", "ffff", "8000", "1"].join(":");
    expect(scanText(publicMapped)).toMatchObject([{ rule: "ip-address", value: publicMapped }]);
  });
  it("does not let a NUL hide aliases, addresses, or domains", () => {
    const short = ["8", "", ""].join(":");
    expect(scanText("\0binary " + short + " " + alias)).toEqual(expect.arrayContaining([
      expect.objectContaining({ rule: "fleet-alias" }), expect.objectContaining({ rule: "ip-address" }),
    ]));
    expect(scanText("\0binary " + fqdn)).toEqual(expect.arrayContaining([expect.objectContaining({ rule: "org-fqdn" })]));
  });
  it("never needs an exception for its own guard, policy, or test bytes", () => {
    for (const file of [guard, path.resolve("scripts/public-privacy-exceptions.json"), path.resolve("tests/public-privacy-guard.test.ts")]) {
      expect(scanText(fs.readFileSync(file, "utf8")), file).toEqual([]);
    }
  });
});
