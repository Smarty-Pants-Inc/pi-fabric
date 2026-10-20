import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import type { AgentTransportLaunch } from "../../agents/types.js";
import type { SandboxComplexity, SandboxProfile, SandboxSpawnIntent } from "./types.js";

const RUN_ID = /^[a-z0-9](?:[a-z0-9-]{0,55}[a-z0-9])?$/;
const NAME = /^[A-Za-z0-9][A-Za-z0-9_.:/@+-]{0,255}$/;
const THINKING = new Set(["off", "minimal", "low", "medium", "high", "xhigh", "max"]);
const COMPLEXITIES = new Set<SandboxComplexity>(["simple", "normal", "complex", "delicate"]);
const PROFILES = new Set<SandboxProfile>(["lead", "lane", "task", "org", "actor"]);

export interface SandboxRepositoryContext {
  repository: string;
  cwd: string;
}

export interface SandboxIntentInput {
  runId: string;
  cwd: string;
  needs: readonly string[];
  complexity?: SandboxComplexity;
  profile?: SandboxProfile;
  parentSession: string;
  org: string;
  timeoutMs: number;
  now: number;
  workerArguments: readonly string[];
  resolveRepository?: (cwd: string) => SandboxRepositoryContext;
}

const argument = (args: readonly string[], name: string): string | undefined => {
  for (let index = 0; index < args.length; index += 2) {
    if (args[index] === name) return args[index + 1];
  }
  return undefined;
};

const name = (value: string, field: string): string => {
  if (!NAME.test(value)) throw new Error(`Invalid sandbox intent ${field}`);
  return value;
};

const repositoryUrl = (remote: string): string => {
  let candidate = remote.trim();
  const scp = /^git@([A-Za-z0-9.-]+):([^/?#]+\/[^/?#]+)$/u.exec(candidate);
  const ssh = /^ssh:\/\/git@([A-Za-z0-9.-]+)\/([^?#]+)$/u.exec(candidate);
  if (scp) candidate = `https://${scp[1]}/${scp[2]}`;
  else if (ssh) candidate = `https://${ssh[1]}/${ssh[2]}`;

  let url: URL;
  try { url = new URL(candidate); } catch { throw new Error("Sandbox repository must be a credential-free HTTPS repository URL"); }
  const segments = url.pathname.slice(1).split("/");
  if (url.protocol !== "https:" || url.username || url.password || url.port || url.search || url.hash ||
    url.pathname.includes("%") || segments.length !== 2 || segments.some(segment =>
      !/^[A-Za-z0-9_-][A-Za-z0-9_.-]*$/u.test(segment) || segment === "." || segment === "..")) {
    throw new Error("Sandbox repository must be a credential-free HTTPS host/org/repo URL");
  }
  const normalized = `https://${url.hostname.toLowerCase()}/${segments[0]}/${segments[1]}`;
  if (normalized.length > 512) throw new Error("Sandbox repository URL is too long");
  return normalized;
};

export const resolveSandboxRepository = (cwd: string): SandboxRepositoryContext => {
  const root = fs.realpathSync(execFileSync("git", ["-C", cwd, "rev-parse", "--show-toplevel"], { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim());
  const remote = execFileSync("git", ["-C", root, "config", "--get", "remote.origin.url"], { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim();
  const workdir = fs.realpathSync(cwd);
  const relative = path.relative(root, workdir).split(path.sep).join("/");
  if (relative === ".." || relative.startsWith("../") || path.isAbsolute(relative)) throw new Error("Sandbox cwd must be inside its Git repository");
  const repoCwd = relative || ".";
  if (repoCwd.length > 1_024 || repoCwd !== "." && repoCwd.split("/").some(segment =>
    segment === "." || segment === ".." || !/^[A-Za-z0-9_.@+-][A-Za-z0-9_.@+ -]*$/u.test(segment) || segment.endsWith(" "))) {
    throw new Error("Sandbox cwd must be a normalized repository-relative path");
  }
  return { repository: repositoryUrl(remote), cwd: repoCwd };
};

export const buildSandboxIntent = (input: SandboxIntentInput): SandboxSpawnIntent => {
  if (!RUN_ID.test(input.runId)) throw new Error("Invalid sandbox intent runId");
  const org = name(input.org, "org");
  const parentSession = name(input.parentSession, "parentSession");
  const rawModel = argument(input.workerArguments, "--model");
  if (!rawModel) throw new Error("Sandbox intent requires the exact child model");
  const model = name(rawModel, "model");
  const rawThinking = argument(input.workerArguments, "--thinking");
  const thinking = rawThinking ?? "high";
  if (!THINKING.has(thinking)) throw new Error("Invalid sandbox intent thinking");
  const complexity = input.complexity ?? "normal";
  if (!COMPLEXITIES.has(complexity)) throw new Error("Invalid sandbox intent complexity");
  const profile = input.profile ?? "task";
  if (!PROFILES.has(profile)) throw new Error("Invalid sandbox intent profile");
  if (!Number.isSafeInteger(input.timeoutMs) || input.timeoutMs < 1 || input.timeoutMs > 604_800_000 ||
    !Number.isSafeInteger(input.now) || !Number.isSafeInteger(input.now + input.timeoutMs)) {
    throw new Error("Invalid sandbox intent timeout");
  }
  const timeoutSeconds = Math.max(1, Math.ceil(input.timeoutMs / 1_000));
  const deadlineMs = Math.ceil((input.now + input.timeoutMs) / 1_000) * 1_000;
  const deadline = new Date(deadlineMs).toISOString().replace(".000Z", "Z");
  const needs = [...new Set(input.needs.map(need => name(need, "needs")))].sort();
  if (needs.length > 64) throw new Error("Sandbox intent has too many needs");
  const repository = (input.resolveRepository ?? resolveSandboxRepository)(input.cwd);
  return {
    v: 1,
    runId: input.runId,
    kind: "sandbox",
    org,
    parentSession,
    role: "task-agent",
    profile,
    complexity,
    model,
    thinking,
    needs,
    repository: repositoryUrl(repository.repository),
    cwd: repository.cwd,
    expectedSeconds: timeoutSeconds,
    timeoutSeconds,
    deadline,
  };
};
