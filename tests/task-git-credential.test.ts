import { spawn, spawnSync, execFileSync } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import https from "node:https";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { taskAgentEnvironment } from "../src/agents/task-environment.js";

const roots: string[] = [];
const servers: Array<http.Server | https.Server> = [];
const sockets = new Set<{ destroy: () => void }>();
afterEach(async () => {
  for (const socket of sockets) socket.destroy();
  sockets.clear();
  await Promise.all(servers.splice(0).map(server => new Promise<void>(resolve => server.close(() => resolve()))));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const root = (): string => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-task-git-credential-"));
  roots.push(directory);
  return directory;
};

const git = (args: string[], cwd: string, env: NodeJS.ProcessEnv, input?: string | Buffer) =>
  spawnSync("git", args, { cwd, env, input, encoding: "utf8", timeout: 30_000, maxBuffer: 8 * 1024 * 1024 });

const gitAsync = (args: string[], cwd: string, env: NodeJS.ProcessEnv): Promise<{ status: number | null; stdout: string; stderr: string }> =>
  new Promise(resolve => {
    const child = spawn("git", args, { cwd, env, detached: process.platform !== "win32", windowsHide: true });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8"); child.stdout.on("data", chunk => { stdout += chunk; });
    child.stderr.setEncoding("utf8"); child.stderr.on("data", chunk => { stderr += chunk; });
    const timer = setTimeout(() => {
      try {
        if (child.pid && process.platform !== "win32") process.kill(-child.pid, "SIGKILL");
        else child.kill("SIGKILL");
      } catch {}
    }, 30_000);
    timer.unref();
    child.once("error", error => { clearTimeout(timer); resolve({ status: null, stdout, stderr: `${stderr}${error.message}` }); });
    child.once("close", code => { clearTimeout(timer); resolve({ status: code, stdout, stderr }); });
    child.stdin.end();
  });

const requireGit = (args: string[], cwd: string, env: NodeJS.ProcessEnv): void => {
  const result = git(args, cwd, env);
  if (result.status !== 0) throw new Error(`git ${args.join(" ")} failed (${result.status}): ${result.stderr}`);
};

const fixture = () => {
  const directory = root();
  const home = path.join(directory, "home"); fs.mkdirSync(home);
  const callsFile = path.join(directory, "read-token-calls");
  const writeFile = path.join(directory, "write-helper-calls");
  const wrapper = path.join(directory, "fake token wrapper");
  fs.writeFileSync(wrapper, `#!/bin/sh\nprintf '%s\\n' "$*" >> "$FAKE_TOKEN_CALLS"\n[ "\${FAKE_TOKEN_FAIL:-0}" = 1 ] && exit 1\nprintf '%s\\n' 'ghs_read_fixture_token_01234567890123456789'\n`, { mode: 0o700 });
  const writeHelper = path.join(directory, "inherited-write-helper");
  fs.writeFileSync(writeHelper, `#!/bin/sh\nprintf '%s\\n' "$*" >> "$FAKE_WRITE_CALLS"\nprintf '%s\\n' 'username=write-helper' 'password=ghs_write_fixture_token_01234567890123456789'\n`, { mode: 0o700 });
  const globalConfig = path.join(home, ".gitconfig");
  const baseEnv: NodeJS.ProcessEnv = {
    ...process.env,
    HOME: home,
    GIT_CONFIG_GLOBAL: globalConfig,
    GIT_CONFIG_SYSTEM: path.join(directory, "no-system-config"),
    GIT_TERMINAL_PROMPT: "0",
    NO_PROXY: "",
    no_proxy: "",
    HTTPS_PROXY: "",
    https_proxy: "",
    FAKE_TOKEN_CALLS: callsFile,
    FAKE_WRITE_CALLS: writeFile,
    FAKE_TOKEN_FAIL: "0",
    SMARTY_GITHUB_APP_TOKEN_BIN: wrapper,
  };
  execFileSync("git", ["config", "--file", globalConfig, "--add", "credential.helper", writeHelper], { env: baseEnv });
  const parent = {
    ...baseEnv,
    GIT_CONFIG_COUNT: "1",
    GIT_CONFIG_KEY_0: "credential.helper",
    GIT_CONFIG_VALUE_0: writeHelper,
    GIT_CONFIG_PARAMETERS: "'credential.helper=inherited-write-helper'",
  };
  const environment = taskAgentEnvironment(parent);
  return { directory, home, callsFile, writeFile, wrapper, globalConfig, env: environment };
};

const readFileOrEmpty = (file: string): string => fs.existsSync(file) ? fs.readFileSync(file, "utf8") : "";

const readBody = async (request: http.IncomingMessage): Promise<Buffer> => {
  const chunks: Buffer[] = [];
  for await (const chunk of request) chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  return Buffer.concat(chunks);
};

const cgiResponse = (stdout: Buffer): { status: number; headers: Record<string, string>; body: Buffer } | undefined => {
  let divider = stdout.indexOf(Buffer.from("\r\n\r\n"));
  let dividerBytes = 4;
  if (divider < 0) { divider = stdout.indexOf(Buffer.from("\n\n")); dividerBytes = 2; }
  if (divider < 0) return undefined;
  const lines = stdout.subarray(0, divider).toString("latin1").split(/\r?\n/);
  let status = 200;
  const headers: Record<string, string> = {};
  for (const line of lines) {
    const separator = line.indexOf(":");
    if (separator <= 0) continue;
    const name = line.slice(0, separator).trim();
    const value = line.slice(separator + 1).trim();
    if (name.toLowerCase() === "status") status = Number(value.slice(0, 3));
    else headers[name] = value;
  }
  delete headers.Connection;
  delete headers.connection;
  return { status, headers, body: stdout.subarray(divider + dividerBytes) };
};

const localGitHub = async (directory: string, bareRoot: string, token: string) => {
  const source = path.join(directory, "source");
  const bare = path.join(bareRoot, "Smarty-Pants-Inc", "fetch.git");
  fs.mkdirSync(path.dirname(bare), { recursive: true });
  requireGit(["init", source], directory, process.env);
  requireGit(["-C", source, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "config", "--local", "init.defaultBranch", "main"], directory, process.env);
  fs.writeFileSync(path.join(source, "fetched.txt"), "read-only fetch fixture\n");
  requireGit(["-C", source, "add", "fetched.txt"], directory, process.env);
  requireGit(["-C", source, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-m", "fixture"], directory, process.env);
  requireGit(["init", "--bare", "--initial-branch=main", bare], directory, process.env);
  requireGit(["-C", source, "push", bare, "HEAD:refs/heads/main"], directory, process.env);
  requireGit(["--git-dir", bare, "config", "http.receivepack", "true"], directory, process.env);

  const key = path.join(directory, "server.key");
  const cert = path.join(directory, "server.crt");
  execFileSync("openssl", ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", key, "-out", cert,
    "-days", "1", "-subj", "/CN=github.com"], { stdio: "ignore" });
  const expectedAuthorization = `Basic ${Buffer.from(`x-access-token:${token}`).toString("base64")}`;
  const requests: Array<{ path: string; method: string; authorized: boolean }> = [];
  const server = https.createServer({ key: fs.readFileSync(key), cert: fs.readFileSync(cert) }, async (request, response) => {
    const url = new URL(request.url ?? "/", "https://github.com");
    const authorized = request.headers.authorization === expectedAuthorization;
    requests.push({ path: url.pathname + url.search, method: request.method ?? "GET", authorized });
    if (!authorized) {
      response.writeHead(401, { "WWW-Authenticate": 'Basic realm="task-git-read-test"' });
      response.end();
      return;
    }
    if (url.searchParams.get("service") === "git-receive-pack" || url.pathname.endsWith("/git-receive-pack")) {
      response.writeHead(403);
      response.end("read token cannot push");
      return;
    }
    const body = await readBody(request);
    const cgi = spawnSync("git", ["http-backend"], {
      input: body,
      env: {
        ...process.env,
        GIT_PROJECT_ROOT: bareRoot,
        GIT_HTTP_EXPORT_ALL: "1",
        GATEWAY_INTERFACE: "CGI/1.1",
        SERVER_PROTOCOL: "HTTP/1.1",
        SERVER_NAME: "github.com",
        SERVER_PORT: "443",
        REMOTE_ADDR: "127.0.0.1",
        REMOTE_USER: "x-access-token",
        REQUEST_METHOD: request.method ?? "GET",
        PATH_INFO: decodeURIComponent(url.pathname),
        QUERY_STRING: url.search.slice(1),
        CONTENT_TYPE: String(request.headers["content-type"] ?? ""),
        CONTENT_LENGTH: String(body.length),
      },
      timeout: 20_000,
      maxBuffer: 8 * 1024 * 1024,
    });
    if (cgi.status !== 0 || !cgi.stdout) {
      response.writeHead(500); response.end("git http-backend failed"); return;
    }
    const parsed = cgiResponse(cgi.stdout);
    if (!parsed) { response.writeHead(500); response.end("bad CGI response"); return; }
    response.writeHead(parsed.status, parsed.headers);
    response.end(parsed.body);
  });
  servers.push(server);
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const serverPort = (server.address() as net.AddressInfo).port;

  const proxy = http.createServer();
  const proxyEvents: string[] = [];
  proxy.on("connect", (request, client, head) => {
    proxyEvents.push(request.url ?? "");
    sockets.add(client);
    const upstream = net.connect(serverPort, "127.0.0.1", () => {
      client.write("HTTP/1.1 200 Connection Established\r\n\r\n");
      if (head.length) upstream.write(head);
      client.pipe(upstream).pipe(client);
    });
    sockets.add(upstream);
    upstream.on("error", () => client.destroy());
    client.on("close", () => { sockets.delete(client); upstream.destroy(); });
    upstream.on("close", () => sockets.delete(upstream));
  });
  servers.push(proxy);
  await new Promise<void>(resolve => proxy.listen(0, "127.0.0.1", resolve));
  const proxyPort = (proxy.address() as net.AddressInfo).port;
  return { bare, proxy: `http://127.0.0.1:${proxyPort}`, requests, proxyEvents };
};

describe("task-agent read-only Git credential path", () => {
  it.skipIf(process.platform === "win32")("fetches with only the read helper and denies push credentials", async () => {
    const f = fixture();
    const token = "ghs_read_fixture_token_01234567890123456789";
    const { proxy, requests, proxyEvents } = await localGitHub(f.directory, path.join(f.directory, "repos"), token);
    expect(f.env.GIT_CONFIG_PARAMETERS).toBeUndefined();
    const target = path.join(f.directory, "target"); fs.mkdirSync(target);
    requireGit(["init", target], f.directory, f.env);
    const remote = "https://github.com/Smarty-Pants-Inc/fetch.git";
    const transport = ["-c", `http.proxy=${proxy}`, "-c", "http.sslVerify=false"];
    const fetched = await gitAsync([...transport, "fetch", remote, "main"], target, f.env);
    expect(fetched.status, JSON.stringify({ stdout: fetched.stdout, stderr: fetched.stderr,
      requests, proxyEvents, tokenCalls: readFileOrEmpty(f.callsFile) })).toBe(0);
    expect(git(["cat-file", "-e", "FETCH_HEAD:fetched.txt"], target, f.env).status).toBe(0);
    expect(requests.some(request => request.path.includes("service=git-upload-pack") && request.authorized)).toBe(true);

    const pushed = await gitAsync([...transport, "push", remote, "FETCH_HEAD:refs/heads/push-probe"], target, f.env);
    expect(pushed.status).not.toBe(0);
    expect(requests.some(request => request.path.includes("service=git-receive-pack") && request.authorized)).toBe(true);
    expect(readFileOrEmpty(f.callsFile).trim().split("\n")).toEqual(["token Smarty-Pants-Inc", "token Smarty-Pants-Inc"]);
    expect(readFileOrEmpty(f.writeFile)).toBe("");
  });

  it("quits on read-token failure and ignores credential storage operations", () => {
    const f = fixture();
    f.env.FAKE_TOKEN_FAIL = "1";
    const input = "protocol=https\nhost=github.com\npath=Smarty-Pants-Inc/repo.git\n\n";
    const failed = git(["credential", "fill"], f.directory, f.env, input);
    expect(failed.status).not.toBe(0);
    expect(failed.stderr.toLowerCase()).toContain("quit");
    expect(readFileOrEmpty(f.callsFile).trim()).toBe("token Smarty-Pants-Inc");
    expect(readFileOrEmpty(f.writeFile)).toBe("");

    f.env.FAKE_TOKEN_FAIL = "0";
    const otherHost = git(["credential", "fill"], f.directory, f.env,
      "protocol=https\nhost=github.com.attacker.invalid\npath=Smarty-Pants-Inc/repo.git\n\n");
    expect(otherHost.status).not.toBe(0);
    expect(readFileOrEmpty(f.callsFile).trim()).toBe("token Smarty-Pants-Inc");
    expect(readFileOrEmpty(f.writeFile)).toBe("");
    const stored = git(["credential", "approve"], f.directory, f.env,
      "protocol=https\nhost=github.com\npath=Smarty-Pants-Inc/repo.git\nusername=x-access-token\npassword=ghs_test_fixture_token_01234567890123456789\n\n");
    expect(stored.status).toBe(0);
    const erased = git(["credential", "reject"], f.directory, f.env, input);
    expect(erased.status).toBe(0);
    expect(readFileOrEmpty(f.callsFile).trim()).toBe("token Smarty-Pants-Inc");
    expect(readFileOrEmpty(f.writeFile)).toBe("");
  });
});
