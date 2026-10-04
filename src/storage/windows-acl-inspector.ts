import childProcess from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHmac, randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import { windowsSecurityPowerShell } from "./windows-powershell.js";

// Capture the real host, not a later test's process.platform/tmpdir substitution.
// No process, directory or timer is allocated at import/registration/idle.
const hostPlatform = process.platform;
const hostTemp = os.tmpdir();
const inspectorKey = Symbol.for("pi-fabric.windows-acl-inspector.v1");
const MAX_REPLY_BYTES = 1024 * 1024;
const TIMEOUT_MS = 15_000;

export const windowsAclInspectorMode = (platform: NodeJS.Platform): "resident" | "oneshot" =>
  platform === "win32" ? "resident" : "oneshot";

interface Reply { nonce: string; ok: boolean; payload: string; mac: string }
export const checkedWindowsAclReply = (text: string, nonce: string, key: Buffer): string => {
  if (Buffer.byteLength(text) > MAX_REPLY_BYTES) throw new Error("Oversized Windows ACL reply");
  const value: unknown = JSON.parse(text);
  if (!value || typeof value !== "object") throw new Error("Invalid Windows ACL reply");
  const reply = value as Reply;
  if (reply.nonce !== nonce || typeof reply.ok !== "boolean" || typeof reply.payload !== "string" ||
      typeof reply.mac !== "string" || !/^[a-f0-9]{64}$/.test(reply.mac)) throw new Error("Invalid Windows ACL reply");
  const expected = createHmac("sha256", key).update(`${nonce}\n${reply.ok ? "ok" : "error"}\n${reply.payload}`).digest();
  if (!timingSafeEqual(expected, Buffer.from(reply.mac, "hex"))) throw new Error("Unconfirmed Windows ACL inspector");
  if (!reply.ok) throw new Error("Windows ACL inspection failed");
  return reply.payload;
};

// A fresh native snapshot for EVERY request, not an ACL/ownership cache. Keep
// PowerShell and QueryDosDevice's compiled type warm: starting 5.1 and compiling
// C# several times per launch/close blocks the owner loop for seconds on Windows.
// The mailbox is test/run-independent and contains only paths/ACL metadata, no
// executable code or authority. Authenticate replies with an in-memory key passed
// only to the owned child environment: replacing a mailbox cannot forge safety.
// Nonces bind replies to one request; windowsDataRoot still checks the exact chain.
const serverSource = (source: string): string => String.raw`
$directory = $env:PI_FABRIC_ACL_MAILBOX
$key = [Convert]::FromBase64String($env:PI_FABRIC_ACL_REPLY_KEY)
$hmac = [System.Security.Cryptography.HMACSHA256]::new($key)
$owner = [System.Diagnostics.Process]::GetProcessById([int]$env:PI_FABRIC_ACL_OWNER_PID)
$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
$acl = [System.Security.AccessControl.DirectorySecurity]::new()
$acl.SetOwner($user)
$acl.SetAccessRuleProtection($true, $false)
foreach ($sid in @($user.Value, 'S-1-5-18', 'S-1-5-32-544')) {
  $acl.AddAccessRule([System.Security.AccessControl.FileSystemAccessRule]::new([System.Security.Principal.SecurityIdentifier]::new($sid), 'FullControl', 'ContainerInherit,ObjectInherit', 'None', 'Allow'))
}
$item = Get-Item -Force -LiteralPath $directory
$prior = Get-Acl -LiteralPath $directory
if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0 -or
    $prior.GetOwner([System.Security.Principal.SecurityIdentifier]).Value -notin @($user.Value, 'S-1-5-18', 'S-1-5-32-544')) { throw 'Unowned ACL mailbox' }
Set-Acl -LiteralPath $directory -AclObject $acl
function Reply($nonce, $ok, $payload) {
  $status = if ($ok) { 'ok' } else { 'error' }
  $bytes = [System.Text.Encoding]::UTF8.GetBytes($nonce + [char]10 + $status + [char]10 + $payload)
  $mac = ([BitConverter]::ToString($hmac.ComputeHash($bytes))).Replace('-', '').ToLowerInvariant()
  $text = @{ nonce = $nonce; ok = $ok; payload = $payload; mac = $mac } | ConvertTo-Json -Compress
  $pending = [System.IO.Path]::Combine($directory, 'pending.json')
  [System.IO.File]::WriteAllText($pending, $text, [System.Text.UTF8Encoding]::new($false))
  [System.IO.File]::Move($pending, [System.IO.Path]::Combine($directory, 'reply.json'))
}
$inspect = {
${source}
}
try {
  Reply $env:PI_FABRIC_ACL_READY_NONCE $true 'ready'
  while (-not $owner.HasExited) {
    $requestFile = [System.IO.Path]::Combine($directory, 'request.json')
    if (-not [System.IO.File]::Exists($requestFile)) { Start-Sleep -Milliseconds 5; continue }
    $request = [System.IO.File]::ReadAllText($requestFile) | ConvertFrom-Json
    [System.IO.File]::Delete($requestFile)
    $env:PI_FABRIC_ACL_CHAIN = $request.chain
    try { $payload = (& $inspect | Out-String).Trim(); Reply $request.nonce $true $payload }
    catch { Reply $request.nonce $false 'native inspection failed' }
  }
} finally { $hmac.Dispose(); $owner.Dispose() }
`;

export interface WindowsAclInspector { readonly closed: boolean; inspect(chain: string): string; close(): void }
export const createWindowsAclInspector = (source: string, env: NodeJS.ProcessEnv, tempRoot = hostTemp): WindowsAclInspector => {
  const directory = fs.mkdtempSync(path.join(tempRoot, "pi-fabric-acl-inspector-"));
  const identity = fs.lstatSync(directory);
  const sameMailbox = (): boolean => {
    try {
      const current = fs.lstatSync(directory);
      return current.isDirectory() && !current.isSymbolicLink() && current.dev === identity.dev && current.ino === identity.ino;
    } catch { return false; }
  };
  const key = randomBytes(32), readyNonce = randomUUID();
  let child: childProcess.ChildProcess | undefined;
  let closed = false;
  const replyFile = path.join(directory, "reply.json");
  const pause = new Int32Array(new SharedArrayBuffer(4));
  const close = () => {
    if (closed) return;
    closed = true;
    child?.kill();
    if (!sameMailbox()) return;
    // Do not recursively prune a possibly replaced mailbox. Only known protocol
    // files and an empty directory are owned by this helper.
    for (const name of ["request-pending.json", "request.json", "reply.json", "pending.json"]) {
      try { fs.unlinkSync(path.join(directory, name)); } catch { /* fail closed */ }
    }
    try { fs.rmdirSync(directory); } catch { /* a Windows handle may still be closing */ }
  };
  const receive = (nonce: string): string => {
    const deadline = performance.now() + TIMEOUT_MS;
    for (;;) {
      if (!sameMailbox()) throw new Error("Windows ACL mailbox replaced");
      if (closed || child?.exitCode !== null || child?.killed) throw new Error("Windows ACL inspector closed");
      try {
        const stat = fs.lstatSync(replyFile);
        if (!stat.isFile() || stat.isSymbolicLink() || stat.size > MAX_REPLY_BYTES) throw new Error("Unsafe Windows ACL reply");
        const text = fs.readFileSync(replyFile, "utf8");
        fs.unlinkSync(replyFile);
        return checkedWindowsAclReply(text, nonce, key);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      }
      if (performance.now() >= deadline) throw new Error("Windows ACL inspector timed out");
      Atomics.wait(pause, 0, 0, 5);
    }
  };
  try {
    const command = windowsSecurityPowerShell(serverSource(source), {
      ...env, PI_FABRIC_ACL_MAILBOX: directory, PI_FABRIC_ACL_REPLY_KEY: key.toString("base64"),
      PI_FABRIC_ACL_READY_NONCE: readyNonce, PI_FABRIC_ACL_OWNER_PID: String(process.pid),
    });
    child = childProcess.spawn(command.file, command.args, { env: command.env, windowsHide: true, stdio: "ignore" });
    child.on("error", close);
    child.unref();
    if (!child.pid) throw new Error("Windows ACL inspector did not start");
    if (receive(readyNonce) !== "ready") throw new Error("Windows ACL inspector did not join");
    return {
      get closed() { return closed; },
      inspect(chain) {
        const nonce = randomUUID();
        try {
          if (closed || !sameMailbox()) throw new Error("Windows ACL mailbox closed or replaced");
          fs.writeFileSync(path.join(directory, "request-pending.json"), JSON.stringify({ nonce, chain }), { flag: "wx", mode: 0o600 });
          fs.renameSync(path.join(directory, "request-pending.json"), path.join(directory, "request.json"));
          return receive(nonce);
        } catch (error) { close(); throw error; }
      }, close,
    };
  } catch (error) { close(); throw error; }
};

/** One owned read-only inspector per host process, including Vitest module reloads.
 * Cold/unsupported hosts retain the bounded one-shot seam; never bypass a check. */
export const inspectWindowsAclChain = (source: string, env: NodeJS.ProcessEnv): string => {
  if (windowsAclInspectorMode(hostPlatform) === "oneshot") {
    const command = windowsSecurityPowerShell(source, env);
    return childProcess.execFileSync(command.file, command.args, {
      env: command.env, encoding: "utf8", windowsHide: true, timeout: TIMEOUT_MS, maxBuffer: MAX_REPLY_BYTES,
      stdio: ["ignore", "pipe", "pipe"],
    });
  }
  const owner = process as typeof process & { [inspectorKey]?: { source: string; inspector: WindowsAclInspector } };
  if (!owner[inspectorKey]) process.once("exit", () => owner[inspectorKey]?.inspector.close());
  if (!owner[inspectorKey] || owner[inspectorKey]!.inspector.closed || owner[inspectorKey]!.source !== source) {
    owner[inspectorKey]?.inspector.close();
    owner[inspectorKey] = { source, inspector: createWindowsAclInspector(source, env) };
  }
  return owner[inspectorKey]!.inspector.inspect(env.PI_FABRIC_ACL_CHAIN!);
};
