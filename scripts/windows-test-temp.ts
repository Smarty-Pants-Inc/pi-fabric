import childProcess from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { windowsSecurityPowerShell } from "../src/storage/windows-powershell.js";
import { windowsDataRoot } from "../src/storage/windows-temp-root.js";

/** A private directory on the ordinary temp filesystem, never a mounted disk.
 * Only fresh test-owned directories receive ACLs; existing namespaces must prove
 * custody without repair. CI reuses one directory per job for MSYS's /tmp mount. */
export const privateWindowsTestTemp = (): { directory: string; close(): void } => {
  const native = (source: string, env: NodeJS.ProcessEnv = process.env) => {
    const command = windowsSecurityPowerShell(source, env);
    return childProcess.execFileSync(command.file, command.args, {
      env: command.env, encoding: "utf8", windowsHide: true, timeout: 30_000, stdio: ["ignore", "pipe", "pipe"],
    }).trim();
  };
  // RUNNER_TEMP can live on a broadly writable CI work drive. Select the normal
  // per-user Windows temp hierarchy using the native known folder, not that drive.
  const tempRoot = native("[System.IO.Path]::Combine([Environment]::GetFolderPath('LocalApplicationData'), 'Temp')");
  const stable = !!(process.env.CI && process.env.RUNNER_TEMP);
  const job = createHash("sha256").update(JSON.stringify([
    process.env.RUNNER_TEMP, process.env.GITHUB_RUN_ID, process.env.GITHUB_RUN_ATTEMPT, process.env.GITHUB_JOB,
  ])).digest("hex").slice(0, 24);
  const directory = stable ? path.join(tempRoot, `fabric-private-tests-${job}`)
    : fs.mkdtempSync(path.join(tempRoot, "fabric-private-tests-"));
  let fresh = !stable;
  if (stable) {
    try { fs.mkdirSync(directory); fresh = true; }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
  }
  const identity = fresh ? fs.lstatSync(directory) : undefined;
  const remove = () => {
    if (!identity) return;
    const current = fs.lstatSync(directory);
    if (!current.isDirectory() || current.isSymbolicLink() || current.dev !== identity.dev || current.ino !== identity.ino) {
      throw new Error("Private Windows test directory was replaced");
    }
    fs.rmSync(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 50 });
  };
  try {
    if (fresh) native(String.raw`
$identity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
$user = $identity.User
$principal = [System.Security.Principal.WindowsPrincipal]::new($identity)
# IsInRole tests the enabled token role: UAC-filtered (non-elevated) admins return false.
$adminOwner = $principal.IsInRole([System.Security.Principal.WindowsBuiltInRole]::Administrator)
$item = Get-Item -Force -LiteralPath $env:FABRIC_TEST_DIRECTORY
$prior = Get-Acl -LiteralPath $env:FABRIC_TEST_DIRECTORY
$allowedOwners = @($user.Value)
if ($adminOwner) { $allowedOwners += 'S-1-5-32-544' }
if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0 -or
    $prior.GetOwner([System.Security.Principal.SecurityIdentifier]).Value -notin $allowedOwners) { throw 'Unowned test directory' }
$acl = [System.Security.AccessControl.DirectorySecurity]::new()
$acl.SetOwner($user)
$acl.SetAccessRuleProtection($true, $false)
foreach ($sid in @($user.Value, 'S-1-5-18', 'S-1-5-32-544')) {
  $identity = [System.Security.Principal.SecurityIdentifier]::new($sid)
  $acl.AddAccessRule([System.Security.AccessControl.FileSystemAccessRule]::new($identity, 'FullControl', 'ContainerInherit,ObjectInherit', 'None', 'Allow'))
}
Set-Acl -LiteralPath $env:FABRIC_TEST_DIRECTORY -AclObject $acl
`, { ...process.env, FABRIC_TEST_DIRECTORY: directory });
    windowsDataRoot(directory, { private: true });
    // The disposable CI job owns this stable parent; no suite may remove the
    // shared MSYS mount while a shell from another suite still references it.
    return { directory, close: stable ? () => {} : remove };
  } catch (error) { if (fresh) remove(); throw error; }
};
