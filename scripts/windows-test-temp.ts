import childProcess from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { windowsSecurityPowerShell } from "../src/storage/windows-powershell.js";
import { windowsDataRoot } from "../src/storage/windows-temp-root.js";

/** Only test-owned NTFS volumes get new ACLs. Never repair/whitelist a CI
 * runner's C:/D: drive, whose ancestors may be foreign-owned or writable. */
export const privateWindowsTestTemp = (): { directory: string; close(): void } => {
  // Git Bash keeps its /tmp mount across processes. CI must reuse this volume
  // for the job (the disposable runner owns teardown), not detach it per suite.
  const stable = process.env.CI && process.env.RUNNER_TEMP;
  const backing = stable ? path.join(stable, "fabric-private-test-volume")
    : fs.mkdtempSync(path.join(os.tmpdir(), "fabric-private-test-volume-"));
  fs.mkdirSync(backing, { recursive: true });
  const manifest = path.join(backing, "namespace.json");
  if (fs.existsSync(manifest)) {
    const receipt = JSON.parse(fs.readFileSync(manifest, "utf8"));
    if (receipt.vhd !== path.join(backing, "tests.vhd") || receipt.directory !== path.win32.join(receipt.volume, "tmp")) throw new Error("Invalid private test volume receipt");
    windowsDataRoot(receipt.directory, { private: true });
    return { directory: receipt.directory, close() {} };
  }
  if (fs.existsSync(path.join(backing, "tests.vhd"))) throw new Error("Unconfirmed private test volume setup; manual fixture cleanup required");
  if (/["\r\n]/.test(backing)) throw new Error("Unsafe test VHD backing path");
  const vhd = path.join(backing, "tests.vhd");
  let sequence = 0, volume = "";
  const native = (source: string, env: NodeJS.ProcessEnv = process.env) => {
    const command = windowsSecurityPowerShell(source, env);
    return childProcess.execFileSync(command.file, command.args, {
      env: command.env, encoding: "utf8", windowsHide: true, timeout: 30_000, stdio: ["ignore", "pipe", "pipe"],
    }).trim();
  };
  const diskpart = (commands: string[]) => {
    const file = path.join(backing, `diskpart-${sequence++}.txt`);
    fs.writeFileSync(file, commands.join("\r\n") + "\r\nexit\r\n");
    return childProcess.execFileSync(path.join(process.env.SystemRoot!, "System32", "diskpart.exe"), ["/s", file], {
      encoding: "utf8", windowsHide: true, timeout: 90_000, stdio: ["ignore", "pipe", "pipe"],
    });
  };
  const detach = () => {
    if (fs.existsSync(vhd)) diskpart([`select vdisk file="${vhd}"`, "detach vdisk noerr"]);
    if (volume && fs.existsSync(volume)) throw new Error(`Private Windows test volume did not detach: ${volume}`);
    fs.rmSync(backing, { recursive: true, force: true });
  };
  try {
    const drives = JSON.parse(native("ConvertTo-Json -Compress -InputObject @([System.IO.Directory]::GetLogicalDrives())")) as string[];
    const letter = [..."ZYXWVUTSRQPONMLKJIHGFE"].find(candidate => !drives.some(drive => drive[0]!.toUpperCase() === candidate));
    if (!letter) throw new Error("No free drive for private test volume");
    volume = `${letter}:\\`;
    const output = diskpart([`create vdisk file="${vhd}" maximum=512 type=expandable`, `select vdisk file="${vhd}"`,
      "attach vdisk", "create partition primary", 'format fs=ntfs label="fabric-private-tests" quick', `assign letter=${letter}`]);
    if (!fs.existsSync(volume)) throw new Error(`Private Windows test volume setup failed: ${output}`);
    native(String.raw`
$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
$acl = [System.Security.AccessControl.DirectorySecurity]::new()
$acl.SetOwner($user)
$acl.SetAccessRuleProtection($true, $false)
foreach ($sid in @($user.Value, 'S-1-5-18', 'S-1-5-32-544')) {
  $identity = [System.Security.Principal.SecurityIdentifier]::new($sid)
  $acl.AddAccessRule([System.Security.AccessControl.FileSystemAccessRule]::new($identity, 'FullControl', 'ContainerInherit,ObjectInherit', 'None', 'Allow'))
}
Set-Acl -LiteralPath $env:FABRIC_TEST_VOLUME -AclObject $acl
`, { ...process.env, FABRIC_TEST_VOLUME: volume });
    windowsDataRoot(volume, { private: true });
    const directory = path.join(volume, "tmp");
    fs.mkdirSync(directory);
    windowsDataRoot(directory, { private: true });
    if (stable) fs.writeFileSync(manifest, JSON.stringify({ volume, vhd, directory }), { flag: "wx" });
    return { directory, close: stable ? () => {} : detach };
  } catch (error) { detach(); throw error; }
};
