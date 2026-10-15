import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { afterEach, describe, expect, it } from "vitest";
import { windowsMeshCacheFixture } from "./windows-mesh-cache-fixture.js";
import { windowsHelperBootstrap } from "../src/storage/verified-remove.js";
import { windowsSecurityPowerShell } from "../src/storage/windows-powershell.js";
import { forgetWindowsMeshCapability, prepareWindowsMeshCapability, windowsMeshReleaseTestHooks } from "../src/storage/windows-mesh-release.js";

// Hosted Windows ONLY. Keep: PR1191's real DACL trust-boundary failure; mocks cannot prove creation-time ACLs.
// The verified-remove VALUE import deliberately excludes this native/deleting fixture from local detector admission.
// PowerShell is test-only ACL setup/readback, never the production private-creation path. No tests run off-host.
const hosted = process.platform === "win32" && process.env.GITHUB_ACTIONS === "true";
const fixtures: string[] = [];
type Ace = { sid: string; rights: number; inherited: boolean; inheritance: number; propagation: number; allow: boolean };
type Acl = { protected: boolean; owner: string; user: string; tokenOwner: string; aces: Ace[]; sddl: string };

const powershell = (script: string, file: string): string => {
  const invocation = windowsSecurityPowerShell(script, process.env);
  const result = spawnSync(invocation.file, invocation.args, {
    encoding: "utf8", timeout: 10_000, windowsHide: true, maxBuffer: 64 * 1024,
    env: { ...invocation.env, FMR_ACL_FIXTURE: file },
  });
  expect(result.error).toBeUndefined();
  expect(result.status, result.stderr).toBe(0);
  return result.stdout.trim();
};
const readAcl = (file: string): Acl => JSON.parse(powershell(`
  $identity = [System.Security.Principal.WindowsIdentity]::GetCurrent();
  $acl = Get-Acl -LiteralPath $env:FMR_ACL_FIXTURE;
  $rules = @($acl.GetAccessRules($true, $true, [System.Security.Principal.SecurityIdentifier]) | ForEach-Object {
    @{ sid = $_.IdentityReference.Value; rights = [int]$_.FileSystemRights; inherited = $_.IsInherited;
       inheritance = [int]$_.InheritanceFlags; propagation = [int]$_.PropagationFlags; allow = ($_.AccessControlType -eq 'Allow') }
  });
  @{ protected = $acl.AreAccessRulesProtected; owner = $acl.GetOwner([System.Security.Principal.SecurityIdentifier]).Value;
     user = $identity.User.Value; tokenOwner = $identity.Owner.Value; aces = $rules; sddl = $acl.Sddl } | ConvertTo-Json -Depth 5 -Compress;
`, file));

// Hostile INHERIT_ONLY Users write template: ordinary mkdir would inherit effective writes, but it grants no
// effective replacement right on this parent. Users Modify on the parent itself is NOT a valid harmless fixture:
// Modify includes DELETE, which SEC-1191-01 must now refuse even when protected children do not inherit it.
// This does not identify the SID behind PR1191's original hosted failure.
const broadParent = (file: string): void => { powershell(`
  $identity = [System.Security.Principal.WindowsIdentity]::GetCurrent();
  $acl = New-Object System.Security.AccessControl.DirectorySecurity;
  $acl.SetAccessRuleProtection($true, $false);
  $acl.SetOwner($identity.User);
  $sids = @($identity.User.Value, $identity.Owner.Value, 'S-1-5-18', 'S-1-5-32-544') | Select-Object -Unique;
  foreach ($sid in $sids) {
    $rule = [System.Security.AccessControl.FileSystemAccessRule]::new(
      [System.Security.Principal.SecurityIdentifier]::new($sid), 'FullControl', 'ContainerInherit, ObjectInherit', 'None', 'Allow');
    $acl.AddAccessRule($rule);
  }
  $acl.AddAccessRule([System.Security.AccessControl.FileSystemAccessRule]::new(
    [System.Security.Principal.SecurityIdentifier]::new('S-1-5-32-545'), 'Modify', 'ContainerInherit, ObjectInherit', 'InheritOnly', 'Allow'));
  Set-Acl -LiteralPath $env:FMR_ACL_FIXTURE -AclObject $acl;
`, file); };
// Use SDDL to seed exact masks (including raw generic rights), then inspect the ACL actually stored by Windows.
const setAncestorAce = (file: string, rights: string, flags: string, trustee: string): { sid: string; mask: number; flags: number; sddl: string } => JSON.parse(powershell(`
  $identity = [System.Security.Principal.WindowsIdentity]::GetCurrent();
  $acl = New-Object System.Security.AccessControl.DirectorySecurity;
  $acl.SetSecurityDescriptorSddlForm("O:$($identity.User.Value)D:P(A;OICI;FA;;;$($identity.User.Value))(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)(A;${flags};${rights};;;${trustee})");
  Set-Acl -LiteralPath $env:FMR_ACL_FIXTURE -AclObject $acl;
  $actual = Get-Acl -LiteralPath $env:FMR_ACL_FIXTURE;
  $raw = [System.Security.AccessControl.RawSecurityDescriptor]::new($actual.GetSecurityDescriptorBinaryForm(), 0);
  $ace = @($raw.DiscretionaryAcl | Where-Object { $_.SecurityIdentifier.Value -eq '${trustee}' })[0];
  @{ sid = $ace.SecurityIdentifier.Value; mask = $ace.AccessMask; flags = [int]$ace.AceFlags; sddl = $actual.Sddl } | ConvertTo-Json -Compress;
`, file));

const trustedOnly = (acl: Acl): void => {
  const trusted = new Set([acl.user, acl.tokenOwner, "S-1-5-18", "S-1-5-32-544"]);
  expect([acl.user, acl.tokenOwner]).toContain(acl.owner);
  expect(acl.aces.length).toBeGreaterThan(0);
  for (const ace of acl.aces.filter(ace => ace.allow)) expect(trusted.has(ace.sid), JSON.stringify(ace)).toBe(true);
  for (const sid of [acl.user, "S-1-5-18", "S-1-5-32-544"]) expect(acl.aces.some(ace => ace.allow && ace.sid === sid)).toBe(true);
};
const setup = (): { fixture: string; local: string } => {
  expect(windowsHelperBootstrap(), "hosted build must supply digest-pinned bootstrap").toBeDefined();
  const fixture = windowsMeshCacheFixture("fmacl-");
  fixtures.push(fixture);
  const local = path.join(fixture, "profile");
  fs.mkdirSync(local);
  broadParent(local);
  // Isolate ACL poisoning below the actual profile, without relabeling runner temp or overriding LOCALAPPDATA.
  windowsMeshReleaseTestHooks.cacheRoot = path.join(local, "pi-fabric");
  return { fixture, local };
};
afterEach(() => {
  forgetWindowsMeshCapability();
  for (const key of Object.keys(windowsMeshReleaseTestHooks)) delete (windowsMeshReleaseTestHooks as Record<string, unknown>)[key];
  for (const dir of fixtures.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

describe.skipIf(!hosted)("PR1191 private cache creation and real ACL readback (hosted Windows only)", () => {
  it.each([
    { name: "generic read", rights: "GR", flags: "", allowed: true },
    { name: "generic execute", rights: "GX", flags: "", allowed: true },
    { name: "generic read and execute", rights: "GRGX", flags: "", allowed: true },
    { name: "generic write", rights: "GW", flags: "", allowed: false },
    { name: "generic all", rights: "GA", flags: "", allowed: false },
    { name: "inherit-only all", rights: "GA", flags: "OICIIO", allowed: true },
  ])("native DACL classification: $name", ({ rights, flags, allowed }) => {
    const { local } = setup();
    const existing = path.join(local, "pi-fabric");
    fs.mkdirSync(existing);
    // SDDL preserves generic masks; read back the actual stored ACE rather than inferring its representation.
    const stored = JSON.parse(powershell(`
      $identity = [System.Security.Principal.WindowsIdentity]::GetCurrent();
      $acl = New-Object System.Security.AccessControl.DirectorySecurity;
      $acl.SetSecurityDescriptorSddlForm("O:$($identity.User.Value)D:P(A;;FA;;;$($identity.User.Value))(A;;FA;;;SY)(A;;FA;;;BA)(A;${flags};${rights};;;BU)");
      Set-Acl -LiteralPath $env:FMR_ACL_FIXTURE -AclObject $acl;
      $actual = Get-Acl -LiteralPath $env:FMR_ACL_FIXTURE;
      $raw = [System.Security.AccessControl.RawSecurityDescriptor]::new($actual.GetSecurityDescriptorBinaryForm(), 0);
      $ace = @($raw.DiscretionaryAcl | Where-Object { $_.SecurityIdentifier.Value -eq 'S-1-5-32-545' })[0];
      @{ sid = $ace.SecurityIdentifier.Value; mask = $ace.AccessMask; flags = [int]$ace.AceFlags; sddl = $actual.Sddl } | ConvertTo-Json -Compress;
    `, existing));
    const boot = windowsHelperBootstrap()!;
    const checked = spawnSync(boot.file, ["--cache-create-v1", path.toNamespacedPath(existing)], {
      encoding: "utf8", timeout: 10_000, windowsHide: true,
    });
    expect(checked.status, JSON.stringify({ stored, stdout: checked.stdout, stderr: checked.stderr })).toBe(allowed ? 0 : 3);
    if (allowed) expect(checked.stdout.trim()).toBe("ok created");
    else {
      expect(checked.stdout).toMatch(/writable by another SID; ace=\d+ sid=S-1-5-32-545 mask=0x[0-9a-f]+ mapped=0x[0-9a-f]+ flags=0x[0-9a-f]+ sddl=D:/);
    }
    expect(readAcl(existing).sddl).toBe(stored.sddl);
    expect(fs.readdirSync(existing)).toEqual([]);
  }, 60_000);

  it.each([
    { name: "parent DELETE_CHILD alone", rights: "0x40", allowed: false },
    { name: "parent DELETE_CHILD plus ADD_SUBDIRECTORY", rights: "0x44", allowed: false },
    { name: "component DELETE", rights: "SD", allowed: false },
    { name: "ancestor WRITE_DAC", rights: "WD", allowed: false },
    { name: "ancestor WRITE_OWNER", rights: "WO", allowed: false },
    { name: "ancestor generic write", rights: "GW", allowed: false },
    { name: "ancestor mapped generic write", rights: "FW", allowed: false },
    { name: "ancestor generic all", rights: "GA", allowed: false },
    { name: "grandparent DELETE_CHILD", rights: "0x40", grandparent: true, allowed: false },
    { name: "C-style Authenticated Users create-folder only", rights: "0x4", sid: "S-1-5-11", allowed: true },
    { name: "Authenticated Users create-file only", rights: "0x2", sid: "S-1-5-11", allowed: true },
    { name: "Authenticated Users create-file and folder only", rights: "0x6", sid: "S-1-5-11", allowed: true },
    { name: "Users read and execute", rights: "GRGX", sid: "S-1-5-32-545", allowed: true },
    { name: "Users inherit-only all", rights: "GA", flags: "OICIIO", sid: "S-1-5-32-545", allowed: true },
    { name: "harmless Creator Owner template", rights: "GA", flags: "OICIIO", sid: "S-1-3-0", allowed: true },
    { name: "trusted TrustedInstaller full control", rights: "GA", sid: "S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464", allowed: true },
  ])("SEC-1191-01 cache ancestor replacement policy: $name", ({ rights, allowed, sid, flags, grandparent }) => {
    const { fixture, local } = setup();
    const boot = windowsHelperBootstrap()!;
    // First establish a private chain. Poisoning only an ancestor afterwards must also be caught by cache-private,
    // independently of the cache-create check. The leaf need not be executable for the native privacy-only mode.
    const probe = path.join(local, "private-probe");
    const leaf = path.join(probe, "privacy-probe.bin");
    const initial = spawnSync(boot.file, ["--cache-create-v1", path.toNamespacedPath(probe)], {
      encoding: "utf8", timeout: 10_000, windowsHide: true,
    });
    expect(initial.error).toBeUndefined();
    expect(initial.status, initial.stdout).toBe(0);
    fs.writeFileSync(leaf, "private ACL probe");
    const ancestor = grandparent ? fixture : local;
    const trustee = sid ?? "S-1-5-21-111111111-222222222-333333333-4444";
    const stored = setAncestorAce(ancestor, rights, flags ?? "", trustee);
    expect(stored.sid).toBe(trustee);
    expect(stored.mask).not.toBe(0);
    expect((stored.flags & 8) !== 0).toBe((flags ?? "").includes("IO"));
    const before = fs.readdirSync(local);
    const top = path.join(local, "pi-fabric");
    const modes = [
      ["--cache-private-v1", probe, leaf],
      ["--cache-create-v1", top, path.join(top, "native")],
    ];
    for (const [mode, ...files] of modes) {
      const checked = spawnSync(boot.file, [mode!, ...files.map(file => path.toNamespacedPath(file))], {
        encoding: "utf8", timeout: 10_000, windowsHide: true,
      });
      expect(checked.error).toBeUndefined();
      expect(checked.status, JSON.stringify({ mode, stored, stdout: checked.stdout, stderr: checked.stderr })).toBe(allowed ? 0 : 3);
      if (allowed) expect(checked.stdout.trim()).toBe(mode === "--cache-create-v1" ? "ok created" : "ok private");
      else {
        expect(checked.stdout).toMatch(/ancestor replaceable by another SID; ace=\d+ sid=.* mask=0x[0-9a-f]+ mapped=0x[0-9a-f]+ flags=0x[0-9a-f]+ sddl=D:/);
        expect(checked.stdout).toContain(`sid=${trustee} `);
      }
    }
    let staged = 0;
    windowsMeshReleaseTestHooks.afterCacheStaged = () => { staged++; };
    if (allowed) {
      const capability = prepareWindowsMeshCapability();
      expect(staged).toBe(1);
      trustedOnly(readAcl(capability.absolutePath));
    } else {
      expect(() => prepareWindowsMeshCapability()).toThrow(/private creation refused at .*pi-fabric.*ancestor replaceable by another SID/);
      expect(staged).toBe(0);
      expect(fs.existsSync(top)).toBe(false); // No native cache creation, Node staging, digest publication or helper query.
      expect(fs.readdirSync(local)).toEqual(before);
      expect(fs.readFileSync(leaf, "utf8")).toBe("private ACL probe");
    }
    expect(readAcl(ancestor).sddl).toBe(stored.sddl); // Refuse/read only; never heal existing ancestor ACLs.
  }, 60_000);

  it("a safe preexisting generic-read profile with CREATOR OWNER inherit-only templates remains usable", () => {
    const { local } = setup();
    const existing = path.join(local, "pi-fabric");
    fs.mkdirSync(existing);
    powershell(`
      $identity = [System.Security.Principal.WindowsIdentity]::GetCurrent();
      $acl = New-Object System.Security.AccessControl.DirectorySecurity;
      $acl.SetSecurityDescriptorSddlForm("O:$($identity.User.Value)D:P(A;OICI;FA;;;$($identity.User.Value))(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)(A;;GRGX;;;BU)(A;OICIIO;GA;;;CO)");
      Set-Acl -LiteralPath $env:FMR_ACL_FIXTURE -AclObject $acl;
    `, existing);
    const before = readAcl(existing).sddl;
    const capability = prepareWindowsMeshCapability();
    expect(readAcl(existing).sddl).toBe(before);
    trustedOnly(readAcl(path.dirname(capability.absolutePath)));
    trustedOnly(readAcl(capability.absolutePath));
  }, 60_000);

  it.each(["profile-chain-two", "explicit-chain-one"] as const)("%s: broad inheritance never enters a new cache or its staged/published executable", kind => {
    const { local } = setup();
    expect(readAcl(local).aces.some(ace => ace.sid === "S-1-5-32-545" && ace.allow && ace.inheritance !== 0 && (ace.propagation & 2) !== 0)).toBe(true);
    const base = kind === "profile-chain-two" ? path.join(local, "pi-fabric", "native") : path.join(local, "test-cache");
    const chain = kind === "profile-chain-two" ? [path.dirname(base), base] : [base];
    if (kind === "profile-chain-two") {
      // Exercise the same trusted two-component native creation as production, only inside our ACL fixture.
      // The separate default-path regression uses actual LOCALAPPDATA without any cacheRoot hook.
      const created = spawnSync(windowsHelperBootstrap()!.file, ["--cache-create-v1", ...chain.map(file => path.toNamespacedPath(file))], {
        encoding: "utf8", timeout: 10_000, windowsHide: true,
      });
      expect(created.error).toBeUndefined();
      expect(created.status, created.stdout).toBe(0);
    }
    windowsMeshReleaseTestHooks.cacheRoot = base;
    let staged = 0;
    windowsMeshReleaseTestHooks.afterCacheStaged = staging => {
      staged++;
      trustedOnly(readAcl(staging));
      trustedOnly(readAcl(path.join(staging, "fabric-win-remove.exe")));
    };
    const capability = prepareWindowsMeshCapability();
    expect(staged).toBe(1);
    for (const component of chain) {
      const acl = readAcl(component);
      expect(acl.protected).toBe(true);
      trustedOnly(acl);
      expect(acl.aces.every(ace => !ace.inherited)).toBe(true);
      expect(acl.aces.filter(ace => ace.allow).every(ace => ace.inheritance === 3)).toBe(true);
    }
    trustedOnly(readAcl(path.dirname(capability.absolutePath)));
    trustedOnly(readAcl(capability.absolutePath));
    const boot = windowsHelperBootstrap()!;
    const check = spawnSync(boot.file, ["--cache-private-v1", ...[...chain, path.dirname(capability.absolutePath), capability.absolutePath].map(file => path.toNamespacedPath(file))], {
      encoding: "utf8", timeout: 10_000, windowsHide: true,
    });
    expect(check.status, check.stdout).toBe(0);
    expect(check.stdout.trim()).toBe("ok private");
  }, 60_000);

  it("an explicit unrelated user SID on a preexisting protected cache is refused, without changing its ACL", () => {
    const { local } = setup();
    const existing = path.join(local, "pi-fabric");
    const boot = windowsHelperBootstrap()!;
    const created = spawnSync(boot.file, ["--cache-create-v1", path.toNamespacedPath(existing)], {
      encoding: "utf8", timeout: 10_000, windowsHide: true,
    });
    expect(created.status, created.stdout).toBe(0);
    trustedOnly(readAcl(existing));
    // An unmapped but valid user SID is sufficient to exercise a real allow ACE, without creating accounts or
    // guessing the unidentified offending SID in the hosted failure log. It is neither a token group nor well-known.
    const other = "S-1-5-21-111111111-222222222-333333333-4444";
    powershell(`
      $acl = Get-Acl -LiteralPath $env:FMR_ACL_FIXTURE;
      $acl.AddAccessRule([System.Security.AccessControl.FileSystemAccessRule]::new(
        [System.Security.Principal.SecurityIdentifier]::new('${other}'), 'Modify', 'ContainerInherit, ObjectInherit', 'None', 'Allow'));
      Set-Acl -LiteralPath $env:FMR_ACL_FIXTURE -AclObject $acl;
    `, existing);
    const before = readAcl(existing);
    expect(before.protected).toBe(true);
    expect(before.aces.some(ace => ace.sid === other && ace.allow)).toBe(true);
    expect(() => prepareWindowsMeshCapability()).toThrow(/private creation refused at .*pi-fabric.*writable by another SID/);
    expect(readAcl(existing).sddl).toBe(before.sddl);
    expect(fs.readdirSync(existing)).toEqual([]);
  }, 60_000);

  it.each(["empty", "contents"] as const)("preexisting other-SID-writable %s cache is refused and its ACL/contents are not healed", kind => {
    const { local } = setup();
    const existing = path.join(local, "pi-fabric");
    fs.mkdirSync(existing); // intentionally turns the parent's inherit-only Users write template into effective child rights
    const sentinel = path.join(existing, "operator-evidence");
    if (kind === "contents") fs.writeFileSync(sentinel, "preserve exactly");
    const before = readAcl(existing);
    expect(before.aces.some(ace => ace.sid === "S-1-5-32-545" && ace.allow && ace.inherited)).toBe(true);
    expect(() => prepareWindowsMeshCapability()).toThrow(/private creation refused at .*pi-fabric.*writable by another SID/);
    expect(readAcl(existing).sddl).toBe(before.sddl);
    expect(fs.readdirSync(existing)).toEqual(kind === "contents" ? ["operator-evidence"] : []);
    if (kind === "contents") expect(fs.readFileSync(sentinel, "utf8")).toBe("preserve exactly");
    expect(fs.existsSync(path.join(existing, "native"))).toBe(false);
  }, 60_000);
});
