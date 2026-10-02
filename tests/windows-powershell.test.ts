import { describe, expect, it } from "vitest";
import { windowsSecurityPowerShell } from "../src/storage/windows-powershell.js";

// Portable command-construction coverage for both production Get-Acl and native
// test Set-Acl launches. Execution of the real ACL cmdlets remains Windows CI's job.
describe("fixed Windows security PowerShell invocation", () => {
  it("pins the executable, flags and system module path despite a pwsh parent", () => {
    const inherited = {
      SystemRoot: "C:\\Windows",
      PSModulePath: "C:\\Program Files\\PowerShell\\7\\Modules",
      psmodulepath: "D:\\foreign-modules",
      PsModulePath: "E:\\foreign-modules",
      PATH: "C:\\custom-bin",
      FABRIC_ACL_TEST_VALUES: JSON.stringify({ path: "R:\\quote'$;日本語" }),
    };
    const source = "Set-Acl -LiteralPath $p.path -AclObject $acl";
    const command = windowsSecurityPowerShell(source, inherited);
    expect(command.file).toBe("C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe");
    expect(command.args).toEqual([
      "-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand",
      Buffer.from("$ErrorActionPreference = 'Stop'\nImport-Module Microsoft.PowerShell.Security -ErrorAction Stop\n" + source, "utf16le").toString("base64"),
    ]);
    expect(command.env).toEqual({
      SystemRoot: inherited.SystemRoot,
      PSModulePath: "C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\Modules",
      PATH: inherited.PATH,
      FABRIC_ACL_TEST_VALUES: inherited.FABRIC_ACL_TEST_VALUES,
    });
    expect(inherited.PSModulePath).toBe("C:\\Program Files\\PowerShell\\7\\Modules");
    expect(inherited.psmodulepath).toBe("D:\\foreign-modules");
  });

  it("uses the interpreter's own module directory when the parent has no module path", () => {
    const command = windowsSecurityPowerShell("Get-Acl -LiteralPath $env:PI_FABRIC_ACL_CHAIN", { SystemRoot: "D:\\Windows" });
    expect(command.env.PSModulePath).toBe("D:\\Windows\\System32\\WindowsPowerShell\\v1.0\\Modules");
    expect(command.file).toBe("D:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe");
  });

  it("resolves SystemRoot case-insensitively after copying the Windows environment", () => {
    const command = windowsSecurityPowerShell("Get-Acl", { SYSTEMROOT: "C:\\Windows" });
    expect(command.file).toBe("C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe");
    expect(command.env.PSModulePath).toBe("C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\Modules");
  });

  it.each([undefined, "", "relative", "C:Windows"])("rejects an unavailable or relative system root %j", SystemRoot => {
    expect(() => windowsSecurityPowerShell("Get-Acl", { SystemRoot })).toThrow("Windows system directory is unavailable");
  });
});
