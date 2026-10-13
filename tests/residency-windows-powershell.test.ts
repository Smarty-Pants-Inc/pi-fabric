import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { windowsOwnerSids } from "../src/actors-cli.js";
import { windowsSecurityPowerShell } from "../src/storage/windows-powershell.js";

const subprocess = vi.hoisted(() => ({ execFileSync: vi.fn() }));
vi.mock("node:child_process", async importOriginal => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, execFileSync: subprocess.execFileSync };
});

const user = "S-1-5-21-111-222-333-1001";
const modulePrefix = "$ErrorActionPreference = 'Stop'\nImport-Module Microsoft.PowerShell.Security -ErrorAction Stop\n";

beforeEach(() => {
  vi.stubEnv("SystemRoot", "C:\\Windows");
  vi.stubEnv("PSModulePath", "C:\\Program Files\\PowerShell\\7\\Modules");
  vi.stubEnv("psmodulepath", "D:\\foreign-modules");
  vi.stubEnv("PsModulePath", "E:\\foreign-modules");
  subprocess.execFileSync.mockReset();
});
afterEach(() => { vi.unstubAllEnvs(); });

describe("actors CLI owner PowerShell launch under a pwsh parent", () => {
  it("pins owner queries to Windows security PowerShell while keeping paths out of the script", () => {
    const files = ["R:\\resident\\quote'$;日本語", "R:\\resident\\owner.json"];
    subprocess.execFileSync.mockReturnValue(`${user}\r\n${user}\r\n${user}\r\n`);
    expect(windowsOwnerSids(files)).toEqual({ owners: [user, user], user });
    expect(subprocess.execFileSync).toHaveBeenCalledTimes(1);
    const [file, args, options] = subprocess.execFileSync.mock.calls[0]! as [string, string[], { env: NodeJS.ProcessEnv }];
    expect(args).toEqual(["-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand", expect.any(String)]);
    const source = Buffer.from(args[4]!, "base64").toString("utf16le");
    expect(source.startsWith(modulePrefix)).toBe(true);
    const command = windowsSecurityPowerShell(source.slice(modulePrefix.length));
    expect(file).toBe(command.file);
    expect(file).toBe("C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe");
    expect(args).toEqual(command.args);
    expect(options).toMatchObject({ encoding: "utf8", windowsHide: true, timeout: 30_000, stdio: ["ignore", "pipe", "pipe"] });
    expect(options.env).toEqual({ ...command.env, FABRIC_ACTORS_OWNER_PATHS: files.join("\n") });
    expect(options.env.PSModulePath).toBe("C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\Modules");
    expect(Object.keys(options.env).filter(key => key.toLowerCase() === "psmodulepath")).toEqual(["PSModulePath"]);
    expect(source).toContain("$env:FABRIC_ACTORS_OWNER_PATHS");
    expect(source).toContain("Get-Acl -LiteralPath $p");
    expect(source).not.toContain(files[0]);
  });

  it.each(["R:\\resident\\line\nbreak", "R:\\resident\\line\rbreak"])("refuses a line break in a path before launching %j", file => {
    expect(() => windowsOwnerSids([file])).toThrow("path contains a line break");
    expect(subprocess.execFileSync).not.toHaveBeenCalled();
  });

  it.each(["", `${user}\r\n`, `${user}\r\n${user}\r\n${user}\r\n`])("refuses missing or extra owner query rows %j", output => {
    subprocess.execFileSync.mockReturnValue(output);
    expect(() => windowsOwnerSids(["R:\\resident"])).toThrow("unexpected owner query output");
  });
});
