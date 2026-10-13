import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { windowsOwnerSids } from "../src/actors-cli.js";
import { encodeWindowsAclPath, readWindowsDirectoryAcl, readWindowsFileAcls } from "../src/residency/windows-acl.js";
import { windowsSecurityPowerShell } from "../src/storage/windows-powershell.js";

const subprocess = vi.hoisted(() => ({ execFileAsync: vi.fn(), execFileSync: vi.fn() }));
vi.mock("node:child_process", async importOriginal => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return {
    ...actual,
    execFile: Object.assign(vi.fn(), { [Symbol.for("nodejs.util.promisify.custom")]: subprocess.execFileAsync }),
    execFileSync: subprocess.execFileSync,
  };
});

const user = "S-1-5-21-111-222-333-1001";
const privateReport = [
  `user ${user}`, `owner ${user}`, `dacl D:P(A;OICI;FA;;;${user})`, `ace Allow ${user} 2032127`, "end",
].join("\r\n");
const modulePrefix = "$ErrorActionPreference = 'Stop'\nImport-Module Microsoft.PowerShell.Security -ErrorAction Stop\n";

beforeEach(() => {
  vi.stubEnv("SystemRoot", "C:\\Windows");
  vi.stubEnv("PSModulePath", "C:\\Program Files\\PowerShell\\7\\Modules");
  vi.stubEnv("psmodulepath", "D:\\foreign-modules");
  vi.stubEnv("PsModulePath", "E:\\foreign-modules");
  subprocess.execFileAsync.mockReset();
  subprocess.execFileSync.mockReset();
});
afterEach(() => { vi.unstubAllEnvs(); });

function assertSecurityLaunch(call: unknown[], inputName: string, input: string): string {
  const [file, args, options] = call as [string, string[], { env: NodeJS.ProcessEnv }];
  expect(args).toEqual(["-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand", expect.any(String)]);
  const source = Buffer.from(args[4]!, "base64").toString("utf16le");
  expect(source.startsWith(modulePrefix)).toBe(true);
  const command = windowsSecurityPowerShell(source.slice(modulePrefix.length));
  expect(file).toBe(command.file);
  expect(file).toBe("C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe");
  expect(args).toEqual(command.args);
  expect(options).toMatchObject({ encoding: "utf8", windowsHide: true, timeout: 30_000 });
  expect(options.env).toEqual({ ...command.env, [inputName]: input });
  expect(options.env.PSModulePath).toBe("C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\Modules");
  expect(Object.keys(options.env).filter(key => key.toLowerCase() === "psmodulepath")).toEqual(["PSModulePath"]);
  return source;
}

describe("resident ACL and owner PowerShell launches under a pwsh parent", () => {
  it("pins the residency directory DACL check to Windows security PowerShell", async () => {
    const directory = "R:\\resident\\quote'$;日本語";
    subprocess.execFileAsync.mockResolvedValue({ stdout: privateReport });
    await expect(readWindowsDirectoryAcl(directory)).resolves.toMatchObject({ user, owner: user, dacl: "present" });
    expect(subprocess.execFileAsync).toHaveBeenCalledTimes(1);
    const source = assertSecurityLaunch(subprocess.execFileAsync.mock.calls[0]!, "FABRIC_ACL_PATH", directory);
    expect(source).toContain("Get-Acl -LiteralPath $env:FABRIC_ACL_PATH");
    expect(source).not.toContain(directory);
  });

  it("pins batch file ACL checks while preserving encoded paths in the environment", async () => {
    const files = ["R:\\resident\\quote'$;日本語\r\n.json", "R:\\resident\\owner.json"];
    const encoded = files.map(encodeWindowsAclPath);
    subprocess.execFileAsync.mockResolvedValue({ stdout: [
      ...encoded.map((token, index) => `file ${index}\r\npath ${token}\r\nattributes 32\r\n${privateReport}`), "batch-end",
    ].join("\r\n") });
    await expect(readWindowsFileAcls(files)).resolves.toEqual(files.map(() => ({
      attributes: 32, acl: { user, owner: user, dacl: "present", aces: [{ type: "Allow", sid: user, rights: 2032127 }] },
    })));
    expect(subprocess.execFileAsync).toHaveBeenCalledTimes(1);
    const source = assertSecurityLaunch(subprocess.execFileAsync.mock.calls[0]!, "FABRIC_ACL_PATHS", encoded.join("\n"));
    expect(source).toContain("$env:FABRIC_ACL_PATHS");
    expect(source).toContain("Get-Acl -LiteralPath $p");
    expect(source).not.toContain(files[0]);
    expect(source).not.toContain(encoded[0]);
  });

  it("pins actors CLI owner queries while keeping paths out of the script", () => {
    const files = ["R:\\resident\\quote'$;日本語", "R:\\resident\\owner.json"];
    subprocess.execFileSync.mockReturnValue(`${user}\r\n${user}\r\n${user}\r\n`);
    expect(windowsOwnerSids(files)).toEqual({ owners: [user, user], user });
    expect(subprocess.execFileSync).toHaveBeenCalledTimes(1);
    const source = assertSecurityLaunch(subprocess.execFileSync.mock.calls[0]!, "FABRIC_ACTORS_OWNER_PATHS", files.join("\n"));
    expect(source).toContain("$env:FABRIC_ACTORS_OWNER_PATHS");
    expect(source).toContain("Get-Acl -LiteralPath $p");
    expect(source).not.toContain(files[0]);
    expect(subprocess.execFileSync.mock.calls[0]![2]).toMatchObject({ stdio: ["ignore", "pipe", "pipe"] });
  });
});
