import path from "node:path";

/** Pin native ACL commands to Windows PowerShell and its own system modules. */
export const windowsSecurityPowerShell = (source: string, inherited: NodeJS.ProcessEnv = process.env) => {
  const systemRoot = Object.entries(inherited).find(([key]) => key.toLowerCase() === "systemroot")?.[1];
  if (!systemRoot || !path.win32.isAbsolute(systemRoot)) throw new Error("Windows system directory is unavailable");
  const home = path.win32.join(systemRoot, "System32", "WindowsPowerShell", "v1.0");
  // Environment names are case-insensitive on Windows. Remove every spelling so
  // a parent pwsh installation cannot redirect 5.1's security-module resolution.
  const env = Object.fromEntries(Object.entries(inherited).filter(([key]) => key.toLowerCase() !== "psmodulepath"));
  env.PSModulePath = path.win32.join(home, "Modules");
  const script = "$ErrorActionPreference = 'Stop'\nImport-Module Microsoft.PowerShell.Security -ErrorAction Stop\n" + source;
  return {
    file: path.win32.join(home, "powershell.exe"),
    args: ["-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand", Buffer.from(script, "utf16le").toString("base64")],
    env,
  };
};
