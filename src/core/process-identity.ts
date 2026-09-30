import fs from "node:fs";

/** Linux kernel identity, not a wall-clock timestamp or a PID-only liveness guess. */
export interface ProcessStartIdentity { pid: number; startTime: string; kernelId: string }
export interface ProcessIdentity extends ProcessStartIdentity {
  /** Exact /proc cmdline bytes (including NUL separators); only resident hosts need this. */
  commandLine: string;
}

let kernelId: string | undefined;
const localKernelId = (): string | undefined => {
  if (kernelId) return kernelId;
  if (process.platform !== "linux") return undefined;
  try {
    kernelId = `${fs.readFileSync("/proc/sys/kernel/random/boot_id", "utf8").trim()}/${fs.readlinkSync("/proc/self/ns/pid")}`;
    return kernelId;
  } catch { return undefined; }
};

export const validProcessStartIdentity = (value: unknown): value is ProcessStartIdentity => {
  const identity = value as Partial<ProcessStartIdentity> | null;
  return !!identity && Number.isSafeInteger(identity.pid) && identity.pid! > 1 &&
    typeof identity.startTime === "string" && /^\d+$/.test(identity.startTime) &&
    typeof identity.kernelId === "string" && /^[0-9a-f-]{36}\/pid:\[\d+\]$/.test(identity.kernelId);
};
export const validProcessIdentity = (value: unknown): value is ProcessIdentity =>
  validProcessStartIdentity(value) && typeof (value as ProcessIdentity).commandLine === "string" &&
  (value as ProcessIdentity).commandLine.length > 0;

export const readProcessStartIdentity = (pid = process.pid): ProcessStartIdentity | undefined => {
  if (process.platform !== "linux" || !Number.isSafeInteger(pid) || pid <= 1) return undefined;
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
    const startTime = fields[19];
    if (!startTime || !/^\d+$/.test(startTime) || fields[0] === "Z") return undefined;
    const kernelId = localKernelId();
    if (!kernelId || !kernelId.endsWith(`/${fs.readlinkSync(`/proc/${pid}/ns/pid`)}`)) return undefined;
    return { pid, startTime, kernelId };
  } catch { return undefined; }
};

export const readProcessIdentity = (pid = process.pid): ProcessIdentity | undefined => {
  const identity = readProcessStartIdentity(pid);
  if (!identity) return undefined;
  try {
    const commandLine = fs.readFileSync(`/proc/${pid}/cmdline`, "utf8");
    const after = readProcessStartIdentity(pid);
    if (!commandLine || after?.startTime !== identity.startTime) return undefined;
    return { ...identity, commandLine };
  } catch { return undefined; }
};

/** Absent is death only when the kernel proves absence; unreadable/unsupported is unknown. */
export const processStartIdentityState = (expected: ProcessStartIdentity): "alive" | "dead" | "mismatch" | "unknown" => {
  if (process.platform !== "linux" || !validProcessStartIdentity(expected) || expected.kernelId !== localKernelId()) return "unknown";
  try {
    const stat = fs.readFileSync(`/proc/${expected.pid}/stat`, "utf8");
    const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
    if (!fields[19] || !/^\d+$/.test(fields[19])) return "unknown";
    if (fields[19] !== expected.startTime) return "mismatch";
    if (fields[0] === "Z") return "dead";
    return expected.kernelId.endsWith(`/${fs.readlinkSync(`/proc/${expected.pid}/ns/pid`)}`) ? "alive" : "unknown";
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === "ENOENT" && !fs.existsSync(`/proc/${expected.pid}`) ? "dead" : "unknown";
  }
};

export const processIdentityState = (expected: ProcessIdentity): "alive" | "dead" | "mismatch" | "unknown" => {
  if (!validProcessIdentity(expected)) return "unknown";
  const before = processStartIdentityState(expected);
  if (before !== "alive") return before;
  try {
    const commandLine = fs.readFileSync(`/proc/${expected.pid}/cmdline`, "utf8");
    // Fence PID reuse during the multi-file observation, not just before reading cmdline.
    const after = processStartIdentityState(expected);
    if (after !== "alive") return after;
    if (!commandLine) return "unknown"; // an exit transition, never permission to signal
    return commandLine === expected.commandLine ? "alive" : "mismatch";
  } catch { return processStartIdentityState(expected) === "dead" ? "dead" : "unknown"; }
};
