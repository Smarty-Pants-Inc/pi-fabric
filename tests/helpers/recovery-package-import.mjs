import fs from "node:fs";
import { fileURLToPath } from "node:url";

// Fresh-process import through the PUBLIC export. Permit the ESM loader's exact
// bundle read, but reject eager store/host I/O. No Pi session files are accessed.
const moduleFile = fileURLToPath(import.meta.resolve("pi-fabric/recovery"));
const names = ["openSync", "readFileSync", "writeFileSync", "mkdirSync", "readdirSync", "fsyncSync", "renameSync", "unlinkSync"];
const original = new Map(names.map(name => [name, fs[name]]));
let api;
try {
  for (const name of names) fs[name] = (...args) => {
    const file = args[0] instanceof URL ? fileURLToPath(args[0]) : args[0];
    if ((name === "readFileSync" || name === "openSync") && file === moduleFile) return Reflect.apply(original.get(name), fs, args);
    throw new Error(`Eager recovery import called fs.${name}`);
  };
  api = await import("pi-fabric/recovery");
} finally {
  for (const [name, method] of original) fs[name] = method;
}
const required = ["openRecoveryJournal", "openRecoveryDurabilityStore", "createRecoveryAttemptLedger", "buildRecoveryOffVictimInventory", "GatewayJoinCapture", "persistRecoveryAttemptCompletion", "sessionRecoveryAdapters"];
if (required.some(name => typeof api[name] !== "function")) throw new Error("Public recovery export omitted a required API");
const directory = process.argv[2];
const journal = await api.openRecoveryJournal({ directory, storeId: "public-first-use" });
const first = await journal.append("immutable", Buffer.from("retained public-entry evidence"), { deadlineMs: Date.now() + 30_000 });
await journal.close();
const recovered = await api.openRecoveryJournal({ directory, storeId: "public-first-use", required: true });
const same = await recovered.append("immutable", Buffer.from("retained public-entry evidence"), { deadlineMs: Date.now() + 30_000 });
const bytes = Buffer.from(recovered.get("immutable").bytes).toString();
await recovered.close();
process.stdout.write(JSON.stringify({ required, first, same, bytes }));
