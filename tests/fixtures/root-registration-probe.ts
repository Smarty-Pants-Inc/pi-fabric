// Disposable subprocess probe; every path/identity comes from a private test, never a Pi profile.
import { MeshStore } from "../../src/mesh/store.js";
import { RootRegistrationGuard } from "../../src/topology/root-registration.js";
import { ParticipantDirectory } from "../../src/topology/participant-directory.js";
const [meshRoot, sessionId, name] = process.argv.slice(2);
if (!meshRoot || !sessionId || !name) throw new Error("Private root probe requires mesh, session, name");
const mesh = new MeshStore(meshRoot, 64 * 1024, 100);
const id = `session:${sessionId}`;
const identity = { id, name: "main", kind: "main" as const, sessionId };
const observer = new ParticipantDirectory(mesh, { enabled: true, hostId: "probe-observer", rootId: "probe-observer", identity });
const guard = new RootRegistrationGuard(mesh, {
  publishedRoots: () => observer.list({ scope: "project", kinds: ["root"], fresh: true }),
});
let directory: ParticipantDirectory | undefined;
const deadline = setTimeout(() => process.exit(2), 15_000);
try {
  await guard.claim({ sessionId, rootId: `session:${sessionId}`, fabricSessionId: sessionId, name });
  directory = new ParticipantDirectory(mesh, { enabled: true, hostId: id, rootId: id, identity });
  directory.registerSource(() => [directory!.root({
    id, name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host",
    sessionId, cwd: meshRoot, updatedAt: Date.now(), pendingMessages: false, local: true,
  }, true, name, guard.ownerId)]);
  await directory.start();
  process.stdout.write(JSON.stringify({ state: "claimed" }) + "\n");
  await new Promise<void>(resolve => { process.stdin.once("data", () => resolve()); process.stdin.once("end", () => resolve()); });
  await directory.close();
  await guard.close();
} catch (error) {
  process.stdout.write(JSON.stringify({ state: "refused", code: (error as { code?: unknown }).code, error: String(error) }) + "\n");
  await directory?.close();
  await guard.close();
} finally { clearTimeout(deadline); }
