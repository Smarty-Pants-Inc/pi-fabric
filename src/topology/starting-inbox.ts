import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { syncDirectoryChain, writeJsonAtomic } from "../core/atomic-write.js";
import { MeshStore, type MeshEvent } from "../mesh/store.js";
import { openPrivateDir } from "../storage/private-dir.js";
import type { FabricHostLease } from "./host-leases.js";
import type { FabricControlAcceptance } from "./control-plane.js";

export interface StartingFollowUp { format: 1; lease: FabricHostLease; event: MeshEvent; acceptance?: FabricControlAcceptance; }
/** Payload-free successful admission; the private event log already owns the full history. */
export interface StartingFollowUpCompletion {
  format: 1; incarnationToken: string; ownerIncarnation?: string | undefined; senderId: string; messageId?: string;
}

/** Host-only per-session storage, outside shared keyed state and the global event log. */
export const startingInboxRoot = (meshRoot: string, targetId: string): string =>
  path.join(meshRoot, "main-followups", createHash("sha256").update(targetId).digest("hex"), "starting-inbox");

export const openStartingInbox = (mesh: MeshStore, targetId: string, create = false): MeshStore | undefined => {
  const root = startingInboxRoot(mesh.root, targetId);
  if (!create && !fs.existsSync(root)) return undefined;
  for (const directory of [path.dirname(root), root]) {
    const held = openPrivateDir(directory, mesh.root);
    try {
      if (held.fd !== undefined) {
        if (fs.fstatSync(held.fd).uid !== process.getuid?.()) throw new Error("Startup inbox directory is not ours");
        fs.fchmodSync(held.fd, 0o700);
      }
    } finally { held.close(); }
  }
  // Private publication is still real MeshStore publication: never synthesize verification,
  // principal or sender stamps. Only its payload-free wake is published to the global mesh.
  return new MeshStore(root, mesh.maxEventBytes, mesh.maxReadEvents,
    { lockProtocol: mesh.lockProtocol, stateBackend: "file" });
};

const rowPath = (root: string, commandId: string): string => {
  if (!/^[a-f0-9]{64}$/.test(commandId)) throw new Error("Invalid startup command id");
  return path.join(root, `${commandId}.json`);
};
export const readStartingFollowUp = (root: string, commandId: string): StartingFollowUp | undefined => {
  try { return JSON.parse(fs.readFileSync(rowPath(root, commandId), "utf8")) as StartingFollowUp; }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; return undefined; }
};
export const listStartingFollowUps = (root: string): Array<{ commandId: string; row: StartingFollowUp }> => {
  let names: string[];
  try { names = fs.readdirSync(root); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; return []; }
  return names.filter(name => /^[a-f0-9]{64}\.json$/.test(name)).sort().flatMap(name => {
    const commandId = name.slice(0, -5), row = readStartingFollowUp(root, commandId);
    return row ? [{ commandId, row }] : [];
  });
};
export const readStartingFollowUpCompletion = (root: string, commandId: string): StartingFollowUpCompletion | undefined => {
  try { return JSON.parse(fs.readFileSync(rowPath(path.join(root, "completed"), commandId), "utf8")) as StartingFollowUpCompletion; }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; return undefined; }
};
/** Caller holds receiver lease custody. Persist the receipt BEFORE deleting the live payload row. */
export const completeStartingFollowUp = (root: string, commandId: string, row: StartingFollowUp,
  acceptance: FabricControlAcceptance): void => {
  if (!acceptance.accepted || !row.lease.incarnationToken) throw new Error("Startup followUp was not accepted by a fenced owner");
  const directory = path.join(root, "completed");
  const held = openPrivateDir(directory, root);
  try {
    if (held.fd !== undefined) {
      if (fs.fstatSync(held.fd).uid !== process.getuid?.()) throw new Error("Startup receipt directory is not ours");
      fs.fchmodSync(held.fd, 0o700);
    }
    writeJsonAtomic(rowPath(directory, commandId), { format: 1, incarnationToken: row.lease.incarnationToken,
      ownerIncarnation: row.lease.ownerIncarnation, senderId: row.event.from.id,
      ...(acceptance.messageId ? { messageId: acceptance.messageId } : {}),
    } satisfies StartingFollowUpCompletion, { durable: true, mode: 0o600, dirMode: 0o700 });
    removeStartingFollowUp(root, commandId);
  } finally { held.close(); }
};
/** A surviving row after a receipt commit is only compaction work, never another handler call. */
export const removeStartingFollowUp = (root: string, commandId: string): void => {
  try { fs.unlinkSync(rowPath(root, commandId)); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
  syncDirectoryChain(root);
};
/** Caller holds the receiver's lease custody through read/admission/write. */
export const writeStartingFollowUp = (root: string, commandId: string, row: StartingFollowUp): void => {
  writeJsonAtomic(rowPath(root, commandId), row, { durable: true, mode: 0o600, dirMode: 0o700 });
};
