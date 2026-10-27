import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { PEERCRED_PYTHON, peerCredentials } from "../src/records/peer.js";

const linux = process.platform === "linux" && fs.existsSync(PEERCRED_PYTHON);
const cleanups: Array<() => void> = [];
afterEach(() => { for (const cleanup of cleanups.splice(0).reverse()) cleanup(); });

/** A Unix socket pair in a private dir: the server side's peer is this process. */
const connected = async (): Promise<net.Socket> => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "peercred-"));
  const file = path.join(dir, "s.sock");
  const accepted = new Promise<net.Socket>((resolve) => {
    const server = net.createServer(resolve).listen(file);
    cleanups.push(() => { server.close(); fs.rmSync(dir, { recursive: true, force: true }); });
  });
  const client = net.connect(file);
  cleanups.push(() => client.destroy());
  const socket = await accepted;
  cleanups.push(() => socket.destroy());
  return socket;
};

describe.skipIf(!linux)("peerCredentials (SO_PEERCRED through the fixed interpreter)", () => {
  it("reads the real peer, and a python3 planted first in PATH cannot answer for the kernel (#1607 security P2)", async () => {
    const planted = fs.mkdtempSync(path.join(os.tmpdir(), "planted-"));
    cleanups.push(() => fs.rmSync(planted, { recursive: true, force: true }));
    // A lying helper: it would name pid 1 as root.
    fs.writeFileSync(path.join(planted, "python3"), "#!/bin/sh\necho 1 0 0\n", { mode: 0o700 });
    const saved = process.env.PATH;
    process.env.PATH = `${planted}:${saved ?? ""}`;
    cleanups.push(() => { process.env.PATH = saved; });
    const peer = await peerCredentials(await connected());
    expect(peer).toMatchObject({ pid: process.pid, uid: process.getuid!() });
  });
});
