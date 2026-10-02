import fs from "node:fs";
import { describe, expect, it, vi } from "vitest";
import { JevClient, JevCredentials } from "../src/jev/client.js";
import { DEFAULT_JEV_CONFIG } from "../src/jev/config.js";
import { alive, commandFixture, credentialRequest, credentialResponse, delay } from "./jev-command-test-helpers.js";

// Command-backed credentials are POSIX-only: Windows refuses before spawning
// (see tests/jev-command-tree-failure.test.ts).
describe.skipIf(process.platform === "win32")("SR-7 real credential-command lifetime", () => {
  it.skipIf(process.platform === "win32").each(["caller", "request deadline"] as const)("%s cancellation stays prompt but drain waits for forced tree exit", async cancellation => {
    const fixture = commandFixture(cancellation === "caller");
    const credentials = new JevCredentials(fixture.command, {});
    const fetcher = vi.fn(async () => new Response(JSON.stringify(credentialResponse)));
    const client = new JevClient({ ...DEFAULT_JEV_CONFIG, requestTimeoutMs: cancellation === "caller" ? 20000 : 2000 }, fetcher, credentials);
    const controller = new AbortController();
    const pending = client.evaluate(credentialRequest, controller.signal).then(() => "unexpected success", error => String(error));
    let drained: Promise<void> | undefined;
    try {
      const pid = await fixture.ready();
      if (cancellation === "caller") controller.abort(new Error("offline caller cancelled"));
      const error = await pending;
      expect(error).not.toContain("FAKE_SECRET");
      expect(error).not.toBe("unexpected success");
      await vi.waitFor(() => expect(fs.existsSync(fixture.terminated)).toBe(true), { interval: 10 });
      let joined = false;
      drained = client.drainCredentials().then(() => { joined = true; });
      await delay(50);
      expect(alive(pid), "the command ignores SIGTERM through the grace period").toBe(true);
      expect(joined, "cleanup must not report joined while its command is alive").toBe(false);
      expect(fetcher).not.toHaveBeenCalled();
      await drained;
      expect(fixture.pids().map(alive)).toEqual(fixture.pids().map(() => false));
      expect(joined).toBe(true);
    } finally {
      controller.abort();
      await fixture.cleanup();
      await pending; await drained; await client.drainCredentials(); client.close();
    }
  });

  it.skipIf(process.platform === "win32")("the resolver's own five-second deadline also forces exit and joins", async () => {
    const fixture = commandFixture();
    const credentials = new JevCredentials(fixture.command, {});
    const pending = credentials.resolve(new AbortController().signal).then(() => "unexpected success", error => String(error));
    const client = new JevClient(DEFAULT_JEV_CONFIG, undefined, credentials);
    try {
      const pid = await fixture.ready();
      expect(await Promise.race([pending, delay(6500).then(() => "resolver did not cancel")])).toBe("Error: Jev credential resolver failed");
      let joined = false;
      const drained = client.drainCredentials().then(() => { joined = true; });
      await delay(50);
      expect(alive(pid)).toBe(true); expect(joined).toBe(false);
      await drained; expect(alive(pid)).toBe(false);
    } finally {
      await fixture.cleanup(); await pending; await client.drainCredentials(); client.close();
    }
  }, 10000);

  it.skipIf(process.platform === "win32")("joins escalation even when a TERM-cooperative parent closes before its stubborn descendant", async () => {
    const fixture = commandFixture(true, false, false);
    const controller = new AbortController();
    const credentials = new JevCredentials(fixture.command, {});
    const client = new JevClient(DEFAULT_JEV_CONFIG, undefined, credentials);
    const pending = credentials.resolve(controller.signal).catch(error => String(error));
    let drained: Promise<void> | undefined;
    try {
      const pid = await fixture.ready();
      controller.abort();
      expect(await pending).toBe("Error: Jev credential resolver failed");
      await vi.waitFor(() => expect(alive(pid)).toBe(false), { interval: 10 });
      expect(alive(fixture.pids()[0]!)).toBe(true);
      let joined = false;
      drained = client.drainCredentials().then(() => { joined = true; });
      await delay(50); expect(joined).toBe(false);
      await drained;
      await vi.waitFor(() => expect(fixture.pids().map(alive)).toEqual([false, false]), { interval: 10 });
    } finally { controller.abort(); await fixture.cleanup(); await pending; await drained; await client.drainCredentials(); }
  });

  it.skipIf(process.platform === "win32").each([0, 7])("parent exit %i does not join a pipe-independent descendant still in the owned group", async exitCode => {
    // No inherited pipes: parent close is real and prompt, while its unref'ed
    // descendant remains independently alive in the captured POSIX group.
    const fixture = commandFixture(true, true, false, exitCode);
    const credentials = new JevCredentials(fixture.command, {});
    const client = new JevClient(DEFAULT_JEV_CONFIG, undefined, credentials);
    const controller = new AbortController();
    const pending = credentials.resolve(controller.signal).then(value => value, error => String(error));
    let drained: Promise<void> | undefined;
    try {
      const parent = await fixture.ready();
      const descendant = fixture.pids()[0]!;
      fs.writeFileSync(fixture.exitFile, "spontaneous parent exit");
      expect(await pending).toBe(exitCode === 0 ? "FAKE_SECRET_MUST_NOT_BE_USED" : "Error: Jev credential resolver failed");
      expect(alive(parent)).toBe(false);
      expect(alive(descendant)).toBe(true);
      // The group, unlike the parent/streams, still exists at this point.
      expect(() => process.kill(-parent, 0)).not.toThrow();
      let joined = false;
      drained = client.drainCredentials().then(() => { joined = true; });
      await delay(50);
      expect(alive(descendant)).toBe(true);
      expect(joined, "parent close alone must not discharge the owned-group obligation").toBe(false);
      await drained;
      expect(alive(descendant)).toBe(false);
      expect(() => process.kill(-parent, 0)).toThrow(expect.objectContaining({ code: "ESRCH" }));
      expect(joined).toBe(true);
    } finally {
      controller.abort(); await fixture.cleanup(); await pending; await drained; await client.drainCredentials();
    }
  });

  it.each(["stdout", "stderr"] as const)("bounds and sanitizes excessive %s", async stream => {
    const credentials = new JevCredentials([process.execPath, "-e", `process.${stream}.write('FAKE_SECRET'.repeat(2000)); setInterval(() => {}, 1000)`], {});
    const client = new JevClient(DEFAULT_JEV_CONFIG, undefined, credentials);
    await expect(credentials.resolve(new AbortController().signal)).rejects.toThrow(/^Jev credential resolver failed$/);
    await client.drainCredentials();
  });

  it("synchronous spawn validation errors are sanitized without inventing a close obligation", async () => {
    const credentials = new JevCredentials([process.execPath, "FAKE_SECRET\u0000"], {});
    await expect(credentials.resolve(new AbortController().signal)).rejects.toThrow(/^Jev credential resolver failed$/);
    await new JevClient(DEFAULT_JEV_CONFIG, undefined, credentials).drainCredentials();
  });

  it("successful command output remains cached, validated and drained", async () => {
    const credentials = new JevCredentials([process.execPath, "-e", "console.log('  offline-key  ')"], {});
    const client = new JevClient(DEFAULT_JEV_CONFIG, undefined, credentials);
    expect(await credentials.resolve(new AbortController().signal)).toBe("offline-key");
    await client.drainCredentials();
    expect(await credentials.resolve(new AbortController().signal)).toBe("offline-key");
    client.close();
    const invalid = new JevCredentials([process.execPath, "-e", "console.log('fake\\nsecret')"], {});
    await expect(invalid.resolve(new AbortController().signal)).rejects.toThrow(/^Jev credential resolver returned an invalid credential$/);
    await new JevClient(DEFAULT_JEV_CONFIG, undefined, invalid).drainCredentials();
  });

  it("failed spawn is sanitized and its close obligation drains", async () => {
    const credentials = new JevCredentials(["nonexistent-jev-command-FAKE_SECRET"], {});
    await expect(credentials.resolve(new AbortController().signal)).rejects.toThrow(/^Jev credential resolver failed$/);
    await new JevClient(DEFAULT_JEV_CONFIG, undefined, credentials).drainCredentials();
  });
});
