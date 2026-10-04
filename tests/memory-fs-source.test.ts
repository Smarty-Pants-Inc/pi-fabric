import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { normalizeFabricConfig } from "../src/config.js";
import type { FabricComponentContext } from "../src/components/types.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import {
  createFileSystemMemorySource,
  createMemorySourceRegistry,
  defineMemorySource,
  type MemorySourceListPage,
  type MemorySourceSessionDescriptor,
} from "../src/memory.js";
import { MemoryProvider } from "../src/providers/memory-provider.js";
import { RuntimeStateBuiltins } from "../src/runtime-state-builtins.js";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { FabricInvocationContext } from "../src/protocol.js";
import {
  assistantText,
  messageEntry,
  sessionHeader,
  userMessage,
  writeSessionFile,
} from "./fixtures/memory.js";

const rootDir = (prefix: string): string =>
  fs.mkdtempSync(path.join(os.tmpdir(), `pi-fabric-fs-source-${prefix}-`));

const directories: string[] = [];
afterEach(() => {
  while (directories.length > 0) fs.rmSync(directories.pop()!, { recursive: true, force: true });
});

const track = (dir: string): string => {
  directories.push(dir);
  return dir;
};

const recordsFor = (id: string, cwd: string, texts: string[]) => [
  sessionHeader(id, cwd),
  ...texts.map((text, index) =>
    messageEntry(
      id + "-" + index,
      index === 0 ? null : id + "-" + (index - 1),
      new Date(1_700_000_000_000 + index * 1_000).toISOString(),
      index % 2 === 0 ? userMessage(text) : assistantText(text),
    ),
  ),
];

const setMtime = (file: string, mtimeMs: number): void =>
  fs.utimesSync(file, new Date(mtimeMs), new Date(mtimeMs));

const asArray = (
  listed: readonly MemorySourceSessionDescriptor[] | MemorySourceListPage,
): { sessions: readonly MemorySourceSessionDescriptor[]; coverageReason: string | undefined } =>
  "sessions" in listed
    ? {
        sessions: listed.sessions,
        coverageReason: listed.coverage?.complete === false ? listed.coverage.reason : undefined,
      }
    : { sessions: listed, coverageReason: undefined };

describe("filesystem memory source adapter", () => {
  it("enumerates native agent trees and flat archives with opaque keys", async () => {
    const root = track(rootDir("layouts"));
    const native = writeSessionFile(
      path.join(root, "--home-e41q-projects-archive--"),
      "2026-01-02-10-00-00.jsonl",
      recordsFor("tree-session", "/home/e41q/projects/archive", ["native tree needle"]),
    );
    const flat = writeSessionFile(
      root,
      "laptop-export.jsonl",
      recordsFor("flat-session", "/home/e41q/projects/flat", ["flat archive needle"]),
    );
    setMtime(native, 2_000);
    setMtime(flat, 1_000);
    const source = createFileSystemMemorySource({ id: "laptop", root });
    expect(defineMemorySource(source)).toBe(source);

    const { sessions } = asArray(await source.listSessions({ limit: 10 }));
    expect(sessions.map((session) => session.sessionKey)).toEqual([
      "--home-e41q-projects-archive--/2026-01-02-10-00-00.jsonl",
      "laptop-export.jsonl",
    ]);
    expect(sessions[0]!.sessionId).toBe("tree-session");
    expect(sessions[0]!.revision).toMatch(/^[0-9a-f]{64}$/);
    expect(sessions[0]!.metadata?.cwd).toBe("/home/e41q/projects/archive");
    expect(sessions[0]!.metadata?.updatedAt).toBe(2_000);
  });

  it("reports enumeration caps as incomplete coverage", async () => {
    const root = track(rootDir("caps"));
    for (let index = 0; index < 3; index++) {
      const file = writeSessionFile(
        root,
        `session-${index}.jsonl`,
        recordsFor("cap-session-" + index, "/work", ["cap needle " + index]),
      );
      setMtime(file, 1_000 + index);
    }
    const source = createFileSystemMemorySource({ id: "capped", root });

    const capped = asArray(await source.listSessions({ limit: 2 }));
    expect(capped.sessions).toHaveLength(2);
    expect(capped.sessions.map((session) => session.sessionKey)).toEqual([
      "session-2.jsonl",
      "session-1.jsonl",
    ]);
    expect(capped.coverageReason).toBe("fs_source_max_sessions");

    const complete = asArray(await source.listSessions({ limit: 10 }));
    expect(complete.sessions).toHaveLength(3);
    expect(complete.coverageReason).toBeUndefined();
  });

  it("keeps revisions stable across mtime-only touches and invalidates on content change", async () => {
    const root = track(rootDir("revisions"));
    const file = writeSessionFile(
      root,
      "rev.jsonl",
      recordsFor("rev-session", "/work", ["revision needle"]),
    );
    const source = createFileSystemMemorySource({ id: "revs", root });
    const before = asArray(await source.listSessions({ limit: 1 })).sessions[0]!;
    expect(before).toBeDefined();

    setMtime(file, 9_999);
    const afterTouch = asArray(await source.listSessions({ limit: 1 }));
    expect(afterTouch.sessions[0]!.revision).toBe(before.revision);

    fs.appendFileSync(file, JSON.stringify(
      messageEntry("rev-session-1", "rev-session-0", new Date(1_700_000_001_000).toISOString(), userMessage("appended")),
    ) + "\n", "utf8");
    const afterAppend = asArray(await source.listSessions({ limit: 1 }));
    expect(afterAppend.sessions[0]!.revision).not.toBe(before.revision);
  });

  it("rejects a parent replacement between validation and open without reading the escaped handle", async () => {
    const root = track(rootDir("race"));
    const outside = track(rootDir("race-outside"));
    const dir = path.join(root, "nested");
    const file = writeSessionFile(dir, "s.jsonl", recordsFor("safe", "/work", ["safe"]));
    writeSessionFile(outside, "s.jsonl", recordsFor("secret", "/private", ["secret"]));
    const open = fs.openSync.bind(fs);
    const read = vi.spyOn(fs, "readFileSync");
    const spy = vi.spyOn(fs, "openSync").mockImplementation(((target: any, flags: any, mode: any) => {
      if (target === file) {
        fs.renameSync(dir, dir + "-original");
        fs.symlinkSync(outside, dir, "dir");
      }
      return open(target, flags, mode);
    }) as typeof fs.openSync);
    try {
      const source = createFileSystemMemorySource({ id: "race", root });
      expect(await source.loadSession("nested/s.jsonl", {})).toBeNull();
      expect(read).not.toHaveBeenCalled();
    } finally { spy.mockRestore(); read.mockRestore(); }
  });

  it("loads sessions with normalizeSession-compatible parsing and no filesystem escape", async () => {
    const root = track(rootDir("loads"));
    const outside = track(rootDir("loads-outside"));
    writeSessionFile(
      root,
      "good.jsonl",
      recordsFor("load-session", "/work/load", ["loadable needle"]),
    );
    fs.writeFileSync(path.join(root, "broken.jsonl"), "{not json}\n\n", "utf8");
    writeSessionFile(outside, "secret.jsonl", recordsFor("secret", "/work", ["secret needle"]));
    const source = createFileSystemMemorySource({ id: "loads", root });

    const snapshot = await source.loadSession("good.jsonl", {});
    expect(snapshot).not.toBeNull();
    expect(snapshot!.sessionKey).toBe("good.jsonl");
    expect(snapshot!.sessionId).toBe("load-session");
    expect(snapshot!.records[0]).toMatchObject({ type: "session", id: "load-session" });
    expect(snapshot!.records.filter((record) => record.type === "message")).toHaveLength(1);
    expect(snapshot!.revision).toMatch(/^[0-9a-f]{64}$/);

    const empty = await source.loadSession("broken.jsonl", {});
    expect(empty).not.toBeNull();
    expect(empty!.records).toEqual([]);

    expect(await source.loadSession("missing.jsonl", {})).toBeNull();
    expect(await source.loadSession("../" + path.basename(outside) + "/secret.jsonl", {})).toBeNull();
    expect(await source.loadSession(path.resolve(outside, "secret.jsonl"), {})).toBeNull();
    if (process.platform !== "win32") {
      fs.symlinkSync(path.join(outside, "secret.jsonl"), path.join(root, "file-link.jsonl"));
      fs.symlinkSync(outside, path.join(root, "directory-link"), "dir");
      expect(await source.loadSession("file-link.jsonl", {})).toBeNull();
      expect(await source.loadSession("directory-link/secret.jsonl", {})).toBeNull();
      expect(asArray(await source.listSessions({ limit: 20 })).sessions.map(session => session.sessionKey))
        .not.toEqual(expect.arrayContaining(["file-link.jsonl", "directory-link/secret.jsonl"]));
    }
  });
});

describe("memory.sources configuration", () => {
  it("parses validated entries and keeps absent values absent", () => {
    const root = "/tmp/archive";
    const withSources = normalizeFabricConfig({
      memory: { sources: [{ id: "laptop", kind: "fs", root }] },
    });
    expect(withSources.memory.sources).toEqual([{ id: "laptop", kind: "fs", root }]);

    const absent = normalizeFabricConfig({});
    expect(absent.memory.sources).toBeUndefined();
    expect("sources" in absent.memory).toBe(false);

    expect(normalizeFabricConfig({ memory: { sources: [] } }).memory.sources).toBeUndefined();
    expect(normalizeFabricConfig({ memory: { sources: "nope" } }).memory.sources).toBeUndefined();
  });

  it.each([
    [{ id: "Bad ID", kind: "fs", root: "/tmp/a" }, /memory.sources\[0\].*id/],
    [{ id: "ok", kind: "http", root: "https://x" }, /unknown kind/],
    [{ id: "ok", kind: "fs", root: "relative/path" }, /root must be an absolute/],
    [{ id: "ok", kind: "fs", root: "/tmp/a" }, /duplicate source id/],
  ])("rejects malformed entry %j", (entry, pattern) => {
    expect(() =>
      normalizeFabricConfig({ memory: { sources: [entry, entry] } }),
    ).toThrow(pattern);
  });
});

describe("runtime memory source wiring", () => {
  const invocation = (): FabricInvocationContext => ({
    cwd: "/work/nowhere",
    signal: undefined,
    parentToolCallId: "fs-source-wiring",
    nestedToolCallId: "fs-source-wiring-nested",
    extensionContext: {} as FabricInvocationContext["extensionContext"],
    update() {},
  });

  const extensionContext = (dir: string): ExtensionContext =>
    ({
      cwd: dir,
      sessionManager: {
        getSessionFile: () => null,
        getBranch: () => [],
        getLeafId: () => null,
      },
    }) as unknown as ExtensionContext;

  const installMemoryProvider = async (config: ReturnType<typeof normalizeFabricConfig>) => {
    const manifest = { install: vi.fn(async (_component: unknown) => {}), assertActive: vi.fn() };
    const builtins = new RuntimeStateBuiltins(
      manifest as unknown as ConstructorParameters<typeof RuntimeStateBuiltins>[0],
      new ActionRegistry(),
      vi.fn(),
    );
    await builtins.memory(extensionContext("/work/nowhere"), config, "wiring-session");
    const component = manifest.install.mock.calls[0]![0] as unknown as {
      definition: { activate(context: FabricComponentContext): Promise<void> };
    };
    let created: unknown;
    await component.definition.activate({
      provide: (provider: unknown) => {
        created = provider;
      },
      defer: () => {},
    } as unknown as FabricComponentContext);
    return created as MemoryProvider;
  };

  it("routes source-qualified recall through the configured fs source", async () => {
    const root = track(rootDir("wiring"));
    writeSessionFile(root, "exported.jsonl", recordsFor("wired-session", "/work", ["wired needle"]));
    const provider = await installMemoryProvider(normalizeFabricConfig({
      memory: { enabled: true, sources: [{ id: "laptop", kind: "fs", root }] },
    }));

    const recalled = (await provider.invoke(
      "recall",
      { source: "laptop", query: "wired needle" },
      invocation(),
    )) as { total: number; hits: Array<{ sessionId: string }>; coverage: { complete: boolean } };
    expect(recalled.total).toBeGreaterThan(0);
    expect(recalled.hits[0]!.sessionId).toBe("wired-session");
    expect(recalled.coverage.complete).toBe(true);

    const sessions = (await provider.invoke(
      "sessions",
      { source: "laptop" },
      invocation(),
    )) as { sessions: Array<{ id: string; file: string }> };
    expect(sessions.sessions.map((session) => session.id)).toContain("wired-session");
    expect(sessions.sessions[0]!.file.startsWith("memory-source:laptop/")).toBe(true);
  });

  it("reports a missing configured archive as non-exhaustive recall", async () => {
    const root = path.join(track(rootDir("missing-recall")), "absent");
    const provider = await installMemoryProvider(normalizeFabricConfig({
      memory: { enabled: true, sources: [{ id: "laptop", kind: "fs", root }] },
    }));
    const recalled = await provider.invoke("recall", { source: "laptop", query: "anything" }, invocation()) as {
      total: number; coverage: { complete: boolean; reasons: string[] };
    };
    expect(recalled.total).toBe(0);
    expect(recalled.coverage.complete).toBe(false);
    expect(recalled.coverage.reasons).toContain("source_coverage:fs_source_unavailable");
    expect(JSON.stringify(recalled)).not.toContain(root);
  });

  it("keeps source-less configuration failing closed for unknown sources", async () => {
    const provider = await installMemoryProvider(normalizeFabricConfig({
      memory: { enabled: true },
    }));
    const recalled = (await provider.invoke(
      "recall",
      { source: "laptop", query: "anything" },
      invocation(),
    )) as { total: number; coverage: { reasons: string[] }; error: { code: string } };
    expect(recalled.total).toBe(0);
    expect(recalled.error.code).toBe("source_not_found");
    expect(recalled.coverage.reasons).toContain("source_not_found");
  });

  it("registers configured ids on one registry", () => {
    const registry = createMemorySourceRegistry();
    registry.register(createFileSystemMemorySource({ id: "one", root: "/tmp/one" }));
    registry.register(createFileSystemMemorySource({ id: "two", root: "/tmp/two" }));
    expect(registry.ids()).toEqual(["one", "two"]);
  });
});
