import { existsSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

// smarty-dev#7504 phase 1 slice 1: scoped-signing-key role templates (design c4d26466 sections 1.4, 9.2, 9.3).
// RED-first: written before src/mesh/nats-auth/permissions.ts exists. Each case loads the module itself after asserting
// the file is present, so absence fails every case on its own. The GREEN run uses these same cases and code paths.
// No keys or credentials are involved in this file.
//
// Decision C (why unit, not E2E): the exact allow/deny lists and the deny-wins evaluation below are a model of the
// server's subject matching and {{tag()}}/{{subject()}} substring expansion, not a measurement. Server acceptance of the
// templates and the real allowed/refused matrix are E1 T2 (role matrix) and T8 (single-node operator auth) on the pinned
// real nats-server 2.14.7 in hosted CI, owner fabric-store.

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const PERMISSIONS_SOURCE = join(repoRoot, "src/mesh/nats-auth/permissions.ts");

type NatsRole = "agent" | "principal" | "maintainer" | "break-glass";
interface RoleTemplate {
  pub: { allow: string[]; deny: string[] };
  sub: { allow: string[] };
}
interface PermissionsModule {
  roleTemplate(role: NatsRole, input: { custodyStream: string; livenessStream: string }): RoleTemplate;
}

async function loadPermissions(): Promise<PermissionsModule> {
  // Absence is labelled as RED for a contract not yet written, not as a regression of earlier behavior.
  expect(
    existsSync(PERMISSIONS_SOURCE),
    `RED (#7504, implementation absent): ${relative(repoRoot, PERMISSIONS_SOURCE)} does not exist yet`,
  ).toBe(true);
  return (await import(/* @vite-ignore */ PERMISSIONS_SOURCE)) as PermissionsModule;
}

// Fixture names only. The real <CUS>/<LIVE> names are open item O-2 and are not invented here.
const CUS = "FIXTURE_CUSTODY";
const LIVE = "KV_FIXTURE_LIVENESS";
const STREAMS = { custodyStream: CUS, livenessStream: LIVE };
const B = "FABRIC_MESH_{{tag(mesh)}}";
const MESH_HEX = "0123456789abcdef0123456789abcdef";
const OTHER_HEX = "fedcba9876543210fedcba9876543210";
const CONSUMER_VERBS = ["CREATE", "MSG.NEXT", "DELETE", "INFO"];
const INBOX = ["_INBOX.{{subject()}}.>"];

const DENY = [
  `$JS.API.DIRECT.GET.${CUS}`,
  `$JS.API.DIRECT.GET.${CUS}.>`,
  `$JS.API.STREAM.PURGE.${CUS}`,
  `$JS.API.STREAM.PURGE.${LIVE}`,
  `$JS.API.STREAM.DELETE.${CUS}`,
  `$JS.API.STREAM.DELETE.${LIVE}`,
];
const NORMAL_ROLE_DENY = [
  ...DENY,
  `$JS.API.STREAM.CREATE.${CUS}`,
  `$JS.API.STREAM.CREATE.${LIVE}`,
  `$JS.API.STREAM.UPDATE.${CUS}`,
  `$JS.API.STREAM.UPDATE.${LIVE}`,
];

const ALLOW: Record<NatsRole, string[]> = {
  agent: [
    `${B}.state`,
    `${B}.snapshot`,
    "$JS.API.INFO",
    `$JS.API.STREAM.INFO.${B}`,
    `$JS.API.STREAM.MSG.GET.${B}`,
    ...CONSUMER_VERBS.map((verb) => `$JS.API.CONSUMER.${verb}.${B}.>`),
    "$SYS.REQ.USER.INFO",
  ],
  principal: [
    "$JS.API.INFO",
    "$JS.API.STREAM.INFO.*",
    "$JS.API.STREAM.MSG.GET.*",
    ...CONSUMER_VERBS.map((verb) => `$JS.API.CONSUMER.${verb}.*.>`),
    "$JS.API.STREAM.NAMES",
    "$SYS.REQ.USER.INFO",
  ],
  maintainer: [
    "*.state",
    "*.snapshot",
    "$JS.API.INFO",
    ...["INFO", "MSG.GET", "CREATE", "UPDATE", "DELETE", "PURGE"].map((verb) => `$JS.API.STREAM.${verb}.*`),
    "$JS.API.STREAM.NAMES",
    ...CONSUMER_VERBS.map((verb) => `$JS.API.CONSUMER.${verb}.*.>`),
    "$SYS.REQ.USER.INFO",
  ],
  "break-glass": [
    "$JS.API.INFO",
    ...[CUS, LIVE].flatMap((stream) => ["INFO", "CREATE", "UPDATE"].map((verb) => `$JS.API.STREAM.${verb}.${stream}`)),
    "$SYS.REQ.USER.INFO",
  ],
};
const ROLES = Object.keys(ALLOW) as NatsRole[];

const sorted = (values: readonly string[]) => [...values].sort();

// NATS subject matching: "*" is exactly one token, ">" is one or more trailing tokens; tokens are literal otherwise.
function subjectMatches(pattern: string, subject: string): boolean {
  const p = pattern.split(".");
  const s = subject.split(".");
  for (let i = 0; i < p.length; i++) {
    if (p[i] === ">") return s.length > i;
    if (i >= s.length) return false;
    if (p[i] !== "*" && p[i] !== s[i]) return false;
  }
  return p.length === s.length;
}

// Server-side template expansion modelled as in nats-server auth.go: substring replacement, one subject per tag value;
// an allow entry whose tag is missing is dropped.
function expand(entries: string[], user: { subject: string; tags: string[] }): string[] {
  const mesh = user.tags.filter((tag) => tag.startsWith("mesh:")).map((tag) => tag.slice("mesh:".length));
  return entries.flatMap((entry) => {
    const withSubject = entry.split("{{subject()}}").join(user.subject);
    if (!withSubject.includes("{{tag(mesh)}}")) return [withSubject];
    return mesh.map((hex) => withSubject.split("{{tag(mesh)}}").join(hex));
  });
}

function mayPublish(template: RoleTemplate, user: { subject: string; tags: string[] }, subject: string): boolean {
  const allow = expand(template.pub.allow, user);
  const deny = expand(template.pub.deny, user);
  return allow.some((pattern) => subjectMatches(pattern, subject)) && !deny.some((pattern) => subjectMatches(pattern, subject));
}

const agentUser = { subject: "UFIXTUREAGENT", tags: ["role:agent", `mesh:${MESH_HEX}`] };
const anyUser = { subject: "UFIXTUREUSER", tags: [] as string[] };

describe("#7504 roleTemplate: exact section 1.4 matrix", () => {
  it.each(ROLES)("%s has exactly the 1.4 publish allow, the 9.2 publish deny and only its own inbox", async (role) => {
    const { roleTemplate } = await loadPermissions();
    const template = roleTemplate(role, STREAMS);
    expect(Object.keys(template).sort()).toEqual(["pub", "sub"]);
    expect(Object.keys(template.pub).sort()).toEqual(["allow", "deny"]);
    expect(Object.keys(template.sub)).toEqual(["allow"]);
    expect(sorted(template.pub.allow)).toEqual(sorted(ALLOW[role]));
    expect(new Set(template.pub.allow).size).toBe(template.pub.allow.length);
    expect(sorted(template.pub.deny)).toEqual(sorted(role === "break-glass" ? DENY : NORMAL_ROLE_DENY));
    expect(new Set(template.pub.deny).size).toBe(template.pub.deny.length);
    expect(template.sub.allow).toEqual(INBOX);
  });
});

describe("#7504 roleTemplate: refused verbs", () => {
  it("gives agents no purge, delete, create, update, direct get, account or durable verbs, and binds every stream verb to <B>", async () => {
    const { roleTemplate } = await loadPermissions();
    const { pub } = roleTemplate("agent", STREAMS);
    const forbidden = ["PURGE", "STREAM.DELETE", "STREAM.CREATE", "STREAM.UPDATE", "DIRECT.GET", "$JS.API.ACCOUNT", "DURABLE", "STREAM.MSG.DELETE", "STREAM.NAMES"];
    for (const fragment of forbidden) expect(pub.allow.filter((subject) => subject.includes(fragment))).toEqual([]);
    for (const subject of pub.allow.filter((s) => s.startsWith("$JS.API.STREAM.") || s.startsWith("$JS.API.CONSUMER."))) {
      expect(subject.includes(B), subject).toBe(true);
    }
    expect(pub.allow.filter((subject) => subject.startsWith("*") || subject.startsWith(">"))).toEqual([]);
    const bound = `FABRIC_MESH_${MESH_HEX}`;
    const other = `FABRIC_MESH_${OTHER_HEX}`;
    const template = roleTemplate("agent", STREAMS);
    expect(mayPublish(template, agentUser, `${bound}.state`)).toBe(true);
    expect(mayPublish(template, agentUser, `$JS.API.CONSUMER.CREATE.${bound}.c1`)).toBe(true);
    expect(mayPublish(template, agentUser, `${other}.state`)).toBe(false);
    expect(mayPublish(template, agentUser, `$JS.API.STREAM.MSG.GET.${other}`)).toBe(false);
    expect(mayPublish(template, agentUser, `$JS.API.STREAM.PURGE.${bound}`)).toBe(false);
    expect(mayPublish(template, agentUser, `$JS.API.STREAM.DELETE.${bound}`)).toBe(false);
    // A user minted without a mesh tag gets no stream subject at all.
    expect(mayPublish(template, { subject: "UNOMESH", tags: ["role:agent"] }, `${bound}.state`)).toBe(false);
  });

  it.each(ROLES)("%s holds no DIRECT.GET, no broad root, no ack/flow-control, no unnamed consumer create, no other $SYS", async (role) => {
    const { roleTemplate } = await loadPermissions();
    const { pub, sub } = roleTemplate(role, STREAMS);
    expect(pub.allow.filter((subject) => subject.includes("DIRECT.GET"))).toEqual([]);
    expect(pub.allow.filter((subject) => [">", "*", "$JS.>", "$JS.API.>", "$SYS.>", "_INBOX.>"].includes(subject))).toEqual([]);
    expect(pub.allow.filter((subject) => subject.startsWith("$JS.ACK") || subject.startsWith("$JS.FC"))).toEqual([]);
    expect(pub.allow.filter((subject) => subject.startsWith("$JS.API.CONSUMER.") && !subject.endsWith(".>"))).toEqual([]);
    expect(pub.allow.filter((subject) => subject.startsWith("$SYS.") && subject !== "$SYS.REQ.USER.INFO")).toEqual([]);
    // A missing tag in a deny entry fails authentication outright, so denies are literal.
    expect(pub.deny.filter((subject) => subject.includes("{{"))).toEqual([]);
    expect(sub.allow).toEqual(INBOX);
  });

  it.each(ROLES)("%s has no debug-attach subjects (9.3)", async (role) => {
    const { roleTemplate } = await loadPermissions();
    const { pub, sub } = roleTemplate(role, STREAMS);
    const all = [...pub.allow, ...pub.deny, ...sub.allow].map((subject) => subject.toLowerCase());
    for (const word of ["attach", "debug", "pi-server", "watch", "drive"]) {
      expect(all.filter((subject) => subject.includes(word))).toEqual([]);
    }
  });

  it.each(ROLES)("%s denies custody DIRECT.GET and custody/liveness PURGE and DELETE even where an allow matches", async (role) => {
    const { roleTemplate } = await loadPermissions();
    const template = roleTemplate(role, STREAMS);
    for (const subject of [
      `$JS.API.DIRECT.GET.${CUS}`,
      `$JS.API.DIRECT.GET.${CUS}.owner.x`,
      `$JS.API.STREAM.PURGE.${CUS}`,
      `$JS.API.STREAM.PURGE.${LIVE}`,
      `$JS.API.STREAM.DELETE.${CUS}`,
      `$JS.API.STREAM.DELETE.${LIVE}`,
    ]) {
      expect(mayPublish(template, anyUser, subject), subject).toBe(false);
    }
    for (const stream of [CUS, LIVE]) {
      for (const verb of ["CREATE", "UPDATE"]) {
        expect(mayPublish(template, anyUser, `$JS.API.STREAM.${verb}.${stream}`), `${role} ${verb} ${stream}`).toBe(role === "break-glass");
      }
    }
  });

  it("keeps maintainer mesh-root lifecycle control while normal roles cannot create or update custody/liveness", async () => {
    const { roleTemplate } = await loadPermissions();
    const template = roleTemplate("maintainer", STREAMS);
    const root = `FABRIC_MESH_${MESH_HEX}`;
    for (const verb of ["CREATE", "UPDATE", "DELETE", "PURGE", "INFO", "MSG.GET"]) {
      expect(mayPublish(template, anyUser, `$JS.API.STREAM.${verb}.${root}`), verb).toBe(true);
    }
    expect(mayPublish(template, anyUser, `${root}.snapshot`)).toBe(true);
    for (const stream of [CUS, LIVE]) {
      for (const verb of ["CREATE", "UPDATE", "PURGE", "DELETE"]) {
        expect(mayPublish(template, anyUser, `$JS.API.STREAM.${verb}.${stream}`), `${verb} ${stream}`).toBe(false);
      }
    }
    expect(mayPublish(template, anyUser, `$JS.API.STREAM.MSG.DELETE.${root}`)).toBe(false);
  });

  it("gives break-glass only INFO/CREATE/UPDATE on literal custody and liveness streams", async () => {
    const { roleTemplate } = await loadPermissions();
    const template = roleTemplate("break-glass", STREAMS);
    expect(mayPublish(template, anyUser, "$JS.API.INFO")).toBe(true);
    expect(mayPublish(template, anyUser, "$SYS.REQ.USER.INFO")).toBe(true);
    for (const stream of [CUS, LIVE]) {
      for (const verb of ["INFO", "CREATE", "UPDATE"]) {
        expect(mayPublish(template, anyUser, `$JS.API.STREAM.${verb}.${stream}`), `${verb} ${stream}`).toBe(true);
      }
    }
    for (const subject of [
      "$JS.API.STREAM.INFO.OTHER",
      `$JS.API.STREAM.MSG.GET.${CUS}`,
      `$JS.API.STREAM.CREATE.FABRIC_MESH_${MESH_HEX}`,
      `FABRIC_MESH_${MESH_HEX}.state`,
      `$JS.API.STREAM.PURGE.${CUS}`,
      `$JS.API.STREAM.DELETE.${LIVE}`,
      `$JS.API.DIRECT.GET.${CUS}`,
      `$JS.API.DIRECT.GET.${CUS}.owner.x`,
      "$JS.API.STREAM.MSG.DELETE.FIXTURE",
    ]) {
      expect(mayPublish(template, anyUser, subject), subject).toBe(false);
    }
    const all = [...template.pub.allow, ...template.pub.deny, ...template.sub.allow].join(" ").toLowerCase();
    for (const word of ["attach", "debug", "pi-server", "watch", "drive"]) expect(all.includes(word)).toBe(false);
  });

  it("gives the principal read-only access: no state writes and no stream lifecycle", async () => {
    const { roleTemplate } = await loadPermissions();
    const template = roleTemplate("principal", STREAMS);
    const root = `FABRIC_MESH_${MESH_HEX}`;
    expect(mayPublish(template, anyUser, `$JS.API.STREAM.MSG.GET.${root}`)).toBe(true);
    for (const subject of [`${root}.state`, `${root}.snapshot`, ...["CREATE", "UPDATE", "DELETE", "PURGE"].map((v) => `$JS.API.STREAM.${v}.${root}`)]) {
      expect(mayPublish(template, anyUser, subject), subject).toBe(false);
    }
  });
});

describe("#7504 roleTemplate: input refusals", () => {
  it.each(["admin", "sys", "Agent", "", "agent ", "operator"])("refuses unknown role %j", async (role) => {
    const { roleTemplate } = await loadPermissions();
    expect(() => roleTemplate(role as NatsRole, STREAMS)).toThrow();
  });

  it.each(ROLES)("%s refuses a missing custody or liveness mapping instead of omitting deny rows", async (role) => {
    const { roleTemplate } = await loadPermissions();
    const cases: unknown[] = [
      undefined,
      {},
      { custodyStream: CUS },
      { livenessStream: LIVE },
      { custodyStream: "", livenessStream: LIVE },
      { custodyStream: CUS, livenessStream: "" },
      { custodyStream: CUS, livenessStream: undefined },
      { custodyStream: 7, livenessStream: LIVE },
    ];
    for (const input of cases) {
      expect(() => roleTemplate(role, input as { custodyStream: string; livenessStream: string }), JSON.stringify(input)).toThrow();
    }
  });

  it.each(ROLES)("%s refuses custody equal to liveness", async (role) => {
    const { roleTemplate } = await loadPermissions();
    expect(() => roleTemplate(role, { custodyStream: CUS, livenessStream: CUS })).toThrow();
  });

  it.each(ROLES)("%s refuses wildcard, multi-token, template, reserved and mesh-root stream names", async (role) => {
    const { roleTemplate } = await loadPermissions();
    const bad = [
      "*",
      ">",
      "CUS.>",
      "CUS.*",
      "A.B",
      "A B",
      "A\tB",
      "A\nB",
      "A/B",
      "A\\B",
      "{{tag(mesh)}}",
      "CUS_{{subject()}}",
      "$SYS",
      "$JS",
      "_INBOX",
      `FABRIC_MESH_${MESH_HEX}`,
      "FABRIC_MESH_",
    ];
    for (const name of bad) {
      expect(() => roleTemplate(role, { custodyStream: name, livenessStream: LIVE }), `custody ${JSON.stringify(name)}`).toThrow();
      expect(() => roleTemplate(role, { custodyStream: CUS, livenessStream: name }), `liveness ${JSON.stringify(name)}`).toThrow();
    }
  });

  it("returns a fresh template each call, so mutating one cannot widen the next", async () => {
    const { roleTemplate } = await loadPermissions();
    const first = roleTemplate("agent", STREAMS);
    first.pub.allow.push(">");
    first.pub.deny.length = 0;
    first.sub.allow.push(">");
    const second = roleTemplate("agent", STREAMS);
    expect(sorted(second.pub.allow)).toEqual(sorted(ALLOW.agent));
    expect(sorted(second.pub.deny)).toEqual(sorted(NORMAL_ROLE_DENY));
    expect(second.sub.allow).toEqual(INBOX);
  });
});
