import { pathToFileURL } from "node:url";
// Node >= 24 (package engines) strips the helper's types; the product adapter shares the same selection (#13112).
import { selectNatsSyncAlways } from "../src/mesh/nats-monitor-config.ts";

// Test/CI only: this module is not in the product startup graph. NATS monitor
// endpoints have no readiness push event, so polling is the bounded observation
// mechanism. One absolute deadline covers every node, endpoint, request, and
// response body; a failed observation is never silently skipped.
export const READINESS_TIMEOUT_MS = 60_000;
export const READINESS_POLL_MS = 200;
const REQUEST_TIMEOUT_MS = 1_000;

const message = error => error instanceof Error ? error.message : String(error);
const nonempty = value => typeof value === "string" && value.trim().length > 0;

/** @param {{name: string, monitorUrl: string}[]} nodes */
function endpoints(nodes) {
  if (!Array.isArray(nodes) || nodes.length === 0) throw new Error("provide at least one name=http://monitor endpoint");
  const normalized = nodes.map(({ name, monitorUrl }) => {
    if (!nonempty(name)) throw new Error("each monitor endpoint needs a node name");
    const url = new URL(monitorUrl);
    if (!["http:", "https:"].includes(url.protocol) || url.search || url.hash || url.pathname !== "/") {
      throw new Error(`invalid monitor base URL for ${name}: ${monitorUrl}`);
    }
    return { name, monitorUrl: url.origin };
  });
  if (new Set(normalized.map(node => node.name)).size !== nodes.length ||
      new Set(normalized.map(node => node.monitorUrl)).size !== nodes.length) {
    throw new Error("node names and monitor URLs must be distinct");
  }
  return normalized;
}

// The race is intentional in addition to AbortController: a custom or broken
// fetch/body implementation must not keep the caller alive beyond the gate.
async function readJson(url, { fetchImpl, now, deadline }) {
  const remaining = deadline - now();
  if (remaining <= 0) throw new Error("readiness deadline exceeded");
  const controller = new AbortController();
  let timer;
  const expired = new Promise((_, reject) => {
    timer = setTimeout(() => {
      const error = new Error(now() >= deadline ? "readiness deadline exceeded" : "request timed out after 1000 ms");
      reject(error);
      controller.abort(error);
    }, Math.min(REQUEST_TIMEOUT_MS, remaining));
  });
  try {
    return await Promise.race([
      expired,
      (async () => {
        const response = await fetchImpl(url, { signal: controller.signal });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const body = await response.json();
        if (now() >= deadline) throw new Error("readiness deadline exceeded");
        if (!body || typeof body !== "object" || Array.isArray(body)) throw new Error("expected a JSON object");
        return { status: response.status, body };
      })(),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

function summarize(observations) {
  const clustered = observations.length > 1;
  const states = observations.map(({ name, monitorUrl, results }) => {
    const result = key => results[key];
    const body = key => result(key)?.value?.body;
    const health = body("health");
    const healthJs = body("healthJs");
    const server = body("server");
    const routez = body("routes");
    const jetstream = body("jetstream");
    const metaCluster = jetstream?.meta_cluster;
    const peers = [...new Set((Array.isArray(routez?.routes) ? routez.routes : [])
      .map(route => route?.remote_id).filter(nonempty))];
    const errors = Object.fromEntries(Object.entries(results).filter(([, entry]) => entry.error)
      .map(([key, entry]) => [key, entry.error]));
    const diagnostics = Object.fromEntries(Object.entries(results).map(([key, entry]) => [key,
      entry.error ? { error: entry.error } : { status: entry.value.status, body: entry.value.body }]));
    const state = {
      name, monitorUrl,
      healthHttpStatus: result("health")?.value?.status ?? null,
      health: health?.status ?? null,
      healthJsHttpStatus: result("healthJs")?.value?.status ?? null,
      healthJs: healthJs?.status ?? null,
      serverId: server?.server_id ?? null,
      serverName: server?.server_name ?? null,
      syncAlways: selectNatsSyncAlways(server, jetstream) === true,
      routeCount: routez?.num_routes ?? null,
      peers,
      leader: metaCluster?.leader ?? null,
      metaCluster: metaCluster ?? null,
      diagnostics,
      errors,
      problems: [],
    };
    if (state.healthHttpStatus !== 200 || typeof state.health !== "string" || state.health.toLowerCase() !== "ok") {
      state.problems.push("/healthz is not HTTP 200/OK");
    }
    if (state.healthJsHttpStatus !== 200 || typeof state.healthJs !== "string" || state.healthJs.toLowerCase() !== "ok") {
      state.problems.push("/healthz?js-enabled-only=true is not HTTP 200/OK");
    }
    if (!nonempty(state.serverId)) state.problems.push("server_id is missing");
    if (state.serverName !== name) state.problems.push(`server_name is not ${name}`);
    if (!state.syncAlways) state.problems.push("sync_always is not true");
    for (const [key, error] of Object.entries(errors)) state.problems.push(`${key}: ${error}`);
    return state;
  });

  const ids = states.map(state => state.serverId);
  for (const state of states) {
    if (nonempty(state.serverId) && ids.filter(id => id === state.serverId).length > 1) {
      state.problems.push("server_id is not distinct");
    }
    if (!clustered) continue;
    const expectedPeers = states.filter(peer => peer !== state).map(peer => peer.serverId);
    const hasDistinctPeerRoutes = expectedPeers.every(id => nonempty(id) && state.peers.includes(id)) &&
      state.peers.length >= expectedPeers.length;
    if (!(state.routeCount >= expectedPeers.length) || !hasDistinctPeerRoutes) {
      state.problems.push(`missing distinct peer routes; expected ${JSON.stringify(expectedPeers)}`);
    }
    if (!nonempty(state.leader)) state.problems.push("JetStream metadata leader is missing");
    else if (!states.some(peer => peer.name === state.leader || peer.serverId === state.leader)) {
      state.problems.push(`metadata leader ${state.leader} does not map to a node name/server_id`);
    }
  }
  const leaders = new Set(states.map(state => state.leader));
  if (clustered && (leaders.size !== 1 || !nonempty(states[0]?.leader))) {
    for (const state of states) state.problems.push(`metadata leaders disagree: ${JSON.stringify([...leaders])}`);
  } else if (clustered) {
    const leader = states[0].leader;
    const leaderState = states.find(state => state.name === leader || state.serverId === leader);
    if (!leaderState) {
      for (const state of states) state.problems.push(`metadata leader ${leader} does not map to a node name/server_id`);
    } else {
      const expectedNonleaders = states.filter(state => state !== leaderState);
      if (leaderState.metaCluster?.cluster_size !== states.length) {
        leaderState.problems.push(`metadata leader cluster_size is not ${states.length}`);
      }
      const replicas = Array.isArray(leaderState.metaCluster?.replicas) ? leaderState.metaCluster.replicas : [];
      for (const member of expectedNonleaders) {
        const replica = replicas.find(candidate => [candidate?.name, candidate?.server_id].includes(member.name) ||
          [candidate?.name, candidate?.server_id].includes(member.serverId));
        if (!replica) {
          leaderState.problems.push(`metadata leader replica is missing for ${member.name}/${member.serverId}`);
        } else if (replica.current !== true || replica.offline === true) {
          leaderState.problems.push(`metadata leader replica ${member.name}/${member.serverId} is not current/online: ${JSON.stringify(replica)}`);
        }
      }
    }
  }
  return states;
}

/**
 * Shared by the Node CLI and the owned election fixture. Tests may inject a
 * shorter deadline, but callers cannot extend the production 60 s upper bound.
 * @param {{name: string, monitorUrl: string}[]} nodes
 * @param {{fetch?: typeof globalThis.fetch, now?: () => number, timeoutMs?: number}} options
 */
export async function waitForNatsCluster(nodes, options = {}) {
  const targets = endpoints(nodes);
  const { fetch: fetchImpl = globalThis.fetch, now = () => performance.now(), timeoutMs = READINESS_TIMEOUT_MS } = options;
  if (typeof fetchImpl !== "function") throw new Error("a fetch implementation is required");
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0 || timeoutMs > READINESS_TIMEOUT_MS) {
    throw new Error(`readiness timeout must be > 0 and <= ${READINESS_TIMEOUT_MS} ms`);
  }
  const deadline = now() + timeoutMs;
  let states = [];
  do {
    const observations = targets.map(node => ({ ...node, results: {} }));
    await Promise.all(observations.flatMap(node => {
      const paths = { health: "/healthz", healthJs: "/healthz?js-enabled-only=true", server: "/varz" };
      if (targets.length > 1) Object.assign(paths, { routes: "/routez", jetstream: "/jsz?raft=true" });
      return Object.entries(paths).map(async ([key, endpoint]) => {
        try {
          node.results[key] = { value: await readJson(`${node.monitorUrl}${endpoint}`, { fetchImpl, now, deadline }) };
        } catch (error) {
          node.results[key] = { error: message(error) };
        }
      });
    }));
    states = summarize(observations);
    if (now() < deadline && states.every(state => state.problems.length === 0)) {
      return { nodes: states, metaLeader: targets.length > 1 ? states[0].leader : undefined };
    }
    const remaining = deadline - now();
    if (remaining <= 0) break;
    await new Promise(resolve => setTimeout(resolve, Math.min(READINESS_POLL_MS, remaining)));
  } while (now() < deadline);
  throw new Error(`NATS cluster not formed: ${JSON.stringify(states)}`);
}

/** Explicit CLI arguments: n1=http://127.0.0.1:8222 n2=http://127.0.0.1:8223 ... */
export async function runReadinessCli(args, { stdout = console.log, stderr = console.error, ...options } = {}) {
  try {
    const nodes = args.map(arg => {
      const split = arg.indexOf("=");
      if (split <= 0) throw new Error(`expected name=http://monitor, got ${arg}`);
      return { name: arg.slice(0, split), monitorUrl: arg.slice(split + 1) };
    });
    const result = await waitForNatsCluster(nodes, options);
    stdout(`NATS ready: ${result.nodes.length} node(s)${result.metaLeader ? `, meta leader ${result.metaLeader}` : ""}`);
    return 0;
  } catch (error) {
    stderr(message(error));
    return 1;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  process.exitCode = await runReadinessCli(process.argv.slice(2));
}
