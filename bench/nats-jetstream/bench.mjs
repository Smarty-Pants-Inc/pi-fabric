// smarty-dev#7504 bench: 3-node R3 JetStream on 127.0.0.1 (see docs/design/nats-jetstream-mesh.md).
// Usage: node bench.mjs <base-client-port> stream|kv|fail   (env DUR=ms; fail: PIDS='{"n1":pid,...}', KILL=lease)
// Needs the `nats` npm package (v2) in the working directory; not a pi-fabric dependency.
import { connect, StorageType } from "nats";
const base = Number(process.argv[2]);
const phase = process.argv[3] ?? "all";
const servers = [0, 1, 2].map((i) => `127.0.0.1:${base + i}`);
const DUR = Number(process.env.DUR ?? 6000);
const pct = (a, p) => { const s = [...a].sort((x, y) => x - y); return s.length ? s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))] : NaN; };
const ms = (ns) => Number(ns) / 1e6;
const now = () => process.hrtime.bigint();
const f2 = (x) => x.toFixed(2);

async function conns(n) { return Promise.all(Array.from({ length: n }, (_, i) => connect({ servers: [servers[i % 3], servers[(i + 1) % 3], servers[(i + 2) % 3]], noRandomize: true, maxReconnectAttempts: -1, reconnectTimeWait: 100 }))); }
async function closeAll(cs) { await Promise.all(cs.map((c) => c.drain().catch(() => {}))); }

async function streamBench() {
  const [admin] = await conns(1);
  const jsm = await admin.jetstreamManager();
  const out = [];
  for (const size of [256, 4096]) {
    for (const n of [1, 16, 64]) {
      const name = `B${size}_${n}`;
      await jsm.streams.add({ name, subjects: [`${name}.>`], num_replicas: 3, storage: StorageType.File });
      const cs = await conns(n);
      const payload = new Uint8Array(size).fill(97);
      const lat = []; let count = 0, errs = 0;
      const stop = Date.now() + DUR;
      const t0 = now();
      await Promise.all(cs.map(async (c, i) => {
        const js = c.jetstream({ timeout: 5000 });
        while (Date.now() < stop) {
          const s = now();
          try { await js.publish(`${name}.p${i}`, payload); lat.push(ms(now() - s)); count++; } catch { errs++; }
        }
      }));
      const secs = ms(now() - t0) / 1000;
      const info = await jsm.streams.info(name);
      out.push({ size, publishers: n, msgs: count, stored: info.state.messages, errs, msgPerSec: Math.round(count / secs), p50: f2(pct(lat, 50)), p99: f2(pct(lat, 99)), leader: info.cluster?.leader });
      console.log(JSON.stringify(out.at(-1)));
      await closeAll(cs);
      await jsm.streams.delete(name);
    }
  }
  await admin.drain();
  return out;
}

async function kvBench() {
  const [admin] = await conns(1);
  const js = admin.jetstream();
  const kv = await js.views.kv("kvbench", { replicas: 3, history: 1 });
  const jsm = await admin.jetstreamManager();
  const res = {};
  const run = async (label, n, fn) => {
    const cs = await conns(n); const lat = []; let errs = 0; const stop = Date.now() + DUR / 2; let k = 0;
    await Promise.all(cs.map(async (c, i) => {
      const kvi = await c.jetstream().views.kv("kvbench");
      const jsmi = await c.jetstreamManager();
      let rev = 0; let j = 0;
      while (Date.now() < stop) {
        const s = now();
        try { rev = await fn({ kv: kvi, jsm: jsmi, i, j: j++, rev }); lat.push(ms(now() - s)); k++; } catch (e) { errs++; rev = (await kvi.get(`k${i}`))?.revision ?? 0; }
      }
    }));
    await closeAll(cs);
    res[label] = { clients: n, ops: k, errs, p50: f2(pct(lat, 50)), p99: f2(pct(lat, 99)) };
    console.log(label, JSON.stringify(res[label]));
  };
  const val = new Uint8Array(256).fill(98);
  for (const n of [1, 16]) {
    await run(`put c${n}`, n, async ({ kv, i }) => kv.put(`k${i}`, val));
    await run(`get-direct c${n}`, n, async ({ kv, i }) => (await kv.get(`k${i}`)).revision);
    // leader read: $JS.API.STREAM.MSG.GET goes to the stream leader (allow_direct bypassed)
    await run(`get-leader c${n}`, n, async ({ jsm, i }) => (await jsm.streams.getMessage("KV_kvbench", { last_by_subj: `$KV.kvbench.k${i}` })).seq);
    await run(`cas c${n}`, n, async ({ kv, i, rev }) => kv.update(`k${i}`, val, rev));
  }
  // contended CAS: 16 clients race on one key; count wins and conflicts
  {
    const cs = await conns(16); let wins = 0, conflicts = 0; const lat = []; const stop = Date.now() + DUR / 2;
    await kv.put("hot", val);
    await Promise.all(cs.map(async (c) => {
      const kvi = await c.jetstream().views.kv("kvbench");
      while (Date.now() < stop) {
        const e = await kvi.get("hot"); const s = now();
        try { await kvi.update("hot", val, e.revision); wins++; lat.push(ms(now() - s)); } catch { conflicts++; }
      }
    }));
    await closeAll(cs);
    res["cas contended c16 (1 key)"] = { clients: 16, ops: wins, errs: conflicts, p50: f2(pct(lat, 50)), p99: f2(pct(lat, 99)) };
    console.log(JSON.stringify(res["cas contended c16 (1 key)"]));
  }
  await jsm.streams.delete("KV_kvbench");
  await admin.drain();
  return res;
}

// Leader kill under load. Publishers retry with the same Nats-Msg-Id until acked; lease
// contenders run CAS on one key (leader reads), TTL 3 s, renew every 500 ms.
const retry = async (f) => { for (let k = 0; ; k++) { try { return await f(); } catch (e) { if (k > 20) throw e; await new Promise((r) => setTimeout(r, 250)); } } };
async function failover(pids) {
  const [admin] = await conns(1);
  const jsm = await retry(() => admin.jetstreamManager({ timeout: 10000 }));
  for (const s of ["FAIL", "KV_lease"]) await jsm.streams.delete(s).catch(() => {});
  await jsm.streams.add({ name: "FAIL", subjects: ["fail.>"], num_replicas: 3, storage: StorageType.File, duplicate_window: 120e9 });
  const kv = await admin.jetstream().views.kv("lease", { replicas: 3, history: 64 });
  const N = 16, L = 6, TTL = 3000, RUN = 25000, KILL_AT = 6000;
  const acked = new Map(); let dupAcks = 0, retries = 0; const ackTimes = [];
  const t0 = Date.now();
  const pubs = await conns(N);
  const pubDone = Promise.all(pubs.map(async (c, i) => {
    const js = c.jetstream({ timeout: 2000 }); let seq = 0;
    while (Date.now() - t0 < RUN) {
      const id = `p${i}-${seq++}`;
      for (;;) {
        try {
          const a = await js.publish(`fail.p${i}`, new Uint8Array(256), { msgID: id });
          if (a.duplicate) dupAcks++;
          if (acked.has(id) && acked.get(id) !== a.seq) console.log("SEQ MISMATCH", id);
          acked.set(id, a.seq); ackTimes.push(Date.now() - t0); break;
        } catch { retries++; if (Date.now() - t0 > RUN + 60000) { console.log('GAVE UP', id); break; } await new Promise((r) => setTimeout(r, 50)); }
      }
    }
  }));
  // lease contenders
  const grants = []; // {holder, expectRev, newRev, from, until}
  let casErr = 0;
  const lcs = await conns(L);
  const leaseDone = Promise.all(lcs.map(async (c, h) => {
    const jsmL = await retry(() => c.jetstreamManager({ timeout: 10000 })); const kvL = await retry(() => c.jetstream({ timeout: 2000 }).views.kv("lease"));
    let mine = null; // {rev, until}
    while (Date.now() - t0 < RUN) {
      try {
        let cur = null;
        try { const m = await jsmL.streams.getMessage("KV_lease", { last_by_subj: "$KV.lease.L" }); cur = { rev: m.seq, v: JSON.parse(new TextDecoder().decode(m.data)) }; } catch (e) { if (!String(e).includes("no message found")) throw e; }
        const t = Date.now();
        const free = !cur || cur.v.until < t;
        const renew = mine && cur && cur.rev === mine.rev && cur.v.holder === h;
        if (free || renew) {
          const until = t + TTL; const body = JSON.stringify({ holder: h, until });
          const rev = cur ? await kvL.update("L", body, cur.rev) : await kvL.create("L", body);
          // hold from the moment we sent the CAS until `until`; conservative for overlap checks
          if (mine && renew) grants.at(mine.idx).until = until;
          if (!renew) { grants.push({ holder: h, expectRev: cur?.rev ?? 0, newRev: rev, from: t, until }); }
          mine = { rev, until, idx: renew ? mine.idx : grants.length - 1 };
        } else if (mine && mine.until < t) mine = null;
        else if (mine && cur && cur.v.holder !== h) { grants.at(mine.idx).until = Math.min(grants.at(mine.idx).until, t); mine = null; }
      } catch { casErr++; }
      await new Promise((r) => setTimeout(r, 500));
    }
  }));
  // kill the FAIL stream leader
  await new Promise((r) => setTimeout(r, KILL_AT));
  const info = await retry(() => jsm.streams.info("FAIL"));
  const linfo = await retry(() => jsm.streams.info("KV_lease"));
  const leader = process.env.KILL === "lease" ? linfo.cluster.leader : info.cluster.leader;
  const killT = Date.now() - t0;
  process.kill(pids[leader], "SIGKILL");
  console.log("killed", leader, "pid", pids[leader], "at", killT, "ms; lease leader was", linfo.cluster.leader);
  await pubDone; console.log('pubs done'); await leaseDone; console.log('leases done');
  await Promise.all([...pubs, ...lcs].map((c) => c.close().catch(() => {}))); console.log('closed');
  const before = ackTimes.filter((t) => t < killT); const after = ackTimes.filter((t) => t >= killT).sort((a, b) => a - b);
  const resume = after[0] - killT; const lastBefore = Math.max(...before);
  // largest ack gap anywhere after the kill
  let gap = 0; const all = [...ackTimes].sort((a, b) => a - b); for (let k = 1; k < all.length; k++) if (all[k] >= killT - 50) gap = Math.max(gap, all[k] - all[k - 1]);
  // read back the stream: count unique msg ids and duplicates
  const rb = await connect({ servers: servers.filter((_, k) => `n${k + 1}` !== leader) }); console.log('rb connected');
  const jsm2 = await retry(() => rb.jetstreamManager({ timeout: 10000 }));
  const info2 = await retry(() => jsm2.streams.info("FAIL")); console.log('info2', info2.state.messages);
  const ids = new Map(); let read = 0;
  for (let q = info2.state.first_seq; q <= info2.state.last_seq; q++) { const m = await retry(() => jsm2.streams.getMessage("FAIL", { seq: q })); const id = m.header?.get("Nats-Msg-Id"); ids.set(id, (ids.get(id) ?? 0) + 1); read++; }
  console.log('read back', read);
  let lost = 0; for (const id of acked.keys()) if (!ids.has(id)) lost++;
  let dupStored = 0; for (const v of ids.values()) if (v > 1) dupStored += v - 1;
  // split lease: two grants with the same expected revision, or overlapping hold windows
  const byExpect = new Map(); for (const g of grants) byExpect.set(g.expectRev, (byExpect.get(g.expectRev) ?? 0) + 1);
  const sameRev = [...byExpect.values()].filter((v) => v > 1).length;
  const gs = [...grants].sort((a, b) => a.from - b.from); let overlap = 0;
  for (let k = 1; k < gs.length; k++) if (gs[k].from < gs[k - 1].until && gs[k].holder !== gs[k - 1].holder) overlap++;
  const r = { killed: leader, killAtMs: killT, resumeMs: resume, maxAckGapMsAfterKill: gap, lastAckBeforeKillMs: lastBefore, acked: acked.size, stored: info2.state.messages, readBack: read, lost, dupStored, dupAcks, retries, newStreamLeader: info2.cluster.leader, leaseGrants: grants.length, leaseSameExpectedRev: sameRev, leaseOverlaps: overlap, leaseCasErrors: casErr };
  console.log(JSON.stringify(r));
  console.log("grants", JSON.stringify(grants.map((g) => [g.holder, g.expectRev, g.newRev, g.from, g.until])));
  await rb.close(); await admin.close();
  return r;
}

const pids = process.env.PIDS ? JSON.parse(process.env.PIDS) : {};
if (phase === "stream" || phase === "all") await streamBench();
if (phase === "kv" || phase === "all") await kvBench();
if (phase === "fail") await failover(pids);
process.exit(0);
