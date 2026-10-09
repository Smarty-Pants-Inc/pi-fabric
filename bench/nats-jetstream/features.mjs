// Probe 2.11/2.12 headers the design depends on.
import { connect, headers, StorageType } from "nats";
const nc = await connect({ servers: `127.0.0.1:${process.argv[2]}` });
const jsm = await nc.jetstreamManager();
await jsm.streams.delete("FEAT").catch(() => {});
const r = await nc.request("$JS.API.STREAM.CREATE.FEAT", JSON.stringify({ name: "FEAT", subjects: ["ms.>", "me.>"], num_replicas: 3, storage: "file", max_msgs_per_subject: 1, allow_atomic: true, allow_msg_ttl: true, allow_direct: true }), { timeout: 10000 });
const cfg = JSON.parse(new TextDecoder().decode(r.data));
console.log("create:", cfg.error ?? { allow_atomic: cfg.config?.allow_atomic, allow_msg_ttl: cfg.config?.allow_msg_ttl });
const js = nc.jetstream();
const pub = async (subj, h, label) => { const hd = headers(); for (const [k, v] of Object.entries(h)) hd.set(k, v); try { const a = await js.publish(subj, "x", { headers: hd }); console.log(label, "OK seq", a.seq); return a.seq; } catch (e) { console.log(label, "REJECT", e.message); } };
const a = await pub("ms.lease", {}, "seed ms.lease");
// fence on another subject
await pub("me.ev", { "Nats-Expected-Last-Subject-Sequence": String(a), "Nats-Expected-Last-Subject-Sequence-Subject": "ms.lease" }, "event fenced on current lease rev");
await pub("ms.lease", { "Nats-Expected-Last-Subject-Sequence": String(a) }, "lease CAS (advance)");
await pub("me.ev", { "Nats-Expected-Last-Subject-Sequence": String(a), "Nats-Expected-Last-Subject-Sequence-Subject": "ms.lease" }, "event fenced on STALE lease rev");
// per-message TTL
await pub("ms.presence", { "Nats-TTL": "2s" }, "presence with TTL 2s");
await new Promise((r) => setTimeout(r, 3500));
try { await jsm.streams.getMessage("FEAT", { last_by_subj: "ms.presence" }); console.log("presence after 3.5s: STILL THERE"); } catch (e) { console.log("presence after 3.5s: gone (", e.message, ")"); }
// atomic batch: 3 messages, last one carries a stale expectation -> whole batch must fail
const batch = async (id, stale) => {
  const cur = (await jsm.streams.getMessage("FEAT", { last_by_subj: "ms.lease" })).seq;
  const msgs = [["ms.a", {}], ["ms.b", {}], ["ms.lease", { "Nats-Expected-Last-Subject-Sequence": String(stale ? cur - 1 : cur) }]];
  for (let k = 0; k < msgs.length; k++) {
    const hd = headers(); hd.set("Nats-Batch-Id", id); hd.set("Nats-Batch-Sequence", String(k + 1));
    for (const [h, v] of Object.entries(msgs[k][1])) hd.set(h, v);
    if (k === msgs.length - 1) { hd.set("Nats-Batch-Commit", "1"); const m = await nc.request(msgs[k][0], "y", { headers: hd, timeout: 5000 }); console.log(`batch ${id} commit reply:`, new TextDecoder().decode(m.data)); }
    else nc.publish(msgs[k][0], "y", { headers: hd });
  }
  const st = await jsm.streams.info("FEAT"); console.log(`after batch ${id}: last_seq`, st.state.last_seq);
};
await batch("b-ok", false);
await batch("b-stale", true);
await jsm.streams.delete("FEAT");
await nc.close();
