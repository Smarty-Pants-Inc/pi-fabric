import { Kvm, type KV } from "@nats-io/kv";
import { jetstream, StorageType } from "@nats-io/jetstream";
import type { NatsConnection, Subscription } from "@nats-io/transport-node";
import type { LeaseSnapshot } from "../../src/topology/nats-kv-leases.js";
import { NatsLeaseCluster } from "./nats-lease-cluster.js";

export interface ProtectedWrite { owner: string; fence: number; writeId: string }
export interface ObservedWrite extends ProtectedWrite { resourceRevision: number }
export interface WriteReply { accepted: boolean; resourceRevision?: number; reason?: string }

/** TEST ONLY. Persistent downstream fence, separate from lease admission/Date.now().
 * Owners submit actual writes over NATS; the resource enforces its stored high-water
 * fence with a resource-key CAS. An independent connection observes accepted KV writes.
 * This is NOT runtime wiring, nor protection against an ACL-authorized forged token. */
export class FencedLeaseResource {
  readonly observed: ObservedWrite[] = [];
  readonly rejected: ProtectedWrite[] = [];
  readonly violations: string[] = [];
  private constructor(readonly subject: string, private readonly nc: NatsConnection,
    private readonly kv: KV, private readonly sub: Subscription,
    private readonly watch: Awaited<ReturnType<KV["watch"]>>) {}
  private serving!: Promise<void>;
  private observing!: Promise<void>;
  private notify: (() => void) | undefined;

  static async open(cluster: NatsLeaseCluster, label: string): Promise<FencedLeaseResource> {
    const nc = await cluster.connection(), observerNc = await cluster.connection();
    const bucket = `${cluster.bucket}_${label}_RESOURCE`;
    const opts = { replicas: 3, storage: StorageType.File, history: 64, allow_direct: false };
    const kv = await new Kvm(jetstream(nc)).create(bucket, opts);
    const observerKv = await new Kvm(jetstream(observerNc)).create(bucket, opts);
    const watch = await observerKv.watch({ key: "protected", include: "history" });
    const subject = `u2.resource.${bucket}`;
    const sub = nc.subscribe(subject);
    await nc.flush();
    const resource = new FencedLeaseResource(subject, nc, kv, sub, watch);
    resource.observing = (async () => {
      let previous: ObservedWrite | undefined;
      for await (const entry of watch) {
        if (entry.operation !== "PUT") continue;
        const write = { ...entry.json<ProtectedWrite>(), resourceRevision: entry.revision };
        if (previous && (write.fence < previous.fence ||
          (write.fence === previous.fence && write.owner !== previous.owner))) {
          resource.violations.push(`Fence regressed/owner changed: ${JSON.stringify({ previous, write })}`);
        }
        // KV stream order, NOT returned lease handles or wall-clock authority estimates.
        resource.observed.push(write); previous = write; resource.notify?.();
      }
    })();
    resource.serving = (async () => {
      for await (const message of sub) {
        let reply: WriteReply;
        try { reply = await resource.accept(message.json<ProtectedWrite>()); }
        catch (error) { reply = { accepted: false, reason: String(error) }; }
        message.respond(JSON.stringify(reply));
      }
    })();
    return resource;
  }
  private async accept(write: ProtectedWrite): Promise<WriteReply> {
    if (!Number.isSafeInteger(write.fence) || write.fence <= 0 || !write.owner || !write.writeId) {
      return { accepted: false, reason: "Invalid fence/write" };
    }
    const entry = await this.kv.get("protected");
    const previous = entry?.json<ProtectedWrite>();
    if (previous && (write.fence < previous.fence ||
      (write.fence === previous.fence && write.owner !== previous.owner))) {
      this.rejected.push(write); return { accepted: false, reason: "Superseded fence" };
    }
    const value = JSON.stringify(write);
    const revision = entry ? await this.kv.update("protected", value, entry.revision) :
      await this.kv.create("protected", value);
    return { accepted: true, resourceRevision: revision };
  }
  async write(snapshot: LeaseSnapshot, owner: string, writeId: string): Promise<WriteReply> {
    const reply = (await this.nc.request(this.subject, JSON.stringify({ owner,
      fence: snapshot.revision, writeId } satisfies ProtectedWrite), { timeout: 2_000 })).json<WriteReply>();
    if (reply.accepted) await this.waitObserved(writeId);
    return reply;
  }
  async waitObserved(writeId: string): Promise<void> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([new Promise<void>(resolve => {
        const check = (): void => { if (this.observed.some(w => w.writeId === writeId)) resolve(); };
        this.notify = check; check();
      }), new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`Resource observer missed ${writeId}`)), 3_000);
      })]);
    } finally { if (timer) clearTimeout(timer); this.notify = undefined; }
  }
  async close(): Promise<void> {
    this.sub.unsubscribe(); this.watch.stop();
    await Promise.all([this.serving, this.observing]);
  }
}
