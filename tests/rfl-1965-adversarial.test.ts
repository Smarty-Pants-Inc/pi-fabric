import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { SessionManager } from '@earendil-works/pi-coding-agent';
import { MeshStore } from '../src/mesh/store.js';
import { RootInbox } from '../src/topology/root-inbox.js';
import * as shared from '../src/topology/root-inbox-shared.js';
import type { SessionReceiptManager } from '../src/core/session-receipts.js';

// Independent judge: data-safety / cache invalidation at the real RootInbox boundary.
// Assertion failures intentionally demonstrate counterexamples; no product changes.
const cleanup: Array<()=>unknown|Promise<unknown>>=[];
afterEach(async()=>{ vi.restoreAllMocks(); for(const close of cleanup.splice(0).reverse()) await close(); });
const topic='fleet.work.rfl1965';
const peer={id:'session:peer',name:'peer',kind:'main' as const};
const me={id:'session:judge',name:'judge',kind:'main' as const};
const encode=(xs:unknown[])=>xs.map(x=>JSON.stringify(x)+'\n').join('');
const unrelated=(text='unchanged-tail'.repeat(30))=>({type:'message',message:{role:'user',content:text}});
const native=(id:string, at=Date.now())=>({type:'custom_message',customType:'pi-fabric-agent-message',timestamp:at,
  details:{id,from:{id:peer.id},data:{deliveryId:id,topic}}});
function fixture(initial:unknown[]=[]){
 const root=fs.mkdtempSync(path.join(os.tmpdir(),'rfl1965-'));
 cleanup.push(()=>fs.rmSync(root,{recursive:true,force:true}));
 const file=path.join(root,'session.jsonl'); fs.writeFileSync(file,encode(initial));
 const manager:SessionReceiptManager={getEntries:()=>[],getSessionFile:()=>file,isPersisted:()=>true,getSessionId:()=>me.id};
 let issuer=shared.rootInboxReceiptIssuer();
 // Base lacks this factory: use its semantically equivalent unversioned verifier there.
 const factory=(shared as any).rootInboxReceiptVerifier;
 const verifier:shared.RootInboxReceiptVerifier=factory ? factory(()=>issuer) : (d,c,a)=>shared.verifyRootInboxReceipt(issuer,d,c,a);
 const read=()=>shared.confirmedRootInboxSession(manager,verifier);
 const full=()=>shared.rootInboxSession(fs.readFileSync(file,'utf8').split('\n').slice(0,-1).map(l=>{try{return JSON.parse(l);}catch{return null;}}),verifier);
 const issue=(id:string,at=Date.now())=>{const e={type:'custom_message',customType:shared.ROOT_INBOX_CUSTOM_TYPE,timestamp:at,content:'received '+id,details:{ids:[id],nonce:''}};shared.registerRootInboxReceipt(issuer,e.details,e.content);return e;};
 const equal=()=>{const a=read(),b=full();expect([...a.delivered!]).toEqual([...b.delivered!]);expect([...a.deliveredAt!]).toEqual([...b.deliveredAt!]);return a;};
 const mesh=()=>{const m=new MeshStore(path.join(root,'mesh'),64*1024,500);cleanup.push(()=>m.closeState());return m;};
 const inbox=(m:MeshStore)=>{const i=new RootInbox(m,me,()=>[me.id],{steerGraceMs:0});cleanup.push(()=>i.close());i.start();return i;};
 return {root,file,manager,read,full,equal,issue,mesh,inbox,issuer:()=>issuer,rotate:()=>{issuer=shared.rootInboxReceiptIssuer();}};
}

describe('RFL independent receipt-cache adversaries',()=>{
 it('invalidates inode rotation even with identical byte length and restored mtime',()=>{
  const h=fixture([native('old'),unrelated()]);h.equal();const old=fs.statSync(h.file);
  const replacement=h.file+'.replacement';fs.writeFileSync(replacement,encode([native('new'),unrelated()]));
  fs.utimesSync(replacement,old.atime,old.mtime);fs.renameSync(replacement,h.file);
  expect(fs.statSync(h.file).ino).not.toBe(old.ino);expect(fs.statSync(h.file).size).toBe(old.size);h.equal();
 });
 it('invalidates a shrink and a delete/recreate using the same manager and session id',()=>{
  const h=fixture([native('old'),unrelated()]);h.equal();fs.truncateSync(h.file,0);expect(h.equal().delivered!.size).toBe(0);
  fs.unlinkSync(h.file);expect(h.read().delivered!.size).toBe(0);fs.writeFileSync(h.file,encode([native('new')]));h.equal();
 });
 it('does not miss a middle-history native receipt after same-inode same-size rewrite (20527)',async()=>{
  const at=Date.now();const h=fixture([native('old',at),unrelated()]);h.equal();const old=fs.statSync(h.file);
  // Actual filesystem mutation, no mocked stat. Last 64 bytes and old length stay identical.
  await new Promise(r=>setTimeout(r,15));fs.writeFileSync(h.file,encode([native('new',at),unrelated()]));fs.utimesSync(h.file,old.atime,old.mtime);
  const after=fs.statSync(h.file);expect(after.ino).toBe(old.ino);expect(after.size).toBe(old.size);expect(after.ctimeMs).not.toBe(old.ctimeMs);
  const mesh=h.mesh(),inbox=h.inbox(mesh);
  const event=await mesh.publish({topic,kind:'handoff',from:peer,to:me.id,text:'new delivered natively',data:{deliveryId:'new'}});
  const stale=h.read(),truth=h.full();
  const match=shared.eventReceipts(event).some(id=>truth.delivered!.has(id));expect(match).toBe(true);
  const offered=await inbox.next(stale);
  console.log('RFL same-size rewrite',JSON.stringify({oldIno:old.ino,newIno:after.ino,size:after.size,oldCtime:old.ctimeMs,newCtime:after.ctimeMs,staleMatches:shared.eventReceipts(event).some(id=>stale.delivered!.has(id)),coldMatches:match,offered:offered.events.map(e=>e.data)}));
  expect(offered.events,'a canonical native receipt must suppress its duplicate shadow').toEqual([]);
 });
 it('does not hide unseen work after an earlier receipt is removed but the tail is unchanged (20527)',async()=>{
  const h=fixture([native('old'),unrelated()]);h.read();
  // Rebuild transcript prefix without this receipt, while retaining exact length and tail.
  const first=fs.readFileSync(h.file,'utf8').split('\n')[0]!;
  const replacement=JSON.stringify({type:'custom',data:'x'.repeat(first.length-27)});
  const padded=replacement.length<=first.length?replacement+' '.repeat(first.length-replacement.length):' '.repeat(first.length);
  fs.writeFileSync(h.file,padded+'\n'+encode([unrelated()]));
  const mesh=h.mesh(),inbox=h.inbox(mesh);
  const event=await mesh.publish({topic,kind:'handoff',from:peer,to:me.id,text:'unseen old work',data:{deliveryId:'old'}});
  const truth=h.full();expect(truth.delivered!.size).toBe(0);
  const offered=await inbox.next(h.read());
  expect(offered.events.map(e=>e.id),'removed receipt must not permanently advance past unseen work').toEqual([event.id]);
 });
 it('does not miss receipts inserted before a long unchanged suffix (non-append growth)',()=>{
  const h=fixture();const first=encode([native('old')]);
  const padded=(value:unknown)=>JSON.stringify(value).padEnd(511,' ')+'\n';
  const suffix=padded(unrelated('padding')).repeat(8);
  fs.writeFileSync(h.file,first+suffix);h.equal();
  // Insert a whole valid receipt before a repeated 512-byte suffix. The 64-byte guard at
  // the prior complete offset is identical although a receipt appeared in the prefix.
  fs.writeFileSync(h.file,first+padded(native('new'))+suffix);
  h.equal();
 });
 it('sees appends despite restored mtime; clock rollback and future-dated receipts recompute authority',()=>{
  const h=fixture();let now=1_800_000_000_000;vi.spyOn(Date,'now').mockImplementation(()=>now);
  fs.writeFileSync(h.file,encode([native('old',now+60_100)]));h.equal();expect(h.read().deliveredAt!.size).toBe(0);
  const old=fs.statSync(h.file);fs.appendFileSync(h.file,encode([native('new',now)]));fs.utimesSync(h.file,old.atime,old.mtime);h.equal();
  now+=100;h.equal();expect(h.read().deliveredAt!.size).toBeGreaterThan(2);now-=101;h.equal();
 });
 it('invalidates issuer rotation, registration, revoke, clear, eviction and same-closure replacement',()=>{
  const h=fixture();const e=h.issue('one');fs.writeFileSync(h.file,encode([e]));expect(h.equal().holdsBatch(['one'])).toBe(true);
  const snapshot=h.issuer().receipts.get(e.details.nonce)!;h.issuer().receipts.delete(e.details.nonce);expect(h.equal().holdsBatch(['one'])).toBe(false);
  h.issuer().receipts.set(e.details.nonce,snapshot);expect(h.equal().holdsBatch(['one'])).toBe(true);
  h.issuer().receipts.clear();expect(h.equal().holdsBatch(['one'])).toBe(false);
  const second=h.issue('two');fs.appendFileSync(h.file,encode([second]));expect(h.equal().holdsBatch(['two'])).toBe(true);
  for(let n=0;n<2048;n++)h.issue('evict-'+n);expect(h.equal().holdsBatch(['two'])).toBe(false);
  const third=h.issue('three');fs.appendFileSync(h.file,encode([third]));expect(h.equal().holdsBatch(['three'])).toBe(true);
  h.rotate();expect(h.equal().holdsBatch(['three'])).toBe(false);
 });
 it('does not treat an unconfirmed partial line as delivery; completes exactly once after confirmation',async()=>{
  const h=fixture([unrelated()]);const mesh=h.mesh(),inbox=h.inbox(mesh);
  const event=await mesh.publish({topic,kind:'handoff',from:peer,to:me.id,text:'partial',data:{deliveryId:'partial'}});
  const line=encode([native('partial')]);fs.appendFileSync(h.file,line.slice(0,-1));h.equal();
  expect((await inbox.next(h.read())).events.map(e=>e.id)).toEqual([event.id]);
  fs.appendFileSync(h.file,'\n');h.equal();expect((await inbox.next(h.read())).events).toEqual([]);
  expect((await inbox.next(h.read())).events).toEqual([]);
 });
 it('rejects fsync failures on unchanged hits and recovers only after successful confirmation',()=>{
  const h=fixture([native('old')]);const first=h.equal();expect(first.delivered!.size).toBeGreaterThan(0);
  const spy=vi.spyOn(fs,'fsyncSync').mockImplementation(()=>{throw new Error('simulated power-loss uncertainty');});
  expect(h.read().delivered!.size).toBe(0);spy.mockRestore();h.equal();
 });
 it('keeps two independent Mains on one mesh coherent while publisher advances between reader awaits',async()=>{
  const h=fixture([unrelated()]);const reader=h.mesh(),writer=h.mesh(),inbox=h.inbox(reader);const before=h.read();
  let release!:()=>void,entered!:()=>void;const gate=new Promise<void>(r=>release=r),started=new Promise<void>(r=>entered=r);
  const put=reader.put.bind(reader);vi.spyOn(reader,'put').mockImplementationOnce(async input=>{const result=await put(input);entered();await gate;return result;});
  const reading=inbox.next(before);await started;
  const event=await writer.publish({topic,kind:'handoff',from:peer,to:me.id,text:'interleaved',data:{deliveryId:'interleaved'}});
  release();await reading;
  expect(h.read()).toBe(before);const offered=await inbox.next(h.read());expect(offered.events.map(e=>e.id)).toEqual([event.id]);
  fs.appendFileSync(h.file,encode([native('interleaved')]));h.equal();expect((await inbox.next(h.read())).events).toEqual([]);
  const other=await writer.publish({topic,kind:'handoff',from:peer,to:me.id,text:'later',data:{deliveryId:'later'}});
  expect((await inbox.next(h.read())).events.map(e=>e.id)).toEqual([other.id]);
 });
 for(const mode of ['inode','truncate','same-size','generation-reset'] as const)it(`still scans mesh events after ${mode} with an unchanged receipt view`,async()=>{
  const h=fixture([native('delivered')]);const mesh=h.mesh(),inbox=h.inbox(mesh);
  if(mode==='generation-reset')fs.writeFileSync(path.join(mesh.root,'generation'),'7');
  const before=h.read();await inbox.next(before);
  const event=await mesh.publish({topic,kind:'handoff',from:peer,to:me.id,text:'message',data:{deliveryId:'brand-new'}});
  const file=path.join(mesh.root,'events.jsonl');const body=fs.readFileSync(file,'utf8');
  if(mode==='inode'){fs.writeFileSync(file+'.swap',body);fs.renameSync(file+'.swap',file);}
  if(mode==='truncate'){fs.truncateSync(file,0);fs.writeFileSync(file,body);}
  if(mode==='same-size'){const changed=body.replace('message','changed');expect(changed.length).toBe(body.length);fs.writeFileSync(file,changed);}
  if(mode==='generation-reset')fs.writeFileSync(path.join(mesh.root,'generation'),'0');
  expect(h.read()).toBe(before);expect((await inbox.next(h.read())).events.map(e=>e.id)).toEqual([event.id]);
 });
 // expected to fail until https://github.com/Smarty-Pants-Inc/smarty-dev/issues/20542 is fixed; it.fails flips red when fixed, forcing removal
 for(const mode of ['inode','same-size-in-place','truncate-observed','generation-bump','generation-reset'] as const)(mode==='same-size-in-place'||mode==='truncate-observed'?it.fails:it)(`a warmed mesh cursor sees replacement work after ${mode}`,async()=>{
  const h=fixture([native('delivered')]);const mesh=h.mesh(),inbox=h.inbox(mesh);
  fs.writeFileSync(path.join(mesh.root,'generation'),'7');
  const original=await mesh.publish({topic,kind:'handoff',from:peer,to:'session:other',text:'message',data:{deliveryId:'brand-new'}});
  await inbox.next(h.read()); // original is not addressed to this Main; advances cursor and read hints
  const file=path.join(mesh.root,'events.jsonl');const old=fs.readFileSync(file,'utf8');
  const replacement={...original,id:original.id.replace(/^./,original.id[0]==='a'?'b':'a'),sequence:original.sequence+1,to:me.id};
  const body=encode([replacement]);expect(body.length).toBe(old.length);
  if(mode==='inode'){fs.writeFileSync(file+'.swap',body);fs.renameSync(file+'.swap',file);}
  else {
   if(mode==='truncate-observed'){fs.truncateSync(file,0);await inbox.next(h.read());}
   fs.writeFileSync(file,body);
  }
  fs.writeFileSync(path.join(mesh.root,'sequence'),String(replacement.sequence));
  if(mode==='generation-bump')fs.writeFileSync(path.join(mesh.root,'generation'),'8');
  if(mode==='generation-reset')fs.writeFileSync(path.join(mesh.root,'generation'),'0');
  expect((await inbox.next(h.read())).events.map(e=>e.id),'fresh work above the inbox high-water mark must not be hidden by a stale byte-offset hint').toEqual([replacement.id]);
 });
 it('follows actual persisted SessionManager appends and new-session transitions',()=>{
  const h=fixture();const manager=SessionManager.create(h.root,path.join(h.root,'sessions'));
  manager.appendMessage({role:'user',content:'start',timestamp:Date.now()});
  manager.appendCustomMessageEntry('pi-fabric-agent-message','one',true,native('one').details);
  const before=shared.confirmedRootInboxSession(manager);
  manager.appendCustomMessageEntry('pi-fabric-agent-message','two',true,native('two').details);
  const after=shared.confirmedRootInboxSession(manager);expect(after.delivered!.size).toBeGreaterThan(before.delivered!.size);
  manager.newSession();expect(shared.confirmedRootInboxSession(manager).delivered!.size).toBe(0);
 });
});
