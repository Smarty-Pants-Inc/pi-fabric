import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ExecutionDeadline } from '../src/runtime/execution-deadline.js';
import { humanWaitDeadlineClock } from '../src/runtime/kernel.js';
import { HumanWaitDeadlinePause } from '../src/runtime/deadline-pause.js';
import { QuickJsRuntime } from '../src/runtime/quickjs-runtime.js';
import { NodeProcessRuntime } from '../src/runtime/node-process-runtime.js';
import { CPythonRuntime } from '../src/runtime/cpython-runtime.js';
import { MontyRuntime } from '../src/runtime/monty-runtime.js';
import { createMainExecutionCeilingError, mainExecutionCeilingAbortReason, registerCancellationEffect } from '../src/async-settlement.js';

const controllers: AbortController[] = [];
beforeEach(() => vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] }));
afterEach(() => { for (const c of controllers.splice(0)) c.abort(); vi.useRealTimers(); });
const deferred = <T>() => { let resolve!: (x:T)=>void; const promise=new Promise<T>(r=>resolve=r); return {promise,resolve}; };

describe('F1 shared paused deadline bridge', () => {
  it('pauses the shared object, accounts overlapping waits once, raises floors, and restores methods', async () => {
    const d=new ExecutionDeadline({timeoutMs:1000}); const originalClear=d.clear;
    const schedule=vi.fn(); const expire=vi.fn();
    const p=new HumanWaitDeadlinePause(humanWaitDeadlineClock(()=>d,{},schedule,expire));
    p.enter(); p.enter(); expect(d.at).toBe(Infinity);
    await vi.advanceTimersByTimeAsync(60000); expect(d.reached).toBe(false);
    p.leave(); expect(d.at).toBe(Infinity); p.raise(5000); p.leave();
    expect(d.at).toBe(Date.now()+5000); expect(d.clear).toBe(originalClear);
    expect(schedule).toHaveBeenCalledTimes(1); expect(expire).not.toHaveBeenCalled(); d.clear();
  });
  it('keeps the opaque hard cause sticky even after a backwards clock and pause completion', async () => {
    const reason=createMainExecutionCeilingError(5000); const max=Date.now()+5000;
    const opts={timeoutMs:1000, maximumDeadlineAt:max, maximumDeadlineReason:reason};
    const d=new ExecutionDeadline(opts); const expire=vi.fn();
    const p=new HumanWaitDeadlinePause(humanWaitDeadlineClock(()=>d,opts,vi.fn(),expire));
    p.enter(); expect(d.at).toBe(max); await vi.advanceTimersByTimeAsync(4999); expect(d.reached).toBe(false);
    await vi.advanceTimersByTimeAsync(1); expect(expire).toHaveBeenCalledTimes(1); expect(d.reason).toBe(reason);
    vi.setSystemTime(max-10000); p.leave(); expect(d.reached).toBe(true); expect(d.extend(900000)).toBe(false);
    expect(d.timeoutResult([]).deadlineReason).toBe(reason); d.clear();
  });
  it('cannot pause or revive an already expired executable budget', async () => {
    const d=new ExecutionDeadline({timeoutMs:10}); await vi.advanceTimersByTimeAsync(11);
    const expire=vi.fn();const p=new HumanWaitDeadlinePause(humanWaitDeadlineClock(()=>d,{},vi.fn(),expire));
    p.enter(); expect(d.reached).toBe(true); expect(expire).toHaveBeenCalledTimes(1); p.raise(900000);p.leave();
    expect(d.reached).toBe(true); expect(d.timeoutResult([]).deadlineReason).toBeUndefined(); d.clear();
  });
  it('clear during custody teardown cancels the ceiling timer and does not restart execution', async () => {
    const opts={timeoutMs:1000,maximumDeadlineAt:Date.now()+5000};const d=new ExecutionDeadline(opts);
    const expire=vi.fn();const schedule=vi.fn();const p=new HumanWaitDeadlinePause(humanWaitDeadlineClock(()=>d,opts,schedule,expire));
    p.enter();d.clear();await vi.advanceTimersByTimeAsync(10000);p.leave();
    expect(expire).not.toHaveBeenCalled();expect(schedule).not.toHaveBeenCalled();expect(d.reached).toBe(true);
  });
  it('binds to the current QuickJS execution clock after trusted setup replaces its clock', async () => {
    let d=new ExecutionDeadline({timeoutMs:10});const p=new HumanWaitDeadlinePause(humanWaitDeadlineClock(()=>d,{},vi.fn(),vi.fn()));
    d=new ExecutionDeadline({timeoutMs:1000});p.enter();await vi.advanceTimersByTimeAsync(60000);p.leave();
    expect(d.reached).toBe(false);expect(d.at).toBe(Date.now()+1000);d.clear();
  });
});

const runtimes={quickjs:QuickJsRuntime, 'node-process':NodeProcessRuntime, cpython:CPythonRuntime, monty:MontyRuntime};
for(const [backend,Runtime] of Object.entries(runtimes)) {
  const code=backend==='cpython'||backend==='monty' ? 'return await extensions.ask({"receipt":"one"})' : 'return await extensions.ask({receipt:"one"})';
  describe(`F1 direct ${backend} human waits and safety fences`,()=>{
    it.each([false,true])('pauses soft time with correlated receipt delivery (shared=%s)',async shared=>{
      const gate=deferred<AbortSignal>();const answer=deferred<unknown>();const controller=new AbortController();controllers.push(controller);
      const d=new ExecutionDeadline({timeoutMs:1000});const delivered=vi.fn();
      const pending=new Runtime().execute(code,async(_ref,_args,signal)=>{gate.resolve(signal);return answer.promise;},{
        timeoutMs:1000,memoryLimitBytes:256*1024*1024,signal:controller.signal,
        ...(shared?{executionDeadline:d}:{}),isHumanWaitHostCall:ref=>ref==='extensions.ask',onHostResultDelivered:delivered,
      });
      const signal=await gate.promise;await vi.advanceTimersByTimeAsync(60000);expect(signal.aborted).toBe(false);
      if(shared) expect(d.reached).toBe(false);answer.resolve({answer:'okay'});
      expect(await pending).toMatchObject({terminationReason:'completed',value:{answer:'okay'}});
      expect(delivered).toHaveBeenCalledTimes(1);expect(delivered).toHaveBeenCalledWith({receipt:'one'});d.clear();
    },15000);
    it('hard expiry while paused retains Main identity and committed resident custody',async()=>{
      const gate=deferred<AbortSignal>();const answer=deferred<unknown>();const controller=new AbortController();controllers.push(controller);
      const reason=createMainExecutionCeilingError(5000);const max=Date.now()+5000;
      const receipt={requestId:'f1-request',state:'committed',operation:'createActor',entityKind:'actor',id:'f1-actor',ownerHostId:'owner'} as const;
      const d=new ExecutionDeadline({timeoutMs:1000,maximumDeadlineAt:max,maximumDeadlineReason:reason});
      const pending=new Runtime().execute(code,async(_ref,_args,signal)=>{
        registerCancellationEffect(signal,()=>Object.assign(new Error('Committed. Do not retry or reassign.'),{residentOutcome:receipt}));
        gate.resolve(signal);return answer.promise;
      },{timeoutMs:1000,memoryLimitBytes:256*1024*1024,signal:controller.signal,executionDeadline:d,
        maximumDeadlineAt:max,maximumDeadlineReason:reason,isHumanWaitHostCall:ref=>ref==='extensions.ask'});
      const signal=await gate.promise;await vi.advanceTimersByTimeAsync(5001);
      expect(signal.aborted).toBe(true);expect(mainExecutionCeilingAbortReason(signal)).toBe(reason);answer.resolve({answer:'late'});
      const result=await pending;expect(result).toMatchObject({terminationReason:'timed_out',residentOutcomes:[receipt]});
      expect(result.deadlineReason).toBe(reason);expect(result.error).toContain('Do not retry or reassign');d.clear();
    },15000);
    it('user cancellation still aborts an entered human wait immediately',async()=>{
      const gate=deferred<AbortSignal>();const answer=deferred<unknown>();const controller=new AbortController();controllers.push(controller);
      const pending=new Runtime().execute(code,async(_ref,_args,signal)=>{gate.resolve(signal);return answer.promise;},{
        timeoutMs:1000,memoryLimitBytes:256*1024*1024,signal:controller.signal,isHumanWaitHostCall:()=>true});
      const signal=await gate.promise;controller.abort(new Error('user cancelled'));answer.resolve({answer:'late'});
      expect(signal.aborted).toBe(true);expect((await pending).terminationReason).toBe('aborted');
    },15000);
  });
}
