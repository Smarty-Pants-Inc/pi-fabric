import { afterEach, describe, expect, it, vi } from 'vitest';
import { setupJev, launch, callProgram } from './jev-test-helpers.js';
const fixtures: ReturnType<typeof setupJev>[]=[];
afterEach(async()=>{for(const fixture of fixtures.splice(0)){await fixture.provider.close();await fixture.registry.close();}});
describe('F1 Jev provider finalizer outcome',()=>{
 it.each([
  ['return 7;', {}, 'completed', 'succeeded'],
  ['throw new Error("guest failed");', {}, 'failed', 'failed'],
  ['return "not an integer";', {type:'integer'}, 'failed', 'failed'],
 ] as const)('finalizes %s without treating an unproven exit as success',async(code,outputSchema,state,outcome)=>{
  const f=setupJev();fixtures.push(f);const finalize=vi.fn(async()=>undefined);
  f.registry.register({name:'cleanupwitness',description:'finalizer witness',list:async()=>[],describe:async()=>undefined,invoke:async()=>undefined,invocationEnded:finalize});
  const info=await callProgram(f.provider,'run',launch(code,{outputSchema}));
  expect(info.state).toBe(state);expect(info.endedAt).toBeTypeOf('number');
  expect(finalize).toHaveBeenCalledExactlyOnceWith(`jev:${info.id}`,outcome);
 });
});
