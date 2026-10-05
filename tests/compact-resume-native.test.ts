import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { describe, it, expect } from 'vitest';
describe('native Pi compaction recovery and acceptance', () => {
  it('refuses reused evidence directories instead of accepting stale X output', () => {
    const out = fs.mkdtempSync(path.join(os.tmpdir(),'compact-native-reused-'));
    fs.mkdirSync(path.join(out,'compound'));
    try {
      const result = spawnSync(process.execPath,['scripts/probe-compact-resume.mjs','compound'],{
        env:{...process.env,COMPACT_PROBE_OUT:out},encoding:'utf8',timeout:15000});
      expect(result.status).not.toBe(0);
      expect(result.stderr).toContain('Lane output already exists');
    } finally { fs.rmSync(out,{recursive:true,force:true}); }
  });

  it.each(['restart','restart-actor','receipt','compound','plain'])('%s uses the real native lifecycle', mode => {
    const out = fs.mkdtempSync(path.join(os.tmpdir(),'compact-native-test-'));
    try {
      const result = spawnSync(process.execPath,['scripts/probe-compact-resume.mjs',mode],{env:{...process.env,COMPACT_PROBE_OUT:out,COMPACT_PROBE_SOURCE:process.env.COMPACT_PROBE_SOURCE ?? '1'},encoding:'utf8',timeout:65000,maxBuffer:4*1024*1024});
      expect(result.status, result.stdout+'\n'+result.stderr).toBe(0);
    } finally { fs.rmSync(out,{recursive:true,force:true}); }
  },70000);
});
