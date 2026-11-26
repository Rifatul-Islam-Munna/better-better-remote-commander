import {test,after,mock} from 'node:test';
import assert from 'node:assert/strict';
import os from 'node:os';
import fs from 'node:fs/promises';
import path from 'node:path';
import {lightweightHardwareLimit,pressureConcurrencyLimit} from '../dist/runtime/parallel-capacity.js';

const scratch=await fs.mkdtemp(path.join(os.tmpdir(),'brc-parallel-capacity-'));
process.env.BRC_HOME=scratch;process.env.BRC_ERROR_LOG='0';process.env.BRC_LOG_LEVEL='silent';
mock.timers.enable({apis:['setInterval']});
const {OperationScheduler}=await import('../dist/runtime/operations.js');
const {configManager}=await import('../dist/core/config-manager.js');

const GiB=1024**3;
test('lightweight worker ceiling scales down for small PCs and reaches six on 16-thread 32-GiB hardware',()=>{
 for(const [cpu,memory,expected]of [[16,32,6],[24,64,6],[8,8,4],[4,4,2],[2,2,1],[1,32,1],[32,2,1],[0,0,1]])
  assert.equal(lightweightHardwareLimit(cpu,memory*GiB),expected,`${cpu} threads / ${memory} GiB`);
});

test('normal/moderate/high/critical load caps lightweight work at 6/3/2/1 without exceeding hardware',()=>{
 assert.deepEqual(['normal','warm','high','critical'].map(level=>pressureConcurrencyLimit(level,6)),[6,3,2,1]);
 assert.deepEqual(['normal','warm','high','critical'].map(level=>pressureConcurrencyLimit(level,2)),[2,2,2,1]);
});

test('transport admits six light calls, then lowers admission without cancelling in-flight work',async()=>{
 let limit=6,entered=0;
 const scheduler=new OperationScheduler(6,16,()=>limit);
 const releases=[];
 const pending=Array.from({length:6},(_,i)=>scheduler.run('capacity-'+i,'read_file',5000,async()=>{
  entered++;await new Promise(resolve=>{releases[i]=resolve;});return i;
 }));
 await new Promise(resolve=>setImmediate(resolve));assert.equal(entered,6);
 limit=3;
 const seventh=scheduler.run('capacity-seven','read_file',5000,async()=>{entered++;return 7;});
 for(let i=0;i<3;i++)releases[i]();
 await new Promise(resolve=>setImmediate(resolve));assert.equal(entered,6,'three existing calls still occupy the reduced limit');
 releases[3]();await new Promise(resolve=>setImmediate(resolve));assert.equal(entered,7);
 releases[4]();releases[5]();
 assert.deepEqual(await Promise.all(pending),[0,1,2,3,4,5]);assert.equal(await seventh,7);
});

const hardware={availableParallelism:os.availableParallelism,totalmem:os.totalmem,freemem:os.freemem};
after(async()=>{Object.assign(os,hardware);mock.timers.reset();await configManager.close();await fs.rm(scratch,{recursive:true,force:true,maxRetries:6,retryDelay:80});});
test('real pressure sampler and task policy apply six-to-one limits and respect explicit caller caps',async()=>{
 os.availableParallelism=()=>16;os.totalmem=()=>32*GiB;
 const {adaptiveTaskConcurrency}=await import('../dist/runtime/task-parallelism.js');
 const calls=Array.from({length:6},()=>({tool:'read_file',args:{path:'fixture.txt'}}));
 for(const [free,expected]of [[20,6],[5,3],[2.5,2],[0.5,1]]){
  os.freemem=()=>free*GiB;
  assert.equal(adaptiveTaskConcurrency(calls).effective,expected);
 }
 os.freemem=()=>20*GiB;
 assert.equal(adaptiveTaskConcurrency(calls,3).effective,3);
 const single=adaptiveTaskConcurrency(calls.slice(0,1));
 assert.equal(single.available,6,'available capacity must not shrink to the submitted call count');
 assert.equal(single.effective,1,'capacity must not invent additional work');
 assert.equal(adaptiveTaskConcurrency([{tool:'run_command',args:{command:'pnpm build'}},{tool:'run_project_checks'}],6).effective,1);
});
