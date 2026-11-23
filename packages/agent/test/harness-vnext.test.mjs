import {test,after} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import {mkdtempSync,realpathSync} from 'node:fs';
import {tmpdir} from 'node:os';
import path from 'node:path';

const scratch=mkdtempSync(path.join(realpathSync.native(tmpdir()),'brc-harness-vnext-'));
const home=path.join(scratch,'home');
const project=path.join(scratch,'project');
process.env.BRC_HOME=home;
process.env.BRC_LOG_FILE='0';
process.env.BRC_LOG_LEVEL='silent';
process.env.BRC_ERROR_LOG='0';
await fs.mkdir(project,{recursive:true});
await fs.writeFile(path.join(project,'package.json'),'{}');

const {configManager}=await import('../dist/core/config-manager.js');
await configManager.setValue('allowedDirectories',[project]);
const {
  BoundedLocalStore,HARNESS_STATE_BYTES,SEMANTIC_CACHE_BYTES,
}=await import('../dist/runtime/bounded-local-store.js');
const {
  beginDurableExecution,durablePrepareStep,durableSettleStep,finishDurableExecution,
  harnessVNextStatus,
}=await import('../dist/runtime/harness-vnext.js');
const {semanticSearch,semanticSearchStatus}=await import('../dist/runtime/semantic-project-search.js');
const {getProjectSession,roundTripAcceleratorStatus}=await import('../dist/runtime/roundtrip-accelerator.js');
const {codeModeScheduler}=await import('../dist/runtime/code-mode-scheduler.js');
const {systemPressureStatus}=await import('../dist/runtime/system-pressure.js');
const {runProjectPreflight}=await import('../dist/runtime/preflight-self-heal.js');
const {adaptiveTaskConcurrency}=await import('../dist/runtime/task-parallelism.js');
const {checkpointRecoveryCapsule}=await import('../dist/runtime/recovery-capsule.js');
const {recordProjectMutation,recordProjectVerification,getVerificationEvidence}=await import('../dist/runtime/verification-evidence.js');
const {mutationChainStatus}=await import('../dist/runtime/mutation-chain.js');
const {callTool}=await import('../dist/dispatcher.js');
const {withCallContext}=await import('../dist/context.js');
const {executeCodeMode}=await import('../dist/runtime/code-mode.js');

after(async()=>{
  await configManager.close();
  await fs.rm(scratch,{recursive:true,force:true,maxRetries:6,retryDelay:50});
});

const text=value=>({content:[{type:'text',text:value}]});
const args=(source,opts={})=>({source,max_runtime_ms:8000,max_calls:24,...opts});

test('bounded local store evicts oldest records and honors hard byte quota',async()=>{
  const store=new BoundedLocalStore('test-vnext-quota',1024*1024,800*1024);
  await store.put('row','one',{payload:'a'.repeat(610*1024)});
  await new Promise(r=>setTimeout(r,8));
  await store.put('row','two',{payload:'b'.repeat(610*1024)});
  const status=await store.status();
  assert.ok(status.bytes<=1024*1024,status.bytes);
  assert.equal(await store.get('row','one'),null);
  assert.ok(await store.get('row','two'));
  assert.ok(status.removedRecords>=1);
});

test('semantic project search persists locally, ranks code context, and stays within 64 MiB',async()=>{
  await fs.writeFile(path.join(project,'auth.guard.ts'),[
    'export class AuthGuard {',
    '  canActivate(token: string) {',
    '    return token.startsWith("Bearer ") && this.authorizeLogin(token);',
    '  }',
    '  authorizeLogin(token: string) { return token.length > 20; }',
    '}',
  ].join('\n'));
  await fs.writeFile(path.join(project,'login.service.ts'),[
    'export class LoginService {',
    '  login(email: string) { return { email, session: true }; }',
    '}',
  ].join('\n'));
  const first=await semanticSearch({path:project,query:'login token authorization guard',limit:5,refresh:true});
  assert.equal(first.index.rebuilt,true);
  assert.ok(first.results.some(r=>r.path==='auth.guard.ts'),JSON.stringify(first.results));
  const second=await semanticSearch({path:project,query:'login authorization',limit:5});
  assert.equal(second.index.rebuilt,false);
  const status=await semanticSearchStatus();
  assert.equal(status.store.maxBytes,SEMANTIC_CACHE_BYTES);
  assert.ok(status.store.bytes<=SEMANTIC_CACHE_BYTES);
});

test('durable source reads refresh on resume instead of replaying stale contents',async()=>{
  const source='// durable-replay-test\nreturn 1;';
  const first=await beginDurableExecution(source);
  assert.equal(first.resumed,false);
  const prepared=await durablePrepareStep(first.execution,'read_file',{path:'demo.ts'},true,first.trace);
  assert.equal(prepared.replay,undefined);
  await durableSettleStep(first.execution,prepared.index,{ok:true,value:{text:'cached'}},1,first.trace);
  // Simulate an agent interruption: leave the durable execution in running state.
  const resumed=await beginDurableExecution(source);
  assert.equal(resumed.resumed,true);
  const replay=await durablePrepareStep(resumed.execution,'read_file',{path:'demo.ts'},true,resumed.trace);
  assert.equal(replay.replay,undefined);
  assert.equal(replay.mustStop,undefined);
  await finishDurableExecution(resumed.execution,'completed',{ok:true},resumed.trace);
});

test('Code Mode retries only transient read-only failures',async()=>{
  let calls=0;
  const catalog=[{name:'read_file',description:'read',inputSchema:{type:'object'},annotations:{readOnlyHint:true}}];
  const out=await executeCodeMode(args(
    'const r=await tools.call("read_file",{path:"x"});return r.text;'
  ),catalog,async()=>{
    calls++;
    if(calls<3)return {isError:true,content:[{type:'text',text:'busy'}],
      structuredContent:{error:{code:'SEARCH_BUSY',message:'busy'}}};
    return text('ok');
  });
  assert.equal(out.isError,undefined,JSON.stringify(out));
  assert.equal(calls,3);
  assert.equal(out.structuredContent.result,'ok');
  assert.equal(out.structuredContent.retries,2);
  assert.equal(out.structuredContent.steps[0].attempts,3);
});

test('Code Mode never automatically retries a non-read-only tool',async()=>{
  let calls=0;
  const catalog=[{name:'write_file',description:'write',inputSchema:{type:'object'},annotations:{readOnlyHint:false}}];
  const out=await executeCodeMode(args(
    'await tools.call("write_file",{path:"x",content:"y"});return "unexpected";'
  ),catalog,async()=>{
    calls++;
    return {isError:true,content:[{type:'text',text:'busy'}],
      structuredContent:{error:{code:'BUSY',message:'busy'}}};
  });
  assert.equal(calls,1);
  assert.equal(out.structuredContent.status,'stopped_on_step_error');
  assert.equal(out.structuredContent.automaticRetry.safeReadOnlyOnly,true);
});

test('saved Code Mode snippets can be saved, searched and loaded inside sandbox',async()=>{
  const source=[
    'const saved=await tools.snippets.save({name:"next-fix",description:"Next build fix",tags:["nextjs"],source:"return 42;"});',
    'const hits=await tools.snippets.search("next build",5);',
    'const loaded=await tools.snippets.get(saved.id);',
    'return {hits:hits.length,name:loaded.name,source:loaded.source};',
  ].join('\n');
  const out=await executeCodeMode(args(source),[],async()=>{throw new Error('no BRC tool expected');});
  assert.equal(out.isError,undefined,JSON.stringify(out));
  assert.ok(out.structuredContent.result.hits>=1);
  assert.equal(out.structuredContent.result.name,'next-fix');
  assert.equal(out.structuredContent.result.source,'return 42;');
});

test('specialist agent roles execute scoped child steps without a second model',async()=>{
  let calls=0;
  const catalog=[{name:'read_file',description:'read',inputSchema:{type:'object'},annotations:{readOnlyHint:true}}];
  const source=[
    'const roles=await tools.agent.roles();',
    'const child=await tools.agent.run({role:"explorer",task:"inspect auth",steps:[{tool:"read_file",args:{path:"auth.ts"}}]});',
    'return {roles:roles.map(x=>x.role),ok:child.allSucceeded,text:child.results[0].value.text,separateModel:child.separateModel};',
  ].join('\n');
  const out=await executeCodeMode(args(source),catalog,async()=>{calls++;return text('child-read');});
  assert.equal(out.isError,undefined,JSON.stringify(out));
  assert.equal(calls,1);
  assert.equal(out.structuredContent.result.ok,true);
  assert.equal(out.structuredContent.result.text,'child-read');
  assert.equal(out.structuredContent.result.separateModel,false);
  assert.ok(out.structuredContent.result.roles.includes('coder'));
});

test('specialist read-only roles cannot tunnel a write',async()=>{
  let calls=0;
  const catalog=[{name:'write_file',description:'write',inputSchema:{type:'object'},annotations:{readOnlyHint:false}}];
  const source=[
    'const child=await tools.agent.run({role:"explorer",task:"inspect only",steps:[{tool:"write_file",args:{path:"x",content:"no"}}]});',
    'return child;',
  ].join('\n');
  const out=await executeCodeMode(args(source),catalog,async()=>{calls++;return text('wrong');});
  assert.equal(out.isError,undefined,JSON.stringify(out));
  assert.equal(calls,0);
  assert.equal(out.structuredContent.result.allSucceeded,false);
  assert.equal(out.structuredContent.result.results[0].error.code,'SPECIALIST_TOOL_DENIED');
});

test('new harness capabilities are discoverable through the legacy Code Mode tool contract',async()=>{
  const source=[
    'const hits=await tools.search("semantic project search");',
    'const schema=await tools.describe("semantic_project_search");',
    'const found=await tools.call("semantic_project_search",{path:'+JSON.stringify(project)+',query:"authorization guard",limit:3});',
    'return {names:hits.map(x=>x.name),schema:schema.name,results:found.results.map(x=>x.path)};',
  ].join('\n');
  const out=await executeCodeMode(args(source),[],async()=>{throw new Error('no native BRC tool expected');});
  assert.equal(out.isError,undefined,JSON.stringify(out));
  assert.ok(out.structuredContent.result.names.includes('semantic_project_search'));
  assert.equal(out.structuredContent.result.schema,'semantic_project_search');
  assert.ok(out.structuredContent.result.results.includes('auth.guard.ts'));
});

test('named tools.step checkpoint executes its function only once inside Code Mode',async()=>{
  const source=[
    'let n=0;',
    'const a=await tools.step("inspect",async()=>++n);',
    'const b=await tools.step("inspect",async()=>++n);',
    'return {a,b,n};',
  ].join('\n');
  const out=await executeCodeMode(args(source),[],async()=>{throw new Error('no native tool expected');});
  assert.equal(out.isError,undefined,JSON.stringify(out));
  assert.deepEqual(out.structuredContent.result,{a:1,b:1,n:1});
});

test('persistent project sessions retain bounded state and tool observations',async()=>{
  const source=[
    'const s=await tools.sessions.open({name:"persist-project",root:'+JSON.stringify(project)+',state:{branch:"main",password:"do-not-store"}});',
    'await tools.call("read_file",{path:'+JSON.stringify(path.join(project,'auth.guard.ts'))+'});',
    'return s.id;',
  ].join('\n');
  const catalog=[{name:'read_file',description:'read',inputSchema:{type:'object'},annotations:{readOnlyHint:true}}];
  const out=await executeCodeMode(args(source),catalog,async()=>text('ok'));
  assert.equal(out.isError,undefined,JSON.stringify(out));
  const saved=await getProjectSession(out.structuredContent.result);
  assert.equal(saved.state.branch,'main');
  assert.equal(saved.state.password,'[REDACTED]');
  assert.ok(saved.recentTools.includes('read_file'));
  assert.ok(saved.recentPaths.some(x=>x.endsWith('auth.guard.ts')));
});

test('large nested results stay local behind result handles and can be searched in same Code Mode call',async()=>{
  const catalog=[{name:'read_file',description:'read',inputSchema:{type:'object'},annotations:{readOnlyHint:true}}];
  const marker='UNIQUE_LOCAL_HANDLE_MARKER';
  const huge='x'.repeat(90000)+marker+'y'.repeat(30000);
  const source=[
    'const r=await tools.call("read_file",{path:"x"});',
    'const found=await tools.results.get(r.resultHandle.id,{query:'+JSON.stringify(marker)+'});',
    'return {handle:!!r.resultHandle,hits:found.hits.length,preview:r.text.length,partial:r.resultHandle.partial};',
  ].join('\n');
  const out=await executeCodeMode(args(source),catalog,async()=>text(huge));
  assert.equal(out.isError,undefined,JSON.stringify(out));
  assert.equal(out.structuredContent.result.handle,true);
  assert.ok(out.structuredContent.result.hits>=1);
  assert.ok(out.structuredContent.result.preview<huge.length);
});

test('saved workflow can auto-match and execute locally without another calling-AI round trip',async()=>{
  const source=[
    'await tools.snippets.save({name:"auto-math-workflow",description:"auto math workflow",tags:["math","automatic"],source:"return {answer:context.n+1};"});',
    'const run=await tools.call("saved_workflow_run",{query:"auto math workflow",context:{n:4}});',
    'return run;',
  ].join('\n');
  const out=await executeCodeMode(args(source),[],async()=>{throw new Error('no native tool expected');});
  assert.equal(out.isError,undefined,JSON.stringify(out));
  assert.equal(out.structuredContent.result.matched,true);
  assert.equal(out.structuredContent.result.value.answer,5);
});

test('named terminal session is reused instead of launching a second process',async()=>{
  const counts={start:0,read:0};
  const catalog=[
    {name:'start_process',description:'start',inputSchema:{type:'object'},annotations:{readOnlyHint:false}},
    {name:'read_process_output',description:'read',inputSchema:{type:'object'},annotations:{readOnlyHint:true}},
    {name:'interact_with_process',description:'send',inputSchema:{type:'object'},annotations:{readOnlyHint:false}},
  ];
  const source=[
    'const one=await tools.terminal.use({op:"start",name:"dev",startArgs:{command:"dev"}});',
    'const two=await tools.terminal.use({op:"start",name:"dev",startArgs:{command:"dev"}});',
    'return {firstReused:one.reused,secondReused:two.reused};',
  ].join('\n');
  const out=await executeCodeMode(args(source),catalog,async(name)=>{
    if(name==='start_process'){counts.start++;return {content:[{type:'text',text:'started'}],structuredContent:{pid:7,sessionId:'sess-1',processMayStillBeRunning:true}};}
    if(name==='read_process_output'){counts.read++;return {content:[{type:'text',text:'running'}],structuredContent:{sessionId:'sess-1',processRunning:true}};}
    return text('ok');
  });
  assert.equal(out.isError,undefined,JSON.stringify(out));
  assert.equal(counts.start,1);
  assert.equal(counts.read,1);
  assert.deepEqual(out.structuredContent.result,{firstReused:false,secondReused:true});
});

test('reusable browser alias calls the same existing plugin connection',async()=>{
  const calls=[];
  const catalog=[{name:'call_plugin_tool',description:'plugin',inputSchema:{type:'object'},annotations:{readOnlyHint:false}}];
  const source=[
    'await tools.browser.use({op:"bind",name:"ui",plugin:"playwright"});',
    'await tools.browser.use({op:"call",name:"ui",tool:"browser_snapshot",args:{}});',
    'await tools.browser.use({op:"call",name:"ui",tool:"browser_get_title",args:{}});',
    'return "ok";',
  ].join('\n');
  const out=await executeCodeMode(args(source),catalog,async(name,a)=>{calls.push({name,a});return text('ok');});
  assert.equal(out.isError,undefined,JSON.stringify(out));
  assert.equal(calls.length,2);
  assert.ok(calls.every(x=>x.a.plugin==='playwright'));
});

test('durable fiber uses persistent jobs and completes locally through wait/status',async()=>{
  const counts={spawn:0,wait:0,get:0};
  const catalog=[
    {name:'spawn_job',description:'spawn',inputSchema:{type:'object'},annotations:{readOnlyHint:false}},
    {name:'wait_jobs',description:'wait',inputSchema:{type:'object'},annotations:{readOnlyHint:true}},
    {name:'get_job',description:'get',inputSchema:{type:'object'},annotations:{readOnlyHint:true}},
    {name:'read_file',description:'read',inputSchema:{type:'object'},annotations:{readOnlyHint:true}},
  ];
  const source=[
    'await tools.fibers.run({op:"start",name:"scan",steps:[{tool:"read_file",args:{path:"x"}}]});',
    'const done=await tools.fibers.run({op:"wait",name:"scan",wait_ms:10});',
    'return {status:done.status,completed:done.completedSteps};',
  ].join('\n');
  const out=await executeCodeMode(args(source,{max_calls:12}),catalog,async(name)=>{
    if(name==='spawn_job'){counts.spawn++;return {content:[{type:'text',text:'started'}],structuredContent:{jobId:'job-1',pending:true,status:'running'}};}
    if(name==='wait_jobs'){counts.wait++;return text('done');}
    if(name==='get_job'){counts.get++;return {content:[{type:'text',text:'done'}],structuredContent:{jobId:'job-1',status:'succeeded',steps:1,results:1}};}
    return text('unused');
  });
  assert.equal(out.isError,undefined,JSON.stringify(out));
  assert.equal(counts.spawn,1);
  assert.equal(counts.wait,1);
  assert.equal(counts.get,1);
  assert.deepEqual(out.structuredContent.result,{status:'succeeded',completed:1});
});

test('concurrent Code Mode workflows get isolated UUIDs and obey adaptive runnable slots',async()=>{
  const before=codeModeScheduler.status();
  const count=Math.min(before.admittedLimit,Math.max(2,before.activeLimit+1));
  let running=0,maxRunning=0;
  const catalog=[{name:'read_file',description:'read',inputSchema:{type:'object'},annotations:{readOnlyHint:true}}];
  const source='const r=await tools.call("read_file",{path:"same"}); return r.text;';
  const outputs=await Promise.all(Array.from({length:count},()=>executeCodeMode(
    args(source,{max_runtime_ms:5000,max_calls:4}),catalog,async()=>{
      running++;maxRunning=Math.max(maxRunning,running);
      await new Promise(r=>setTimeout(r,90));
      running--;return text('ok');
    }
  )));
  assert.ok(outputs.every(x=>x.isError===undefined),JSON.stringify(outputs));
  const workflowIds=outputs.map(x=>x.structuredContent.workflowId);
  const executionIds=outputs.map(x=>x.structuredContent.executionId);
  assert.equal(new Set(workflowIds).size,count);
  assert.equal(new Set(executionIds).size,count);
  assert.ok(maxRunning<=before.activeLimit,{maxRunning,limit:before.activeLimit});
  // Identical safe reads may intentionally coalesce into one underlying call;
  // workflow isolation/concurrency is verified by unique workflow/execution IDs
  // and the scheduler/capacity tests below.
  const after=codeModeScheduler.status();
  assert.equal(after.active,0);
  assert.equal(after.queued,0);
  assert.equal(after.admitted,0);
  assert.ok(after.hardMaxAdmitted<=12);
  assert.ok(after.hardMaxActive<=8);
});

test('Code Mode capacity queues conservatively and rejects only above the hardware-aware admitted limit',async()=>{
  const before=codeModeScheduler.status();
  const count=before.admittedLimit+1;
  const catalog=[{name:'read_file',description:'read',inputSchema:{type:'object'},annotations:{readOnlyHint:true}}];
  const source='const r=await tools.call("read_file",{path:"capacity"}); return r.text;';
  const outputs=await Promise.all(Array.from({length:count},()=>executeCodeMode(
    args(source,{max_runtime_ms:5000,max_calls:4}),catalog,async()=>{
      await new Promise(r=>setTimeout(r,120));
      return text('ok');
    }
  )));
  const capacity=outputs.filter(x=>x.isError===true&&x.structuredContent?.error?.code==='CODE_MODE_CAPACITY');
  assert.ok(capacity.length>=1,{count,limit:before.admittedLimit,codes:outputs.map(x=>x.structuredContent?.error?.code)});
  assert.ok(outputs.filter(x=>x.isError===undefined).length>=1);
  const after=codeModeScheduler.status();
  assert.equal(after.active,0);
  assert.equal(after.queued,0);
  assert.equal(after.admitted,0);
});

test('identical concurrent safe reads coalesce into one underlying execution',async()=>{
  let calls=0;
  const target=path.join(project,'shared-read.ts');
  const catalog=[{name:'read_file',description:'read',inputSchema:{type:'object'},annotations:{readOnlyHint:true}}];
  const source='const r=await tools.call("read_file",{path:'+JSON.stringify(target)+'}); return r.text;';
  const outputs=await Promise.all([0,1].map(()=>executeCodeMode(
    args(source,{max_runtime_ms:5000,max_calls:4}),catalog,async()=>{
      calls++;
      await new Promise(r=>setTimeout(r,120));
      return text('shared-value');
    }
  )));
  const runnable=outputs[0]?.structuredContent?.scheduler?.activeLimitAtStart??1;
  assert.ok(outputs.every(x=>x.isError===undefined));
  if(runnable>1){
    assert.equal(calls,1,JSON.stringify(outputs));
    assert.ok(outputs.some(x=>x.structuredContent.steps.some(s=>s.coalesced===true)));
  }else{
    // Live pressure intentionally serialized the workflows, so there was no
    // simultaneous in-flight read to coalesce.
    assert.equal(calls,2);
  }
});

test('per-project fairness caps simultaneous work from one repository',async()=>{
  const status=codeModeScheduler.status();
  const count=Math.min(status.admittedLimit,status.perProjectActiveLimit+2);
  let running=0,maxRunning=0;
  const catalog=[{name:'read_file',description:'read',inputSchema:{type:'object'},annotations:{readOnlyHint:true}}];
  const outputs=await Promise.all(Array.from({length:count},(_,i)=>{
    const target=path.join(project,'fair-'+i+'.ts');
    const source='const r=await tools.call("read_file",{path:'+JSON.stringify(target)+'}); return r.text;';
    return executeCodeMode(args(source,{max_runtime_ms:5000,max_calls:4}),catalog,async()=>{
      running++;maxRunning=Math.max(maxRunning,running);
      await new Promise(r=>setTimeout(r,120));
      running--;return text('ok');
    });
  }));
  assert.ok(outputs.every(x=>x.isError===undefined),JSON.stringify(outputs));
  assert.ok(maxRunning<=status.perProjectActiveLimit,{maxRunning,limit:status.perProjectActiveLimit});
  if(status.activeLimit>status.perProjectActiveLimit&&count>status.perProjectActiveLimit)
    assert.ok(codeModeScheduler.status().fairnessYields>=1);
});

test('parked wait-heavy workflows free a runnable slot and wake afterward',async()=>{
  const before=codeModeScheduler.status();
  const leases=[];
  const prefix='park-test-'+Date.now()+'-';
  for(let i=0;i<before.activeLimit;i++)leases.push(await codeModeScheduler.acquire(prefix+i));
  let startedResolve;
  const started=new Promise(r=>{startedResolve=r;});
  let finishResolve;
  const finish=new Promise(r=>{finishResolve=r;});
  const parked=leases[0].parkWhile('test-wait',async()=>{startedResolve();await finish;return 'done';});
  await started;
  const extra=await codeModeScheduler.acquire(prefix+'extra');
  assert.equal(codeModeScheduler.status().active,before.activeLimit);
  assert.ok(codeModeScheduler.status().parks>=1);
  extra.release();
  finishResolve();
  assert.equal(await parked,'done');
  for(const lease of leases)lease.release();
  const after=codeModeScheduler.status();
  assert.equal(after.active,0);
  assert.equal(after.parked,0);
  assert.equal(after.queued,0);
  assert.ok(after.wakes>=1);
});

test('repeated transient plugin failures open a circuit and stop hammering the dependency',async()=>{
  let calls=0;
  const plugin='circuit-test-'+Date.now();
  const catalog=[{name:'call_plugin_tool',description:'plugin',inputSchema:{type:'object'},annotations:{readOnlyHint:false}}];
  const source='await tools.call("call_plugin_tool",{plugin:'+JSON.stringify(plugin)+',tool:"ping",args:{}}); return "unexpected";';
  const executor=async()=>{
    calls++;
    return {isError:true,content:[{type:'text',text:'plugin unavailable'}],
      structuredContent:{error:{code:'PLUGIN_START_FAILED',message:'connection refused'}}};
  };
  for(let i=0;i<3;i++){
    const out=await executeCodeMode(args(source,{max_runtime_ms:5000,max_calls:4}),catalog,executor);
    assert.equal(out.structuredContent.status,'stopped_on_step_error',JSON.stringify(out));
  }
  const blocked=await executeCodeMode(args(source,{max_runtime_ms:5000,max_calls:4}),catalog,executor);
  assert.equal(calls,3);
  assert.equal(blocked.structuredContent.status,'stopped_on_step_error',JSON.stringify(blocked));
  assert.equal(blocked.structuredContent.errorCode,'CIRCUIT_OPEN');
});

test('tools.parallelSafe runs only verified read-only calls through bounded run_parallel',async()=>{
  const seen=[];
  const catalog=[
    {name:'read_file',description:'read',inputSchema:{type:'object'},annotations:{readOnlyHint:true}},
    {name:'write_file',description:'write',inputSchema:{type:'object'},annotations:{readOnlyHint:false}},
    {name:'run_parallel',description:'parallel',inputSchema:{type:'object'},annotations:{readOnlyHint:false}},
  ];
  const source=[
    'const r=await tools.parallelSafe([',
    ' {tool:"read_file",args:{path:"a"}},',
    ' {tool:"read_file",args:{path:"b"},depends_on:[0]}',
    '],{max_concurrency:3});',
    'return {total:r.structuredContent.total,max:r.structuredContent.maxConcurrency};',
  ].join('\n');
  const out=await executeCodeMode(args(source,{max_calls:8}),catalog,async(name,a)=>{
    seen.push({name,a});
    if(name!=='run_parallel')throw new Error('unexpected native call '+name);
    return {content:[{type:'text',text:'parallel ok'}],
      structuredContent:{pending:false,total:a.calls.length,maxConcurrency:a.max_concurrency}};
  });
  assert.equal(out.isError,undefined,JSON.stringify(out));
  assert.equal(seen.length,1);
  assert.equal(seen[0].name,'run_parallel');
  assert.equal(seen[0].a.calls.length,2);
  assert.ok(seen[0].a.max_concurrency>=1&&seen[0].a.max_concurrency<=3);
  assert.equal(out.structuredContent.result.total,2);
});

test('tools.parallelSafe rejects mutations before executing anything',async()=>{
  let calls=0;
  const catalog=[
    {name:'write_file',description:'write',inputSchema:{type:'object'},annotations:{readOnlyHint:false}},
    {name:'run_parallel',description:'parallel',inputSchema:{type:'object'},annotations:{readOnlyHint:false}},
  ];
  const source='await tools.parallelSafe([{tool:"write_file",args:{path:"x",content:"y"}}]); return "bad";';
  const out=await executeCodeMode(args(source,{max_calls:4}),catalog,async()=>{calls++;return text('wrong');});
  assert.equal(calls,0);
  assert.equal(out.isError,true);
  assert.equal(out.structuredContent.error.code,'CODE_MODE_SCRIPT_FAILED');
  assert.match(out.content[0].text,/PARALLEL_SAFE_TOOL_DENIED/);
});

test('definite heavy Code Mode commands auto-escalate to durable jobs',async()=>{
  const calls=[];
  const catalog=[
    {name:'run_command',description:'command',inputSchema:{type:'object'},annotations:{readOnlyHint:false}},
    {name:'spawn_job',description:'job',inputSchema:{type:'object'},annotations:{readOnlyHint:false}},
  ];
  const source=[
    'const r=await tools.call("run_command",{command:"pnpm build",cwd:'+JSON.stringify(project)+'});',
    'return {pending:r.pending,jobId:r.structuredContent.jobId,auto:r.structuredContent.autoEscalated};',
  ].join('\n');
  const out=await executeCodeMode(args(source,{max_calls:6}),catalog,async(name,a)=>{
    calls.push({name,a});
    if(name!=='spawn_job')throw new Error('heavy command executed directly');
    return {content:[{type:'text',text:'PENDING job'}],
      structuredContent:{pending:true,jobId:'job-heavy-1',status:'running'}};
  });
  assert.equal(calls.length,1);
  assert.equal(calls[0].name,'spawn_job');
  assert.equal(calls[0].a.steps[0].tool,'run_command');
  assert.equal(out.structuredContent.status,'pending_jobs');
  assert.equal(out.structuredContent.result.auto,true);
  assert.equal(out.structuredContent.steps[0].autoEscalated,true);
  assert.equal(out.structuredContent.steps[0].jobId,'job-heavy-1');
});

test('short Code Mode commands remain direct and are not over-escalated',async()=>{
  const calls=[];
  const catalog=[
    {name:'run_command',description:'command',inputSchema:{type:'object'},annotations:{readOnlyHint:false}},
    {name:'spawn_job',description:'job',inputSchema:{type:'object'},annotations:{readOnlyHint:false}},
  ];
  const out=await executeCodeMode(args('const r=await tools.call("run_command",{command:"git status -s"}); return r.text;'),
    catalog,async(name,a)=>{calls.push({name,a});return text('clean');});
  assert.equal(out.isError,undefined,JSON.stringify(out));
  assert.equal(calls.length,1);
  assert.equal(calls[0].name,'run_command');
  assert.equal(out.structuredContent.steps[0].autoEscalated,undefined);
});

test('live system pressure is exposed and scheduler capacity is pressure-aware',async()=>{
  await new Promise(r=>setTimeout(r,25));
  const pressure=systemPressureStatus();
  const scheduler=codeModeScheduler.status();
  assert.ok(['normal','warm','high','critical'].includes(pressure.level));
  assert.ok(pressure.scale>0&&pressure.scale<=1);
  assert.ok(pressure.suggestedSafeParallelism>=1&&pressure.suggestedSafeParallelism<=3);
  assert.equal(scheduler.pressure.level,pressure.level);
  assert.ok(scheduler.activeLimit>=1&&scheduler.activeLimit<=scheduler.hardMaxActive);
  assert.ok(scheduler.cpu.slots<=scheduler.cpu.baseSlots);
});

test('Code Mode exposes a compact model-facing state without locally planning tasks',async()=>{
  const target=path.join(project,'auth.guard.ts');
  if(!await fs.stat(target).catch(()=>null))await fs.writeFile(target,'export const guard = true;\n');
  const catalog=[{name:'read_file',description:'read',inputSchema:{type:'object'},annotations:{readOnlyHint:true}}];
  const source='const r=await tools.call("read_file",{path:'+JSON.stringify(target)+'}); return r.text;';
  const out=await executeCodeMode(args(source,{max_calls:4}),catalog,async()=>text('export const guard = true;'));
  assert.equal(out.isError,undefined,JSON.stringify(out));
  const state=out.structuredContent.modelState;
  assert.equal(state.version,1);
  assert.equal(state.status,'completed');
  assert.equal(state.progress.calls,1);
  assert.equal(state.tools.find(x=>x.tool==='read_file').calls,1);
  assert.ok(state.touchedPaths.some(x=>x.endsWith('auth.guard.ts')));
  assert.ok(state.preflight.some(x=>path.resolve(x.project)===path.resolve(project)));
  assert.equal('plan' in state,false);
  assert.equal('tasks' in state,false);
});

test('preflight self-healing checks readiness but never modifies ordinary project source',async()=>{
  const file=path.join(project,'preflight-source.ts');
  await fs.writeFile(file,'export const untouched = 1;\n');
  const before=await fs.readFile(file,'utf8');
  const status=await runProjectPreflight(project,'read');
  assert.equal(status.project,path.resolve(project));
  assert.equal(status.ok,true);
  assert.ok(Array.isArray(status.healed));
  assert.equal(await fs.readFile(file,'utf8'),before);
  assert.ok(status.diskFreeBytes===undefined||status.diskFreeBytes>=0);
});

test('task-type parallelism keeps builds serialized and caps mixed/mutation workloads',async()=>{
  const reads=adaptiveTaskConcurrency([
    {tool:'read_file',args:{path:'a'}},{tool:'git_status',args:{path:'b'}},{tool:'read_file',args:{path:'c'}},
  ],3);
  assert.ok(reads.effective>=1&&reads.effective<=3);
  const builds=adaptiveTaskConcurrency([
    {tool:'run_command',args:{command:'pnpm build'}},{tool:'run_project_checks',args:{path:'x'}},
  ],3);
  assert.equal(builds.effective,1);
  const mixed=adaptiveTaskConcurrency([
    {tool:'run_command',args:{command:'pnpm build'}},{tool:'read_file',args:{path:'x'}},
  ],3);
  assert.ok(mixed.effective<=2);
  const mutations=adaptiveTaskConcurrency([
    {tool:'write_file',args:{path:'a'}},{tool:'edit_block',args:{file_path:'b'}},{tool:'read_file',args:{path:'c'}},
  ],3);
  assert.ok(mutations.effective<=2);
});

test('round-trip accelerator shares the existing 30 MiB harness quota',async()=>{
  const status=await roundTripAcceleratorStatus();
  assert.equal(status.store.maxBytes,HARNESS_STATE_BYTES);
  assert.ok(status.store.bytes<=HARNESS_STATE_BYTES);
  assert.ok(status.resultHandles.maxRecordBytes<=2*1024*1024);
});

test('harness status exposes bounded storage, traces, retries and semantic quota',async()=>{
  const status=await harnessVNextStatus();
  assert.equal(status.enabled,true);
  assert.equal(status.store.maxBytes,HARNESS_STATE_BYTES);
  assert.equal(status.semantic.store.maxBytes,SEMANTIC_CACHE_BYTES);
  assert.equal(status.retry.mutationsAutomaticallyRetried,false);
  assert.ok(status.lifecycleHooks.includes('beforeToolCall'));
  assert.ok(status.specialistAgents.some(x=>x.role==='reviewer'));
  assert.ok(status.store.bytes<=HARNESS_STATE_BYTES);
});


test('Code Mode rejects a mutation when a previously read file changed externally',async()=>{
  const target=path.join(project,'stale-observation.ts');
  await fs.writeFile(target,'export const stale = 1;\n');
  let editExecutions=0;
  const catalog=[
    {name:'read_file',description:'read',inputSchema:{type:'object'},annotations:{readOnlyHint:true}},
    {name:'git_status',description:'status',inputSchema:{type:'object'},annotations:{readOnlyHint:true}},
    {name:'edit_block',description:'edit',inputSchema:{type:'object'},annotations:{readOnlyHint:false}},
  ];
  const source=[
    'await tools.call("read_file",{path:'+JSON.stringify(target)+'});',
    'await tools.call("git_status",{path:'+JSON.stringify(project)+'});',
    'await tools.call("edit_block",{file_path:'+JSON.stringify(target)+',old_string:"export const stale = 1;",new_string:"export const stale = 3;",expected_replacements:1});',
    'return "should not reach";',
  ].join('\n');
  const out=await executeCodeMode(args(source,{max_calls:8}),catalog,async(name)=>{
    if(name==='read_file')return text('export const stale = 1;');
    if(name==='git_status'){
      await fs.writeFile(target,'export const stale = 2;\n');
      return text('external modification simulated');
    }
    if(name==='edit_block'){editExecutions++;return text('unexpected edit');}
    throw new Error('unexpected tool '+name);
  });
  assert.equal(editExecutions,0);
  assert.equal(out.isError,undefined,JSON.stringify(out));
  assert.equal(out.structuredContent.status,'stopped_on_step_error');
  assert.equal(out.structuredContent.errorCode,'STALE_SOURCE_OBSERVATION');
  assert.equal(await fs.readFile(target,'utf8'),'export const stale = 2;\n');
  assert.ok(out.structuredContent.modelState.sourceObservations.staleRejected>=1);
});

test('next Code Mode workflow receives a compact crash recovery capsule for its project',async()=>{
  const target=path.join(project,'recovery-target.ts');
  await fs.writeFile(target,'export const recovery = true;\n');
  const oldId='recovery-test-'+Date.now();
  await checkpointRecoveryCapsule({
    id:oldId,workflowId:'old-workflow',executionId:'old-execution',status:'uncertain',
    projects:[project],touchedPaths:[target],pendingJobs:['jabcdefg'],
    lastSafeStep:{index:2,tool:'read_file',outcome:'ok'},
    uncertainMutation:{index:3,tool:'edit_block',code:'TOOL_OUTCOME_UNCERTAIN'},
    rollbackSnapshotIds:['a'.repeat(64)],
    verification:[{project,state:'stale'}],
  });
  const catalog=[{name:'read_file',description:'read',inputSchema:{type:'object'},annotations:{readOnlyHint:true}}];
  const source='const r=await tools.call("read_file",{path:'+JSON.stringify(target)+'}); return r.recoveryCapsule;';
  const out=await executeCodeMode(args(source,{max_calls:4}),catalog,async()=>text('export const recovery = true;'));
  assert.equal(out.isError,undefined,JSON.stringify(out));
  assert.equal(out.structuredContent.result.status,'uncertain');
  assert.equal(out.structuredContent.result.uncertainMutation.tool,'edit_block');
  assert.deepEqual(out.structuredContent.result.pendingJobs,['jabcdefg']);
  assert.match(out.structuredContent.result.guidance,/Inspect current effects/);
  assert.equal(out.structuredContent.modelState.recovery.status,'uncertain');
});

test('verification evidence becomes stale after an external change to a verified file',async()=>{
  const target=path.join(project,'verification-evidence.ts');
  await fs.writeFile(target,'export const verified = 1;\n');
  await recordProjectMutation(project,'write_file',{path:target});
  const verified=await recordProjectVerification(project,'verify_project',true);
  assert.equal(verified.state,'current');
  assert.ok(verified.evidenceId);
  await fs.writeFile(target,'export const verified = 2;\n');
  const stale=await getVerificationEvidence(project);
  assert.equal(stale.state,'stale');
  assert.ok(stale.changedSinceVerification.some(x=>path.resolve(x)===path.resolve(target)));
});

test('adaptive result budgeting keeps large nested and final results behind local handles',async()=>{
  const target=path.join(project,'adaptive-result.ts');
  await fs.writeFile(target,'export const small = true;\n');
  const catalog=[{name:'read_file',description:'read',inputSchema:{type:'object'},annotations:{readOnlyHint:true}}];
  const nestedSource=[
    'const r=await tools.call("read_file",{path:'+JSON.stringify(target)+'});',
    'return {hasHandle:!!r.resultHandle,textLength:r.text.length,adaptive:r.resultBudget&&r.resultBudget.adaptive};',
  ].join('\n');
  const nested=await executeCodeMode(args(nestedSource,{max_calls:4}),catalog,async()=>text('line\n'.repeat(18000)));
  assert.equal(nested.isError,undefined,JSON.stringify(nested));
  assert.equal(nested.structuredContent.result.hasHandle,true);
  assert.equal(nested.structuredContent.result.adaptive,true);
  assert.ok(nested.structuredContent.result.textLength<40000,nested.structuredContent.result.textLength);

  const final=await executeCodeMode(args('return "Z".repeat(30000);',{max_calls:2}),[],async()=>text('unused'));
  assert.equal(final.isError,undefined,JSON.stringify(final));
  assert.ok(final.structuredContent.resultHandle);
  assert.equal(final.structuredContent.result,undefined);
  assert.equal(final.structuredContent.outputTruncated,true);
  assert.ok(final.content[0].text.length<22000,final.content[0].text.length);
});

test('dispatcher mutation receipt chain links before/after hashes to later verification evidence',async()=>{
  const target=path.join(project,'mutation-chain.ts');
  await fs.writeFile(target,'export const chain = 1;\n');
  const edit=await withCallContext({id:'mutation-chain-call-edit',client:null,remote:false},()=>
    callTool('edit_block',{
      file_path:target,
      old_string:'export const chain = 1;',
      new_string:'export const chain = 2;',
      expected_replacements:1,
    }));
  assert.equal(edit.isError,undefined,JSON.stringify(edit));
  assert.ok(edit._meta?.mutationReceipt);
  assert.ok(edit._meta?.mutationChain?.id);

  const verify=await withCallContext({id:'mutation-chain-call-verify',client:null,remote:false},()=>
    callTool('verify_project',{
      cwd:project,
      commands:[{name:'node-ok',program:'node',args:['-e','process.exit(0)']}],
      timeout_ms:10000,
      stopOnFailure:true,
    }));
  assert.notEqual(verify.isError,true,JSON.stringify(verify));
  assert.equal(verify.structuredContent.passed,true);

  const chain=await mutationChainStatus(project);
  const row=chain.recent.find(x=>x.tool==='edit_block'&&x.targets.some(t=>t.label.endsWith('mutation-chain.ts')));
  assert.ok(row,JSON.stringify(chain));
  assert.ok(row.targets[0].before);
  assert.ok(row.targets[0].after);
  assert.notEqual(row.targets[0].before,row.targets[0].after);
  assert.ok(row.verificationEvidenceId,JSON.stringify(row));
});


test('semantic project discovery is compact, pageable, and exposes Code Mode continuation',async()=>{
  const file=path.join(project,'semantic-paging.ts');
  const lines=Array.from({length:180},(_,i)=>`export const semanticPageNeedle${i} = "semantic page needle value ${i}";`).join('\n')+'\n';
  await fs.writeFile(file,lines);
  const first=await semanticSearch({
    path:project,query:'semantic page needle',limit:1,offset:0,preview_chars:100,refresh:true,
  });
  assert.equal(first.results.length,1);
  assert.equal(first.page.offset,0);
  assert.equal(first.page.limit,1);
  assert.equal(first.page.hasMore,true,JSON.stringify(first.page));
  assert.ok(first.page.totalMatches>1,JSON.stringify(first.page));
  assert.ok(first.results[0].preview.length<=102,first.results[0].preview.length);
  assert.equal(first.continuation.stayInCodeMode,true);
  assert.equal(first.continuation.directFallbackRecommended,false);
  const second=await semanticSearch(first.continuation.next);
  assert.equal(second.page.offset,first.page.nextOffset);
  assert.notEqual(second.results[0].lineStart,first.results[0].lineStart);
});

test('large nested result exposes unresolved stay-in-Code-Mode continuation instead of suggesting direct fallback',async()=>{
  const target=path.join(project,'large-continuation.ts');
  await fs.writeFile(target,'export const continuation = true;\n');
  const catalog=[{name:'read_file',description:'read',inputSchema:{type:'object'},annotations:{readOnlyHint:true}}];
  const source=[
    'const r=await tools.call("read_file",{path:'+JSON.stringify(target)+'});',
    'return {done:true,handle:r.resultHandle&&r.resultHandle.id,localContinuation:r.continuation&&r.continuation.stayInCodeMode};',
  ].join('\n');
  const out=await executeCodeMode(args(source,{max_calls:4}),catalog,async()=>text('needle\n'+'x'.repeat(50000)));
  assert.equal(out.isError,undefined,JSON.stringify(out));
  assert.equal(out.structuredContent.result.done,true);
  assert.equal(out.structuredContent.result.localContinuation,true);
  assert.ok(out.structuredContent.result.handle);
  assert.equal(out.structuredContent.continuation.required,true);
  assert.equal(out.structuredContent.continuation.stayInCodeMode,true);
  assert.equal(out.structuredContent.continuation.directFallbackRecommended,false);
  assert.equal(out.structuredContent.continuation.resultHandles.length,1);
  assert.match(out.structuredContent.continuation.nextActions.join(' '),/do not switch to direct/i);
});

test('Code Mode can resolve a large result locally with tools.results.search and clear unresolved continuation',async()=>{
  const target=path.join(project,'large-local-search.ts');
  await fs.writeFile(target,'export const localSearch = true;\n');
  const catalog=[{name:'read_file',description:'read',inputSchema:{type:'object'},annotations:{readOnlyHint:true}}];
  const source=[
    'const r=await tools.call("read_file",{path:'+JSON.stringify(target)+'});',
    'const hits=await tools.results.search(r.resultHandle.id,"UNIQUE_LOCAL_NEEDLE",{length:2000});',
    'return {hits:hits.hits.length,first:hits.hits[0]&&hits.hits[0].text.includes("UNIQUE_LOCAL_NEEDLE")};',
  ].join('\n');
  const out=await executeCodeMode(args(source,{max_calls:4}),catalog,async()=>text('a'.repeat(32000)+'\nUNIQUE_LOCAL_NEEDLE\n'+'b'.repeat(16000)));
  assert.equal(out.isError,undefined,JSON.stringify(out));
  assert.ok(out.structuredContent.result.hits>=1,JSON.stringify(out.structuredContent.result));
  assert.equal(out.structuredContent.result.first,true);
  assert.ok(!out.structuredContent.continuation||out.structuredContent.continuation.required===false,
    JSON.stringify(out.structuredContent.continuation));
});

test('Code Mode semantic paging can finish discovery locally without direct fallback',async()=>{
  const file=path.join(project,'semantic-code-mode.ts');
  const lines=Array.from({length:150},(_,i)=>`export function semanticCodeModeNeedle${i}(){ return "semantic code mode needle ${i}"; }`).join('\n')+'\n';
  await fs.writeFile(file,lines);
  const source=[
    'const first=await tools.semantic.search({path:'+JSON.stringify(project)+',query:"semantic code mode needle",limit:1,preview_chars:90,refresh:true});',
    'const second=await tools.semantic.search(first.continuation.next);',
    'return {first:first.results[0].lineStart,second:second.results[0].lineStart,pageOffset:second.page.offset,stay:first.continuation.stayInCodeMode};',
  ].join('\n');
  const out=await executeCodeMode(args(source,{max_calls:6}),[],async()=>{throw new Error('no native direct fallback expected');});
  assert.equal(out.isError,undefined,JSON.stringify(out));
  assert.equal(out.structuredContent.result.stay,true);
  assert.notEqual(out.structuredContent.result.first,out.structuredContent.result.second);
  assert.ok(out.structuredContent.result.pageOffset>0);
});
