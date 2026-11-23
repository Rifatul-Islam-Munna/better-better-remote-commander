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

test('durable execution replays a settled step without executing it again',async()=>{
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
  assert.deepEqual(replay.replay,{ok:true,value:{text:'cached'}});
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

test('saved workflow can auto-match and execute locally without another ChatGPT round trip',async()=>{
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
