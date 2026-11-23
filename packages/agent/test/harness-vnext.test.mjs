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
