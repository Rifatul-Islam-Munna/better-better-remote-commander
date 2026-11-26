// Real HTTP MCP -> relay -> WebSocket -> QuickJS. Synthetic reads use barriers
// to prove overlap, rather than relying on exit codes or timing speedups.
import {test,mock} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';

test('Code Mode discovery, parallel workflows, read DAGs and cancellation traverse MCP',{timeout:30000},async t=>{
 const scratch=await fs.mkdtemp(path.join(os.tmpdir(),'brc-code-parallel-'));
 const envKeys=['BRC_HOME','BRC_TOOL_HISTORY','BRC_ERROR_LOG','BRC_PLUGINS','BRC_MCP_TOOL_PROFILE'];
 const envBefore=Object.fromEntries(envKeys.map(k=>[k,process.env[k]]));
 Object.assign(process.env,{BRC_HOME:path.join(scratch,'home'),BRC_TOOL_HISTORY:'0',BRC_ERROR_LOG:'0',BRC_PLUGINS:'none',BRC_MCP_TOOL_PROFILE:'codemode'});
 // Deterministic roomy hardware; production pressure/admission code is unchanged.
 const hardware={availableParallelism:os.availableParallelism,totalmem:os.totalmem,freemem:os.freemem};
 os.availableParallelism=()=>24;os.totalmem=()=>64*1024**3;os.freemem=()=>48*1024**3;
 mock.timers.enable({apis:['setInterval']});
 let relay,client,configManager,searchManager;
 let release=()=>{};
 const outstanding=[];
 try{
  const {buildRelay}=await import('../../relay/dist/app.js');
  const {WsClient}=await import('../dist/transport/ws-client.js');
  const {executeCodeMode,CodeModeArgsSchema}=await import('../dist/runtime/code-mode.js');
  const {getCallContext}=await import('../dist/context.js');
  const {operationStatus,cancelOperation}=await import('../dist/runtime/operations.js');
  const {codeModeScheduler}=await import('../dist/runtime/code-mode-scheduler.js');
  const {buildCodeModeToolDefinitions}=await import('../dist/tools/code-mode-tools.js');
  const {handleRunParallel,setJobToolExecutor}=await import('../dist/tools/jobs.js');
  const {handleStartSearch}=await import('../dist/core/handlers/search-handlers.js');
  ({searchManager}=await import('../dist/core/search-manager.js'));
  ({configManager}=await import('../dist/core/config-manager.js'));
  const project=path.join(scratch,'project');
  await fs.mkdir(project);
  await fs.writeFile(path.join(project,'package.json'),'{}');
  await fs.writeFile(path.join(project,'alpha.ts'),'export const alpha = "alphaNeedle";\n');
  await fs.writeFile(path.join(project,'beta.ts'),'export const beta = "betaNeedle";\n');
  await configManager.setValue('allowedDirectories',[project]);
  const catalog=[...buildCodeModeToolDefinitions(),
   {name:'read_file',description:'Synthetic barrier read',inputSchema:{type:'object'},annotations:{readOnlyHint:true}},
   {name:'start_search',description:'Real blocking search behind a dispatch barrier',inputSchema:{type:'object'},annotations:{readOnlyHint:true}},
   {name:'run_parallel',description:'Bounded parallel DAG',inputSchema:{type:'object'}},
   {name:'agent_status',description:'Live test status',inputSchema:{type:'object'},annotations:{readOnlyHint:true}},
   {name:'cancel_operation',description:'Cancel test operation',inputSchema:{type:'object'}}];
  let active=0,peak=0,target=4,arrived=0,finished=new Set();
  const searches=[];
  let barrier;
  function reset(count){target=count;arrived=0;peak=0;finished=new Set();barrier=new Promise(r=>{release=r;});}
  reset(4);
  const executor=async(name,a)=>{
   if(name==='run_parallel')return handleRunParallel(a);
   assert.ok(name==='read_file'||name==='start_search');
   const signal=getCallContext()?.signal;
   if(a.depends)assert.ok(finished.has(a.depends),'dependent read started before its predecessor settled');
   active++;peak=Math.max(peak,active);arrived++;
   if(arrived===target)release();
   try{
    await new Promise((resolve,reject)=>{
     const abort=()=>{signal?.removeEventListener('abort',abort);reject(signal.reason);};
     signal?.addEventListener('abort',abort,{once:true});
     if(signal?.aborted)abort();
     barrier.then(()=>{signal?.removeEventListener('abort',abort);resolve();});
    });
    if(name==='start_search'){
     assert.equal(a.wait,true,'parallelSafe must not launch a streaming search');
     const result=await handleStartSearch(a);
     searches.push({args:a,result});
     return result;
    }
    finished.add(a.token);
    return {content:[{type:'text',text:'read '+a.token}]};
   }finally{active--;}
  };
  setJobToolExecutor(executor);
  relay=await buildRelay({port:0,host:'127.0.0.1',publicUrl:'http://127.0.0.1:3210',trustProxy:false,
   dbFile:path.join(scratch,'relay.sqlite'),sessionSecret:'parallel-test-only-stable-secret-at-least-32-characters',adminUser:'admin',adminPassword:'test-only',relayVersion:'test'},()=>{});
  await new Promise(r=>relay.http.listen(0,'127.0.0.1',r));
  const url=`http://127.0.0.1:${relay.http.address().port}`,user=relay.store.getUserByName('admin');
  const token=relay.store.issueDeviceToken('parallel-pc',user.id,'Parallel fixture','win32');
  const key=relay.store.createApiKey(user.id,'parallel fixture');
  client=new WsClient({relayUrl:url,token,deviceId:'parallel-pc',name:'Parallel fixture',agentVersion:'test',coreVersion:'test',tools:catalog,log:()=>{},
   callTool:async(name,a)=>{
    if(name==='agent_status')return {content:[],structuredContent:{operations:operationStatus(),scheduler:codeModeScheduler.status()}};
    if(name==='cancel_operation')return {content:[],structuredContent:{cancelRequested:cancelOperation(a.callId)}};
    assert.equal(name,'code_mode');
    return executeCodeMode(CodeModeArgsSchema.parse(a),catalog,executor,getCallContext()?.signal);
   }});
  client.start();
  let seq=0;
  async function rpc(method,params){
   const response=await fetch(url+'/mcp',{method:'POST',headers:{authorization:'Bearer '+key.raw,'content-type':'application/json',accept:'application/json, text/event-stream'},body:JSON.stringify({jsonrpc:'2.0',id:++seq,method,params}),signal:AbortSignal.timeout(12000)});
   const body=await response.text();assert.equal(response.status,200,body);
   const messages=body.trim().startsWith('{')?[JSON.parse(body)]:body.split('\n').filter(l=>l.startsWith('data: ')).map(l=>JSON.parse(l.slice(6)));
   const answer=messages.find(x=>x.result!==undefined||x.error!==undefined);assert.ok(answer,body);assert.equal(answer.error,undefined,JSON.stringify(answer));return answer.result;
  }
  const tool=(name,a={})=>{const p=rpc('tools/call',{name,arguments:a});outstanding.push(p);return p;};
  const until=async(fn)=>{const end=Date.now()+6000;while(Date.now()<end){if(await fn())return;await new Promise(r=>setTimeout(r,20));}throw new Error('Barrier never reached; expected real concurrent execution');};
  await until(()=>client.status().online);
  const initialized=await rpc('initialize',{protocolVersion:'2024-11-05',capabilities:{},clientInfo:{name:'parallel-test',version:'1'}});
  assert.match(initialized.instructions,/tools.parallelSafe/);
  await t.test('public descriptions and executable help expose parallelism/features',async()=>{
   const listing=await rpc('tools/list',{}),description=listing.tools.find(x=>x.name==='code_mode').description;
   assert.match(description,/tools.help/);assert.match(description,/tools.parallelSafe/);assert.match(description,/12 admitted \/ 8 runnable/);
   const h=await tool('code_mode',{source:'const a=await tools.help("features"); const b=await tools.call("code_mode_help",{topic:"parallelism"}); return {features:a.features.map(x=>x.api),limits:b.limits};'});
   assert.equal(h.isError,false,JSON.stringify(h));
   assert.ok(h.structuredContent.result.features.some(x=>x.includes('tools.fibers')));
   assert.equal(h.structuredContent.result.limits.maxAdmitted,12);
   const help=await tool('code_mode',{source:'return await tools.help("parallelism");'});
   assert.match(help.structuredContent.result.default,/Prefer parallel Code Mode/);
   assert.match(help.structuredContent.result.searches,/start_search/);
   assert.match(help.structuredContent.result.ordering,/omitted wait defaults to true/);
  });
  await t.test('four independent Code Mode workers overlap beyond the old transport cap',async()=>{
   const calls=Array.from({length:4},(_,i)=>tool('code_mode',{source:`const r=await tools.call("read_file",{token:${i}});return r.text;`}));
   await until(()=>arrived===4);
   const results=await Promise.all(calls);
   assert.ok(results.every(r=>!r.isError),JSON.stringify(results));
   assert.equal(peak,4,'four underlying operations actually entered the barrier together');
   assert.ok(codeModeScheduler.status().maxObservedActive>=4);
   assert.equal(new Set(results.map(r=>r.structuredContent.workflowId)).size,4);
  });
  await t.test('parallelSafe executes concurrent reads and honors DAG dependencies',async()=>{
   reset(2);
   const result=await tool('code_mode',{source:'const r=await tools.parallelSafe([{tool:"read_file",args:{token:"a"}},{tool:"read_file",args:{token:"b"}},{tool:"read_file",args:{token:"c",depends:"a"},depends_on:[0,1]}]); return r.structuredContent;'});
   assert.equal(result.isError,false,JSON.stringify(result));
   assert.equal(result.structuredContent.allCallsSucceeded,true,JSON.stringify(result));
   assert.equal(peak,2);assert.equal(arrived,3);assert.ok(finished.has('c'));
   assert.equal(result.structuredContent.result.pending,false);
  });
  await t.test('parallelSafe overlaps real blocking searches through MCP with isolated results',async()=>{
   reset(2);
   const calls=[
    {tool:'start_search',args:{path:project,pattern:'alphaNeedle',searchType:'content',maxResults:5,includeSource:false}},
    {tool:'start_search',args:{path:project,pattern:'betaNeedle',searchType:'content',maxResults:5,includeSource:false,wait:true}},
   ];
   const result=await tool('code_mode',{source:'const r=await tools.parallelSafe('+JSON.stringify(calls)+');return r.structuredContent;'});
   assert.equal(result.isError,false,JSON.stringify(result));
   assert.equal(result.structuredContent.allCallsSucceeded,true,JSON.stringify(result));
   assert.equal(result.structuredContent.result.total,2);
   assert.equal(peak,2,'both search dispatches must enter the barrier together');
   assert.equal(searches.length,2);
   assert.equal(new Set(searches.map(s=>s.result.structuredContent.sessionId)).size,2);
   for(const {args,result} of searches){
    assert.notEqual(result.isError,true,JSON.stringify(result));
    assert.equal(result.structuredContent.totalMatches,1);
    const text=result.content.map(c=>c.text??'').join('\n');
    assert.match(text,new RegExp(args.pattern));
    assert.doesNotMatch(text,new RegExp(args.pattern==='alphaNeedle'?'betaNeedle':'alphaNeedle'));
   }
   await until(()=>searchManager.getActiveSessionCount()===0);
  });
  await t.test('the separate Code Mode lane remains visible and cancellable',async()=>{
   reset(99);
   const pending=tool('code_mode',{source:'await tools.call("read_file",{token:"cancel"});return 1;'});
   await until(()=>arrived===1);
   const status=await tool('agent_status');
   const op=status.structuredContent.operations.find(x=>x.tool==='code_mode');assert.ok(op);
   const cancelled=await tool('cancel_operation',{callId:op.id});assert.equal(cancelled.structuredContent.cancelRequested,true);
   const result=await pending;assert.equal(result.isError,true);
   await until(()=>active===0);
  });
 }finally{
  release();client?.stop();await Promise.allSettled(outstanding);
  if(relay){relay.http.closeAllConnections();await relay.close();}
  for(const session of searchManager?.listSearchSessions()??[])searchManager.terminateSearch(session.id);
  await configManager?.close();mock.timers.reset();Object.assign(os,hardware);
  for(const key of envKeys)if(envBefore[key]===undefined)delete process.env[key];else process.env[key]=envBefore[key];
  await fs.rm(scratch,{recursive:true,force:true,maxRetries:6,retryDelay:80});
 }
});
