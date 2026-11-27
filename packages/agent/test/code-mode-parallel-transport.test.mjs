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
  const {activity}=await import('../dist/runtime/activity.js');
  const {buildCodeModeToolDefinitions}=await import('../dist/tools/code-mode-tools.js');
  const {handleRunParallel,handleGetJob,handleSpawnJob,setJobToolExecutor}=await import('../dist/tools/jobs.js');
  const {rgPath}=await import('@vscode/ripgrep');
  const {handleStartSearch}=await import('../dist/core/handlers/search-handlers.js');
  const {handleReadFile}=await import('../dist/core/handlers/filesystem-handlers.js');
  const {EXPERT_TOOL_HANDLERS}=await import('../dist/tools/expert-tools.js');
  const {CODING_TOOL_HANDLERS}=await import('../dist/tools/coding-tools.js');
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
   {name:'read_code_file',description:'Real source read behind a dispatch barrier',inputSchema:{type:'object'},annotations:{readOnlyHint:true}},
   {name:'start_search',description:'Real blocking search behind a dispatch barrier',inputSchema:{type:'object'},annotations:{readOnlyHint:true}},
   {name:'project_map',description:'Real bounded project map behind a dispatch barrier',inputSchema:{type:'object'},annotations:{readOnlyHint:true}},
   {name:'code_graph',description:'Synthetic CPU-heavy operation for scheduler admission',inputSchema:{type:'object'},annotations:{readOnlyHint:true}},
   {name:'run_parallel',description:'Bounded parallel DAG',inputSchema:{type:'object'}},
   {name:'get_job',description:'Real job status/output inspection',inputSchema:{type:'object'},annotations:{readOnlyHint:true}},
   {name:'run_command',description:'Explicit native command using normal job policies',inputSchema:{type:'object'},annotations:{readOnlyHint:false}},
   {name:'agent_status',description:'Live test status',inputSchema:{type:'object'},annotations:{readOnlyHint:true}},
   {name:'cancel_operation',description:'Cancel test operation',inputSchema:{type:'object'}}];
  let active=0,peak=0,target=4,arrived=0,finished=new Set(),heavyActive=0,heavyPeak=0;
  const searches=[];
  let barrier;
  function reset(count){target=count;arrived=0;peak=0;heavyPeak=0;finished=new Set();barrier=new Promise(r=>{release=r;});}
  reset(4);
  const executor=async(name,a)=>{
   if(name==='run_parallel')return handleRunParallel(a);
   if(name==='get_job')return handleGetJob(a);
   assert.ok(name==='read_file'||name==='start_search'||name==='read_code_file'||name==='project_map'||name==='code_graph');
   const signal=getCallContext()?.signal;
   if(a.depends)assert.ok(finished.has(a.depends),'dependent read started before its predecessor settled');
   active++;peak=Math.max(peak,active);arrived++;
   if(name==='code_graph'){heavyActive++;heavyPeak=Math.max(heavyPeak,heavyActive);}
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
    if(name==='read_code_file')return CODING_TOOL_HANDLERS.read_code_file(a);
    if(name==='project_map')return EXPERT_TOOL_HANDLERS.project_map(a);
    if(name==='read_file'&&a.path)return handleReadFile(a);
    if(a.fail)return {isError:true,content:[{type:'text',text:'READ_FAILED'}],structuredContent:{error:{code:'READ_FAILED'}}};
    finished.add(a.token);
    return {content:[{type:'text',text:'read '+a.token}]};
   }finally{active--;if(name==='code_graph')heavyActive--;}
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
   assert.match(listing.tools.find(x=>x.name==='code_mode').inputSchema.properties.source.description,/PARALLEL FIRST/);
   const h=await tool('code_mode',{source:'const a=await tools.help("features"); const b=await tools.call("code_mode_help",{topic:"parallelism"}); return {features:a.features.map(x=>x.api),limits:b.limits};'});
   assert.equal(h.isError,false,JSON.stringify(h));
   assert.ok(h.structuredContent.result.features.some(x=>x.includes('tools.fibers')));
   assert.equal(h.structuredContent.result.limits.maxAdmitted,12);
   const help=await tool('code_mode',{source:'return await tools.help("parallelism");'});
   assert.match(help.structuredContent.result.default,/Prefer parallel Code Mode/);
   assert.match(help.structuredContent.result.searches,/start_search/);
   assert.match(help.structuredContent.result.ordering,/omitted wait defaults to true/);
  });
  await t.test('logged search-object and text helper scripts succeed through MCP',async()=>{
   const result=await tool('code_mode',{source:'const h=await tools.help();const s=await tools.search({query:"project context and parallel safe read tools"});text({topics:h.topics,names:s.map(x=>x.name)});'});
   assert.equal(result.isError,false,JSON.stringify(result));
   assert.ok(result.structuredContent.result.topics.includes('parallelism'));
   assert.ok(result.structuredContent.result.names.includes('parallel_safe'));
   const help=await tool('code_mode',{source:'const h=await tools.help();text(h);'});
   assert.equal(help.isError,false,JSON.stringify(help));
   assert.ok(help.structuredContent.result.topics.includes('features'));
  });
  await t.test('parallelSafe inspects real active/finished jobs without replay or false completion',async()=>{
   try{
   reset(99);
   const started=await handleSpawnJob({steps:[{tool:'read_file',args:{token:'inspected-job'}}]});
   const id=started.structuredContent.jobId;
   await until(()=>arrived===1);
   const pending=await tool('code_mode',{source:'const r=await tools.parallelSafe([{tool:"get_job",args:{id:'+JSON.stringify(id)+'}}]);return r.results[0];'});
   assert.equal(pending.isError,false,JSON.stringify(pending));
   assert.equal(pending.structuredContent.pending,true);
   assert.equal(pending.structuredContent.executionCompleted,false);
   assert.deepEqual(pending.structuredContent.pendingJobs,[id]);
   assert.equal(pending.structuredContent.result.pending,true);
   assert.equal(pending.structuredContent.result.jobId,id);
   assert.equal(pending.structuredContent.result.next.args.id,id);
   release();
   await until(async()=> (await handleGetJob({id})).structuredContent.status==='succeeded');
   reset(1);
   const finished=await tool('code_mode',{source:'const r=await tools.parallelSafe([{tool:"read_file",args:{token:"mixed-read"}},{tool:"get_job",args:{id:'+JSON.stringify(id)+',output_chars:12000}}]);return r.results;'});
   assert.equal(finished.isError,false,JSON.stringify(finished));
   assert.equal(finished.structuredContent.allCallsSucceeded,true);
   assert.equal(finished.structuredContent.executionCompleted,true);
   assert.equal(finished.structuredContent.result[1].structuredContent.status,'succeeded');
   assert.equal(finished.structuredContent.result[1].jobId,id);
   assert.match(finished.structuredContent.result[0].text,/mixed-read/);
   assert.equal(arrived,1,'inspection must not re-execute the original job');
   }finally{
    release();await until(()=>active===0);reset(4);
   }
  });
  await t.test('four independent Code Mode workers overlap beyond the old transport cap',async()=>{
   const calls=Array.from({length:4},(_,i)=>tool('code_mode',{source:`const r=await tools.call("read_file",{token:${i}});return r.text;`}));
   await until(()=>arrived===4);
   const results=await Promise.all(calls);
   assert.ok(results.every(r=>!r.isError),JSON.stringify(results));
   assert.equal(peak,4,'four underlying operations actually entered the barrier together');
   assert.ok(codeModeScheduler.status().maxObservedActive>=4);
   assert.ok(activity.recent().some(e=>e.level==='parallel'&&/CODE MODE.*active 4\//.test(e.message)),
    'actual concurrent Code Mode workers must be visible in live counters');
   assert.equal(new Set(results.map(r=>r.structuredContent.workflowId)).size,4);
  });
  await t.test('editing help, aliases, unknown topics and tool-name help do not reject ordinary discovery',async()=>{
   const result=await tool('code_mode',{source:'const a=await tools.help("editing");const b=await tools.help("parallel");const c=await tools.help("future-topic");const d=await tools.help("run_command");return {editTools:a.tools,commands:b.commands,fallback:c.topicFound,topics:c.topics,tool:d.name};'});
   assert.equal(result.isError,false,JSON.stringify(result));
   const value=result.structuredContent.result;
   assert.ok(value.editTools.includes('edit_files'));assert.match(value.commands,/run_command/);
   assert.equal(value.fallback,false);assert.ok(value.topics.includes('editing'));assert.equal(value.tool,'run_command');
  });
  await t.test('logged parallel run_command pattern accepts native rg and treats no matches as a completed search',async()=>{
   const calls=['alphaNeedle','betaNeedle','missing_parallel_fixture_123'].map(pattern=>({tool:'run_command',args:{program:rgPath,args:['-n',pattern,'.'],cwd:project,maxOutputChars:22000}}));
   const result=await tool('code_mode',{source:'const r=await tools.parallelSafe('+JSON.stringify(calls)+',{max_concurrency:3});return {routing:r.parallelRouting,calls:r.commandCalls,results:r.map(v=>({isError:v.isError,exit:v.structuredContent.exitCode,noMatches:v.structuredContent.noMatches,text:v.text}))};'});
   assert.equal(result.isError,false,JSON.stringify(result));
   assert.equal(result.structuredContent.allCallsSucceeded,true,JSON.stringify(result));
   const value=result.structuredContent.result;
   assert.equal(value.routing,'run_parallel');assert.equal(value.calls,3);
   assert.deepEqual(value.results.map(v=>v.exit),[0,0,1]);
   assert.ok(value.results.every(v=>!v.isError));assert.equal(value.results[2].noMatches,true);
   assert.match(value.results[2].text,/No matches/);
  });
  await t.test('parallelSafe executes concurrent reads and honors DAG dependencies',async()=>{
   reset(2);
   const result=await tool('code_mode',{source:'const r=await tools.parallelSafe([{tool:"read_file",args:{token:"a"}},{tool:"read_file",args:{token:"b"}},{tool:"read_file",args:{token:"c",depends:"a"},depends_on:[0,1]}]); return r.structuredContent;'});
   assert.equal(result.isError,false,JSON.stringify(result));
   assert.equal(result.structuredContent.allCallsSucceeded,true,JSON.stringify(result));
   assert.equal(peak,2);assert.equal(arrived,3);assert.ok(finished.has('c'));
   assert.equal(result.structuredContent.result.pending,false);
  });
  await t.test('default parallelSafe fanout executes six native file reads concurrently through MCP',async()=>{
   reset(6);
   const files=Array.from({length:6},(_,i)=>path.join(project,'six-'+i+'.txt'));
   await Promise.all(files.map((file,i)=>fs.writeFile(file,'six-way-fixture-'+i)));
   const source='const paths='+JSON.stringify(files)+';const r=await tools.parallelSafe(paths.map(path=>({tool:"read_file",args:{path}})));return {limit:r.structuredContent.effectiveConcurrency,texts:r.map(v=>v.text)};';
   const result=await tool('code_mode',{source});
   assert.equal(result.isError,false,JSON.stringify(result));assert.equal(result.structuredContent.allCallsSucceeded,true,JSON.stringify(result));
   assert.equal(result.structuredContent.result.limit,6);assert.equal(peak,6);
   for(const [i,value]of result.structuredContent.result.texts.entries())assert.match(value,new RegExp('six-way-fixture-'+i));
   assert.ok(activity.recent().some(e=>e.level==='parallel'&&/PARALLEL JOBS.*active 6\/6/.test(e.message)));
  });
  await t.test('three-call batches report six-slot capacity even when the guest returns only mapped text',async()=>{
   reset(3);
   const calls=Array.from({length:3},(_,i)=>({tool:'read_file',args:{token:'explicit-three-'+i}}));
   const result=await tool('code_mode',{source:'const r=await tools.parallelSafe('+JSON.stringify(calls)+',{max_concurrency:3});return r.results.map(v=>v.text);'});
   assert.equal(result.isError,false,JSON.stringify(result));assert.equal(peak,3,'explicit caller limit must be preserved');
   assert.deepEqual({...result.structuredContent.parallelism,guidance:undefined},
    {capacity:6,submitted:3,requested:3,batchLimit:3,pressure:'normal',batches:1,guidance:undefined});
   assert.match(result.content[0].text,/PARALLEL PLAN \| capacity 6 \| submitted 3 \| requested 3 \| batch limit 3/);
   assert.match(result.structuredContent.parallelism.guidance,/more independent work/);
   assert.ok(activity.recent().some(e=>e.level==='parallel'&&/capacity 6.*active 3\/3/.test(e.message)));
  });
  await t.test('six worker slots still serialize CPU-heavy work beside five light reads',async()=>{
   reset(6);
   const calls=[{tool:'code_graph',args:{token:'heavy-a'}},
    ...Array.from({length:5},(_,i)=>({tool:'read_file',args:{token:'light-'+i}})),
    {tool:'code_graph',args:{token:'heavy-b'}}];
   const result=await tool('code_mode',{source:'const r=await tools.parallelSafe('+JSON.stringify(calls)+');return r.structuredContent;'});
   assert.equal(result.isError,false,JSON.stringify(result));assert.equal(result.structuredContent.allCallsSucceeded,true,JSON.stringify(result));
   assert.equal(peak,6);assert.equal(arrived,7);assert.equal(heavyPeak,1);
  });
  await t.test('parallel counters update while real MCP child calls are still running',async()=>{
   reset(99);
   const pending=tool('code_mode',{source:'const r=await tools.parallelSafe([{tool:"read_file",args:{token:"live-a"}},{tool:"read_file",args:{token:"live-b"}},{tool:"read_file",args:{token:"live-c"}}]);return r.structuredContent;'});
   await until(()=>arrived===3);
   const live=activity.recent().filter(e=>e.level==='parallel').map(e=>e.message);
   assert.ok(live.some(line=>/PARALLEL JOBS.*active 3\/3.*queued 0.*done 0\/3.*peak 3/.test(line)),live.join('\n'));
   release();
   const result=await pending;assert.equal(result.isError,false,JSON.stringify(result));
   assert.equal(result.structuredContent.result.observedPeakConcurrency,undefined,'live peak counters must not enter durable tool replies');
   assert.ok(activity.recent().some(e=>e.level==='parallel'&&/PARALLEL JOBS.*active 0\/3.*done 3\/3.*peak 3/.test(e.message)));
  });
  await t.test('parallelSafe overlaps real blocking searches through MCP with isolated results',async()=>{
   reset(2);
   const calls=[
    {tool:'start_search',args:{path:project,pattern:'alphaNeedle',searchType:'content',maxResults:5,includeSource:false}},
    {tool:'start_search',args:{path:project,pattern:'betaNeedle',searchType:'content',maxResults:5,includeSource:false,wait:true}},
   ];
   const result=await tool('code_mode',{source:'const r=await tools.parallelSafe('+JSON.stringify(calls)+');return {...r.structuredContent,counts:r.results.map(v=>v.structuredContent.totalMatches)};'});
   assert.equal(result.isError,false,JSON.stringify(result));
   assert.equal(result.structuredContent.allCallsSucceeded,true,JSON.stringify(result));
   assert.equal(result.structuredContent.result.total,2);
   assert.deepEqual(result.structuredContent.result.counts,[1,1]);
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
  await t.test('latest logged search plus project map script succeeds through MCP',async()=>{
   reset(2);
   const source='const root='+JSON.stringify(project)+';const r=await tools.parallelSafe([{tool:"project_map",args:{path:root,limit:250}},{tool:"start_search",args:{path:root,pattern:"alphaNeedle|betaNeedle",searchType:"content",filePattern:"*.{ts,kt,java,dart,xml}",wait:true,maxResults:90,contextLines:0}}],{max_concurrency:2});return {total:r.structuredContent.total,search:r.results[1]};';
   const result=await tool('code_mode',{source});
   assert.equal(result.isError,false,JSON.stringify(result));
   assert.equal(result.structuredContent.allCallsSucceeded,true,JSON.stringify(result));
   assert.equal(result.structuredContent.result.total,2);assert.equal(peak,2);
   assert.equal(result.structuredContent.result.search.structuredContent.totalMatches,2);
  });
  await t.test('latest logged three native read_file calls support map and overlap through MCP',async()=>{
   reset(3);
   const files=['receiver.kt','transport.kt','adaptive.dart'];
   const prefixes=['camera','transport','adaptive'];
   for(const [i,file]of files.entries())await fs.writeFile(path.join(project,file),Array.from({length:321},(_,line)=>prefixes[i]+'-line-'+line).join('\n'));
   const calls=files.map((file,i)=>({tool:'read_file',args:{path:path.join(project,file),offset:i===0?100:0,length:[210,130,220][i],...(i===0?{maxChars:16000}:{})}}));
   const result=await tool('code_mode',{source:'const a=await tools.parallelSafe('+JSON.stringify(calls)+',{max_concurrency:3});return a.map(x=>x.text);'});
   assert.equal(result.isError,false,JSON.stringify(result));
   assert.equal(result.structuredContent.allCallsSucceeded,true,JSON.stringify(result));
   assert.equal(peak,3);
   assert.equal(result.structuredContent.result.length,3);
   for(const [i,text]of result.structuredContent.result.entries())assert.ok(text.includes(prefixes[i]+'-line-'+(i===0?100:0)),text);
  });
  await t.test('parallelSafe exposes ordered native replies and supports the logged map pattern',async()=>{
   reset(2);
   const files=['alpha.ts','beta.ts'];
   const source='const files='+JSON.stringify(files.map(f=>path.join(project,f)))+';'+
    'const r=await tools.parallelSafe(files.map(path=>({tool:"read_code_file",args:{path}})));'+
    'return {summary:r.structuredContent.total,items:r.map((v,i)=>({i,index:v.index,text:v.structuredContent.text,sha:v.structuredContent.sha256})),count:r.results.length};';
   const result=await tool('code_mode',{source});
   assert.equal(result.isError,false,JSON.stringify(result));
   const value=result.structuredContent.result;
   assert.equal(value.summary,2);assert.equal(value.count,2);assert.equal(peak,2);
   for(const [i,item]of value.items.entries()){
    assert.equal(item.index,i);assert.equal(item.i,i);
    assert.equal(item.text,await fs.readFile(path.join(project,files[i]),'utf8'));
    assert.match(item.sha,/^[a-f0-9]{64}$/);
   }
   const jobFiles=await fs.readdir(path.join(scratch,'home','jobs'));
   for(const file of jobFiles.filter(f=>f.endsWith('.json'))){
    const job=JSON.parse(await fs.readFile(path.join(scratch,'home','jobs',file),'utf8'));
    assert.equal(job.captureResult,undefined);assert.equal(job.parallelResult,undefined);
    assert.equal(job.parallelProgress,undefined,'progress observers must never be persisted');
    for(const step of job.results)assert.equal(step.structuredContent,undefined,'child metadata must remain request-local');
   }
   const store=path.join(scratch,'home','harness-vnext');
   for(const file of (await fs.readdir(store)).filter(f=>f.startsWith('execution--')&&f.endsWith('.json'))){
    const execution=JSON.parse(await fs.readFile(path.join(store,file),'utf8'));
    for(const step of execution.steps.filter(s=>s.tool==='run_parallel'))
     assert.equal(step.reply?.value?.results,undefined,'projected child results must not enter the durable ledger');
    for(const step of execution.steps.filter(s=>s.tool==='run_parallel'))
     assert.equal(step.reply?.value?.parallelism,undefined,'capacity feedback must remain outside durable tool replies');
   }
  });
  await t.test('parallelSafe bounds source replies and retains exact continuation metadata',async()=>{
   reset(1);
   const file=path.join(project,'large.ts');
   const expected='x'.repeat(40000);await fs.writeFile(file,expected);
   const source='const r=await tools.parallelSafe([{tool:"read_code_file",args:{path:'+JSON.stringify(file)+',charLength:50000,maxChars:50000}}]);const v=r.results[0];return {truncated:v.outputTruncated,chars:JSON.stringify(v).length,textChars:v.structuredContent.text.length,allX:/^x+$/.test(v.structuredContent.text),next:v.structuredContent.next.args.charOffset};';
   const result=await tool('code_mode',{source});
   assert.equal(result.isError,false,JSON.stringify(result));
   const value=result.structuredContent.result;
   assert.equal(value.truncated,true);assert.equal(value.allX,true);
   assert.ok(value.chars<25000);assert.equal(value.next,value.textChars);
  });
  await t.test('parallelSafe reports pending jobs without waiting for late result capture',async()=>{
   reset(99);
   const result=await tool('code_mode',{source:'const r=await tools.parallelSafe([{tool:"read_file",args:{token:"late"}}],{timeout_ms:1000});return r.results[0];'});
   assert.equal(result.structuredContent.pending,true,JSON.stringify(result));
   const value=result.structuredContent.result;
   assert.equal(value.pending,true);assert.equal(value.resultUnavailable,true);
   assert.ok(result.structuredContent.pendingJobs.includes(value.jobId));
   release();await until(()=>active===0);
  });
  await t.test('parallelSafe never labels failed child reads as all calls succeeded',async()=>{
   reset(1);
   const result=await tool('code_mode',{source:'const r=await tools.parallelSafe([{tool:"read_file",args:{fail:true}}]);return r.results[0];'});
   assert.equal(result.structuredContent.allCallsSucceeded,false,JSON.stringify(result));
   assert.equal(result.structuredContent.status,'completed_with_step_errors');
   assert.equal(result.structuredContent.result.isError,true);
   assert.equal(result.structuredContent.result.structuredContent.error.code,'READ_FAILED');
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
