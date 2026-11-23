/**
 * BRC-native Code Mode: one remote MCP invocation coordinates many LOCAL tool
 * calls. Model-supplied JavaScript executes in a bounded QuickJS/WASM worker,
 * never in Node's vm, eval, child_process, or the relay.
 *
 * Every guest tool call passes through the original dispatcher so filesystem
 * permissions, source preconditions, process policy, activity and receipts
 * still apply. No automatic replay of failed/uncertain writes is attempted.
 */
import { Worker } from 'node:worker_threads';
import {randomUUID} from 'node:crypto';
import { z } from 'zod';
import type { ToolResult } from 'brc-shared';
import type { ToolDefinition } from '../core/tool-definitions.js';
import { AgentError, failure } from './errors.js';
import { getCallContext, withCallContext } from '../context.js';
import {
  beginDurableExecution,durablePrepareStep,durableSettleStep,finishDurableExecution,
  durableNamedStepGet,durableNamedStepSave,
  executeWithSafeRetry,beforeToolCall,afterToolCall,searchSnippets,getSnippet,saveSnippet,
  autoSaveSuccessfulWorkflow,selectSavedWorkflow,specialistAllows,specialistRoles,harnessVNextStatus,
  type HarnessReply,
} from './harness-vnext.js';
import {semanticSearch} from './semantic-project-search.js';
import {
  openProjectSession,getProjectSession,updateProjectSession,listProjectSessions,observeProjectSessionBatch,
  putResultHandle,maybePutResultHandle,readResultHandle,dropResultHandle,
  saveResourceAlias,getResourceAlias,listResourceAliases,forgetResourceAlias,
  createFiber,getFiber,updateFiber,listFibers,roundTripAcceleratorStatus,
} from './roundtrip-accelerator.js';
import {codeModeScheduler,type CodeModeLease} from './code-mode-scheduler.js';

type Args = Record<string,unknown>;
type Executor = (name:string,args:Args)=>Promise<ToolResult>;
type RpcRequest = {type:'call'|'search'|'describe'|'semantic'|'snippet'|'agent'|'harness'|'trace'|'step'|'roundtrip'|'workflow';name?:unknown;args?:unknown;query?:unknown};
type RpcReply = {ok:boolean;value?:unknown;error?:{code:string;message:string}};
type Step = {tool:string;outcome:'ok'|'error'|'pending'|'uncertain';durationMs:number;errorCode?:string;jobId?:string;outputTruncated?:boolean;attempts?:number;replayed?:boolean;role?:string};
type WorkerMessage =
  |{type:'rpc';id:number;request:RpcRequest}
  |{type:'done';value:unknown}
  |{type:'error';message:string};

export const CodeModeArgsSchema=z.object({
  source:z.string().min(1).max(50_000).describe(
    'Sandboxed JavaScript function body ending with return. Use tools.search/describe/call to discover native and harness capabilities. No Node, filesystem, fetch or process globals.'),
  max_runtime_ms:z.number().int().min(1000).max(105_000).optional().default(85_000).describe(
    'Total workflow deadline. For long builds use tools.call("spawn_job",...) and inspect the returned job ID in a later MCP call.'),
  max_calls:z.number().int().min(1).max(96).optional().default(48).describe(
    'Maximum nested local tool invocations. Tool discovery does not count against this budget.'),
});

// The source program cannot manufacture an arbitrary Node capability.
// These methods reuse existing BRC tools; they do not widen the account's
// allowedDirectories or bypass normal command, file and plugin policies.
export const CODE_MODE_ALLOWED_TOOLS=new Set([
  'read_file','read_multiple_files','read_code_file','list_directory','get_file_info',
  'start_search','get_more_search_results','stop_search','list_searches',
  'project_context','project_map','context_pack','code_graph','lsp_query',
  'write_file','edit_block','edit_files','apply_patch','create_directory','move_file','delete_file',
  'run_command','discover_checks','run_project_checks',
  'spawn_job','get_job','wait_jobs','cancel_job','run_parallel',
  'start_process','read_process_output','interact_with_process','list_sessions',
  'git_status','git_diff','git_log',
  'list_plugin_tools','call_plugin_tool','agent_status',
  'ast_search','find_symbols','find_references','write_pdf',
]);

const CODE_MODE_VIRTUAL_TOOLS:ToolDefinition[]=[
  {
    name:'semantic_project_search',
    description:'LOCAL semantic/hybrid project search with a persistent 64 MiB oldest-first cache. Use for natural-language code discovery before broad file reads.',
    inputSchema:{type:'object',required:['path','query'],properties:{
      path:{type:'string'},query:{type:'string'},limit:{type:'integer',minimum:1,maximum:30},refresh:{type:'boolean'}
    }},annotations:{readOnlyHint:true}
  },
  {
    name:'saved_workflow_search',
    description:'Search successful/saved Code Mode workflow snippets stored locally inside the 30 MiB harness quota.',
    inputSchema:{type:'object',required:['query'],properties:{query:{type:'string'},limit:{type:'integer',minimum:1,maximum:25}}},
    annotations:{readOnlyHint:true}
  },
  {
    name:'saved_workflow_get',
    description:'Load one saved Code Mode workflow by id or name.',
    inputSchema:{type:'object',required:['id'],properties:{id:{type:'string'}}},annotations:{readOnlyHint:true}
  },
  {
    name:'saved_workflow_save',
    description:'Save or update a reusable Code Mode workflow locally. Storage is bounded and oldest records are evicted.',
    inputSchema:{type:'object',required:['name','source'],properties:{
      name:{type:'string'},description:{type:'string'},source:{type:'string'},
      tags:{type:'array',items:{type:'string'}},tools:{type:'array',items:{type:'string'}}
    }},annotations:{readOnlyHint:false,destructiveHint:false}
  },
  {
    name:'specialist_agent',
    description:'Run a bounded local specialist execution role (explorer/reviewer/tester/coder) over 1-16 planned BRC tool steps. This isolates permissions and trace scope; it does not spawn another LLM.',
    inputSchema:{type:'object',required:['role','task','steps'],properties:{
      role:{type:'string',enum:['explorer','reviewer','tester','coder']},task:{type:'string'},
      steps:{type:'array',minItems:1,maxItems:16,items:{type:'object',required:['tool','args'],properties:{tool:{type:'string'},args:{type:'object'}}}},
      continue_on_error:{type:'boolean'}
    }},annotations:{readOnlyHint:false,destructiveHint:true}
  },
  {
    name:'project_session',
    description:'Open/get/update/list a persistent local project session so later Code Mode runs can reuse project state, recent paths, job IDs and terminal session IDs without rediscovery.',
    inputSchema:{type:'object',required:['op'],properties:{
      op:{type:'string',enum:['open','get','update','list']},id:{type:'string'},name:{type:'string'},
      root:{type:'string'},state:{type:'object'},patch:{type:'object'},limit:{type:'integer',minimum:1,maximum:64}
    }},annotations:{readOnlyHint:false,destructiveHint:false}
  },
  {
    name:'durable_step',
    description:'Named durable checkpoint helper. Preferred form inside Code Mode is await tools.step("name", async()=>{...}); completed values replay locally after interruption.',
    inputSchema:{type:'object',required:['name'],properties:{name:{type:'string'}}},annotations:{readOnlyHint:false,destructiveHint:false}
  },
  {
    name:'result_handle',
    description:'Store/read/search/drop large intermediate results locally instead of returning them through the calling AI. Automatic large nested tool responses also include a local resultHandle.',
    inputSchema:{type:'object',required:['op'],properties:{
      op:{type:'string',enum:['store','get','drop']},id:{type:'string'},value:{},label:{type:'string'},
      query:{type:'string'},offset:{type:'integer',minimum:0},length:{type:'integer',minimum:200,maximum:32768}
    }},annotations:{readOnlyHint:false,destructiveHint:false}
  },
  {
    name:'reusable_terminal',
    description:'Start or reuse a named terminal session, then read/send/forget it by alias. Reuses existing stable BRC terminal session IDs instead of repeatedly launching processes.',
    inputSchema:{type:'object',required:['op','name'],properties:{
      op:{type:'string',enum:['start','get','read','send','list','forget']},name:{type:'string'},
      startArgs:{type:'object'},input:{type:'string'},cursor:{type:'integer',minimum:0},length:{type:'integer',minimum:1,maximum:2000}
    }},annotations:{readOnlyHint:false,destructiveHint:true}
  },
  {
    name:'reusable_browser',
    description:'Bind/reuse one named browser plugin session and call Playwright/plugin tools through the existing plugin connection. Does not start a second browser or change relay tools.',
    inputSchema:{type:'object',required:['op','name'],properties:{
      op:{type:'string',enum:['bind','get','call','list','forget']},name:{type:'string'},plugin:{type:'string'},
      tool:{type:'string'},args:{type:'object'},metadata:{type:'object'}
    }},annotations:{readOnlyHint:false,destructiveHint:true}
  },
  {
    name:'saved_workflow_run',
    description:'Automatically match and execute one unambiguous saved successful Code Mode workflow locally. Use tools.call("saved_workflow_run",{query,context}) or tools.workflows.run(query,context).',
    inputSchema:{type:'object',required:['query'],properties:{query:{type:'string'},context:{type:'object'}}},
    annotations:{readOnlyHint:false,destructiveHint:true}
  },
  {
    name:'durable_fiber',
    description:'Create/start/status/wait/resume/list a named long-running durable fiber backed by existing persistent BRC background jobs. Safe interrupted read-only tails can resume; uncertain mutations are not replayed.',
    inputSchema:{type:'object',required:['op'],properties:{
      op:{type:'string',enum:['create','start','status','wait','resume','list']},name:{type:'string'},
      steps:{type:'array',minItems:1,maxItems:50,items:{type:'object'}},continue_on_error:{type:'boolean'},
      timeout_ms:{type:'integer',minimum:1000,maximum:21600000},wait_ms:{type:'integer',minimum:0,maximum:100000}
    }},annotations:{readOnlyHint:false,destructiveHint:true}
  },
  {
    name:'roundtrip_status',
    description:'Inspect persistent project sessions, local result handles, reusable aliases and durable fibers sharing the 30 MiB harness quota.',
    inputSchema:{type:'object',properties:{}},annotations:{readOnlyHint:true}
  },
  {
    name:'harness_status',
    description:'Inspect durable Code Mode, retry, snippet, trace and local storage quota health.',
    inputSchema:{type:'object',properties:{}},annotations:{readOnlyHint:true}
  },
  {
    name:'workflow_trace',
    description:'Return the current bounded workflow trace summary for this Code Mode execution.',
    inputSchema:{type:'object',properties:{}},annotations:{readOnlyHint:true}
  },
];

const MAX_TOOL_TEXT=36_000;
const MAX_TOOL_RESPONSE=65_000;
const MAX_FINAL_RESPONSE=48_000;
const MAX_TOOL_DISCOVERY=24;
const MAX_SOURCE_OUTPUT=18_000;

function blocked(code:string,message:string,steps:Step[]=[]):ToolResult {
  const error=new AgentError(code,message,false);
  const base=failure(error);
  return {...base,structuredContent:{...base.structuredContent,
    codeMode:{completedSteps:steps,completedCalls:steps.length,partial:steps.length>0,
      automaticRetry:false}}};
}
function limited(value:unknown,size:number):string {
  let result='';
  try{result=JSON.stringify(value)??'null';}catch{return '[not JSON serializable]';}
  return result.length>size?result.slice(0,size):result;
}
/** Small, bounded schema-to-TypeScript hint similar to Cloudflare's
 * focused method declarations. JSON Schema remains the authority.
 */
function schemaAsTs(raw:unknown,depth=0):string{
  if(depth>5||!raw||typeof raw!=='object')return 'unknown';
  const s=raw as Record<string,unknown>;
  if(Array.isArray(s.enum)&&s.enum.length>0&&s.enum.length<=24)
    return s.enum.map(v=>JSON.stringify(v)).join(' | ');
  const variants=(s.anyOf??s.oneOf) as unknown[]|undefined;
  if(Array.isArray(variants)&&variants.length>0&&variants.length<=12)
    return variants.map(x=>schemaAsTs(x,depth+1)).join(' | ');
  if(Array.isArray(s.allOf)&&s.allOf.length>0&&s.allOf.length<=8)
    return (s.allOf as unknown[]).map(x=>schemaAsTs(x,depth+1)).join(' & ');
  if(s.type==='array')return '('+schemaAsTs(s.items,depth+1)+')[]';
  if(s.type==='object'||(s.properties&&typeof s.properties==='object')){
    const props=s.properties as Record<string,unknown>|undefined;
    const required=new Set(Array.isArray(s.required)?s.required:[]);
    const members=Object.entries(props??{}).slice(0,80).map(([name,child])=>
      JSON.stringify(name)+(required.has(name)?': ':'?: ')+schemaAsTs(child,depth+1)+';');
    if(s.additionalProperties===true)members.push('[key: string]: unknown;');
    return '{ '+members.join(' ')+' }';
  }
  if(s.type==='string'||s.type==='number'||s.type==='boolean'||s.type==='null')return s.type;
  if(s.type==='integer')return 'number';
  return 'unknown';
}
function describe(def:ToolDefinition):Record<string,unknown>{
  const signature='declare const tools: { call(name: '+JSON.stringify(def.name)+
    ', args: '+schemaAsTs(def.inputSchema)+'): Promise<ToolResult> };';
  return {name:def.name,description:def.description.slice(0,5000),
    inputSchema:def.inputSchema,types:signature.slice(0,6000)};
}
function search(catalog:ToolDefinition[],query:string):unknown {
  const terms=query.toLowerCase().trim().split(/[^a-z0-9_]+/).filter(x=>x.length>1).slice(0,12);
  if(!terms.length)return [];
  const hits=catalog.map(def=>{
    const name=def.name.toLowerCase();
    const desc=def.description.toLowerCase().slice(0,1300);
    const score=terms.reduce((sum,term)=>sum+
      (name===term?12:name.includes(term)?6:0)+(desc.includes(term)?1:0),0);
    return {def,score};
  }).filter(x=>x.score>0).sort((a,b)=>b.score-a.score||a.def.name.localeCompare(b.def.name));
  return hits.slice(0,12).map(x=>({name:x.def.name,description:x.def.description.slice(0,180)}));
}
async function toolReply(result:ToolResult,label:string):Promise<{value:Record<string,unknown>;truncated:boolean}>{
  const content=Array.isArray(result.content)?result.content:[];
  const textContent=content.filter(c=>c.type==='text').map(c=>c.text).join('\n');
  const mediaOmitted=content.some(c=>c.type!=='text');
  const fullValue:Record<string,unknown>={
    text:textContent,
    isError:!!result.isError,
    pending:result.structuredContent?.pending===true,
    mediaOmitted,
    ...(result.structuredContent?{structuredContent:result.structuredContent}:{})
  };
  const handle=await maybePutResultHandle(fullValue,label).catch(()=>null);
  let value:Record<string,unknown>={
    text:textContent.slice(0,MAX_TOOL_TEXT),
    isError:!!result.isError,
    pending:result.structuredContent?.pending===true,
    mediaOmitted,
    ...(result.structuredContent?{structuredContent:result.structuredContent}:{}),
    ...(handle?{resultHandle:handle}:{})
  };
  let truncated=textContent.length>MAX_TOOL_TEXT;
  if(limited(value,MAX_TOOL_RESPONSE).length>=MAX_TOOL_RESPONSE){
    value={text:textContent.slice(0,MAX_SOURCE_OUTPUT),
      isError:!!result.isError,pending:result.structuredContent?.pending===true,
      outputTruncated:true,mediaOmitted,
      ...(handle?{resultHandle:handle}:{}),
      note:handle
        ?'The full bounded/redacted nested result is retained locally. Query resultHandle inside this Code Mode run instead of returning it through the calling AI.'
        :'The nested result exceeded the Code Mode limit. Request a smaller range or use the direct tool.'};
    truncated=true;
  }
  return {value,truncated};
}

export async function executeCodeMode(
  input:z.infer<typeof CodeModeArgsSchema>,catalog:ToolDefinition[],
  executor:Executor,outerSignal?:AbortSignal,
):Promise<ToolResult>{
  if(outerSignal?.aborted)return blocked('CODE_MODE_CANCELLED','The caller cancelled the workflow before it started.');
  const parent=getCallContext();
  const workflowId=randomUUID();
  let schedulerLease:CodeModeLease;
  try{schedulerLease=await codeModeScheduler.acquire(workflowId,outerSignal);}
  catch(error){
    const e=error instanceof AgentError?error:new AgentError('CODE_MODE_ADMISSION_FAILED',
      error instanceof Error?error.message:'Code Mode admission failed.',true);
    const base=failure(e);
    return {...base,structuredContent:{...base.structuredContent,
      codeMode:{workflowId,completedSteps:[],completedCalls:0,partial:false,
        automaticRetry:false,scheduler:codeModeScheduler.status()}}};
  }
  const steps:Step[]=[];
  const pendingJobs:string[]=[];
  const controller=new AbortController();
  let activeTool=false;
  let closed=false;
  let calls=0;
  let discoveries=0;
  let activeProjectSessionId:string|undefined;
  const projectObservations:Array<{tool:string;args:Args;meta:Record<string,unknown>}>=[];
  const catalogView=catalog.filter(t=>CODE_MODE_ALLOWED_TOOLS.has(t.name));
  const discoveryCatalog=[...catalogView,...CODE_MODE_VIRTUAL_TOOLS];
  const names=new Map(catalogView.map(t=>[t.name,t]));
  const discoverableNames=new Map(discoveryCatalog.map(t=>[t.name,t]));
  const started=Date.now();

  let durability:Awaited<ReturnType<typeof beginDurableExecution>>;
  try{durability=await beginDurableExecution(input.source,parent?.id??workflowId);}
  catch(error){
    schedulerLease.release();
    return blocked('CODE_MODE_DURABILITY_UNAVAILABLE',
      'Durable Code Mode storage could not initialize: '+(error instanceof Error?error.message.slice(0,400):'unknown local storage error'));
  }
  const {execution,trace,resumed}=durability;

  const runToolCall=async(name:string,args:Args,role?:string):Promise<RpcReply>=>{
    if(!names.has(name))
      return {ok:false,error:{code:'CODE_MODE_TOOL_DENIED',message:'Tool not permitted in Code Mode. Use tools.search for available tools.'}};
    if(++calls>input.max_calls)
      return {ok:false,error:{code:'CODE_MODE_CALL_LIMIT',message:'Nested tool call budget exhausted.'}};
    if(activeTool)
      return {ok:false,error:{code:'CODE_MODE_NESTED_PARALLEL_DENIED',
        message:'This Code Mode workflow already has a nested tool call in flight. Keep nested calls sequential, or fan out as separate Code Mode workflows / approved job tools.'}};
    const def=names.get(name)!;
    const readOnly=def.annotations?.readOnlyHint===true;
    if(role&&!specialistAllows(role,name,readOnly))
      return {ok:false,error:{code:'SPECIALIST_TOOL_DENIED',message:'Tool '+name+' is not permitted for specialist role '+role+'.'}};
    if(name==='spawn_job'||name==='run_parallel'){
      const nested=name==='spawn_job'?args.steps:args.calls;
      if(Array.isArray(nested))for(const step of nested){
        if(!step||typeof step!=='object'||Array.isArray(step))continue;
        const tool=(step as Record<string,unknown>).tool;
        if(tool!==undefined&&(!CODE_MODE_ALLOWED_TOOLS.has(String(tool))||
          tool==='spawn_job'||tool==='run_parallel'))
          return {ok:false,error:{code:'CODE_MODE_NESTED_TOOL_DENIED',
            message:'Nested job/parallel tool is not available in Code Mode.'}};
      }
    }
    if(limited(args,512*1024).length>=512*1024)
      return {ok:false,error:{code:'CODE_MODE_INPUT_LIMIT',message:'Nested tool arguments exceed 512 KiB.'}};

    const prepared=await durablePrepareStep(execution,name,args,readOnly,trace);
    if(prepared.mustStop)return prepared.mustStop;
    if(prepared.replay){
      const reply=prepared.replay as RpcReply;
      const replayError=reply.ok?undefined:reply.error?.code;
      steps.push({tool:name,outcome:reply.ok?'ok':'error',durationMs:0,
        ...(replayError?{errorCode:replayError}:{}),replayed:true,attempts:0,...(role?{role}:{})});
      return reply;
    }

    const lifecycle=await beforeToolCall({
      executionId:execution.id,traceId:trace.record.id,tool:name,args,readOnly,
      step:prepared.index,
    },trace);
    const stepStart=Date.now();
    activeTool=true;
    let attempts=0;
    try{
      const invoke=()=>parent
        ?withCallContext({...parent,id:execution.id+':'+prepared.index,signal:controller.signal,
          depth:(parent.depth??0)+1},()=>executor(name,lifecycle.args))
        :executor(name,lifecycle.args);
      const executed=await executeWithSafeRetry(name,readOnly,invoke,trace);
      attempts=executed.attempts;
      const response=executed.result;
      await afterToolCall({
        executionId:execution.id,traceId:trace.record.id,tool:name,args:lifecycle.args,
        readOnly,step:prepared.index,
      },response,Date.now()-stepStart,attempts,trace);
      const meta=(response.structuredContent??{}) as Record<string,unknown>;
      const pending=meta.pending===true;
      const error=meta.error as {code?:unknown}|undefined;
      const code=typeof error?.code==='string'?error.code:'TOOL_FAILED';
      const jobId=typeof meta.jobId==='string'?meta.jobId:undefined;
      const status=response.isError?'error':pending?'pending':'ok';
      const {value,truncated}=await toolReply(response,name);
      const reply:HarnessReply=response.isError
        ?{ok:false,error:{code,message:(typeof value.text==='string'?value.text:'Tool failed.').slice(0,700)}}
        :{ok:true,value};
      await durableSettleStep(execution,prepared.index,reply,attempts,trace);
      steps.push({tool:name,outcome:status,durationMs:Date.now()-stepStart,
        ...(response.isError?{errorCode:code}:{}),...(jobId?{jobId}:{}),
        ...(truncated?{outputTruncated:true}:{}),attempts,...(role?{role}:{})});
      if(pending&&jobId)pendingJobs.push(jobId);
      if(activeProjectSessionId&&projectObservations.length<64)
        projectObservations.push({tool:name,args:lifecycle.args,meta});
      return reply;
    }catch(error){
      const uncertain=controller.signal.aborted||!readOnly;
      const code=controller.signal.aborted?'CODE_MODE_OUTCOME_UNCERTAIN':'CODE_MODE_TOOL_FAILED';
      const reply:HarnessReply={ok:false,error:{code,message:error instanceof Error?error.message.slice(0,500):'Nested tool failed.'}};
      await durableSettleStep(execution,prepared.index,reply,Math.max(1,attempts),trace,
        uncertain?'uncertain':'error');
      steps.push({tool:name,outcome:uncertain?'uncertain':'error',
        durationMs:Date.now()-stepStart,errorCode:code,attempts:Math.max(1,attempts),...(role?{role}:{})});
      return reply;
    }finally{activeTool=false;}
  };

  const handleRpc=async(request:RpcRequest):Promise<RpcReply>=>{
    if(controller.signal.aborted||closed)
      return {ok:false,error:{code:'CODE_MODE_CANCELLED',message:'Workflow has stopped.'}};
    if(!request||typeof request!=='object')
      return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Invalid Code Mode request.'}};
    if(request.type==='search'||request.type==='describe'){
      if(++discoveries>MAX_TOOL_DISCOVERY)
        return {ok:false,error:{code:'CODE_MODE_DISCOVERY_LIMIT',message:'Too many schema lookups.'}};
      if(request.type==='search'){
        if(typeof request.query!=='string'||request.query.length>160)
          return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Search query must be a short string.'}};
        const harnessIntent=/\b(?:semantic|workflow|snippet|specialist|agent|harness|trace|durable|retry|session|result|terminal|browser|fiber|roundtrip)\b/i.test(request.query);
        return {ok:true,value:search(harnessIntent?discoveryCatalog:catalogView,request.query)};
      }
      const name=request.name;
      if(typeof name!=='string'||!discoverableNames.has(name))
        return {ok:false,error:{code:'UNKNOWN_TOOL',message:'Tool is not available in Code Mode. Call tools.search first.'}};
      const result=describe(discoverableNames.get(name)!);
      if(limited(result,MAX_TOOL_RESPONSE).length>=MAX_TOOL_RESPONSE)
        return {ok:false,error:{code:'CODE_MODE_SCHEMA_LIMIT',message:'Tool schema is too large to include in the sandbox.'}};
      return {ok:true,value:result};
    }
    if(request.type==='semantic'){
      if(request.name!=='search'||!request.args||typeof request.args!=='object'||Array.isArray(request.args))
        return {ok:false,error:{code:'INVALID_ARGUMENT',message:'tools.semantic.search requires an argument object.'}};
      if(++calls>input.max_calls)
        return {ok:false,error:{code:'CODE_MODE_CALL_LIMIT',message:'Nested tool call budget exhausted.'}};
      if(activeTool)return {ok:false,error:{code:'CODE_MODE_BUSY',message:'Nested calls must run sequentially.'}};
      activeTool=true;
      try{
        const args=request.args as {path?:unknown;query?:unknown;limit?:unknown;refresh?:unknown};
        if(typeof args.path!=='string'||typeof args.query!=='string')
          return {ok:false,error:{code:'INVALID_ARGUMENT',message:'semantic search requires path and query strings.'}};
        const value=await semanticSearch({path:args.path,query:args.query,
          limit:typeof args.limit==='number'?args.limit:undefined,refresh:args.refresh===true});
        await trace.event('semantic_search',{detail:{path:args.path,query:args.query.slice(0,160),results:value.results.length}});
        return {ok:true,value};
      }catch(error){
        const code=error instanceof AgentError?error.code:'SEMANTIC_SEARCH_FAILED';
        return {ok:false,error:{code,message:error instanceof Error?error.message.slice(0,600):'Semantic search failed.'}};
      }finally{activeTool=false;}
    }
    if(request.type==='snippet'){
      try{
        if(request.name==='search'){
          if(typeof request.query!=='string'||request.query.length>300)
            return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Snippet search requires a short query.'}};
          const limit=request.args&&typeof request.args==='object'&&!Array.isArray(request.args)&&
            typeof (request.args as Record<string,unknown>).limit==='number'
            ?Number((request.args as Record<string,unknown>).limit):10;
          return {ok:true,value:await searchSnippets(request.query,limit)};
        }
        if(request.name==='get'){
          const id=request.args&&typeof request.args==='object'&&!Array.isArray(request.args)
            ?(request.args as Record<string,unknown>).id:undefined;
          if(typeof id!=='string')return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Snippet get requires id or name.'}};
          const value=await getSnippet(id);
          return value?{ok:true,value}:{ok:false,error:{code:'SNIPPET_NOT_FOUND',message:'Saved workflow was not found.'}};
        }
        if(request.name==='save'){
          if(!request.args||typeof request.args!=='object'||Array.isArray(request.args))
            return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Snippet save requires an object.'}};
          const a=request.args as Record<string,unknown>;
          if(typeof a.name!=='string'||typeof a.source!=='string')
            return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Snippet save requires name and source strings.'}};
          const value=await saveSnippet({name:a.name,source:a.source,
            description:typeof a.description==='string'?a.description:undefined,
            tags:Array.isArray(a.tags)?a.tags.filter((x):x is string=>typeof x==='string'):undefined,
            tools:Array.isArray(a.tools)?a.tools.filter((x):x is string=>typeof x==='string'):undefined});
          await trace.event('snippet_saved',{detail:{id:value.id,name:value.name}});
          return {ok:true,value};
        }
        return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Unknown snippet operation.'}};
      }catch(error){
        const code=error instanceof AgentError?error.code:'SNIPPET_FAILED';
        return {ok:false,error:{code,message:error instanceof Error?error.message.slice(0,600):'Snippet operation failed.'}};
      }
    }
    if(request.type==='step'){
      try{
        if(typeof request.name!=='string')
          return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Durable step requires a name.'}};
        const a=request.args&&typeof request.args==='object'&&!Array.isArray(request.args)
          ?request.args as Record<string,unknown>:{};
        if(a.op==='get')return {ok:true,value:await durableNamedStepGet(execution,request.name,trace)};
        if(a.op==='save')return {ok:true,value:await durableNamedStepSave(execution,request.name,a.value,trace)};
        return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Durable step operation must be get or save.'}};
      }catch(error){
        const code=error instanceof AgentError?error.code:'DURABLE_STEP_FAILED';
        return {ok:false,error:{code,message:error instanceof Error?error.message.slice(0,600):'Durable step failed.'}};
      }
    }
    if(request.type==='workflow'){
      if(request.name!=='match'||typeof request.query!=='string'||request.query.length<2||request.query.length>500)
        return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Workflow match requires a 2-500 character query.'}};
      const match=await selectSavedWorkflow(request.query);
      if(!match)return {ok:true,value:{found:false}};
      await trace.event('workflow_auto_match',{detail:{id:match.id,name:match.name,score:match.matchScore}});
      return {ok:true,value:{found:true,id:match.id,name:match.name,score:match.matchScore,
        description:match.description,tags:match.tags,source:match.source}};
    }
    if(request.type==='roundtrip'){
      const a=request.args&&typeof request.args==='object'&&!Array.isArray(request.args)
        ?request.args as Record<string,unknown>:{};
      try{
        if(request.name==='session_open'){
          const record=await openProjectSession({
            name:typeof a.name==='string'?a.name:undefined,
            root:typeof a.root==='string'?a.root:undefined,
            state:a.state&&typeof a.state==='object'&&!Array.isArray(a.state)?a.state as Record<string,unknown>:undefined,
          });
          activeProjectSessionId=record.id;
          await trace.event('project_session_open',{detail:{id:record.id,name:record.name,root:record.root}});
          return {ok:true,value:record};
        }
        if(request.name==='session_get'){
          const id=typeof a.id==='string'?a.id:typeof a.name==='string'?a.name:'';
          if(!id)return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Project session get requires id or name.'}};
          const record=await getProjectSession(id);
          if(!record)return {ok:false,error:{code:'PROJECT_SESSION_NOT_FOUND',message:'Persistent project session was not found.'}};
          activeProjectSessionId=record.id;
          return {ok:true,value:record};
        }
        if(request.name==='session_update'){
          const id=typeof a.id==='string'?a.id:'';
          if(!id||!a.patch||typeof a.patch!=='object'||Array.isArray(a.patch))
            return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Project session update requires id and patch object.'}};
          const record=await updateProjectSession(id,a.patch as Record<string,unknown>);
          activeProjectSessionId=record.id;
          return {ok:true,value:record};
        }
        if(request.name==='session_list')
          return {ok:true,value:await listProjectSessions(typeof a.limit==='number'?a.limit:20)};
        if(request.name==='result_store')
          return {ok:true,value:await putResultHandle(a.value,typeof a.label==='string'?a.label:'result')};
        if(request.name==='result_get'){
          if(typeof a.id!=='string')return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Result handle get requires id.'}};
          return {ok:true,value:await readResultHandle({id:a.id,
            offset:typeof a.offset==='number'?a.offset:undefined,length:typeof a.length==='number'?a.length:undefined,
            query:typeof a.query==='string'?a.query:undefined})};
        }
        if(request.name==='result_drop'){
          if(typeof a.id!=='string')return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Result handle drop requires id.'}};
          return {ok:true,value:await dropResultHandle(a.id)};
        }
        if(request.name==='terminal'){
          const op=typeof a.op==='string'?a.op:'get',name=typeof a.name==='string'?a.name:'';
          if(op==='list')return {ok:true,value:await listResourceAliases('terminal')};
          if(!name)return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Reusable terminal requires a name.'}};
          if(op==='forget')return {ok:true,value:await forgetResourceAlias('terminal',name)};
          const alias=await getResourceAlias('terminal',name);
          if(op==='get')return alias?{ok:true,value:alias}:{ok:false,error:{code:'SESSION_ALIAS_NOT_FOUND',message:'Reusable terminal alias was not found.'}};
          if(op==='read'){
            const sessionId=alias?.reference.sessionId;
            if(typeof sessionId!=='string')return {ok:false,error:{code:'SESSION_ALIAS_NOT_FOUND',message:'Reusable terminal alias has no session ID.'}};
            return runToolCall('read_process_output',{session_id:sessionId,cursor:typeof a.cursor==='number'?a.cursor:0,
              length:typeof a.length==='number'?a.length:200,timeout_ms:0});
          }
          if(op==='send'){
            const sessionId=alias?.reference.sessionId;
            if(typeof sessionId!=='string'||typeof a.input!=='string')
              return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Reusable terminal send requires an existing alias and input.'}};
            return runToolCall('interact_with_process',{session_id:sessionId,input:a.input,timeout_ms:3000});
          }
          if(op==='start'){
            if(alias&&typeof alias.reference.sessionId==='string'){
              const probe=await runToolCall('read_process_output',{session_id:alias.reference.sessionId,cursor:0,length:1,timeout_ms:0});
              const probeValue=probe.ok&&probe.value&&typeof probe.value==='object'?probe.value as Record<string,unknown>:null;
              const structured=probeValue?.structuredContent&&typeof probeValue.structuredContent==='object'
                ?probeValue.structuredContent as Record<string,unknown>:null;
              if(probe.ok&&structured?.processRunning===true)
                return {ok:true,value:{reused:true,alias,probe:probe.value}};
            }
            if(!a.startArgs||typeof a.startArgs!=='object'||Array.isArray(a.startArgs))
              return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Starting a reusable terminal requires startArgs for start_process.'}};
            const startedReply=await runToolCall('start_process',a.startArgs as Args);
            if(!startedReply.ok)return startedReply;
            const startedValue=startedReply.value&&typeof startedReply.value==='object'?startedReply.value as Record<string,unknown>:{};
            const structured=startedValue.structuredContent&&typeof startedValue.structuredContent==='object'
              ?startedValue.structuredContent as Record<string,unknown>:{};
            if(typeof structured.sessionId!=='string')
              return {ok:false,error:{code:'SESSION_ID_MISSING',message:'start_process did not return a stable session ID.'}};
            const saved=await saveResourceAlias({kind:'terminal',name,reference:{sessionId:structured.sessionId,pid:structured.pid},
              metadata:{startedAt:Date.now()}});
            return {ok:true,value:{reused:false,alias:saved,result:startedReply.value}};
          }
          return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Unknown reusable terminal operation.'}};
        }
        if(request.name==='browser'){
          const op=typeof a.op==='string'?a.op:'get',name=typeof a.name==='string'?a.name:'';
          if(op==='list')return {ok:true,value:await listResourceAliases('browser')};
          if(!name)return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Reusable browser requires a name.'}};
          if(op==='forget')return {ok:true,value:await forgetResourceAlias('browser',name)};
          let alias=await getResourceAlias('browser',name);
          if(op==='bind'){
            if(typeof a.plugin!=='string')return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Browser bind requires plugin id.'}};
            alias=await saveResourceAlias({kind:'browser',name,reference:{plugin:a.plugin},
              metadata:a.metadata&&typeof a.metadata==='object'&&!Array.isArray(a.metadata)?a.metadata as Record<string,unknown>:{}});
            return {ok:true,value:alias};
          }
          if(op==='get')return alias?{ok:true,value:alias}:{ok:false,error:{code:'SESSION_ALIAS_NOT_FOUND',message:'Reusable browser alias was not found.'}};
          if(op==='call'){
            const plugin=typeof a.plugin==='string'?a.plugin:alias?.reference.plugin;
            if(typeof plugin!=='string'||typeof a.tool!=='string')
              return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Browser call requires a bound plugin and tool.'}};
            if(!alias)alias=await saveResourceAlias({kind:'browser',name,reference:{plugin},metadata:{}});
            return runToolCall('call_plugin_tool',{plugin,tool:a.tool,
              args:a.args&&typeof a.args==='object'&&!Array.isArray(a.args)?a.args as Record<string,unknown>:{}});
          }
          return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Unknown reusable browser operation.'}};
        }
        if(request.name==='fiber'){
          const op=typeof a.op==='string'?a.op:'status';
          if(op==='list')return {ok:true,value:await listFibers(typeof a.limit==='number'?a.limit:20)};
          const name=typeof a.name==='string'?a.name:'';
          if(!name)return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Durable fiber requires a name.'}};
          let fiber=await getFiber(name);
          if(op==='create'||(!fiber&&Array.isArray(a.steps))){
            if(!Array.isArray(a.steps))return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Fiber create requires steps.'}};
            fiber=await createFiber({name,steps:a.steps.filter(x=>x&&typeof x==='object'&&!Array.isArray(x)) as Array<Record<string,unknown>>,
              continueOnError:a.continue_on_error===true,timeoutMs:typeof a.timeout_ms==='number'?a.timeout_ms:undefined});
            if(op==='create')return {ok:true,value:fiber};
          }
          if(!fiber)return {ok:false,error:{code:'FIBER_NOT_FOUND',message:'Durable fiber was not found.'}};
          const reconcile=async()=>{
            if(!fiber!.currentJobId)return fiber!;
            const job=await runToolCall('get_job',{id:fiber!.currentJobId,output_chars:1500,step_limit:1});
            if(!job.ok)return fiber!;
            const v=job.value&&typeof job.value==='object'?job.value as Record<string,unknown>:{};
            const sc=v.structuredContent&&typeof v.structuredContent==='object'?v.structuredContent as Record<string,unknown>:{};
            const status=typeof sc.status==='string'?sc.status:undefined;
            const results=typeof sc.results==='number'?Math.max(0,Math.trunc(sc.results)):0;
            const base=fiber!.jobStartStep??fiber!.completedSteps;
            fiber=await updateFiber(fiber!.id,{completedSteps:Math.min(fiber!.steps.length,base+results),
              lastJobStatus:status,currentJobId:status&&['queued','running'].includes(status)?fiber!.currentJobId:undefined,
              status:status==='succeeded'?'succeeded':status==='failed'?'failed':status==='cancelled'?'cancelled':
                status==='interrupted'||status==='timed_out'?'interrupted':status==='queued'||status==='running'?'running':fiber!.status});
            return fiber!;
          };
          if(op==='status')return {ok:true,value:await reconcile()};
          if(op==='wait'){
            if(!fiber.currentJobId)return {ok:true,value:fiber};
            await runToolCall('wait_jobs',{ids:[fiber.currentJobId],timeout_ms:typeof a.wait_ms==='number'?a.wait_ms:5000,output_chars:2000});
            return {ok:true,value:await reconcile()};
          }
          if(op==='start'||op==='resume'){
            await reconcile();
            if(fiber.status==='running')return {ok:true,value:fiber};
            if(fiber.status==='succeeded')return {ok:true,value:fiber};
            const remaining=fiber.steps.slice(fiber.completedSteps);
            if(!remaining.length){
              fiber=await updateFiber(fiber.id,{status:'succeeded'});
              return {ok:true,value:fiber};
            }
            if(op==='resume'&&fiber.status==='interrupted'){
              const safe=remaining.every(step=>{
                if(typeof step.tool!=='string')return false;
                return names.get(step.tool)?.annotations?.readOnlyHint===true;
              });
              if(!safe)return {ok:false,error:{code:'FIBER_MUTATION_UNCERTAIN',
                message:'Interrupted fiber has command or non-read-only steps remaining. Inspect effects before starting a fresh fiber; no automatic replay occurred.'}};
            }
            const spawned=await runToolCall('spawn_job',{steps:remaining,label:'fiber:'+fiber.name,
              group:'fiber-'+fiber.id.slice(0,12),continue_on_error:fiber.continueOnError,timeout_ms:fiber.timeoutMs});
            if(!spawned.ok)return spawned;
            const v=spawned.value&&typeof spawned.value==='object'?spawned.value as Record<string,unknown>:{};
            const sc=v.structuredContent&&typeof v.structuredContent==='object'?v.structuredContent as Record<string,unknown>:{};
            if(typeof sc.jobId!=='string')return {ok:false,error:{code:'FIBER_JOB_ID_MISSING',message:'Background job did not return a job ID.'}};
            fiber=await updateFiber(fiber.id,{status:'running',currentJobId:sc.jobId,jobStartStep:fiber.completedSteps,
              jobIds:[...fiber.jobIds,sc.jobId].slice(-16)});
            await trace.event('fiber_started',{detail:{name:fiber.name,jobId:sc.jobId,remaining:remaining.length}});
            return {ok:true,value:fiber};
          }
          return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Unknown durable fiber operation.'}};
        }
        if(request.name==='status')return {ok:true,value:await roundTripAcceleratorStatus()};
        return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Unknown round-trip accelerator operation.'}};
      }catch(error){
        const code=error instanceof AgentError?error.code:'ROUNDTRIP_ACCELERATOR_FAILED';
        return {ok:false,error:{code,message:error instanceof Error?error.message.slice(0,700):'Round-trip accelerator operation failed.'}};
      }
    }
    if(request.type==='harness'&&request.name==='status')
      return {ok:true,value:{...(await harnessVNextStatus()),roundtrip:await roundTripAcceleratorStatus()}};
    if(request.type==='trace'&&request.name==='current')
      return {ok:true,value:trace.snapshot()};
    if(request.type==='agent'){
      if(request.name==='roles')return {ok:true,value:specialistRoles()};
      if(request.name!=='run'||!request.args||typeof request.args!=='object'||Array.isArray(request.args))
        return {ok:false,error:{code:'INVALID_ARGUMENT',message:'tools.agent.run requires an argument object.'}};
      const a=request.args as Record<string,unknown>;
      const role=typeof a.role==='string'?a.role:'';
      const task=typeof a.task==='string'?a.task.slice(0,1000):'';
      const childSteps=Array.isArray(a.steps)?a.steps:[];
      if(!specialistRoles().some(r=>r.role===role)||!task||childSteps.length<1||childSteps.length>16)
        return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Specialist run requires role, task and 1-16 steps.'}};
      const results:Record<string,unknown>[]=[];
      await trace.event('specialist_start',{detail:{role,task,steps:childSteps.length}});
      for(let i=0;i<childSteps.length;i++){
        const raw=childSteps[i];
        if(!raw||typeof raw!=='object'||Array.isArray(raw))
          return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Each specialist step must be an object.'}};
        const step=raw as Record<string,unknown>;
        if(typeof step.tool!=='string'||!step.args||typeof step.args!=='object'||Array.isArray(step.args))
          return {ok:false,error:{code:'INVALID_ARGUMENT',message:'Each specialist step requires tool and args.'}};
        const reply=await runToolCall(step.tool,step.args as Args,role);
        results.push({index:i,tool:step.tool,ok:reply.ok,...(reply.ok?{value:reply.value}:{error:reply.error})});
        if(!reply.ok&&a.continue_on_error!==true)break;
      }
      await trace.event('specialist_finish',{detail:{role,task,completed:results.length,
        errors:results.filter(x=>x.ok===false).length}});
      return {ok:true,value:{role,task,results,completed:results.length,
        allSucceeded:results.every(x=>x.ok===true),separateModel:false,
        note:'Local specialist execution context; it uses the parent-authored plan and existing BRC tools, not another LLM.'}};
    }
    if(request.type==='call'){
      if(typeof request.name!=='string'||!request.args||typeof request.args!=='object'||Array.isArray(request.args))
        return {ok:false,error:{code:'INVALID_ARGUMENT',message:'tools.call requires a permitted tool name and argument object.'}};
      const virtualArgs=request.args as Record<string,unknown>;
      if(request.name==='semantic_project_search')
        return handleRpc({type:'semantic',name:'search',args:virtualArgs});
      if(request.name==='saved_workflow_search'){
        if(typeof virtualArgs.query!=='string')
          return {ok:false,error:{code:'INVALID_ARGUMENT',message:'saved_workflow_search requires query.'}};
        return handleRpc({type:'snippet',name:'search',query:virtualArgs.query,args:{limit:virtualArgs.limit}});
      }
      if(request.name==='saved_workflow_get')
        return handleRpc({type:'snippet',name:'get',args:{id:virtualArgs.id}});
      if(request.name==='saved_workflow_save')
        return handleRpc({type:'snippet',name:'save',args:virtualArgs});
      if(request.name==='specialist_agent')
        return handleRpc({type:'agent',name:'run',args:virtualArgs});
      if(request.name==='project_session'){
        const op=typeof virtualArgs.op==='string'?virtualArgs.op:'get';
        return handleRpc({type:'roundtrip',name:'session_'+op,args:virtualArgs});
      }
      if(request.name==='durable_step'){
        if(typeof virtualArgs.name!=='string')
          return {ok:false,error:{code:'INVALID_ARGUMENT',message:'durable_step requires name. Prefer tools.step(name, async()=>...).'}};
        return handleRpc({type:'step',name:virtualArgs.name,args:{op:'get'}});
      }
      if(request.name==='result_handle'){
        const op=typeof virtualArgs.op==='string'?virtualArgs.op:'get';
        return handleRpc({type:'roundtrip',name:'result_'+op,args:virtualArgs});
      }
      if(request.name==='reusable_terminal')
        return handleRpc({type:'roundtrip',name:'terminal',args:virtualArgs});
      if(request.name==='reusable_browser')
        return handleRpc({type:'roundtrip',name:'browser',args:virtualArgs});
      if(request.name==='durable_fiber')
        return handleRpc({type:'roundtrip',name:'fiber',args:virtualArgs});
      if(request.name==='roundtrip_status')
        return handleRpc({type:'roundtrip',name:'status',args:{}});
      if(request.name==='harness_status')
        return handleRpc({type:'harness',name:'status',args:{}});
      if(request.name==='workflow_trace')
        return handleRpc({type:'trace',name:'current',args:{}});
      return runToolCall(request.name,request.args as Args);
    }
    return {ok:false,error:{code:'CODE_MODE_TOOL_DENIED',message:'Unknown or unavailable Code Mode operation.'}};
  };

  let worker:Worker|undefined;
  let timer:NodeJS.Timeout|undefined;
  let abortListener:(()=>void)|undefined;
  try {
    return await new Promise<ToolResult>((resolve)=>{
      let settled=false;
      const clean=()=>{
        if(timer)clearTimeout(timer);
        if(abortListener)outerSignal?.removeEventListener('abort',abortListener);
        closed=true;
      };
      const finish=(out:ToolResult)=>{
        if(settled)return;
        settled=true;clean();resolve(out);
        void worker?.terminate().catch(()=>undefined);
      };
      const finishPersisted=(status:'completed'|'failed'|'cancelled'|'uncertain',out:ToolResult,result:unknown)=>{
        if(settled)return;
        const writes:Promise<unknown>[]=[finishDurableExecution(execution,status,result,trace)];
        if(activeProjectSessionId&&projectObservations.length)
          writes.push(observeProjectSessionBatch(activeProjectSessionId,projectObservations));
        void Promise.allSettled(writes).finally(()=>finish(out));
      };
      const failureWithSteps=(code:string,message:string)=>{
        const out=blocked(code,message,steps);
        out.structuredContent={...out.structuredContent,workflowId,
          scheduler:{waitedMs:schedulerLease.waitedMs,activeLimitAtStart:schedulerLease.activeLimitAtStart}};
        return out;
      };
      const stop=(code:string,message:string)=>{
        if(settled)return;
        controller.abort();
        worker?.postMessage({type:'cancel'});
        const out=failureWithSteps(code,
          message+(activeTool?' One nested tool may still be running; verify effects before retrying.':''));
        finishPersisted(code==='CODE_MODE_CANCELLED'?'cancelled':'uncertain',out,{code,message});
      };
      abortListener=()=>stop('CODE_MODE_CANCELLED','The workflow was cancelled.');
      outerSignal?.addEventListener('abort',abortListener,{once:true});
      timer=setTimeout(()=>stop('CODE_MODE_TIMEOUT',
        'The Code Mode deadline expired. Use spawn_job for a long build or test.'),input.max_runtime_ms);

      try{
        worker=new Worker(new URL('./code-mode-worker.js',import.meta.url),{
          workerData:{source:input.source,deadline:Date.now()+input.max_runtime_ms},
          resourceLimits:{maxOldGenerationSizeMb:96,maxYoungGenerationSizeMb:16,stackSizeMb:2},
        });
      }catch(error){
        const message=error instanceof Error?error.message:'Could not start the sandbox.';
        const out=failureWithSteps('CODE_MODE_START_FAILED',message);
        finishPersisted('failed',out,{code:'CODE_MODE_START_FAILED',message});
        return;
      }
      worker.on('message',(message:WorkerMessage)=>{
        if(settled)return;
        if(message?.type==='rpc'){
          void handleRpc(message.request).then(reply=>{
            if(!settled)worker?.postMessage({type:'reply',id:message.id,reply});
          }).catch(error=>{
            if(!settled)worker?.postMessage({type:'reply',id:message.id,reply:{
              ok:false,error:{code:'CODE_MODE_BRIDGE_FAILED',
                message:error instanceof Error?error.message.slice(0,300):'RPC failed.'}}});
          });
          return;
        }
        if(message?.type==='error'){
          const failedSteps=steps.filter(s=>s.outcome==='error'||s.outcome==='uncertain');
          if(failedSteps.length){
            const last=failedSteps.at(-1)!;
            const messageText=message.message.slice(0,900);
            const out:ToolResult={
              content:[{type:'text',text:'CODE MODE STOPPED ON STEP ERROR | '+steps.length+
                ' local tool call(s) | '+(Date.now()-started)+' ms\n'+messageText}],
              structuredContent:{
                steps,completedCalls:steps.length,status:'stopped_on_step_error',
                allCallsSucceeded:false,pending:false,pendingJobs:[],
                executionCompleted:true,stoppedOnError:true,
                errorCode:last.errorCode??'TOOL_FAILED',
                workflowId,executionId:execution.id,traceId:trace.record.id,resumed,
                scheduler:{waitedMs:schedulerLease.waitedMs,activeLimitAtStart:schedulerLease.activeLimitAtStart},
                retries:trace.record.retries,replayedSteps:trace.record.replayedSteps,
                automaticRetry:{enabled:true,safeReadOnlyOnly:true,maxAttempts:3},
              },
            };
            finishPersisted(failedSteps.some(s=>s.outcome==='uncertain')?'uncertain':'failed',
              out,{message:messageText,errorCode:last.errorCode??'TOOL_FAILED'});
            return;
          }
          const out=failureWithSteps('CODE_MODE_SCRIPT_FAILED',message.message.slice(0,900));
          out.structuredContent={...out.structuredContent,executionId:execution.id,traceId:trace.record.id,resumed};
          finishPersisted('failed',out,{message:message.message.slice(0,900)});
          return;
        }
        if(message?.type==='done'){
          const envelope=message.value&&typeof message.value==='object'&&!Array.isArray(message.value)
            ?message.value as Record<string,unknown>:null;
          const resultValue=envelope&&Object.prototype.hasOwnProperty.call(envelope,'value')
            ?envelope.value:message.value;
          const unawaitedCalls=envelope&&typeof envelope.unawaitedCalls==='number'
            ?Math.max(0,Math.trunc(envelope.unawaitedCalls)):0;
          const fullRendered=limited(resultValue,Number.MAX_SAFE_INTEGER);
          const outputTruncated=fullRendered.length>MAX_FINAL_RESPONSE;
          const rendered=outputTruncated
            ?fullRendered.slice(0,MAX_FINAL_RESPONSE)+'\n[CODE MODE RESULT TRUNCATED: return a smaller summary or use a direct read tool for the omitted data.]'
            :fullRendered;
          const pending=pendingJobs.length>0;
          const failedSteps=steps.filter(s=>s.outcome==='error'||s.outcome==='uncertain');
          // A script may catch a failed tool response and continue. Do not
          // present that as an entirely clean coding/testing run.
          const status=pending?'pending_jobs':failedSteps.length?'completed_with_step_errors':'completed';
          const summary='CODE MODE '+status.toUpperCase().replaceAll('_',' ')+
            ' | '+steps.length+' local tool call(s) | '+(Date.now()-started)+' ms'+
            (failedSteps.length?' | '+failedSteps.length+' failed/uncertain step(s)':'')+
            (unawaitedCalls?' | drained '+unawaitedCalls+' detached call(s)':'')+
            (outputTruncated?' | final result truncated':'')+
            (resumed?' | durable resume':'')+
            (pending?' | inspect jobs: '+pendingJobs.join(', '):'');
          const out:ToolResult={
            content:[{type:'text',text:summary+'\n'+rendered}],
            structuredContent:{
              result:outputTruncated?undefined:resultValue,steps,completedCalls:steps.length,
              status,allCallsSucceeded:failedSteps.length===0,
              pending,pendingJobs,executionCompleted:!pending,
              unawaitedCallsDrained:unawaitedCalls,outputTruncated,
              workflowId,executionId:execution.id,traceId:trace.record.id,resumed,
              scheduler:{waitedMs:schedulerLease.waitedMs,activeLimitAtStart:schedulerLease.activeLimitAtStart},
              retries:trace.record.retries,replayedSteps:trace.record.replayedSteps,
              storage:{harnessStateMaxBytes:30*1024*1024,semanticCacheMaxBytes:64*1024*1024},
              automaticRetry:{enabled:true,safeReadOnlyOnly:true,maxAttempts:3},
            },
          };
          if(!failedSteps.length)
            void autoSaveSuccessfulWorkflow(input.source,steps.map(s=>s.tool)).catch(()=>undefined);
          finishPersisted('completed',out,{status,result:outputTruncated?'[truncated]':resultValue});
        }
      });
      worker.on('error',error=>{
        const message=error.message.slice(0,500);
        const out=failureWithSteps('CODE_MODE_WORKER_FAILED',message);
        finishPersisted(activeTool?'uncertain':'failed',out,{code:'CODE_MODE_WORKER_FAILED',message});
      });
      worker.on('exit',code=>{
        if(!settled){
          const message='The sandbox exited before returning a result (exit '+code+').';
          const out=failureWithSteps('CODE_MODE_WORKER_EXITED',message);
          finishPersisted(activeTool?'uncertain':'failed',out,{code:'CODE_MODE_WORKER_EXITED',message});
        }
      });
      if(outerSignal?.aborted)stop('CODE_MODE_CANCELLED','The workflow was cancelled.');
    });
  }finally{
    if(!controller.signal.aborted)controller.abort();
    schedulerLease.release();
  }
}
