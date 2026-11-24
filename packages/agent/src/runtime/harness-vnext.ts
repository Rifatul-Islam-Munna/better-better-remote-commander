/** BRC Harness vNext: durable Code Mode metadata/replay, safe retries,
 * saved workflows, lifecycle events, specialist execution roles and traces.
 * All persisted records share the 30 MiB harnessStateStore quota.
 */
import {createHash,randomUUID} from 'node:crypto';
import type {ToolResult} from 'brc-shared';
import {AgentError} from './errors.js';
import {harnessStateStore} from './bounded-local-store.js';
import {semanticSearchStatus} from './semantic-project-search.js';
import {codeModeScheduler} from './code-mode-scheduler.js';
import {readOnlyCoalescer} from './read-only-coalescer.js';
import {circuitBreakers} from './circuit-breakers.js';
import {projectAffinityStatus} from './project-affinity.js';
import {specialistPolicyStatus} from './specialist-policy.js';

const VERSION=1;
const EXECUTION_TTL=24*60*60_000;
const MAX_LEDGER_STEPS=96;
const MAX_REPLAY_REPLY=48*1024;
const MAX_TRACE_EVENTS=400;
const MAX_TRACE_DETAIL=2400;
const MAX_SNIPPETS=120;
const RETRY_CODES=new Set([
  'BUSY','SEARCH_BUSY','INDEX_CHANGED','SEARCH_TIMEOUT','SEARCH_START_FAILED',
  'PLUGIN_BUSY','PLUGIN_START_FAILED','LSP_BUSY','PARSER_BUSY','RESOURCE_BUSY',
  'WORKER_CAPACITY','RATE_LIMITED','TEMPORARY_FAILURE',
]);
const MUTATING_TOOLS=new Set([
  'write_file','edit_block','edit_files','apply_patch','create_directory','move_file','delete_file',
  'write_pdf','workspace_batch','replace_symbol','edit_code_ranges','worker_patch','worker_integrate',
  'git_stage','git_commit','git_push','call_plugin_tool',
]);
const FRESH_SOURCE_READS=new Set(['read_file','read_code_file','read_multiple_files','read_code_ranges']);

export type HarnessReply={ok:boolean;value?:unknown;error?:{code:string;message:string}};
export type DurableStepState='started'|'settled'|'error'|'uncertain';
export interface DurableStep {
  index:number;
  fingerprint:string;
  tool:string;
  readOnly:boolean;
  state:DurableStepState;
  startedAt:number;
  finishedAt?:number;
  attempts:number;
  reply?:HarnessReply;
  replayable:boolean;
}
export interface DurableExecution {
  version:1;
  id:string;
  invocationKey?:string;
  sourceHash:string;
  source:string;
  createdAt:number;
  touchedAt:number;
  status:'running'|'completed'|'failed'|'cancelled'|'uncertain';
  traceId:string;
  cursor:number;
  steps:DurableStep[];
  namedSteps?:Record<string,{value:unknown;storedAt:number;digest:string}>;
  resultDigest?:string;
  finishedAt?:number;
}
export interface WorkflowTraceEvent {
  at:number;
  type:string;
  tool?:string;
  durationMs?:number;
  detail?:unknown;
}
export interface WorkflowTraceRecord {
  version:1;
  id:string;
  executionId:string;
  sourceHash:string;
  startedAt:number;
  touchedAt:number;
  status:'running'|'completed'|'failed'|'cancelled'|'uncertain';
  retries:number;
  replayedSteps:number;
  events:WorkflowTraceEvent[];
  finishedAt?:number;
}
export interface SnippetRecord {
  version:1;
  id:string;
  name:string;
  description:string;
  tags:string[];
  source:string;
  createdAt:number;
  updatedAt:number;
  usedAt:number;
  uses:number;
  automatic:boolean;
  tools:string[];
}
export interface HarnessLifecycleContext {
  executionId:string;
  traceId:string;
  tool:string;
  args:Record<string,unknown>;
  readOnly:boolean;
  step:number;
}
export interface HarnessLifecycleResult {
  args:Record<string,unknown>;
  notes:string[];
}

const hash=(value:string)=>createHash('sha256').update(value).digest('hex');
const stable=(value:unknown):string=>{
  const normalize=(v:unknown):unknown=>{
    if(Array.isArray(v))return v.map(normalize);
    if(v&&typeof v==='object'){
      const out:Record<string,unknown>={};
      for(const key of Object.keys(v as Record<string,unknown>).sort())
        out[key]=normalize((v as Record<string,unknown>)[key]);
      return out;
    }
    return v;
  };
  try{return JSON.stringify(normalize(value));}catch{return String(value);}
};
const boundedDetail=(value:unknown):unknown=>{
  const text=stable(value);
  if(text.length<=MAX_TRACE_DETAIL)return value;
  return {truncated:true,preview:text.slice(0,MAX_TRACE_DETAIL)};
};
const errorCode=(result:ToolResult):string=>{
  const e=result.structuredContent?.error as {code?:unknown}|undefined;
  return typeof e?.code==='string'?e.code:'TOOL_FAILED';
};
const sleep=(ms:number)=>new Promise<void>(resolve=>setTimeout(resolve,ms));

export class WorkflowTrace {
  readonly record:WorkflowTraceRecord;
  private sincePersist=0;
  private constructor(record:WorkflowTraceRecord){this.record=record;}
  static async create(executionId:string,sourceHash:string,traceId:string=randomUUID()){
    const record:WorkflowTraceRecord={
      version:VERSION,id:traceId,executionId,sourceHash,startedAt:Date.now(),touchedAt:Date.now(),
      status:'running',retries:0,replayedSteps:0,events:[],
    };
    const trace=new WorkflowTrace(record);
    await trace.persist();
    return trace;
  }
  static async resume(executionId:string,sourceHash:string,traceId:string){
    const saved=await harnessStateStore.get<WorkflowTraceRecord>('trace',traceId);
    const valid=saved?.version===VERSION&&saved.executionId===executionId&&saved.sourceHash===sourceHash;
    const trace=valid?new WorkflowTrace(saved!):await WorkflowTrace.create(executionId,sourceHash,traceId);
    trace.record.status='running';trace.record.finishedAt=undefined;trace.record.touchedAt=Date.now();
    await trace.event('resume',{detail:{previousEvents:trace.record.events.length}});
    return trace;
  }
  async event(type:string,data:Omit<WorkflowTraceEvent,'at'|'type'>={}){
    const event:WorkflowTraceEvent={at:Date.now(),type,...data};
    if(event.detail!==undefined)event.detail=boundedDetail(event.detail);
    this.record.events.push(event);
    if(this.record.events.length>MAX_TRACE_EVENTS)this.record.events.splice(0,this.record.events.length-MAX_TRACE_EVENTS);
    this.record.touchedAt=Date.now();
    if(++this.sincePersist>=3||type==='error'||type==='retry'||type==='checkpoint'){
      this.sincePersist=0;await this.persist();
    }
  }
  async retry(tool:string,code:string,attempt:number,delayMs:number){
    this.record.retries++;
    await this.event('retry',{tool,detail:{code,attempt,delayMs}});
  }
  async replay(tool:string,index:number){
    this.record.replayedSteps++;
    await this.event('durable_replay',{tool,detail:{index}});
  }
  async finish(status:WorkflowTraceRecord['status'],detail?:unknown){
    this.record.status=status;this.record.finishedAt=Date.now();this.record.touchedAt=Date.now();
    if(detail!==undefined)this.record.events.push({at:Date.now(),type:'finish',detail:boundedDetail(detail)});
    await this.persist();
  }
  private async persist(){await harnessStateStore.put('trace',this.record.id,this.record);}
  snapshot(){return {
    traceId:this.record.id,executionId:this.record.executionId,status:this.record.status,
    retries:this.record.retries,replayedSteps:this.record.replayedSteps,
    events:this.record.events.slice(-40),
  };}
}

export async function beginDurableExecution(source:string,invocationKey?:string):Promise<{
  execution:DurableExecution;trace:WorkflowTrace;resumed:boolean;
}>{
  const sourceHash=hash(source);
  const records=await harnessStateStore.list<DurableExecution>('execution',64);
  const now=Date.now();
  const candidates=records.filter(r=>r.value?.version===VERSION&&r.value.sourceHash===sourceHash&&
    (r.value.status==='running'||(r.value.status==='uncertain'&&
      !r.value.steps.some(s=>s.state==='uncertain'&&!s.readOnly)))&&
    now-r.value.touchedAt<EXECUTION_TTL);
  const previous=invocationKey
    ?candidates.find(r=>r.value.invocationKey===invocationKey)
    :(candidates.length===1?candidates[0]:undefined);
  if(previous){
    const execution=previous.value;
    execution.cursor=0;execution.touchedAt=now;
    const trace=await WorkflowTrace.resume(execution.id,sourceHash,execution.traceId);
    trace.record.replayedSteps=0;
    await trace.event('resume_ledger',{detail:{storedSteps:execution.steps.length}});
    await harnessStateStore.put('execution',execution.id,execution);
    return {execution,trace,resumed:true};
  }
  const id=randomUUID(),traceId=randomUUID();
  const execution:DurableExecution={
    version:VERSION,id,invocationKey,sourceHash,source,createdAt:now,touchedAt:now,status:'running',
    traceId,cursor:0,steps:[],namedSteps:{},
  };
  await harnessStateStore.put('execution',id,execution);
  const trace=await WorkflowTrace.create(id,sourceHash,traceId);
  await trace.event('start');
  return {execution,trace,resumed:false};
}
export function fingerprintStep(tool:string,args:Record<string,unknown>){
  return hash(tool+'\0'+stable(args));
}
export async function durablePrepareStep(
  execution:DurableExecution,tool:string,args:Record<string,unknown>,readOnly:boolean,trace:WorkflowTrace,
):Promise<{index:number;replay?:HarnessReply;mustStop?:HarnessReply}>{
  const index=execution.cursor++;
  if(index>=MAX_LEDGER_STEPS)
    return {index,mustStop:{ok:false,error:{code:'CODE_MODE_CALL_LIMIT',message:'Durable workflow ledger step limit reached.'}}};
  const fingerprint=fingerprintStep(tool,args);
  const existing=execution.steps[index];
  if(existing){
    if(existing.fingerprint!==fingerprint||existing.tool!==tool){
      execution.status='uncertain';execution.touchedAt=Date.now();
      await harnessStateStore.put('execution',execution.id,execution);
      await trace.event('durable_divergence',{tool,detail:{index}});
      return {index,mustStop:{ok:false,error:{code:'DURABLE_REPLAY_DIVERGED',
        message:'Resumed workflow diverged from its durable call sequence. Start a fresh Code Mode workflow instead of replaying mutations.'}}};
    }
    if(existing.state==='settled'&&existing.readOnly&&FRESH_SOURCE_READS.has(tool)){
      await trace.event('durable_refresh_source_read',{tool,detail:{index}});
      return {index};
    }
    if(existing.state==='settled'&&existing.reply){
      await trace.replay(tool,index);
      return {index,replay:existing.reply};
    }
    if(existing.state==='settled'&&existing.readOnly){
      await trace.event('durable_rerun_read',{tool,detail:{index}});
      return {index};
    }
    if(existing.state==='started'||existing.state==='uncertain'){
      if(existing.readOnly){
        await trace.event('durable_rerun_read',{tool,detail:{index,state:existing.state}});
        return {index};
      }
      execution.status='uncertain';execution.touchedAt=Date.now();
      await harnessStateStore.put('execution',execution.id,execution);
      return {index,mustStop:{ok:false,error:{code:'DURABLE_MUTATION_UNCERTAIN',
        message:'A previous mutating step may have executed before interruption. Inspect current state; the harness will not replay it automatically.'}}};
    }
    if(existing.state==='error'&&!existing.readOnly){
      execution.status='uncertain';execution.touchedAt=Date.now();
      await harnessStateStore.put('execution',execution.id,execution);
      return {index,mustStop:{ok:false,error:{code:'DURABLE_MUTATION_REVIEW_REQUIRED',
        message:'A previous non-read-only step failed after execution began. Inspect its effects before starting a fresh workflow; it will not be retried automatically.'}}};
    }
    // Failed read-only steps are safe to attempt again.
  }
  execution.steps[index]={
    index,fingerprint,tool,readOnly,state:'started',startedAt:Date.now(),attempts:0,replayable:readOnly,
  };
  execution.touchedAt=Date.now();
  await harnessStateStore.put('execution',execution.id,execution);
  await trace.event('checkpoint',{tool,detail:{index,state:'started',readOnly}});
  return {index};
}
export async function durableSettleStep(
  execution:DurableExecution,index:number,reply:HarnessReply,attempts:number,trace:WorkflowTrace,
  stateOverride?:DurableStepState,
){
  const step=execution.steps[index];
  if(!step)return;
  step.finishedAt=Date.now();step.attempts=attempts;
  step.state=stateOverride??(reply.ok?'settled':'error');
  const encoded=stable(reply);
  if(encoded.length<=MAX_REPLAY_REPLY||!step.readOnly){
    step.reply=encoded.length<=MAX_REPLAY_REPLY?reply:reply.ok
      ?{ok:true,value:{durableSummary:'Mutation settled; full large result omitted from durable replay.'}}
      :{ok:false,error:reply.error};
    step.replayable=!!step.reply;
  }else{
    step.reply=undefined;step.replayable=false;
  }
  execution.touchedAt=Date.now();
  await harnessStateStore.put('execution',execution.id,execution);
  await trace.event('checkpoint',{tool:step.tool,detail:{index,state:step.state,attempts,replayable:step.replayable}});
}
export async function finishDurableExecution(
  execution:DurableExecution,status:DurableExecution['status'],result:unknown,trace:WorkflowTrace,
){
  execution.status=status;execution.finishedAt=Date.now();execution.touchedAt=Date.now();
  execution.resultDigest=hash(stable(result).slice(0,100_000));
  await harnessStateStore.put('execution',execution.id,execution);
  await trace.finish(status,result);
}

export async function durableNamedStepGet(
  execution:DurableExecution,name:string,trace:WorkflowTrace,
){
  const key=name.trim().slice(0,120);
  if(!key)throw new AgentError('INVALID_ARGUMENT','Durable step name is required.');
  const found=execution.namedSteps?.[key];
  if(found){
    await trace.event('named_step_replay',{detail:{name:key,storedAt:found.storedAt}});
    return {found:true,value:found.value,storedAt:found.storedAt,digest:found.digest};
  }
  await trace.event('named_step_miss',{detail:{name:key}});
  return {found:false};
}
export async function durableNamedStepSave(
  execution:DurableExecution,name:string,value:unknown,trace:WorkflowTrace,
){
  const key=name.trim().slice(0,120);
  if(!key)throw new AgentError('INVALID_ARGUMENT','Durable step name is required.');
  const encoded=stable(value);
  if(Buffer.byteLength(encoded,'utf8')>MAX_REPLAY_REPLY)
    throw new AgentError('DURABLE_STEP_TOO_LARGE','Named durable step value exceeds '+MAX_REPLAY_REPLY+' bytes; store a result handle instead.');
  execution.namedSteps??={};
  if(Object.keys(execution.namedSteps).length>=64&&!execution.namedSteps[key])
    throw new AgentError('DURABLE_STEP_LIMIT','At most 64 named durable steps are retained per Code Mode execution.');
  const storedAt=Date.now(),digest=hash(encoded);
  execution.namedSteps[key]={value,storedAt,digest};
  execution.touchedAt=storedAt;
  await harnessStateStore.put('execution',execution.id,execution);
  await trace.event('named_step_saved',{detail:{name:key,digest}});
  return {saved:true,name:key,storedAt,digest};
}

export async function beforeToolCall(ctx:HarnessLifecycleContext,trace:WorkflowTrace):Promise<HarnessLifecycleResult>{
  const notes:string[]=[];
  const args={...ctx.args};
  if(ctx.readOnly)notes.push('read-only');
  if(MUTATING_TOOLS.has(ctx.tool))notes.push('mutation-guarded');
  await trace.event('before_tool',{tool:ctx.tool,detail:{step:ctx.step,notes}});
  return {args,notes};
}
export async function afterToolCall(
  ctx:HarnessLifecycleContext,result:ToolResult,durationMs:number,attempts:number,trace:WorkflowTrace,
){
  await trace.event(result.isError?'tool_error':'after_tool',{
    tool:ctx.tool,durationMs,
    detail:{step:ctx.step,attempts,error:result.isError?errorCode(result):undefined,
      pending:result.structuredContent?.pending===true},
  });
}

export async function executeWithSafeRetry(
  tool:string,readOnly:boolean,invoke:()=>Promise<ToolResult>,trace:WorkflowTrace,
):Promise<{result:ToolResult;attempts:number}>{
  const maxAttempts=readOnly?3:1;
  let attempts=0,last:ToolResult|undefined;
  while(attempts<maxAttempts){
    attempts++;
    try{last=await invoke();}
    catch(error){
      const code=typeof (error as {code?:unknown})?.code==='string'?String((error as {code?:unknown}).code):'TOOL_THROW';
      if(!readOnly||attempts>=maxAttempts||!RETRY_CODES.has(code))throw error;
      const delay=[70,170,380][attempts-1]+Math.floor(Math.random()*35);
      await trace.retry(tool,code,attempts,delay);await sleep(delay);continue;
    }
    if(!last.isError)return {result:last,attempts};
    const code=errorCode(last);
    if(!readOnly||attempts>=maxAttempts||!RETRY_CODES.has(code))return {result:last,attempts};
    const delay=[70,170,380][attempts-1]+Math.floor(Math.random()*35);
    await trace.retry(tool,code,attempts,delay);await sleep(delay);
  }
  return {result:last!,attempts};
}

const snippetId=(name:string)=>hash(name.trim().toLowerCase()).slice(0,40);
function snippetTokens(value:string){
  return new Set(value.toLowerCase().split(/[^a-z0-9_]+/).filter(x=>x.length>1).slice(0,200));
}
export async function saveSnippet(input:{
  name:string;description?:string;tags?:string[];source:string;automatic?:boolean;tools?:string[];
}){
  const name=input.name.trim();
  if(name.length<2||name.length>100||input.source.length<1||input.source.length>50_000)
    throw new AgentError('INVALID_ARGUMENT','Snippet name must be 2-100 chars and source 1-50000 chars.');
  const id=snippetId(name),now=Date.now();
  const old=await harnessStateStore.get<SnippetRecord>('snippet',id);
  const record:SnippetRecord={
    version:VERSION,id,name,description:(input.description??'').slice(0,1000),
    tags:(input.tags??[]).filter(x=>typeof x==='string').map(x=>x.slice(0,80)).slice(0,20),
    source:input.source,createdAt:old?.createdAt??now,updatedAt:now,usedAt:old?.usedAt??now,
    uses:old?.uses??0,automatic:input.automatic===true,
    tools:(input.tools??[]).filter(x=>typeof x==='string').map(x=>x.slice(0,80)).slice(0,40),
  };
  await harnessStateStore.put('snippet',id,record);
  const all=await harnessStateStore.list<SnippetRecord>('snippet',MAX_SNIPPETS+40);
  for(const extra of all.slice(MAX_SNIPPETS))await harnessStateStore.remove('snippet',extra.id);
  return {id,name:record.name,description:record.description,tags:record.tags,automatic:record.automatic};
}
export async function getSnippet(idOrName:string){
  const id=/^[a-f0-9]{40}$/i.test(idOrName)?idOrName:snippetId(idOrName);
  const record=await harnessStateStore.get<SnippetRecord>('snippet',id);
  if(!record)return null;
  record.uses++;record.usedAt=Date.now();
  await harnessStateStore.put('snippet',id,record);
  return record;
}
export async function searchSnippets(query:string,limit=10){
  const q=snippetTokens(query);
  const all=await harnessStateStore.list<SnippetRecord>('snippet',MAX_SNIPPETS);
  return all.map(row=>{
    const r=row.value,hay=snippetTokens([r.name,r.description,...r.tags,...r.tools].join(' '));
    let score=0;for(const token of q)if(hay.has(token))score+=r.name.toLowerCase().includes(token)?5:2;
    if(!q.size)score=1;
    return {r,score};
  }).filter(x=>x.score>0).sort((a,b)=>b.score-a.score||b.r.usedAt-a.r.usedAt)
    .slice(0,Math.max(1,Math.min(25,limit))).map(({r,score})=>({
      id:r.id,name:r.name,description:r.description,tags:r.tags,tools:r.tools,
      uses:r.uses,usedAt:r.usedAt,automatic:r.automatic,score,
    }));
}
export async function autoSaveSuccessfulWorkflow(source:string,toolNames:string[]){
  if(toolNames.length<2||source.length>50_000)return null;
  const sourceHash=hash(source);
  return saveSnippet({
    name:'auto-'+sourceHash.slice(0,16),
    description:'Successful Code Mode workflow using '+[...new Set(toolNames)].slice(0,12).join(', '),
    tags:['automatic','code-mode'],source,automatic:true,tools:[...new Set(toolNames)],
  });
}
export async function selectSavedWorkflow(query:string){
  const hits=await searchSnippets(query,3);
  if(!hits.length||hits[0].score<4)return null;
  if(hits[1]&&hits[1].score===hits[0].score)return null;
  const record=await getSnippet(hits[0].id);
  return record?{...record,matchScore:hits[0].score}:null;
}

const ROLE_TOOLS:Record<string,Set<string>>={
  explorer:new Set([
    'read_file','read_multiple_files','read_code_file','list_directory','get_file_info','start_search',
    'get_more_search_results','project_context','project_map','context_pack','code_graph','lsp_query',
    'git_status','git_diff','git_log','ast_search','find_symbols','find_references','discover_checks',
  ]),
  reviewer:new Set([
    'read_file','read_multiple_files','read_code_file','list_directory','get_file_info','start_search',
    'get_more_search_results','project_context','context_pack','code_graph','lsp_query','git_status',
    'git_diff','git_log','ast_search','find_symbols','find_references','discover_checks',
  ]),
  tester:new Set([
    'read_file','read_multiple_files','read_code_file','list_directory','get_file_info','run_command',
    'discover_checks','run_project_checks','verify_project','git_status','git_diff','get_job','wait_jobs',
  ]),
  coder:new Set([
    'read_file','read_multiple_files','read_code_file','list_directory','get_file_info','start_search',
    'get_more_search_results','project_context','context_pack','code_graph','lsp_query','write_file',
    'edit_block','edit_files','apply_patch','create_directory','run_command','discover_checks',
    'run_project_checks','git_status','git_diff','ast_search','find_symbols','find_references',
  ]),
};
export function specialistAllows(role:string,tool:string,readOnly:boolean){
  const allowed=ROLE_TOOLS[role];
  if(!allowed)return false;
  if((role==='explorer'||role==='reviewer')&&!readOnly)return false;
  return allowed.has(tool);
}
export function specialistRoles(){
  return Object.entries(ROLE_TOOLS).map(([role,tools])=>({role,tools:[...tools]}));
}

export async function harnessVNextStatus(){
  const [store,semantic,traces,snippets,executions]=await Promise.all([
    harnessStateStore.status(),semanticSearchStatus(),
    harnessStateStore.list<WorkflowTraceRecord>('trace',12),
    harnessStateStore.list<SnippetRecord>('snippet',12),
    harnessStateStore.list<DurableExecution>('execution',12),
  ]);
  return {
    enabled:true,store,semantic,
    durableCodeMode:{ttlMs:EXECUTION_TTL,maxLedgerSteps:MAX_LEDGER_STEPS,
      recent:executions.map(x=>({id:x.value.id,status:x.value.status,touchedAt:x.value.touchedAt,steps:x.value.steps.length}))},
    snippets:{max:MAX_SNIPPETS,recent:snippets.map(x=>({id:x.value.id,name:x.value.name,uses:x.value.uses,automatic:x.value.automatic}))},
    tracing:{maxEventsPerTrace:MAX_TRACE_EVENTS,recent:traces.map(x=>({id:x.value.id,status:x.value.status,retries:x.value.retries,replayedSteps:x.value.replayedSteps}))},
    retry:{maxAttempts:3,retryCodes:[...RETRY_CODES],mutationsAutomaticallyRetried:false},
    concurrency:codeModeScheduler.status(),
    coalescing:readOnlyCoalescer.status(),
    circuitBreakers:circuitBreakers.status(),
    projectAffinity:projectAffinityStatus(),
    lifecycleHooks:['beforeToolCall','afterToolCall','onRetry','checkpoint','finish'],
    specialistAgents:specialistRoles(),specialistPolicy:specialistPolicyStatus(),
    note:'Specialist roles are mandatory/automatic for complex or multi-domain Code Mode work. They are isolated local execution roles using BRC tools; they do not spawn another LLM or require a relay change.',
  };
}
