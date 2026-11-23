/** Agent-local round-trip accelerator.
 * Persistent project sessions, local result handles, named reusable resource aliases
 * and durable fiber metadata. All records share harnessStateStore's 30 MiB quota.
 */
import {createHash,randomUUID} from 'node:crypto';
import path from 'node:path';
import {AgentError} from './errors.js';
import {harnessStateStore} from './bounded-local-store.js';
import {rootPath} from './workspace.js';

const VERSION=1;
const MAX_PROJECT_STATE_BYTES=96*1024;
const MAX_RECENT_ITEMS=32;
const MAX_RESULT_BYTES=2*1024*1024;
const MAX_RESULT_READ=32*1024;
const MAX_RESULT_HITS=20;
const MAX_FIBER_STEPS=50;
const MAX_FIBERS=64;
const MAX_ALIASES=96;
const MAX_PROJECT_SESSIONS=64;

const digest=(value:string)=>createHash('sha256').update(value).digest('hex');
function json(value:unknown):string{
  try{return JSON.stringify(value);}catch{return JSON.stringify({unserializable:true});}
}
function boundedText(value:string,max:number){
  return value.length<=max?value:value.slice(0,max)+'…';
}
function cleanName(value:string,label:string,max=100){
  const out=value.trim();
  if(out.length<1||out.length>max)throw new AgentError('INVALID_ARGUMENT',label+' must be 1-'+max+' characters.');
  return out;
}
function secretKey(key:string){
  return /(?:password|passwd|secret|token|authorization|cookie|api[_-]?key|private[_-]?key|access[_-]?key)/i.test(key);
}
function sanitize(value:unknown,depth=0,maxString=16_000):unknown{
  if(depth>5)return '[depth-limited]';
  if(value===null||typeof value==='number'||typeof value==='boolean')return value;
  if(typeof value==='string'){
    let text=value;
    text=text.replace(/\bBearer\s+[A-Za-z0-9._~+\/-]{12,}/gi,'Bearer [REDACTED]');
    text=text.replace(/\bbrc_(?:ak|dev)_[A-Za-z0-9_-]{10,}/gi,'[REDACTED_BRC_KEY]');
    text=text.replace(/\b(?:sk|pk)_[A-Za-z0-9_-]{16,}/g,'[REDACTED_KEY]');
    return boundedText(text,maxString);
  }
  if(Array.isArray(value))return value.slice(0,128).map(x=>sanitize(x,depth+1,maxString));
  if(typeof value==='object'){
    const out:Record<string,unknown>={};
    for(const [key,item] of Object.entries(value as Record<string,unknown>).slice(0,128))
      out[key]=secretKey(key)?'[REDACTED]':sanitize(item,depth+1,maxString);
    return out;
  }
  return String(value).slice(0,1000);
}
function assertStateSize(value:unknown){
  const bytes=Buffer.byteLength(json(value),'utf8');
  if(bytes>MAX_PROJECT_STATE_BYTES)
    throw new AgentError('PROJECT_SESSION_TOO_LARGE','Persistent project session state exceeds '+MAX_PROJECT_STATE_BYTES+' bytes.');
}
function uniqueRecent(values:string[],item:string){
  const normalized=item.slice(0,500);
  return [normalized,...values.filter(x=>x!==normalized)].slice(0,MAX_RECENT_ITEMS);
}

export interface ProjectSessionRecord {
  version:1;
  id:string;
  key:string;
  name:string;
  root?:string;
  state:Record<string,unknown>;
  createdAt:number;
  updatedAt:number;
  lastUsedAt:number;
  recentTools:string[];
  recentPaths:string[];
  jobIds:string[];
  terminalSessionIds:string[];
}
function projectSessionId(key:string){return digest('project-session\0'+key.toLowerCase()).slice(0,40);}
export async function openProjectSession(input:{name?:string;root?:string;state?:Record<string,unknown>}){
  if(!input.name&&!input.root)throw new AgentError('INVALID_ARGUMENT','Project session requires name or root.');
  const root=input.root?await rootPath(input.root):undefined;
  const name=cleanName(input.name??path.basename(root!),'Project session name');
  const key=(root??name).toLowerCase();
  const id=projectSessionId(key),now=Date.now();
  const old=await harnessStateStore.get<ProjectSessionRecord>('session',id);
  const state=sanitize(input.state??old?.state??{}) as Record<string,unknown>;
  assertStateSize(state);
  const record:ProjectSessionRecord=old&&old.version===VERSION?{
    ...old,name,root:root??old.root,state,updatedAt:now,lastUsedAt:now,
  }:{
    version:VERSION,id,key,name,root,state,createdAt:now,updatedAt:now,lastUsedAt:now,
    recentTools:[],recentPaths:[],jobIds:[],terminalSessionIds:[],
  };
  await harnessStateStore.put('session',id,record);
  const rows=await harnessStateStore.list<ProjectSessionRecord>('session',MAX_PROJECT_SESSIONS+20);
  for(const extra of rows.slice(MAX_PROJECT_SESSIONS))await harnessStateStore.remove('session',extra.id);
  return record;
}
export async function getProjectSession(idOrName:string){
  const direct=await harnessStateStore.get<ProjectSessionRecord>('session',idOrName);
  if(direct)return direct;
  const rows=await harnessStateStore.list<ProjectSessionRecord>('session',MAX_PROJECT_SESSIONS);
  return rows.find(x=>x.value.name.toLowerCase()===idOrName.toLowerCase()||
    x.value.root?.toLowerCase()===idOrName.toLowerCase())?.value??null;
}
export async function updateProjectSession(idOrName:string,patch:Record<string,unknown>){
  const record=await getProjectSession(idOrName);
  if(!record)throw new AgentError('PROJECT_SESSION_NOT_FOUND','Persistent project session was not found.');
  const next=sanitize({...record.state,...patch}) as Record<string,unknown>;
  assertStateSize(next);
  record.state=next;record.updatedAt=Date.now();record.lastUsedAt=Date.now();
  await harnessStateStore.put('session',record.id,record);
  return record;
}
export async function listProjectSessions(limit=20){
  return (await harnessStateStore.list<ProjectSessionRecord>('session',Math.max(1,Math.min(64,limit))))
    .map(x=>({id:x.value.id,name:x.value.name,root:x.value.root??null,updatedAt:x.value.updatedAt,
      lastUsedAt:x.value.lastUsedAt,stateKeys:Object.keys(x.value.state).slice(0,40),
      recentTools:x.value.recentTools.slice(0,12),recentPaths:x.value.recentPaths.slice(0,12),
      jobIds:x.value.jobIds.slice(0,8),terminalSessionIds:x.value.terminalSessionIds.slice(0,8)}));
}
function candidatePaths(args:Record<string,unknown>):string[]{
  const keys=['path','file_path','root','cwd','source','destination'];
  const out:string[]=[];
  for(const key of keys)if(typeof args[key]==='string'&&path.isAbsolute(args[key] as string))
    out.push(path.resolve(args[key] as string));
  if(Array.isArray(args.paths))for(const item of args.paths)
    if(typeof item==='string'&&path.isAbsolute(item))out.push(path.resolve(item));
  return out.slice(0,12);
}
export async function observeProjectSession(
  id:string,tool:string,args:Record<string,unknown>,meta:Record<string,unknown>={},
){
  return observeProjectSessionBatch(id,[{tool,args,meta}]);
}
export async function observeProjectSessionBatch(
  id:string,events:Array<{tool:string;args:Record<string,unknown>;meta?:Record<string,unknown>}>,
){
  const record=await harnessStateStore.get<ProjectSessionRecord>('session',id);
  if(!record)return null;
  for(const event of events.slice(-64)){
    record.recentTools=uniqueRecent(record.recentTools,event.tool);
    for(const p of candidatePaths(event.args))record.recentPaths=uniqueRecent(record.recentPaths,p);
    const meta=event.meta??{};
    if(typeof meta.jobId==='string')record.jobIds=uniqueRecent(record.jobIds,meta.jobId);
    if(typeof meta.sessionId==='string')record.terminalSessionIds=uniqueRecent(record.terminalSessionIds,meta.sessionId);
  }
  record.lastUsedAt=Date.now();record.updatedAt=Date.now();
  await harnessStateStore.put('session',record.id,record);
  return record;
}

export interface ResultHandleRecord {
  version:1;
  id:string;
  label:string;
  createdAt:number;
  touchedAt:number;
  bytes:number;
  originalBytes:number;
  partial:boolean;
  text:string;
}
export async function putResultHandle(value:unknown,label='result'){
  const safe=sanitize(value,0,MAX_RESULT_BYTES);
  let text=json(safe),originalBytes=Buffer.byteLength(text,'utf8'),partial=false;
  if(originalBytes>MAX_RESULT_BYTES){
    text=text.slice(0,MAX_RESULT_BYTES);
    partial=true;
  }
  const id=randomUUID().replaceAll('-','');
  const record:ResultHandleRecord={version:VERSION,id,label:label.slice(0,120),createdAt:Date.now(),
    touchedAt:Date.now(),bytes:Buffer.byteLength(text,'utf8'),originalBytes,partial,text};
  await harnessStateStore.put('result',id,record);
  return {id,label:record.label,bytes:record.bytes,originalBytes,partial,
    local:true,storageQuotaBytes:30*1024*1024};
}
export async function maybePutResultHandle(value:unknown,label:string,threshold=56_000){
  const size=Buffer.byteLength(json(sanitize(value,0,MAX_RESULT_BYTES)),'utf8');
  return size>threshold?putResultHandle(value,label):null;
}
export async function readResultHandle(input:{id:string;offset?:number;length?:number;query?:string}){
  const record=await harnessStateStore.get<ResultHandleRecord>('result',input.id);
  if(!record)throw new AgentError('RESULT_HANDLE_NOT_FOUND','Local result handle was evicted or does not exist.');
  record.touchedAt=Date.now();
  await harnessStateStore.put('result',record.id,record);
  const length=Math.max(200,Math.min(MAX_RESULT_READ,input.length??12_000));
  if(input.query?.trim()){
    const q=input.query.toLowerCase();
    const lower=record.text.toLowerCase();
    const hits:{offset:number;text:string}[]=[];
    let at=0;
    while(hits.length<MAX_RESULT_HITS){
      const found=lower.indexOf(q,at);
      if(found<0)break;
      const start=Math.max(0,found-240),end=Math.min(record.text.length,found+q.length+760);
      hits.push({offset:found,text:record.text.slice(start,end)});
      at=found+Math.max(1,q.length);
    }
    return {id:record.id,label:record.label,query:input.query,hits,partial:record.partial,
      bytes:record.bytes,originalBytes:record.originalBytes};
  }
  const offset=Math.max(0,Math.min(record.text.length,input.offset??0));
  const text=record.text.slice(offset,offset+length),nextOffset=offset+text.length;
  return {id:record.id,label:record.label,offset,nextOffset,hasMore:nextOffset<record.text.length,
    text,bytes:record.bytes,originalBytes:record.originalBytes,partial:record.partial};
}
export async function dropResultHandle(id:string){
  await harnessStateStore.remove('result',id);return {id,removed:true};
}

export type ResourceKind='terminal'|'browser';
export interface ResourceAliasRecord {
  version:1;
  id:string;
  kind:ResourceKind;
  name:string;
  reference:Record<string,unknown>;
  metadata:Record<string,unknown>;
  createdAt:number;
  updatedAt:number;
}
function aliasId(kind:ResourceKind,name:string){return digest('resource-alias\0'+kind+'\0'+name.toLowerCase()).slice(0,40);}
export async function saveResourceAlias(input:{
  kind:ResourceKind;name:string;reference:Record<string,unknown>;metadata?:Record<string,unknown>;
}){
  const name=cleanName(input.name,'Reusable session name');
  const id=aliasId(input.kind,name),now=Date.now();
  const old=await harnessStateStore.get<ResourceAliasRecord>('alias',id);
  const record:ResourceAliasRecord={
    version:VERSION,id,kind:input.kind,name,reference:sanitize(input.reference) as Record<string,unknown>,
    metadata:sanitize(input.metadata??{}) as Record<string,unknown>,createdAt:old?.createdAt??now,updatedAt:now,
  };
  await harnessStateStore.put('alias',id,record);
  const rows=await harnessStateStore.list<ResourceAliasRecord>('alias',MAX_ALIASES+20);
  for(const extra of rows.slice(MAX_ALIASES))await harnessStateStore.remove('alias',extra.id);
  return record;
}
export async function getResourceAlias(kind:ResourceKind,name:string){
  return harnessStateStore.get<ResourceAliasRecord>('alias',aliasId(kind,name));
}
export async function listResourceAliases(kind?:ResourceKind){
  return (await harnessStateStore.list<ResourceAliasRecord>('alias',MAX_ALIASES))
    .map(x=>x.value).filter(x=>!kind||x.kind===kind)
    .map(x=>({id:x.id,kind:x.kind,name:x.name,reference:x.reference,metadata:x.metadata,updatedAt:x.updatedAt}));
}
export async function forgetResourceAlias(kind:ResourceKind,name:string){
  await harnessStateStore.remove('alias',aliasId(kind,name));return {kind,name,removed:true};
}

export interface FiberRecord {
  version:1;
  id:string;
  name:string;
  planHash:string;
  steps:Array<Record<string,unknown>>;
  status:'created'|'running'|'succeeded'|'failed'|'cancelled'|'interrupted'|'uncertain';
  continueOnError:boolean;
  timeoutMs:number;
  createdAt:number;
  updatedAt:number;
  currentJobId?:string;
  jobStartStep?:number;
  jobIds:string[];
  completedSteps:number;
  lastJobStatus?:string;
}
function fiberId(name:string){return digest('fiber\0'+name.toLowerCase()).slice(0,40);}
export async function createFiber(input:{
  name:string;steps:Array<Record<string,unknown>>;continueOnError?:boolean;timeoutMs?:number;
}){
  const name=cleanName(input.name,'Fiber name');
  if(!Array.isArray(input.steps)||input.steps.length<1||input.steps.length>MAX_FIBER_STEPS)
    throw new AgentError('INVALID_ARGUMENT','Fiber requires 1-'+MAX_FIBER_STEPS+' steps.');
  const safeSteps=sanitize(input.steps) as Array<Record<string,unknown>>;
  const planHash=digest(json(safeSteps));
  const id=fiberId(name),now=Date.now();
  const old=await harnessStateStore.get<FiberRecord>('fiber',id);
  if(old&&old.planHash!==planHash&&['running','uncertain'].includes(old.status))
    throw new AgentError('FIBER_PLAN_CONFLICT','A different active fiber plan already uses this name.');
  const record:FiberRecord=old&&old.planHash===planHash?old:{
    version:VERSION,id,name,planHash,steps:safeSteps,status:'created',
    continueOnError:input.continueOnError===true,timeoutMs:Math.max(1000,Math.min(6*60*60_000,input.timeoutMs??30*60_000)),
    createdAt:now,updatedAt:now,jobIds:[],completedSteps:0,
  };
  record.updatedAt=now;
  await harnessStateStore.put('fiber',id,record);
  const rows=await harnessStateStore.list<FiberRecord>('fiber',MAX_FIBERS+20);
  for(const extra of rows.slice(MAX_FIBERS))if(!['running','uncertain'].includes(extra.value.status))
    await harnessStateStore.remove('fiber',extra.id);
  return record;
}
export async function getFiber(nameOrId:string){
  const direct=await harnessStateStore.get<FiberRecord>('fiber',nameOrId);
  if(direct)return direct;
  return harnessStateStore.get<FiberRecord>('fiber',fiberId(nameOrId));
}
export async function updateFiber(nameOrId:string,patch:Partial<FiberRecord>){
  const record=await getFiber(nameOrId);
  if(!record)throw new AgentError('FIBER_NOT_FOUND','Durable fiber was not found.');
  Object.assign(record,patch,{updatedAt:Date.now()});
  await harnessStateStore.put('fiber',record.id,record);
  return record;
}
export async function listFibers(limit=20){
  return (await harnessStateStore.list<FiberRecord>('fiber',Math.max(1,Math.min(MAX_FIBERS,limit))))
    .map(x=>({id:x.value.id,name:x.value.name,status:x.value.status,completedSteps:x.value.completedSteps,
      steps:x.value.steps.length,currentJobId:x.value.currentJobId??null,jobIds:x.value.jobIds.slice(-8),
      updatedAt:x.value.updatedAt}));
}

export async function roundTripAcceleratorStatus(){
  const [store,sessions,aliases,fibers,results]=await Promise.all([
    harnessStateStore.status(),
    harnessStateStore.list<ProjectSessionRecord>('session',10),
    harnessStateStore.list<ResourceAliasRecord>('alias',10),
    harnessStateStore.list<FiberRecord>('fiber',10),
    harnessStateStore.list<ResultHandleRecord>('result',10),
  ]);
  return {
    store,
    projectSessions:{max:MAX_PROJECT_SESSIONS,recent:sessions.map(x=>({id:x.value.id,name:x.value.name,root:x.value.root??null,updatedAt:x.value.updatedAt}))},
    resultHandles:{maxRecordBytes:MAX_RESULT_BYTES,recent:results.map(x=>({id:x.value.id,label:x.value.label,bytes:x.value.bytes,partial:x.value.partial,touchedAt:x.value.touchedAt}))},
    reusableSessions:{max:MAX_ALIASES,recent:aliases.map(x=>({kind:x.value.kind,name:x.value.name,updatedAt:x.value.updatedAt}))},
    fibers:{max:MAX_FIBERS,maxSteps:MAX_FIBER_STEPS,recent:fibers.map(x=>({id:x.value.id,name:x.value.name,status:x.value.status,completedSteps:x.value.completedSteps,steps:x.value.steps.length}))},
  };
}
