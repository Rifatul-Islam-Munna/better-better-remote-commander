/** Temporary Git worktrees for truly parallel writer tasks.
 * Worktrees live only under <project>/.brc-worker-worktrees, are ignored by BRC
 * project scans, and are removed on capture/release/cancel/finish. A stale
 * agent-owned worktree directory is swept on the next multi-writer plan.
 */
import fs from 'node:fs/promises';
import path from 'node:path';
import {execFile} from 'node:child_process';
import {randomBytes} from 'node:crypto';
import {AgentError} from './errors.js';
import {readBounded,decodeCode,sha256} from './file-safety.js';
import {sourceCandidate} from './languages.js';
import {BoundedLocalStore} from './bounded-local-store.js';

const OWNED='.brc-owned';
const OWNED_VALUE='brc-worker-worktrees-v1\n';
const ROOT_NAME='.brc-worker-worktrees';
const MAX_FILE=2*1024*1024;
const MAX_TOTAL=2*1024*1024;
const TIMEOUT=20_000;
const EXCLUDE_MARKER='# BRC temporary worker worktrees';
const EXCLUDE_RULE='/.brc-worker-worktrees/';
const cleanupRegistry=new BoundedLocalStore('worker-worktree-cleanup',1024*1024,16*1024);

export interface WorktreeAssessment {
  enabled:boolean;
  reason:string;
  head?:string;
}
export interface WorktreeLease {
  root:string;
  path:string;
  planId:string;
  taskId:string;
  generation:number;
  head:string;
}
export interface CapturedWorktree {
  changes:Array<{path:string;expected_sha256:string|null;content:string}>;
  files:string[];
  bytes:number;
}
let created=0,cleaned=0,captured=0,orphanSweeps=0,cleanupFailures=0;

function safeSegment(v:string){return v.replace(/[^A-Za-z0-9_.-]/g,'_').slice(0,80)||'task';}
function base(root:string){return path.join(root,ROOT_NAME);}
function planDir(root:string,planId:string){return path.join(base(root),safeSegment(planId));}
function registryId(lease:WorktreeLease){
 return (safeSegment(lease.planId)+'.'+safeSegment(lease.taskId)+'.'+lease.generation+'.'+safeSegment(path.basename(lease.path))).slice(0,128);
}
async function ensureGitExclude(root:string){
 const file=path.join(root,'.git','info','exclude');
 const raw=await fs.readFile(file,'utf8').catch(()=>'');
 if(raw.includes(EXCLUDE_MARKER+'\n'+EXCLUDE_RULE))return;
 await fs.mkdir(path.dirname(file),{recursive:true});
 const prefix=raw&&!raw.endsWith('\n')?'\n':'';
 await fs.appendFile(file,prefix+EXCLUDE_MARKER+'\n'+EXCLUDE_RULE+'\n','utf8');
}
async function removeGitExclude(root:string){
 const file=path.join(root,'.git','info','exclude');
 const raw=await fs.readFile(file,'utf8').catch(()=>null);
 if(raw===null)return;
 const managed=EXCLUDE_MARKER+'\n'+EXCLUDE_RULE+'\n';
 const next=raw.replace(managed,'').replace(EXCLUDE_MARKER+'\r\n'+EXCLUDE_RULE+'\r\n','');
 if(next!==raw)await fs.writeFile(file,next,'utf8').catch(()=>{});
}
async function cleanupBaseIfEmpty(root:string){
 const dir=base(root);
 const entries=await fs.readdir(dir).catch(()=>[]);
 const other=entries.filter(x=>x!==OWNED);
 if(other.length)return;
 await fs.rm(dir,{recursive:true,force:true}).catch(()=>{});
 await removeGitExclude(root);
}
function runGit(cwd:string,args:string[],timeout=TIMEOUT):Promise<string>{
 return new Promise((resolve,reject)=>{
  execFile('git',args,{cwd,timeout,maxBuffer:4*1024*1024,windowsHide:true},(error,stdout,stderr)=>{
   if(error){
    const message=String(stderr||stdout||error.message).trim().slice(0,1000);
    reject(new AgentError('WORKTREE_GIT_FAILED',message||'Git worktree operation failed.',true));
    return;
   }
   resolve(String(stdout));
  });
 });
}
async function ensureOwnedRoot(root:string){
 const dir=base(root);
 await ensureGitExclude(root);
 await fs.mkdir(dir,{recursive:true});
 const marker=path.join(dir,OWNED);
 const existing=await fs.readFile(marker,'utf8').catch(()=>null);
 if(existing===null)await fs.writeFile(marker,OWNED_VALUE,{flag:'wx'}).catch(async e=>{
  const now=await fs.readFile(marker,'utf8').catch(()=>null);
  if(now!==OWNED_VALUE)throw e;
 });
 else if(existing!==OWNED_VALUE)
  throw new AgentError('WORKTREE_ROOT_UNSAFE','Temporary worktree directory exists without the BRC ownership marker.');
 return dir;
}
async function removeOne(root:string,wt:string){
 try{await runGit(root,['worktree','remove','--force',wt]);}
 catch{await fs.rm(wt,{recursive:true,force:true,maxRetries:3,retryDelay:40}).catch(()=>{});}
 await runGit(root,['worktree','prune']).catch(()=>{});
 cleaned++;
}
export async function sweepWorkerWorktrees(root:string){
 const dir=base(root);
 const marker=await fs.readFile(path.join(dir,OWNED),'utf8').catch(()=>null);
 if(marker===null)return {removed:0};
 if(marker!==OWNED_VALUE)throw new AgentError('WORKTREE_ROOT_UNSAFE','Refusing to sweep an unowned worktree directory.');
 let removed=0;
 const plans=await fs.readdir(dir,{withFileTypes:true}).catch(()=>[]);
 for(const p of plans){
  if(!p.isDirectory())continue;
  const pd=path.join(dir,p.name);
  const entries=await fs.readdir(pd,{withFileTypes:true}).catch(()=>[]);
  for(const entry of entries){
   if(!entry.isDirectory())continue;
   await removeOne(root,path.join(pd,entry.name)).catch(()=>{cleanupFailures++;});
   removed++;
  }
  await fs.rm(pd,{recursive:true,force:true}).catch(()=>{});
 }
 if(removed)orphanSweeps++;
 await cleanupBaseIfEmpty(root);
 return {removed};
}
export async function assessWorkerWorktrees(root:string):Promise<WorktreeAssessment>{
 try{
  await startupCleanup;
  const marker=await fs.readFile(path.join(base(root),OWNED),'utf8').catch(()=>null);
  if(marker===OWNED_VALUE)await sweepWorkerWorktrees(root);
  const top=(await runGit(root,['rev-parse','--show-toplevel'])).trim();
  if(path.resolve(top)!==path.resolve(root))
   return {enabled:false,reason:'project root is not the Git top-level directory'};
  const status=await runGit(root,['status','--porcelain=v1','--untracked-files=all']);
  if(status.trim())return {enabled:false,reason:'Git working tree is not clean; use staged worker patches instead'};
  const head=(await runGit(root,['rev-parse','HEAD'])).trim();
  if(!/^[a-f0-9]{40,64}$/i.test(head))return {enabled:false,reason:'Git HEAD could not be resolved'};
  return {enabled:true,reason:'clean Git root with independent writer tasks',head};
 }catch(error){
  return {enabled:false,reason:error instanceof Error?error.message.slice(0,300):'Git worktrees unavailable'};
 }
}
export async function createWriterWorktree(
 root:string,planId:string,taskId:string,generation:number,head:string,
):Promise<WorktreeLease>{
 await startupCleanup;
 await ensureOwnedRoot(root);
 const pd=planDir(root,planId);
 await fs.mkdir(pd,{recursive:true});
 const wt=path.join(pd,safeSegment(taskId)+'-g'+generation+'-'+randomBytes(3).toString('hex'));
 await runGit(root,['worktree','add','--detach',wt,head]);
 const lease={root,path:wt,planId,taskId,generation,head};
 try{await cleanupRegistry.put('lease',registryId(lease),lease);}
 catch(error){await removeOne(root,wt).catch(()=>{});throw error;}
 created++;
 return lease;
}
function safeRelative(value:string){
 const normalized=value.replace(/\\/g,'/');
 if(!normalized||path.isAbsolute(value)||normalized==='..'||normalized.startsWith('../')||normalized.includes('/../'))
  throw new AgentError('WORKTREE_PATH_UNSAFE','Worktree change escaped its temporary checkout.');
 return normalized;
}
export async function captureWriterWorktree(lease:WorktreeLease):Promise<CapturedWorktree>{
 const raw=await runGit(lease.path,['status','--porcelain=v1','-z','--untracked-files=all']);
 const parts=raw.split('\0').filter(Boolean);
 const changed:string[]=[];
 for(let i=0;i<parts.length;i++){
  const rec=parts[i];
  if(rec.length<4)continue;
  const x=rec[0],y=rec[1],rel=safeRelative(rec.slice(3));
  if(x==='R'||x==='C'||y==='R'||y==='C'){
   i++; // porcelain -z includes the second rename/copy path
   throw new AgentError('WORKTREE_COMPLEX_CHANGE','Renames/copies are not auto-captured; submit an explicit worker patch instead.');
  }
  if(x==='D'||y==='D')throw new AgentError('WORKTREE_DELETE_UNSUPPORTED',
    'Deleted files are not auto-captured from temporary worktrees; submit an explicit patch instead.');
  changed.push(rel);
 }
 const unique=[...new Set(changed)];
 const changes:CapturedWorktree['changes']=[];
 let bytes=0;
 for(const rel of unique){
  const wtFile=path.resolve(lease.path,rel);
  if(!sourceCandidate(wtFile))throw new AgentError('BINARY_PATCH_DENIED','Temporary worktree capture supports UTF-8 source/config files only.');
  const data=await readBounded(wtFile,MAX_FILE);
  bytes+=data.length;
  if(bytes>MAX_TOTAL)throw new AgentError('PATCH_TOO_LARGE','Captured worktree changes exceed 2 MiB.');
  const content=decodeCode(data);
  const original=path.resolve(lease.root,rel);
  const before=await fs.readFile(original).catch((e:NodeJS.ErrnoException)=>{
   if(e.code==='ENOENT')return null;throw e;
  });
  changes.push({path:rel,expected_sha256:before?sha256(before):null,content});
 }
 captured++;
 return {changes,files:unique,bytes};
}
export async function cleanupWriterWorktree(lease:WorktreeLease|undefined){
 if(!lease)return;
 const id=registryId(lease);
 try{
  await removeOne(lease.root,lease.path);
  const pd=planDir(lease.root,lease.planId);
  const left=await fs.readdir(pd).catch(()=>[]);
  if(!left.length)await fs.rm(pd,{recursive:true,force:true}).catch(()=>{});
  await cleanupBaseIfEmpty(lease.root);
 }catch{cleanupFailures++;}
 finally{await cleanupRegistry.remove('lease',id).catch(()=>{});}
}
export async function cleanupPlanWorktrees(root:string,planId:string){
 const pd=planDir(root,planId);
 const entries=await fs.readdir(pd,{withFileTypes:true}).catch(()=>[]);
 for(const entry of entries)if(entry.isDirectory())
  await removeOne(root,path.join(pd,entry.name)).catch(()=>{cleanupFailures++;});
 await fs.rm(pd,{recursive:true,force:true}).catch(()=>{});
 const rows=await cleanupRegistry.list<WorktreeLease>('lease',128).catch(()=>[]);
 for(const row of rows)if(row.value.root===root&&row.value.planId===planId)
  await cleanupRegistry.remove('lease',row.id).catch(()=>{});
 await cleanupBaseIfEmpty(root);
}
export async function cleanupRegisteredWorkerWorktrees(){
 const rows=await cleanupRegistry.list<WorktreeLease>('lease',128).catch(()=>[]);
 for(const row of rows){
  const lease=row.value;
  if(!lease||typeof lease.root!=='string'||typeof lease.path!=='string')continue;
  const expected=path.resolve(planDir(lease.root,lease.planId));
  const actual=path.resolve(lease.path);
  if(actual!==expected&&!actual.startsWith(expected+path.sep)){
   await cleanupRegistry.remove('lease',row.id).catch(()=>{});
   continue;
  }
  await cleanupWriterWorktree(lease).catch(()=>{cleanupFailures++;});
 }
}
const startupCleanup=cleanupRegisteredWorkerWorktrees().catch(()=>{});
export async function healWorkerWorktreeMetadata(root:string){
 await startupCleanup;
 await cleanupBaseIfEmpty(root);
 return {healed:true};
}
export function workerWorktreeStatus(){
 return {created,cleaned,captured,orphanSweeps,cleanupFailures,rootName:ROOT_NAME,cleanupRegistryMaxBytes:1024*1024,
  policy:'Only clean Git projects with 2+ independent writer tasks use temporary linked worktrees. Worktrees are removed after capture/release/cancel/expiry/finish; a bounded registry also removes crash leftovers on the next agent process startup.'};
}
