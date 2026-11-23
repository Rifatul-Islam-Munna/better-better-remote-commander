/** Project preflight + conservative self-healing.
 * It never installs dependencies, resets Git, deletes user files, or edits project source.
 */
import fs from 'node:fs/promises';
import path from 'node:path';
import {execFile} from 'node:child_process';
import {workerCoordinator} from './worker-coordination.js';
import {healWorkerWorktreeMetadata} from './worker-worktrees.js';

type Intent='read'|'mutation'|'heavy';
export interface ProjectPreflight {
  project:string;
  ok:boolean;
  safeToMutate:boolean;
  safeForHeavy:boolean;
  warnings:string[];
  healed:string[];
  diskFreeBytes?:number;
  git:{available:boolean;root?:string;dirty?:boolean};
  packageManager:string|null;
  packageManagerAvailable:boolean|null;
  checkedAt:number;
}
const CACHE_MS=30_000;
const cache=new Map<string,{at:number;value:ProjectPreflight}>();
let runs=0,cacheHits=0,heals=0;

function exec(exe:string,args:string[],cwd?:string,timeout=2500):Promise<string>{
  return new Promise((resolve,reject)=>execFile(exe,args,{cwd,timeout,windowsHide:true,maxBuffer:512*1024},
    (error,stdout,stderr)=>error?reject(new Error(String(stderr||error.message).slice(0,500))):resolve(String(stdout))));
}
async function commandExists(name:string){
  try{
    if(process.platform==='win32')await exec('where.exe',[name],undefined,1800);
    else await exec('which',[name],undefined,1800);
    return true;
  }catch{return false;}
}
async function detectPackageManager(root:string){
  try{
    const raw=await fs.readFile(path.join(root,'package.json'),'utf8');
    const parsed=JSON.parse(raw) as {packageManager?:unknown};
    if(typeof parsed.packageManager==='string'&&parsed.packageManager.trim())
      return parsed.packageManager.split('@')[0].trim();
  }catch{}
  for(const [file,pm] of [['pnpm-lock.yaml','pnpm'],['yarn.lock','yarn'],['bun.lockb','bun'],['bun.lock','bun'],['package-lock.json','npm']] as const)
    try{await fs.access(path.join(root,file));return pm;}catch{}
  return null;
}
export async function runProjectPreflight(root:string,intent:Intent='read'):Promise<ProjectPreflight>{
  const project=path.resolve(root),key=(process.platform==='win32'?project.toLowerCase():project)+'|'+intent;
  const saved=cache.get(key);
  if(saved&&Date.now()-saved.at<CACHE_MS){cacheHits++;return saved.value;}
  runs++;
  const warnings:string[]=[],healed:string[]=[];
  let ok=true,safeToMutate=true,safeForHeavy=true,diskFreeBytes:number|undefined;
  const stat=await fs.stat(project).catch(()=>null);
  if(!stat?.isDirectory()){
    ok=false;safeToMutate=false;safeForHeavy=false;warnings.push('Project root is unavailable or not a directory.');
  }
  if(stat){
    try{
      const info=await fs.statfs(project);
      diskFreeBytes=Number(info.bavail)*Number(info.bsize);
      if(diskFreeBytes<128*1024*1024){ok=false;safeToMutate=false;safeForHeavy=false;warnings.push('Critically low disk space (<128 MiB).');}
      else if(diskFreeBytes<512*1024*1024){safeForHeavy=false;warnings.push('Low disk space (<512 MiB); heavy jobs are blocked.');}
      else if(diskFreeBytes<1024*1024*1024){warnings.push('Disk space is below 1 GiB; large builds may fail.');}
    }catch{}
  }
  try{
    const removed=workerCoordinator.prune();
    if(removed>0){healed.push('Pruned '+removed+' expired/idle agent worker plan(s).');heals+=removed;}
  }catch{}
  try{
    await healWorkerWorktreeMetadata(project);
    healed.push('Validated/cleaned agent-owned temporary worktree metadata.');
    heals++;
  }catch(error){
    warnings.push('Agent worktree metadata cleanup could not complete: '+(error instanceof Error?error.message.slice(0,160):'unknown error'));
  }

  const gitAvailable=await commandExists('git');
  const git:{available:boolean;root?:string;dirty?:boolean}={available:gitAvailable};
  if(gitAvailable&&stat){
    try{
      const top=(await exec('git',['rev-parse','--show-toplevel'],project)).trim();
      git.root=top;
      const status=await exec('git',['status','--porcelain=v1','--untracked-files=no'],project);
      git.dirty=status.trim().length>0;
    }catch{}
  }
  const packageManager=stat?await detectPackageManager(project):null;
  const packageManagerAvailable=packageManager?await commandExists(packageManager):null;
  if(packageManager&&packageManagerAvailable===false){
    warnings.push('Detected package manager "'+packageManager+'" is not available on PATH.');
    if(intent==='heavy')safeForHeavy=false;
  }
  if(intent==='mutation'&&!safeToMutate)ok=false;
  if(intent==='heavy'&&!safeForHeavy)ok=false;

  const value:ProjectPreflight={project,ok,safeToMutate,safeForHeavy,warnings,healed,diskFreeBytes,
    git,packageManager,packageManagerAvailable,checkedAt:Date.now()};
  cache.set(key,{at:Date.now(),value});
  if(cache.size>128){
    const oldest=[...cache.entries()].sort((a,b)=>a[1].at-b[1].at).slice(0,cache.size-128);
    for(const [k] of oldest)cache.delete(k);
  }
  return value;
}
export function projectPreflightStatus(){
  return {runs,cacheHits,heals,cacheEntries:cache.size,cacheTtlMs:CACHE_MS,
    policy:'Self-healing is agent-owned only: expired worker plans/claims and temporary-worktree metadata. No dependency install, Git reset, user-file deletion, or source edit is automatic.'};
}
