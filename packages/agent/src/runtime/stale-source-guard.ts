/** Workflow-local stale-source observations.
 * A mutation may not silently use source that changed after this Code Mode workflow read it.
 */
import fs from 'node:fs/promises';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {AgentError} from './errors.js';

const MAX_HASH_BYTES=8*1024*1024;
const MAX_OBSERVATIONS=256;
const READ_TOOLS=new Set(['read_file','read_code_file','read_multiple_files','read_code_ranges']);
const MUTATION_TOOLS=new Set([
 'write_file','edit_block','edit_files','workspace_batch','edit_code_ranges','replace_symbol',
 'delete_file','move_file','apply_patch','worker_integrate',
]);
type Observation={path:string;sha256:string|null;observedAt:number;tool:string};

function key(file:string){const p=path.resolve(file);return process.platform==='win32'?p.toLowerCase():p;}
function rawCandidates(args:Record<string,unknown>){
 const out:string[]=[];
 const baseRaw=[args.root,args.cwd,args.path].find(v=>typeof v==='string'&&path.isAbsolute(v as string)) as string|undefined;
 const base=baseRaw?path.resolve(baseRaw):undefined;
 const push=(v:unknown)=>{
  if(typeof v!=='string'||!v.trim())return;
  if(path.isAbsolute(v))out.push(path.resolve(v));
  else if(base)out.push(path.resolve(base,v));
 };
 for(const k of ['path','file_path','source','destination','outputPath'])push(args[k]);
 if(Array.isArray(args.paths))for(const v of args.paths)push(v);
 for(const listKey of ['files','changes']){
  const list=args[listKey];
  if(Array.isArray(list))for(const item of list)if(item&&typeof item==='object'&&!Array.isArray(item))
    push((item as Record<string,unknown>).path??(item as Record<string,unknown>).file_path);
 }
 return [...new Set(out)].slice(0,48);
}
async function hashFile(file:string):Promise<string|null|undefined>{
 try{
  const st=await fs.lstat(file);
  if(!st.isFile()||st.isSymbolicLink()||st.size>MAX_HASH_BYTES)return undefined;
  const data=await fs.readFile(file);
  return createHash('sha256').update(data).digest('hex');
 }catch(error){
  if((error as NodeJS.ErrnoException)?.code==='ENOENT')return null;
  return undefined;
 }
}
export class SourceObservationLedger{
 private rows=new Map<string,Observation>();
 private reads=0;private guarded=0;private stale=0;private refreshed=0;
 async recordRead(tool:string,args:Record<string,unknown>){
  if(!READ_TOOLS.has(tool))return;
  for(const file of rawCandidates(args)){
   const sha=await hashFile(file);if(sha===undefined)continue;
   const k=key(file);
   this.rows.set(k,{path:path.resolve(file),sha256:sha,observedAt:Date.now(),tool});
   this.reads++;
   if(this.rows.size>MAX_OBSERVATIONS)this.rows.delete(this.rows.keys().next().value!);
  }
 }
 async guardMutation(tool:string,args:Record<string,unknown>){
  if(!MUTATION_TOOLS.has(tool))return {guarded:false,paths:[] as string[]};
  const checked:string[]=[];
  for(const file of rawCandidates(args)){
   const observed=this.rows.get(key(file));if(!observed)continue;
   checked.push(observed.path);
   const current=await hashFile(observed.path);
   if(current===undefined)continue;
   this.guarded++;
   if(current!==observed.sha256){
    this.stale++;
    throw new AgentError('STALE_SOURCE_OBSERVATION',
      'Source changed after this workflow read it: '+observed.path+
      '. Re-read the current file before mutating it. Observed SHA-256='+(observed.sha256??'missing')+
      ', current SHA-256='+(current??'missing')+'. No mutation was started.',true);
   }
  }
  return {guarded:checked.length>0,paths:checked};
 }
 async refreshMutation(tool:string,args:Record<string,unknown>){
  if(!MUTATION_TOOLS.has(tool))return;
  for(const file of rawCandidates(args)){
   const k=key(file);if(!this.rows.has(k))continue;
   const sha=await hashFile(file);if(sha===undefined){this.rows.delete(k);continue;}
   this.rows.set(k,{path:path.resolve(file),sha256:sha,observedAt:Date.now(),tool});
   this.refreshed++;
  }
 }
 snapshot(){return {observations:this.rows.size,reads:this.reads,guarded:this.guarded,staleRejected:this.stale,
   refreshed:this.refreshed,recent:[...this.rows.values()].slice(-12).map(x=>({path:x.path,sha256:x.sha256,observedAt:x.observedAt,tool:x.tool})),
   policy:'Workflow-local: files read by this Code Mode must still match their observed hash before a later mutation. Fresh reads replace stale observations.'};}
}
