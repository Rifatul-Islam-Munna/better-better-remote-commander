/** Persistent verification evidence for agent-mediated mutations.
 * Stores hashes/metadata only, never source contents.
 */
import fs from 'node:fs/promises';
import path from 'node:path';
import {createHash,randomUUID} from 'node:crypto';
import {harnessStateStore} from './bounded-local-store.js';
import {classifyHeavyCodeModeTask} from './heavy-task-policy.js';

const VERSION=1,MAX_PATHS=128,MAX_HASH_BYTES=8*1024*1024,MAX_PROJECTS=64;
type HashState=string|'missing'|'too_large'|'unavailable';
interface EvidenceRecord{
 version:1;id:string;root:string;revision:number;changedPaths:string[];unknownMutation:boolean;
 lastMutationAt?:number;lastMutationTool?:string;
 verification?:{id:string;tool:string;at:number;revision:number;passed:boolean;hashes:Record<string,HashState>;scope:'tracked-files'|'limited'};
}
const digest=(v:string|Buffer)=>createHash('sha256').update(v).digest('hex');
const recId=(root:string)=>digest((process.platform==='win32'?root.toLowerCase():root)).slice(0,40);
function normalize(root:string,p:string){const abs=path.isAbsolute(p)?path.resolve(p):path.resolve(root,p);return abs;}
function candidatePaths(root:string,args:Record<string,unknown>,meta?:Record<string,unknown>){
 const out:string[]=[];const push=(v:unknown)=>{if(typeof v==='string'&&v.trim())out.push(normalize(root,v));};
 for(const k of ['file_path','source','destination','outputPath'])push(args[k]);
 if(typeof args.path==='string'&&path.extname(args.path))push(args.path);
 if(Array.isArray(args.paths))for(const v of args.paths)push(v);
 for(const listKey of ['files','changes']){
  const list=args[listKey];if(Array.isArray(list))for(const item of list)if(item&&typeof item==='object'&&!Array.isArray(item))
   push((item as Record<string,unknown>).path??(item as Record<string,unknown>).file_path);
 }
 const changes=meta?.changes;
 if(Array.isArray(changes))for(const item of changes)if(item&&typeof item==='object'&&!Array.isArray(item))
   push((item as Record<string,unknown>).path);
 return [...new Set(out)].slice(0,MAX_PATHS);
}
async function hashState(file:string):Promise<HashState>{
 try{const st=await fs.lstat(file);if(!st.isFile()||st.isSymbolicLink())return'unavailable';if(st.size>MAX_HASH_BYTES)return'too_large';
  return digest(await fs.readFile(file));}catch(e){return (e as NodeJS.ErrnoException)?.code==='ENOENT'?'missing':'unavailable';}
}
async function load(root:string):Promise<EvidenceRecord>{
 const id=recId(root),old=await harnessStateStore.get<EvidenceRecord>('verification',id);
 return old?.version===VERSION?old:{version:VERSION,id,root:path.resolve(root),revision:0,changedPaths:[],unknownMutation:false};
}
async function save(record:EvidenceRecord){
 await harnessStateStore.put('verification',record.id,record);
 const rows=await harnessStateStore.list<EvidenceRecord>('verification',MAX_PROJECTS+16);
 for(const extra of rows.slice(MAX_PROJECTS))await harnessStateStore.remove('verification',extra.id);
}
export function isTrackedMutation(tool:string,args:Record<string,unknown>){
 if(['write_file','edit_block','edit_files','workspace_batch','edit_code_ranges','replace_symbol','delete_file','move_file','apply_patch'].includes(tool))return true;
 return tool==='worker_integrate'&&['apply','rollback'].includes(String(args.action??''));
}
export function isVerificationOperation(tool:string,args:Record<string,unknown>){
 if(['run_project_checks','verify_project'].includes(tool))return true;
 if(tool!=='run_command')return false;
 const heavy=classifyHeavyCodeModeTask(tool,args,'verify');
 return !!heavy&&/(?:build|test|check|verification)/i.test(heavy.reason);
}
export async function recordProjectMutation(root:string,tool:string,args:Record<string,unknown>,meta?:Record<string,unknown>){
 const r=await load(root);r.revision++;r.lastMutationAt=Date.now();r.lastMutationTool=tool;
 const paths=candidatePaths(root,args,meta);r.unknownMutation=r.unknownMutation||paths.length===0;
 r.changedPaths=[...new Set([...r.changedPaths,...paths])].slice(-MAX_PATHS);
 await save(r);return getVerificationEvidence(root);
}
export async function recordProjectVerification(root:string,tool:string,passed:boolean){
 const r=await load(root),hashes:Record<string,HashState>={};
 for(const p of r.changedPaths)hashes[p]=await hashState(p);
 r.verification={id:randomUUID(),tool,at:Date.now(),revision:r.revision,passed,hashes,
  scope:r.unknownMutation?'limited':'tracked-files'};
 await save(r);return getVerificationEvidence(root);
}
export async function getVerificationEvidence(root:string){
 const r=await load(root);const v=r.verification;
 if(!v)return {project:r.root,state:'unverified' as const,revision:r.revision,changedPaths:r.changedPaths.slice(-24),unknownMutation:r.unknownMutation};
 const changed:string[]=[];
 for(const [p,expected] of Object.entries(v.hashes))if(await hashState(p)!==expected)changed.push(p);
 const stale=!v.passed||v.revision!==r.revision||changed.length>0;
 return {project:r.root,state:stale?'stale' as const:'current' as const,evidenceId:v.id,tool:v.tool,verifiedAt:v.at,
  evidenceRevision:v.revision,currentRevision:r.revision,scope:v.scope,changedSinceVerification:changed.slice(0,24),
  changedPaths:r.changedPaths.slice(-24),unknownMutation:r.unknownMutation};
}
export async function linkEvidenceReset(root:string){
 const r=await load(root);r.changedPaths=[];r.unknownMutation=false;await save(r);
}
export async function verificationEvidenceStatus(){
 const rows=await harnessStateStore.list<EvidenceRecord>('verification',12);
 return {projects:rows.map(x=>({root:x.value.root,revision:x.value.revision,changedPaths:x.value.changedPaths.length,
   evidenceId:x.value.verification?.id??null,verifiedAt:x.value.verification?.at??null})),
   policy:'Evidence is tied to tracked file hashes and a mutation revision. Later agent mutations or external changes to tracked files make it stale.'};
}
