/** Persistent, hash-guarded rollback snapshots for worker integration waves.
 * Snapshots are bounded and contain only the UTF-8 files changed by one wave.
 */
import fs from 'node:fs/promises';
import path from 'node:path';
import {randomUUID} from 'node:crypto';
import type {z} from 'zod';
import type {WorkspaceChangeSchema} from '../tools/workspace-batch.js';
import {BoundedLocalStore} from './bounded-local-store.js';
import {projectPath,slash} from './workspace.js';
import {AgentError} from './errors.js';
import {atomicWrite,decodeCode,readBounded,sha256,withFileLocks} from './file-safety.js';
import {projectIndex} from './project-index.js';
import {codeGraph} from './code-graph.js';

type Change=z.infer<typeof WorkspaceChangeSchema>;
const STORE_BYTES=32*1024*1024;
const RECORD_BYTES=16*1024*1024;
const TTL_MS=24*60*60_000;
const store=new BoundedLocalStore('worker-rollbacks',STORE_BYTES,RECORD_BYTES);

export interface SnapshotFile{
  relative:string;
  beforeText:string|null;
  beforeSha256:string|null;
  afterText:string;
  afterSha256:string;
}
export interface WorkerRollbackSnapshot{
  version:1;
  id:string;
  planId:string;
  root:string;
  taskIds:string[];
  createdAt:number;
  touchedAt:number;
  status:'ready'|'rolled_back';
  files:SnapshotFile[];
  autoCancelledTaskIds:string[];
}
function applyEdits(before:string,change:Change){
  if(change.content!==undefined)return change.content;
  let next=before;
  const crlf=next.includes('\r\n');
  const normalize=(s:string)=>crlf?s.replace(/\r?\n/g,'\r\n'):s;
  for(const edit of change.edits??[]){
    const old=normalize(edit.old_string),parts=next.split(old);
    if(parts.length-1!==edit.expected_replacements)
      throw new AgentError('EDIT_MISMATCH','Rollback snapshot could not reproduce the proposed exact edit.');
    next=parts.join(normalize(edit.new_string));
  }
  return next;
}
async function createExclusive(file:string,bytes:Buffer){
  const tmp=path.join(path.dirname(file),'.brc-rollback-create-'+randomUUID()+'.tmp');
  try{
    const fd=await fs.open(tmp,'wx',0o600);
    try{await fd.writeFile(bytes);await fd.sync();}finally{await fd.close();}
    await fs.link(tmp,file);
  }finally{await fs.unlink(tmp).catch(()=>{});}
}
export async function prepareWorkerRollbackSnapshot(input:{
  root:string;planId:string;taskIds:string[];changes:Change[];
}):Promise<WorkerRollbackSnapshot|null>{
  if(!input.changes.length)return null;
  const files:SnapshotFile[]=[];
  const seen=new Set<string>();
  for(const change of input.changes){
    const abs=await projectPath(input.root,change.path);
    const relative=slash(path.relative(input.root,abs));
    if(seen.has(relative))throw new AgentError('DUPLICATE_TARGET','Rollback snapshot received duplicate paths.');
    seen.add(relative);
    const stat=await fs.lstat(abs).catch((e:NodeJS.ErrnoException)=>{
      if(e.code==='ENOENT')return null;throw e;
    });
    if(stat&&(!stat.isFile()||stat.isSymbolicLink()))
      throw new AgentError('UNSAFE_TARGET','Rollback snapshots require regular non-symlink files.');
    const before=stat?await readBounded(abs,4*1024*1024):null;
    const beforeSha=before?sha256(before):null;
    if(beforeSha!==(change.expected_sha256?.toLowerCase()??null))
      throw new AgentError('FILE_CONFLICT','Source changed before rollback snapshot creation. Re-preview before apply.');
    const beforeText=before?decodeCode(before):null;
    const afterText=applyEdits(beforeText??'',change);
    files.push({relative,beforeText,beforeSha256:beforeSha,afterText,afterSha256:sha256(Buffer.from(afterText,'utf8'))});
  }
  const snapshot:WorkerRollbackSnapshot={
    version:1,id:sha256(input.planId+'\\0'+Date.now()+'\\0'+randomUUID()),planId:input.planId,
    root:input.root,taskIds:[...input.taskIds],createdAt:Date.now(),touchedAt:Date.now(),
    status:'ready',files,autoCancelledTaskIds:[],
  };
  const bytes=Buffer.byteLength(JSON.stringify(snapshot),'utf8');
  if(bytes>RECORD_BYTES-64*1024)
    throw new AgentError('ROLLBACK_SNAPSHOT_TOO_LARGE','Rollback snapshot exceeds 16 MiB. Integrate fewer files in this wave.');
  await store.put('snapshot',snapshot.id,snapshot);
  return snapshot;
}
export async function finalizeWorkerRollbackSnapshot(id:string,autoCancelledTaskIds:string[]){
  const snapshot=await store.get<WorkerRollbackSnapshot>('snapshot',id);
  if(!snapshot)return null;
  snapshot.autoCancelledTaskIds=[...new Set(autoCancelledTaskIds)];
  snapshot.touchedAt=Date.now();
  await store.put('snapshot',id,snapshot);
  return snapshot;
}
export async function getWorkerRollbackSnapshot(id:string){
  const snapshot=await store.get<WorkerRollbackSnapshot>('snapshot',id);
  if(!snapshot)return null;
  if(Date.now()-snapshot.touchedAt>TTL_MS){
    await store.remove('snapshot',id).catch(()=>{});
    return null;
  }
  return snapshot;
}
export async function rollbackWorkerSnapshot(root:string,planId:string,id:string){
  const snapshot=await getWorkerRollbackSnapshot(id);
  if(!snapshot)throw new AgentError('ROLLBACK_NOT_FOUND','Rollback snapshot was evicted, expired, or does not exist.');
  if(snapshot.planId!==planId||path.resolve(snapshot.root)!==path.resolve(root))
    throw new AgentError('ROLLBACK_SCOPE_MISMATCH','Rollback snapshot belongs to a different worker plan/project.');
  if(snapshot.status==='rolled_back')
    return {snapshot,replay:true,files:snapshot.files.map(f=>f.relative)};
  const absolute=await Promise.all(snapshot.files.map(f=>projectPath(root,f.relative)));
  await withFileLocks(absolute,async()=>{
    for(let i=0;i<snapshot.files.length;i++){
      const f=snapshot.files[i],abs=absolute[i];
      const current=await fs.readFile(abs).catch((e:NodeJS.ErrnoException)=>{
        if(e.code==='ENOENT')return null;throw e;
      });
      const currentSha=current?sha256(current):null;
      if(currentSha!==f.afterSha256)
        throw new AgentError('ROLLBACK_CONFLICT','A file changed after this integration wave: '+f.relative+'. Nothing was rolled back.');
    }
    const reverted:number[]=[];
    try{
      for(let i=0;i<snapshot.files.length;i++){
        const f=snapshot.files[i],abs=absolute[i];
        if(f.beforeText===null)await fs.unlink(abs);
        else await atomicWrite(abs,Buffer.from(f.beforeText,'utf8'),f.afterSha256);
        reverted.push(i);projectIndex.invalidate(abs);codeGraph.invalidate(abs);
      }
    }catch(error){
      for(const i of reverted.reverse()){
        const f=snapshot.files[i],abs=absolute[i];
        try{
          if(f.beforeText===null)await createExclusive(abs,Buffer.from(f.afterText,'utf8'));
          else await atomicWrite(abs,Buffer.from(f.afterText,'utf8'),f.beforeSha256!);
          projectIndex.invalidate(abs);codeGraph.invalidate(abs);
        }catch{}
      }
      throw new AgentError('ROLLBACK_FAILED',error instanceof Error?error.message:'Rollback failed.',false);
    }
  });
  snapshot.status='rolled_back';snapshot.touchedAt=Date.now();
  await store.put('snapshot',id,snapshot);
  return {snapshot,replay:false,files:snapshot.files.map(f=>f.relative)};
}
export async function deleteWorkerRollbackSnapshot(id:string){
  await store.remove('snapshot',id).catch(()=>{});
}
export async function deletePlanRollbackSnapshots(planId:string){
  const rows=await store.list<WorkerRollbackSnapshot>('snapshot',256).catch(()=>[]);
  for(const row of rows)if(row.value.planId===planId)await store.remove('snapshot',row.id).catch(()=>{});
}
export async function workerRollbackStatus(){
  const status=await store.status();
  const recent=(await store.list<WorkerRollbackSnapshot>('snapshot',12)).map(x=>({
    id:x.value.id,planId:x.value.planId,status:x.value.status,files:x.value.files.length,
    taskIds:x.value.taskIds,createdAt:x.value.createdAt,
  }));
  return {store:status,ttlMs:TTL_MS,recent,
    policy:'32 MiB bounded local store; each wave snapshot max 16 MiB and expires after 24h. Rollback is allowed only while every current file still matches that wave post-hash.'};
}
