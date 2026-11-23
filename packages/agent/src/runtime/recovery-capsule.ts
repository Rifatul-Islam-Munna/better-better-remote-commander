/** Tiny durable recovery capsule for interrupted Code Mode workflows.
 * It records execution state only; it never plans or replays mutations.
 */
import path from 'node:path';
import {harnessStateStore} from './bounded-local-store.js';

const VERSION=1,TTL=24*60*60_000,MAX=64;
export type RecoveryStatus='running'|'pending'|'failed'|'cancelled'|'uncertain'|'completed';
export interface RecoveryCapsule{
 version:1;id:string;workflowId:string;executionId:string;status:RecoveryStatus;
 createdAt:number;updatedAt:number;projects:string[];touchedPaths:string[];pendingJobs:string[];
 lastSafeStep?:{index:number;tool:string;outcome:string};
 uncertainMutation?:{index:number;tool:string;code?:string};
 rollbackSnapshotIds:string[];
 verification:Array<{project:string;state:string;evidenceId?:string;verifiedAt?:number}>;
}
function norm(p:string){const r=path.resolve(p);return process.platform==='win32'?r.toLowerCase():r;}
export async function checkpointRecoveryCapsule(input:Omit<RecoveryCapsule,'version'|'createdAt'|'updatedAt'>){
 const old=await harnessStateStore.get<RecoveryCapsule>('recovery',input.id),now=Date.now();
 const row:RecoveryCapsule={version:VERSION,...input,createdAt:old?.createdAt??now,updatedAt:now,
   projects:[...new Set(input.projects.map(norm))].slice(0,8),
   touchedPaths:[...new Set(input.touchedPaths)].slice(-32),pendingJobs:[...new Set(input.pendingJobs)].slice(-16),
   rollbackSnapshotIds:[...new Set(input.rollbackSnapshotIds)].slice(-8),verification:input.verification.slice(0,8)};
 await harnessStateStore.put('recovery',row.id,row);
 const rows=await harnessStateStore.list<RecoveryCapsule>('recovery',MAX+20);
 for(const extra of rows.slice(MAX))await harnessStateStore.remove('recovery',extra.id);
 return row;
}
export async function latestRecoveryCapsule(project:string,excludeId?:string){
 const key=norm(project),now=Date.now();const rows=await harnessStateStore.list<RecoveryCapsule>('recovery',MAX);
 const row=rows.map(x=>x.value).find(x=>x.id!==excludeId&&x.version===VERSION&&now-x.updatedAt<TTL&&
   x.projects.includes(key)&&x.status!=='completed');
 if(!row)return null;
 return {id:row.id,workflowId:row.workflowId,executionId:row.executionId,status:row.status,updatedAt:row.updatedAt,
   lastSafeStep:row.lastSafeStep??null,uncertainMutation:row.uncertainMutation??null,
   pendingJobs:row.pendingJobs,touchedPaths:row.touchedPaths.slice(-16),rollbackSnapshotIds:row.rollbackSnapshotIds,
   verification:row.verification,
   guidance:row.uncertainMutation?'Inspect current effects before retrying the uncertain mutation.':
    row.pendingJobs.length?'Resume/query existing pending jobs; do not start duplicates.':
    'Resume from the last safe step only after checking current project state.'};
}
export async function recoveryCapsuleStatus(){
 const rows=await harnessStateStore.list<RecoveryCapsule>('recovery',12),now=Date.now();
 return {ttlMs:TTL,max:MAX,recent:rows.map(x=>({id:x.value.id,status:x.value.status,updatedAt:x.value.updatedAt,
   actionable:x.value.status!=='completed'&&now-x.value.updatedAt<TTL,projects:x.value.projects.length,
   pendingJobs:x.value.pendingJobs.length,rollbackSnapshots:x.value.rollbackSnapshotIds.length})),
   policy:'Metadata-only recovery context. Never automatically replays a mutation or creates a task plan.'};
}
