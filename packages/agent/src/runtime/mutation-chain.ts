/** Bounded mutation receipt chain.
 * Links the existing kernel receipt to workflow, rollback and verification evidence.
 * Stores metadata/hashes only; never source contents or command text.
 */
import path from 'node:path';
import {createHash,randomUUID} from 'node:crypto';
import type {ToolResult} from 'brc-shared';
import {BoundedLocalStore} from './bounded-local-store.js';

const STORE_BYTES=8*1024*1024,RECORD_BYTES=64*1024,MAX=256;
const store=new BoundedLocalStore('mutation-chain',STORE_BYTES,RECORD_BYTES);
const hash=(v:string)=>createHash('sha256').update(v).digest('hex');
type ReceiptMeta={id?:string;tool?:string;outcome?:string;targets?:string[];before?:string[];after?:string[];at?:number;finishedAt?:number};
export interface MutationChainRecord{
 version:1;id:string;previousId?:string;project:string;projectHash:string;
 callHash:string;workflowHash:string;tool:string;at:number;finishedAt?:number;outcome:string;
 targets:Array<{label:string;targetHash?:string;before?:string;after?:string}>;
 rollbackSnapshotId?:string;verificationEvidenceId?:string;
}
function labels(project:string,targets:string[],result:ToolResult){
 const out=targets.map(p=>{
  const rel=path.relative(project,p);return !rel.startsWith('..')&&!path.isAbsolute(rel)?rel.replaceAll('\\','/'):path.basename(p);
 });
 const changes=result.structuredContent?.changes;
 if(!out.length&&Array.isArray(changes))for(const item of changes){
  if(!item||typeof item!=='object'||Array.isArray(item))continue;
  const raw=(item as Record<string,unknown>).path;
  if(typeof raw==='string')out.push(path.isAbsolute(raw)?path.relative(project,raw).replaceAll('\\','/'):raw.replaceAll('\\','/'));
 }
 return out.slice(0,16);
}
export async function recordMutationChain(input:{
 callRef:string;tool:string;project:string;targets:string[];result:ToolResult;
}){
 const receipt=(input.result._meta as Record<string,unknown>|undefined)?.mutationReceipt as ReceiptMeta|undefined;
 const project=path.resolve(input.project),projectHash=hash(process.platform==='win32'?project.toLowerCase():project);
 const rows=await store.list<MutationChainRecord>('chain',MAX);
 const previous=rows.find(x=>x.value.projectHash===projectHash)?.value;
 const targetLabels=labels(project,input.targets,input.result);
 const targetHashes=receipt?.targets??[],before=receipt?.before??[],after=receipt?.after??[];
 const rollbackRaw=input.result.structuredContent?.rollbackSnapshotId;
 const rollbackSnapshotId=typeof rollbackRaw==='string'?rollbackRaw:undefined;
 const record:MutationChainRecord={
  version:1,id:randomUUID().replaceAll('-',''),...(previous?{previousId:previous.id}:{}),
  project,projectHash,callHash:hash(input.callRef),
  workflowHash:hash(input.callRef.split(':')[0]||input.callRef),tool:input.tool,
  at:receipt?.at??Date.now(),finishedAt:receipt?.finishedAt,
  outcome:receipt?.outcome??(input.result.isError?'reported_failure':'returned_success'),
  targets:Array.from({length:Math.max(targetLabels.length,targetHashes.length,before.length,after.length)})
   .slice(0,16).map((_,i)=>({label:targetLabels[i]??('target-'+(i+1)),
    ...(targetHashes[i]?{targetHash:targetHashes[i]}:{}),...(before[i]?{before:before[i]}:{}),...(after[i]?{after:after[i]}:{})})),
  ...(rollbackSnapshotId?{rollbackSnapshotId}:{})
 };
 await store.put('chain',record.id,record);
 const all=await store.list<MutationChainRecord>('chain',MAX+32);
 for(const extra of all.slice(MAX))await store.remove('chain',extra.id);
 return record;
}
export async function linkMutationChainVerification(project:string,evidenceId:string){
 const root=path.resolve(project),projectHash=hash(process.platform==='win32'?root.toLowerCase():root);
 const rows=await store.list<MutationChainRecord>('chain',MAX);
 let linked=0;
 for(const row of rows){
  const r=row.value;
  if(r.projectHash!==projectHash||r.verificationEvidenceId||r.outcome!=='returned_success')continue;
  r.verificationEvidenceId=evidenceId;await store.put('chain',r.id,r);linked++;
  if(linked>=64)break;
 }
 return {linked,evidenceId};
}
export async function mutationChainStatus(project?:string){
 const rows=await store.list<MutationChainRecord>('chain',20);
 const projectHash=project?hash(process.platform==='win32'?path.resolve(project).toLowerCase():path.resolve(project)):undefined;
 return {store:await store.status(),recent:rows.map(x=>x.value).filter(x=>!projectHash||x.projectHash===projectHash).slice(0,12)
   .map(r=>({id:r.id,previousId:r.previousId??null,project:r.project,tool:r.tool,outcome:r.outcome,at:r.at,
    targets:r.targets,rollbackSnapshotId:r.rollbackSnapshotId??null,verificationEvidenceId:r.verificationEvidenceId??null})),
   policy:'Metadata-only chain: workflow/call hashes -> tool -> before/after target fingerprints -> rollback snapshot -> verification evidence. No automatic replay.'};
}
