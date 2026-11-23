/** Cross-workflow in-flight coalescing for safe read-only tools.
 * Identical concurrent reads share one underlying local execution. No mutation,
 * session-creating, cursor-consuming, job-waiting or status tools are coalesced.
 */
import {createHash} from 'node:crypto';
import {AgentError} from './errors.js';

const MAX_INFLIGHT=256;
const SAFE_TOOLS=new Set([
  'read_file','read_multiple_files','read_code_file','read_code_ranges','get_file_info','list_directory',
  'git_status','git_diff','git_log','project_context','project_map','context_pack','code_graph',
  'lsp_query','ast_search','find_symbols','find_references','discover_checks','semantic_project_search',
]);
interface Entry<T>{
  key:string;
  tool:string;
  createdAt:number;
  controller:AbortController;
  promise:Promise<T>;
  subscribers:number;
  settled:boolean;
}
function stable(value:unknown):string{
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
}
function keyFor(tool:string,args:Record<string,unknown>){
  return createHash('sha256').update(tool+'\0'+stable(args)).digest('hex');
}

class ReadOnlyCoalescer {
  private inflight=new Map<string,Entry<unknown>>();
  private started=0;
  private joined=0;
  private completed=0;
  private failed=0;
  private cancelledSubscribers=0;
  private abortedUnderlying=0;
  private maxObserved=0;

  eligible(tool:string,readOnly:boolean){
    return readOnly&&SAFE_TOOLS.has(tool);
  }
  async run<T>(
    tool:string,args:Record<string,unknown>,callerSignal:AbortSignal|undefined,
    invoke:(sharedSignal:AbortSignal)=>Promise<T>,
  ):Promise<{value:T;coalesced:boolean}>{
    if(!this.eligible(tool,true))return {value:await invoke(callerSignal??new AbortController().signal),coalesced:false};
    const key=keyFor(tool,args);
    let entry=this.inflight.get(key) as Entry<T>|undefined;
    let coalesced=true;
    if(!entry){
      if(this.inflight.size>=MAX_INFLIGHT)
        return {value:await invoke(callerSignal??new AbortController().signal),coalesced:false};
      coalesced=false;
      const controller=new AbortController();
      const created:Entry<T>={
        key,tool,createdAt:Date.now(),controller,subscribers:0,settled:false,
        promise:undefined as unknown as Promise<T>,
      };
      created.promise=(async()=>{
        try{
          const value=await invoke(controller.signal);
          this.completed++;
          return value;
        }catch(error){
          this.failed++;
          throw error;
        }finally{
          created.settled=true;
          this.inflight.delete(key);
        }
      })();
      this.inflight.set(key,created as Entry<unknown>);
      entry=created;
      this.started++;
      this.maxObserved=Math.max(this.maxObserved,this.inflight.size);
    }else this.joined++;

    entry.subscribers++;
    let aborted=false;
    let abortReject:(reason?:unknown)=>void=()=>{};
    const abortPromise=new Promise<never>((_,reject)=>{abortReject=reject;});
    const onAbort=()=>{
      aborted=true;
      this.cancelledSubscribers++;
      abortReject(callerSignal?.reason??new AgentError('CANCELLED','Coalesced read caller was cancelled.'));
    };
    callerSignal?.addEventListener('abort',onAbort,{once:true});
    if(callerSignal?.aborted)onAbort();
    try{
      const value=callerSignal?await Promise.race([entry.promise,abortPromise]):await entry.promise;
      return {value,coalesced};
    }finally{
      callerSignal?.removeEventListener('abort',onAbort);
      entry.subscribers=Math.max(0,entry.subscribers-1);
      if(aborted&&!entry.settled&&entry.subscribers===0&&!entry.controller.signal.aborted){
        this.abortedUnderlying++;
        entry.controller.abort(new AgentError('CANCELLED','All coalesced read callers cancelled.'));
      }
    }
  }
  status(){
    const now=Date.now();
    return {
      enabled:true,maxInflight:MAX_INFLIGHT,inflight:this.inflight.size,
      safeTools:[...SAFE_TOOLS],started:this.started,joined:this.joined,completed:this.completed,
      failed:this.failed,cancelledSubscribers:this.cancelledSubscribers,
      abortedUnderlying:this.abortedUnderlying,maxObserved:this.maxObserved,
      active:[...this.inflight.values()].slice(0,24).map(e=>({
        tool:e.tool,ageMs:Math.max(0,now-e.createdAt),subscribers:e.subscribers,
      })),
      policy:'Only explicitly safe, idempotent read-only tools coalesce. Session creation, output cursors, job waits and all mutations always execute independently.',
    };
  }
}
export const readOnlyCoalescer=new ReadOnlyCoalescer();
