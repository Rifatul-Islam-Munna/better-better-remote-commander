/** Adaptive process-local admission for Code Mode workflows.
 * Up to 12 workflows may be admitted (running + queued), but only a conservative
 * hardware-aware subset owns QuickJS workers at once. Heavy nested commands,
 * searches and plugin starts remain subject to their existing resource lanes.
 */
import os from 'node:os';
import {AgentError} from './errors.js';

const MAX_ADMITTED=12;
const HARD_MAX_ACTIVE=8;
const QUEUE_TIMEOUT_MS=90_000;
const PER_WORKER_MEMORY_BUDGET=768*1024*1024;
const MIN_MEMORY_RESERVE=2*1024*1024*1024;

interface ActiveWorkflow {
  id:string;
  admittedAt:number;
  startedAt:number;
  waitedMs:number;
}
interface Waiter {
  id:string;
  admittedAt:number;
  signal?:AbortSignal;
  done:boolean;
  timeout:NodeJS.Timeout;
  abort:()=>void;
  resolve:(lease:CodeModeLease)=>void;
  reject:(error:unknown)=>void;
}
export interface CodeModeLease {
  workflowId:string;
  waitedMs:number;
  activeLimitAtStart:number;
  release:()=>void;
}
function parallelism(){
  try{return Math.max(1,os.availableParallelism());}
  catch{return Math.max(1,os.cpus().length||1);}
}
function dynamicLimit(){
  const cpus=parallelism();
  const cpuSlots=Math.max(1,Math.min(HARD_MAX_ACTIVE,Math.ceil(cpus*0.35)));
  const total=os.totalmem(),free=os.freemem();
  const reserve=Math.max(MIN_MEMORY_RESERVE,Math.floor(total*0.20));
  const usable=Math.max(0,free-reserve);
  const memorySlots=Math.max(1,Math.floor(usable/PER_WORKER_MEMORY_BUDGET));
  const activeLimit=Math.max(1,Math.min(HARD_MAX_ACTIVE,cpuSlots,memorySlots));
  return {
    cpus,total,free,reserve,cpuSlots,memorySlots,activeLimit,
    admittedLimit:Math.min(MAX_ADMITTED,Math.max(2,activeLimit*2)),
  };
}

class CodeModeScheduler {
  private active=new Map<string,ActiveWorkflow>();
  private queue:Waiter[]=[];
  private admitted=0;
  private completed=0;
  private cancelledQueued=0;
  private expiredQueued=0;
  private rejected=0;
  private maxObservedActive=0;
  private maxObservedAdmitted=0;
  private totalWaitMs=0;

  private lease(id:string,admittedAt:number):CodeModeLease{
    const hw=dynamicLimit();
    const waitedMs=Math.max(0,Date.now()-admittedAt);
    this.active.set(id,{id,admittedAt,startedAt:Date.now(),waitedMs});
    this.maxObservedActive=Math.max(this.maxObservedActive,this.active.size);
    this.totalWaitMs+=waitedMs;
    let released=false;
    return {
      workflowId:id,waitedMs,activeLimitAtStart:hw.activeLimit,
      release:()=>{
        if(released)return;
        released=true;
        if(this.active.delete(id)){
          this.admitted=Math.max(0,this.admitted-1);
          this.completed++;
        }
        this.drain();
      },
    };
  }
  private drain(){
    const limit=dynamicLimit().activeLimit;
    while(this.active.size<limit&&this.queue.length){
      const waiter=this.queue.shift()!;
      if(waiter.done)continue;
      waiter.done=true;
      clearTimeout(waiter.timeout);
      waiter.signal?.removeEventListener('abort',waiter.abort);
      waiter.resolve(this.lease(waiter.id,waiter.admittedAt));
    }
  }
  async acquire(id:string,signal?:AbortSignal):Promise<CodeModeLease>{
    if(signal?.aborted)
      throw signal.reason??new AgentError('CODE_MODE_CANCELLED','Code Mode was cancelled before admission.');
    const hardware=dynamicLimit();
    if(this.admitted>=hardware.admittedLimit){
      this.rejected++;
      throw new AgentError('CODE_MODE_CAPACITY',
        'Code Mode capacity is full ('+hardware.admittedLimit+' admitted workflows on this hardware right now). Retry after one workflow completes.',true);
    }
    this.admitted++;
    this.maxObservedAdmitted=Math.max(this.maxObservedAdmitted,this.admitted);
    const admittedAt=Date.now();
    const limit=dynamicLimit().activeLimit;
    if(this.active.size<limit&&this.queue.length===0)return this.lease(id,admittedAt);

    return new Promise<CodeModeLease>((resolve,reject)=>{
      const waiter:Waiter={
        id,admittedAt,signal,done:false,timeout:undefined as unknown as NodeJS.Timeout,
        abort:()=>{},resolve,reject,
      };
      const fail=(error:unknown,expired:boolean)=>{
        if(waiter.done)return;
        waiter.done=true;
        clearTimeout(waiter.timeout);
        waiter.signal?.removeEventListener('abort',waiter.abort);
        const i=this.queue.indexOf(waiter);if(i>=0)this.queue.splice(i,1);
        this.admitted=Math.max(0,this.admitted-1);
        if(expired)this.expiredQueued++;else this.cancelledQueued++;
        reject(error);
        this.drain();
      };
      waiter.abort=()=>fail(signal?.reason??new AgentError('CODE_MODE_CANCELLED',
        'Code Mode was cancelled while waiting for a local worker slot.'),false);
      waiter.timeout=setTimeout(()=>fail(new AgentError('CODE_MODE_QUEUE_TIMEOUT',
        'Code Mode waited too long for a local worker slot; no workflow was started.',true),true),QUEUE_TIMEOUT_MS);
      signal?.addEventListener('abort',waiter.abort,{once:true});
      this.queue.push(waiter);
      if(signal?.aborted)waiter.abort();
      else this.drain();
    });
  }
  status(){
    const hw=dynamicLimit();
    const now=Date.now();
    return {
      hardMaxAdmitted:MAX_ADMITTED,
      admittedLimit:hw.admittedLimit,
      hardMaxActive:HARD_MAX_ACTIVE,
      activeLimit:hw.activeLimit,
      active:this.active.size,
      queued:this.queue.filter(x=>!x.done).length,
      admitted:this.admitted,
      cpu:{parallelism:hw.cpus,slots:hw.cpuSlots},
      memory:{
        totalBytes:hw.total,freeBytes:hw.free,reserveBytes:hw.reserve,
        perWorkerBudgetBytes:PER_WORKER_MEMORY_BUDGET,slots:hw.memorySlots,
      },
      activeWorkflows:[...this.active.values()].map(x=>({
        workflowId:x.id,ageMs:Math.max(0,now-x.startedAt),waitedMs:x.waitedMs,
      })),
      queuedWorkflows:this.queue.filter(x=>!x.done).slice(0,12).map(x=>({
        workflowId:x.id,waitMs:Math.max(0,now-x.admittedAt),
      })),
      completed:this.completed,cancelledQueued:this.cancelledQueued,expiredQueued:this.expiredQueued,
      rejected:this.rejected,maxObservedActive:this.maxObservedActive,
      maxObservedAdmitted:this.maxObservedAdmitted,totalWaitMs:this.totalWaitMs,
      policy:'Adaptive Code Mode only: hard ceiling 12 admitted and 8 runnable, but both scale down on smaller hardware. Normally only about 35% of logical CPUs are assigned QuickJS slots, with a free-RAM reserve. Nested heavy tools retain their own resource gates.',
    };
  }
}
export const codeModeScheduler=new CodeModeScheduler();
