/** Adaptive process-local admission for Code Mode workflows.
 * - Hard ceiling: 12 admitted, 8 runnable.
 * - CPU/free-RAM scale the runnable/admitted limits down on smaller machines.
 * - Per-project fairness prevents one repository from consuming all runnable slots.
 * - Park/wake lets wait-heavy workflows release their runnable slot without losing
 *   their admitted workflow, durable state, or QuickJS continuation.
 */
import os from 'node:os';
import {AgentError} from './errors.js';
import {systemPressureStatus} from './system-pressure.js';
import {CODE_MODE_MAX_ADMITTED,CODE_MODE_MAX_RUNNABLE} from 'brc-shared';

const MAX_ADMITTED=CODE_MODE_MAX_ADMITTED;
const HARD_MAX_ACTIVE=CODE_MODE_MAX_RUNNABLE;
const QUEUE_TIMEOUT_MS=90_000;
const PER_WORKER_MEMORY_BUDGET=768*1024*1024;
const MIN_MEMORY_RESERVE=2*1024*1024*1024;

type WorkflowState='queued'|'active'|'parked';
interface WorkflowRecord {
  id:string;
  admittedAt:number;
  firstStartedAt?:number;
  lastStartedAt?:number;
  waitedMs:number;
  projectKey?:string;
  state:WorkflowState;
  parkReason?:string;
}
interface Waiter {
  id:string;
  enqueuedAt:number;
  signal?:AbortSignal;
  done:boolean;
  timeout:NodeJS.Timeout;
  abort:()=>void;
  resolve:()=>void;
  reject:(error:unknown)=>void;
}
export interface CodeModeLease {
  workflowId:string;
  waitedMs:number;
  activeLimitAtStart:number;
  setProject:(projectKey?:string,signal?:AbortSignal)=>Promise<void>;
  parkWhile:<T>(reason:string,work:()=>Promise<T>,signal?:AbortSignal)=>Promise<T>;
  release:()=>void;
}
function parallelism(){
  try{return Math.max(1,os.availableParallelism());}
  catch{return Math.max(1,os.cpus().length||1);}
}
function dynamicLimit(){
  const cpus=parallelism();
  const pressure=systemPressureStatus();
  const baseCpuSlots=Math.max(1,Math.min(HARD_MAX_ACTIVE,Math.ceil(cpus*0.35)));
  const cpuSlots=Math.max(1,Math.min(baseCpuSlots,Math.floor(baseCpuSlots*pressure.scale)||1));
  const total=os.totalmem(),free=os.freemem();
  const reserve=Math.max(MIN_MEMORY_RESERVE,Math.floor(total*0.20));
  const usable=Math.max(0,free-reserve);
  const memorySlots=Math.max(1,Math.floor(usable/PER_WORKER_MEMORY_BUDGET));
  const activeLimit=Math.max(1,Math.min(HARD_MAX_ACTIVE,cpuSlots,memorySlots));
  const admittedLimit=Math.min(MAX_ADMITTED,Math.max(2,activeLimit*2));
  // Tiny machines may use all their slots for one project. Larger machines keep
  // room for unrelated projects while still allowing useful intra-project fanout.
  const perProjectActiveLimit=activeLimit<=2?activeLimit:Math.min(3,Math.ceil(activeLimit/2));
  return {cpus,total,free,reserve,baseCpuSlots,cpuSlots,memorySlots,activeLimit,admittedLimit,
    perProjectActiveLimit,pressure};
}
function normalizedProjectKey(value?:string){
  const v=value?.trim();
  if(!v)return undefined;
  return process.platform==='win32'?v.toLowerCase():v;
}

class CodeModeScheduler {
  private workflows=new Map<string,WorkflowRecord>();
  private queue:Waiter[]=[];
  private completed=0;
  private cancelledQueued=0;
  private expiredQueued=0;
  private rejected=0;
  private parks=0;
  private wakes=0;
  private fairnessYields=0;
  private maxObservedActive=0;
  private maxObservedAdmitted=0;
  private totalWaitMs=0;

  private activeRecords(){
    return [...this.workflows.values()].filter(w=>w.state==='active');
  }
  private activeForProject(projectKey?:string,excludeId?:string){
    if(!projectKey)return 0;
    return this.activeRecords().filter(w=>w.id!==excludeId&&w.projectKey===projectKey).length;
  }
  private canStart(record:WorkflowRecord){
    const hw=dynamicLimit();
    if(this.activeRecords().length>=hw.activeLimit)return false;
    if(record.projectKey&&this.activeForProject(record.projectKey,record.id)>=hw.perProjectActiveLimit)return false;
    return true;
  }
  private start(record:WorkflowRecord,enqueuedAt:number){
    const now=Date.now(),waited=Math.max(0,now-enqueuedAt);
    record.state='active';
    record.parkReason=undefined;
    record.firstStartedAt??=now;
    record.lastStartedAt=now;
    record.waitedMs+=waited;
    this.totalWaitMs+=waited;
    this.maxObservedActive=Math.max(this.maxObservedActive,this.activeRecords().length);
  }
  private removeWaiter(waiter:Waiter){
    const i=this.queue.indexOf(waiter);
    if(i>=0)this.queue.splice(i,1);
  }
  private drain(){
    let progressed=true;
    while(progressed){
      progressed=false;
      for(let i=0;i<this.queue.length;i++){
        const waiter=this.queue[i];
        if(waiter.done){this.queue.splice(i--,1);continue;}
        const record=this.workflows.get(waiter.id);
        if(!record){waiter.done=true;clearTimeout(waiter.timeout);this.queue.splice(i--,1);continue;}
        if(!this.canStart(record))continue;
        this.queue.splice(i,1);
        waiter.done=true;
        clearTimeout(waiter.timeout);
        waiter.signal?.removeEventListener('abort',waiter.abort);
        this.start(record,waiter.enqueuedAt);
        waiter.resolve();
        progressed=true;
        break;
      }
    }
  }
  private waitForSlot(record:WorkflowRecord,signal?:AbortSignal):Promise<void>{
    if(signal?.aborted)return Promise.reject(signal.reason??new AgentError(
      'CODE_MODE_CANCELLED','Code Mode was cancelled before a local worker slot became available.'));
    if(this.canStart(record)&&this.queue.length===0){
      this.start(record,Date.now());
      return Promise.resolve();
    }
    record.state='queued';
    const enqueuedAt=Date.now();
    return new Promise<void>((resolve,reject)=>{
      const waiter:Waiter={
        id:record.id,enqueuedAt,signal,done:false,timeout:undefined as unknown as NodeJS.Timeout,
        abort:()=>{},resolve,reject,
      };
      const fail=(error:unknown,expired:boolean)=>{
        if(waiter.done)return;
        waiter.done=true;
        clearTimeout(waiter.timeout);
        waiter.signal?.removeEventListener('abort',waiter.abort);
        this.removeWaiter(waiter);
        if(expired)this.expiredQueued++;else this.cancelledQueued++;
        reject(error);
        this.drain();
      };
      waiter.abort=()=>fail(signal?.reason??new AgentError('CODE_MODE_CANCELLED',
        'Code Mode was cancelled while waiting for a local worker slot.'),false);
      waiter.timeout=setTimeout(()=>fail(new AgentError('CODE_MODE_QUEUE_TIMEOUT',
        'Code Mode waited too long for a local worker slot; no new work was started.',true),true),QUEUE_TIMEOUT_MS);
      signal?.addEventListener('abort',waiter.abort,{once:true});
      this.queue.push(waiter);
      if(signal?.aborted)waiter.abort();
      else this.drain();
    });
  }
  async acquire(id:string,signal?:AbortSignal):Promise<CodeModeLease>{
    if(signal?.aborted)
      throw signal.reason??new AgentError('CODE_MODE_CANCELLED','Code Mode was cancelled before admission.');
    const hw=dynamicLimit();
    if(this.workflows.size>=hw.admittedLimit){
      this.rejected++;
      throw new AgentError('CODE_MODE_CAPACITY',
        'Code Mode capacity is full ('+hw.admittedLimit+' admitted workflows on this hardware right now). Retry after one workflow completes.',true);
    }
    const record:WorkflowRecord={id,admittedAt:Date.now(),waitedMs:0,state:'queued'};
    this.workflows.set(id,record);
    this.maxObservedAdmitted=Math.max(this.maxObservedAdmitted,this.workflows.size);
    try{await this.waitForSlot(record,signal);}
    catch(error){this.workflows.delete(id);this.drain();throw error;}
    const activeLimitAtStart=dynamicLimit().activeLimit;
    let released=false;
    const resume=async(resumeSignal?:AbortSignal)=>{
      if(released)throw new AgentError('CODE_MODE_RELEASED','Code Mode workflow has already been released.');
      const current=this.workflows.get(id);
      if(!current)throw new AgentError('CODE_MODE_RELEASED','Code Mode workflow is no longer admitted.');
      if(current.state==='active')return;
      await this.waitForSlot(current,resumeSignal??signal);
      this.wakes++;
    };
    return {
      workflowId:id,
      waitedMs:record.waitedMs,
      activeLimitAtStart,
      setProject:async(projectKey?:string,projectSignal?:AbortSignal)=>{
        if(released)return;
        const current=this.workflows.get(id);if(!current)return;
        const key=normalizedProjectKey(projectKey);
        if(!key||current.projectKey===key)return;
        current.projectKey=key;
        if(current.state==='active'){
          const hwNow=dynamicLimit();
          if(this.activeForProject(key,id)>=hwNow.perProjectActiveLimit){
            current.state='parked';current.parkReason='project-fairness';
            this.fairnessYields++;this.parks++;this.drain();
            await resume(projectSignal);
          }
        }else if(current.state==='queued')this.drain();
      },
      parkWhile:async<T>(reason:string,work:()=>Promise<T>,parkSignal?:AbortSignal):Promise<T>=>{
        if(released)return work();
        const current=this.workflows.get(id);
        if(!current||current.state!=='active')return work();
        current.state='parked';current.parkReason=reason.slice(0,120);
        this.parks++;this.drain();
        try{return await work();}
        finally{await resume(parkSignal);}
      },
      release:()=>{
        if(released)return;
        released=true;
        const current=this.workflows.get(id);
        if(current){
          this.workflows.delete(id);
          this.queue.filter(w=>w.id===id).forEach(w=>{
            if(w.done)return;w.done=true;clearTimeout(w.timeout);
            w.signal?.removeEventListener('abort',w.abort);
            w.reject(new AgentError('CODE_MODE_RELEASED','Code Mode workflow completed while queued.'));
          });
          this.queue=this.queue.filter(w=>w.id!==id);
          this.completed++;
        }
        this.drain();
      },
    };
  }
  status(){
    const hw=dynamicLimit(),now=Date.now();
    const active=this.activeRecords();
    const parked=[...this.workflows.values()].filter(w=>w.state==='parked');
    const queued=[...this.workflows.values()].filter(w=>w.state==='queued');
    const projects=new Map<string,{active:number;queued:number;parked:number}>();
    for(const w of this.workflows.values()){
      if(!w.projectKey)continue;
      const p=projects.get(w.projectKey)??{active:0,queued:0,parked:0};
      p[w.state]++;projects.set(w.projectKey,p);
    }
    return {
      hardMaxAdmitted:MAX_ADMITTED,
      admittedLimit:hw.admittedLimit,
      hardMaxActive:HARD_MAX_ACTIVE,
      activeLimit:hw.activeLimit,
      perProjectActiveLimit:hw.perProjectActiveLimit,
      active:active.length,queued:queued.length,parked:parked.length,admitted:this.workflows.size,
      cpu:{parallelism:hw.cpus,baseSlots:hw.baseCpuSlots,slots:hw.cpuSlots},
      pressure:hw.pressure,
      memory:{
        totalBytes:hw.total,freeBytes:hw.free,reserveBytes:hw.reserve,
        perWorkerBudgetBytes:PER_WORKER_MEMORY_BUDGET,slots:hw.memorySlots,
      },
      activeWorkflows:active.map(x=>({workflowId:x.id,projectKey:x.projectKey??null,
        ageMs:Math.max(0,now-(x.lastStartedAt??x.admittedAt)),waitedMs:x.waitedMs})),
      queuedWorkflows:queued.slice(0,12).map(x=>({workflowId:x.id,projectKey:x.projectKey??null,
        waitMs:Math.max(0,now-x.admittedAt)})),
      parkedWorkflows:parked.slice(0,12).map(x=>({workflowId:x.id,projectKey:x.projectKey??null,
        reason:x.parkReason??'waiting'})),
      projects:[...projects.entries()].slice(0,24).map(([projectKey,counts])=>({projectKey,...counts})),
      completed:this.completed,cancelledQueued:this.cancelledQueued,expiredQueued:this.expiredQueued,
      rejected:this.rejected,parks:this.parks,wakes:this.wakes,fairnessYields:this.fairnessYields,
      maxObservedActive:this.maxObservedActive,maxObservedAdmitted:this.maxObservedAdmitted,
      totalWaitMs:this.totalWaitMs,
      policy:'Adaptive Code Mode: hard ceiling 12 admitted / 8 runnable, continuously scaled by live CPU utilization, free RAM and event-loop lag. Per-project runnable fairness is capped at 1-3. Wait-heavy workflows can park and wake without losing admission.',
    };
  }
}
export const codeModeScheduler=new CodeModeScheduler();
