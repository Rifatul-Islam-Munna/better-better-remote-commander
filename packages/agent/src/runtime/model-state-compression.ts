/** Compact, model-facing execution state.
 * This is telemetry compression only; it never plans tasks or chooses work.
 */
export interface CompactStepLike{
  tool:string;
  outcome:'ok'|'error'|'pending'|'uncertain';
  durationMs:number;
  errorCode?:string;
  jobId?:string;
  attempts?:number;
  replayed?:boolean;
  coalesced?:boolean;
  parked?:boolean;
  projectKey?:string;
  autoEscalated?:boolean;
  escalationReason?:string;
}
export interface CompactPreflightLike{
  project:string;
  ok:boolean;
  warnings?:string[];
  healed?:string[];
  diskFreeBytes?:number;
  packageManager?:string|null;
}
function unique(values:string[],max:number){return [...new Set(values)].slice(0,max);}
export function compactModelState(input:{
  workflowId:string;
  status:string;
  steps:CompactStepLike[];
  pendingJobs:string[];
  retries:number;
  replayedSteps:number;
  touchedPaths?:string[];
  preflights?:CompactPreflightLike[];
  verification?:unknown[];
  recovery?:unknown;
  sourceObservations?:unknown;
  outputTruncated?:boolean;
}){
  const counts=new Map<string,{calls:number;errors:number;pending:number;ms:number}>();
  for(const s of input.steps){
    const row=counts.get(s.tool)??{calls:0,errors:0,pending:0,ms:0};
    row.calls++;row.ms+=Math.max(0,s.durationMs);
    if(s.outcome==='error'||s.outcome==='uncertain')row.errors++;
    if(s.outcome==='pending')row.pending++;
    counts.set(s.tool,row);
  }
  const failed=input.steps.filter(s=>s.outcome==='error'||s.outcome==='uncertain');
  const projects=unique(input.steps.map(s=>s.projectKey).filter((x):x is string=>!!x),8);
  const preflights=(input.preflights??[]).slice(0,8).map(p=>({
    project:p.project,ok:p.ok,
    warnings:(p.warnings??[]).slice(0,4),healed:(p.healed??[]).slice(0,4),
    ...(typeof p.diskFreeBytes==='number'?{diskFreeBytes:p.diskFreeBytes}:{}),
    ...(p.packageManager!==undefined?{packageManager:p.packageManager}:{}),
  }));
  const next=input.pendingJobs.length
    ?'Collect pending durable jobs; do not rerun them.'
    :failed.length
      ?'Inspect failed/uncertain steps before any mutation retry.'
      :'Execution is complete; use detailed results only if more evidence is needed.';
  return {
    version:1,workflowId:input.workflowId,status:input.status,
    progress:{
      calls:input.steps.length,ok:input.steps.filter(s=>s.outcome==='ok').length,
      errors:failed.length,pending:input.steps.filter(s=>s.outcome==='pending').length,
      retries:input.retries,replayedSteps:input.replayedSteps,
      coalesced:input.steps.filter(s=>s.coalesced).length,
      parked:input.steps.filter(s=>s.parked).length,
      autoEscalated:input.steps.filter(s=>s.autoEscalated).length,
    },
    tools:[...counts.entries()].sort((a,b)=>b[1].calls-a[1].calls||a[0].localeCompare(b[0]))
      .slice(0,16).map(([tool,v])=>({tool,...v})),
    errors:failed.slice(-8).map(s=>({tool:s.tool,code:s.errorCode??'TOOL_FAILED',outcome:s.outcome})),
    pendingJobs:unique(input.pendingJobs,16),projects,
    touchedPaths:unique(input.touchedPaths??[],24),
    preflight:preflights,
    verification:(input.verification??[]).slice(0,8),
    recovery:input.recovery??null,
    sourceObservations:input.sourceObservations??null,
    outputTruncated:input.outputTruncated===true,
    next,
  };
}
