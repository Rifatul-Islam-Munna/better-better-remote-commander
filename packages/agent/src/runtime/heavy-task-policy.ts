/** Conservative Code Mode heavy-task detection.
 * Only definite long-running build/test/install/container-style work is escalated.
 * Watch/dev servers and ordinary short commands keep their existing behavior.
 */
type Args=Record<string,unknown>;

export interface HeavyEscalation {
  heavy:true;
  reason:string;
  label:string;
  spawnArgs:Record<string,unknown>;
}

const WATCH_OR_SERVER=/\b(?:--watch|watch|dev\b|serve\b|start\b.*(?:server|dev)|next\s+dev|vite(?:\s|$)|webpack\s+serve|nodemon)\b/i;
const HEAVY_PATTERNS:Array<[RegExp,string]>= [
  [/\b(?:npm|pnpm|yarn|bun)\s+(?:ci|install|i|update|upgrade|rebuild)\b/i,'package install/update'],
  [/\b(?:npm|pnpm|yarn|bun)\s+(?:(?:run\s+)?(?:build|test|e2e|integration|typecheck|check))\b/i,'package build/test'],
  [/\b(?:docker|podman)\s+(?:build|pull)\b/i,'container build/pull'],
  [/\b(?:docker|podman)\s+compose\s+(?:build|pull|up)\b/i,'container compose operation'],
  [/\bcargo\s+(?:build|test|check|clippy)\b/i,'Rust build/check'],
  [/\bgo\s+test\b/i,'Go test'],
  [/\bdotnet\s+(?:restore|build|test|publish)\b/i,'.NET build/test'],
  [/\b(?:mvn|mvnw|gradle|gradlew)(?:\.bat)?\b.*\b(?:test|build|assemble|check)\b/i,'JVM build/test'],
  [/\bflutter\s+(?:build|test|pub\s+get)\b/i,'Flutter build/test'],
  [/\bpytest\b/i,'Python test suite'],
  [/\b(?:playwright|cypress)\s+test\b/i,'browser test suite'],
];

function commandText(args:Args){
  if(typeof args.command==='string')return args.command.trim();
  if(typeof args.program==='string'){
    const argv=Array.isArray(args.args)?args.args.filter((x):x is string=>typeof x==='string'):[];
    return [args.program,...argv].join(' ').trim();
  }
  return '';
}
function labelFor(text:string,reason:string){
  const first=text.replace(/\s+/g,' ').trim().slice(0,80);
  return ('auto '+reason+(first?' - '+first:'')).slice(0,120);
}
export function classifyHeavyCodeModeTask(tool:string,args:Args,workflowId:string):HeavyEscalation|null{
  if(tool==='run_project_checks'){
    return {
      heavy:true,reason:'project verification suite',label:'auto project checks',
      spawnArgs:{
        steps:[{tool:'run_project_checks',args}],
        label:'auto project checks',group:'cm-'+workflowId.slice(0,12),
        retain_output:true,timeout_ms:30*60_000,
      },
    };
  }
  if(tool!=='run_command')return null;
  const text=commandText(args);
  if(!text||WATCH_OR_SERVER.test(text))return null;
  let reason:string|undefined;
  for(const [pattern,name] of HEAVY_PATTERNS){
    if(pattern.test(text)){reason=name;break;}
  }
  const timeout=typeof args.timeout_ms==='number'?args.timeout_ms:0;
  if(!reason&&timeout>=120_000)reason='explicit long command';
  if(!reason)return null;
  const jobStep={tool:'run_command',args};
  return {
    heavy:true,reason,label:labelFor(text,reason),
    spawnArgs:{
      steps:[jobStep],label:labelFor(text,reason),group:'cm-'+workflowId.slice(0,12),
      retain_output:true,timeout_ms:Math.max(5*60_000,Math.min(6*60*60_000,
        typeof args.timeout_ms==='number'?args.timeout_ms*3:30*60_000)),
    },
  };
}
export function heavyTaskPolicyStatus(){
  return {
    enabled:true,
    escalatedTools:['run_command','run_project_checks'],
    exclusions:['watch/dev/server commands'],
    policy:'Only definite long-running build/test/install/container/check work is auto-escalated to the existing durable job system. Short commands and persistent dev/watch servers are left unchanged.',
  };
}
