/** Task-type-aware parallelism policy.
 * This does not plan work; it only caps concurrency for work the calling AI already requested.
 */
import {systemPressureStatus} from './system-pressure.js';

type StepLike={tool?:string;args?:Record<string,unknown>;command?:string;cwd?:string};
export type WorkloadClass='read'|'search'|'semantic'|'plugin'|'mutation'|'verification'|'build'|'process'|'general';

const READ=new Set(['read_file','read_code_file','read_multiple_files','read_code_ranges','list_directory','get_file_info','git_status','git_diff','git_log','project_context','project_map','context_pack']);
const SEARCH=new Set(['start_search','get_more_search_results','ast_search','find_symbols','find_references','code_graph','lsp_query']);
const SEMANTIC=new Set(['semantic_project_search']);
const PLUGIN=new Set(['call_plugin_tool','list_plugin_tools']);
const MUTATION=new Set(['write_file','edit_block','edit_files','apply_patch','workspace_batch','edit_code_ranges','replace_symbol','move_file','delete_file','create_directory','worker_integrate']);
const VERIFY=new Set(['run_project_checks','discover_checks']);
const PROCESS=new Set(['start_process','read_process_output','interact_with_process']);
const HEAVY_COMMAND=/\b(?:build|test|typecheck|check|lint|pytest|playwright|cypress|cargo|dotnet|gradle|mvn|docker\s+(?:build|pull)|(?:npm|pnpm|yarn|bun)\s+(?:ci|install|build|test|run\s+(?:build|test|check|typecheck)))\b/i;

export function classifyWorkload(step:StepLike):WorkloadClass{
  if(typeof step.command==='string')return HEAVY_COMMAND.test(step.command)?'build':'process';
  const tool=step.tool??'';
  if(READ.has(tool))return 'read';
  if(SEARCH.has(tool))return 'search';
  if(SEMANTIC.has(tool))return 'semantic';
  if(PLUGIN.has(tool))return 'plugin';
  if(MUTATION.has(tool))return 'mutation';
  if(VERIFY.has(tool))return 'verification';
  if(tool==='run_command'){
    const command=typeof step.args?.command==='string'?step.args.command:'';
    return HEAVY_COMMAND.test(command)?'build':'process';
  }
  if(PROCESS.has(tool))return 'process';
  return 'general';
}
export function adaptiveTaskConcurrency(calls:readonly StepLike[],requested=3){
  const pressure=systemPressureStatus();
  const classes=calls.map(classifyWorkload);
  const counts=Object.fromEntries([...new Set(classes)].map(c=>[c,classes.filter(x=>x===c).length])) as Partial<Record<WorkloadClass,number>>;
  const heavy=(counts.build??0)+(counts.verification??0);
  let base=3;
  if(classes.length<=1)base=1;
  else if(classes.every(c=>c==='build'||c==='verification'))base=1;
  else if(heavy>0)base=2; // one heavy lane + one responsive/light lane
  else if((counts.mutation??0)>0)base=2;
  else if((counts.semantic??0)>0)base=2;
  else if((counts.plugin??0)>0&&classes.every(c=>c==='plugin'))base=2;
  if(pressure.level==='warm')base=Math.min(base,2);
  if(pressure.level==='high'||pressure.level==='critical')base=1;
  const effective=Math.max(1,Math.min(3,Math.trunc(requested)||1,base,pressure.suggestedSafeParallelism));
  return {requested:Math.max(1,Math.min(3,Math.trunc(requested)||1)),effective,classes,counts,pressure:pressure.level,
    policy:heavy>0?'Heavy verification/build work keeps one slot while allowing at most one light companion lane.':
      (counts.mutation??0)>0?'Mutation groups are capped at two; file/path locks remain authoritative.':
      'Read/search fanout may use up to three slots when machine pressure is normal.'};
}
export function taskParallelismStatus(){
  return {max:3,classes:['read','search','semantic','plugin','mutation','verification','build','process','general'],
    policy:'Concurrency follows requested task type plus live CPU/RAM/event-loop pressure. This layer never creates tasks or chooses a plan.'};
}
