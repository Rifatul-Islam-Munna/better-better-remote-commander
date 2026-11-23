/** Adaptive model-facing result projection for Code Mode.
 * Full bounded/redacted data remains available through local result handles.
 */
import type {ToolResult} from 'brc-shared';

const ERROR=/\b(?:error|failed|failure|exception|fatal|panic|traceback|ts\d{3,5}|enoent|eacces|conflict)\b/i;
const BUILD=/^(?:run_command|run_project_checks|verify_project|get_job|wait_jobs)$/;
const SEARCH=/(?:search|symbol|reference|code_graph|lsp|semantic)/;
const READ=/^(?:read_file|read_code_file|read_multiple_files|read_code_ranges)$/;
function relevant(text:string,budget:number,isError:boolean){
 if(text.length<=budget)return text;
 const lines=text.split(/\r?\n/),chosen:string[]=[];
 const seen=new Set<number>();
 const add=(i:number)=>{if(i>=0&&i<lines.length&&!seen.has(i)){seen.add(i);chosen.push(lines[i]);}};
 for(let i=0;i<Math.min(16,lines.length);i++)add(i);
 if(isError||ERROR.test(text))for(let i=0;i<lines.length&&chosen.join('\n').length<budget*0.75;i++)
   if(ERROR.test(lines[i])){add(i-1);add(i);add(i+1);}
 for(let i=Math.max(0,lines.length-16);i<lines.length;i++)add(i);
 let out=chosen.join('\n');if(out.length>budget)out=out.slice(0,budget);
 return out+'\n[... full result retained in local resultHandle ...]';
}
function compactStructured(meta:Record<string,unknown>|undefined){
 if(!meta)return undefined;
 const keep=['error','pending','jobId','jobIds','status','exitCode','passed','failed','total','results','changed',
   'changes','currentSha256','next','recovery','rollbackSnapshotId','rollbackSnapshotIds','verificationEvidence'];
 const out:Record<string,unknown>={};
 for(const k of keep)if(meta[k]!==undefined)out[k]=meta[k];
 return Object.keys(out).length?out:undefined;
}
export function adaptiveToolBudget(tool:string,result:ToolResult){
 const isError=!!result.isError;
 const textBudget=isError?24_000:BUILD.test(tool)?16_000:SEARCH.test(tool)?18_000:READ.test(tool)?30_000:22_000;
 const handleThreshold=textBudget;
 return {textBudget,handleThreshold,isError};
}
export function projectAdaptiveToolResult(tool:string,result:ToolResult,text:string,hasHandle:boolean){
 const b=adaptiveToolBudget(tool,result),selected=relevant(text,b.textBudget,b.isError);
 if(!hasHandle)return {text:selected,structuredContent:result.structuredContent,budget:b};
 return {text:selected,structuredContent:compactStructured(result.structuredContent),budget:b};
}
export function adaptiveFinalText(text:string,budget=18_000){
 return relevant(text,budget,false);
}
