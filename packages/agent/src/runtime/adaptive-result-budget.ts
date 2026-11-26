/** Adaptive model-facing result projection for Code Mode.
 * Full bounded/redacted data remains available through local result handles.
 */
import type {ToolResult} from 'brc-shared';
import {DEFAULT_READ_CHARS,DEFAULT_BATCH_READ_CHARS} from './read-budget.js';

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
export function compactStructured(meta:Record<string,unknown>|undefined,tool='',sourceBudget=DEFAULT_READ_CHARS){
 if(!meta)return undefined;
 const keep=['error','currentSha256','sha256','sourceSha256','path','mode','offset','length','totalLines',
   'charOffset','charLength','startCharOffset','totalChars','nextCharOffset','sizeBytes','read','batch','truncated','moreLines',
   'candidateLines','actualReplacements','failedFileIndex','failedEditIndex','write',
   'next','recovery','rollbackSnapshotId','rollbackSnapshotIds','verificationEvidence',
   'pending','jobId','jobIds','status','exitCode','passed','failed','total','changed','results','changes'];
 const out:Record<string,unknown>={};
 for(const k of keep)if(meta[k]!==undefined)out[k]=meta[k];
 if(tool==='read_code_file'&&typeof meta.text==='string'){
  // Source text is part of this tool's API, not expendable display metadata.
  // Keep a contiguous prefix and advance by that prefix, never an unseen tail.
  out.text=meta.text.slice(0,sourceBudget);
  if(meta.text.length>sourceBudget){
   out.truncated=true;out.outputTruncated=true;
   const start=meta.mode==='characters'?meta.charOffset:meta.startCharOffset;
   if(typeof start==='number'&&typeof meta.path==='string'){
    out.nextCharOffset=start+sourceBudget;
    out.next={tool:'read_code_file',args:{path:meta.path,charOffset:out.nextCharOffset,charLength:sourceBudget}};
    if(meta.mode==='characters')out.charLength=sourceBudget;
   }
  }
 }
 const bounded:Record<string,unknown>={},omitted:string[]=[];let chars=2;
 // Large nested results/previews cannot displace hashes and recovery cursors.
 // Omission remains explicit; the full reply stays in its result handle.
 for(const [key,value]of Object.entries(out)){
  let size:number;try{size=JSON.stringify({[key]:value}).length;}catch{omitted.push(key);continue;}
  if(chars+size>32_000){omitted.push(key);continue;}
  bounded[key]=value;chars+=size;
 }
 if(omitted.length){bounded.omittedStructuredFields=omitted;bounded.outputTruncated=true;}
 return Object.keys(bounded).length?bounded:undefined;
}
export function adaptiveToolBudget(tool:string,result:ToolResult){
 const isError=!!result.isError;
 const textBudget=isError?12_000:BUILD.test(tool)?16_000:SEARCH.test(tool)?18_000:
   READ.test(tool)?(tool==='read_multiple_files'?DEFAULT_BATCH_READ_CHARS:DEFAULT_READ_CHARS)+1024:22_000;
 const handleThreshold=textBudget;
 return {textBudget,handleThreshold,isError};
}
export function projectAdaptiveToolResult(tool:string,result:ToolResult,text:string,hasHandle:boolean){
 const b=adaptiveToolBudget(tool,result);
 if(tool==='read_code_file'&&typeof result.structuredContent?.text==='string'){
  const meta=compactStructured(result.structuredContent,tool)!;
  const source=typeof meta.text==='string'?meta.text:'';
  const header='Source: '+String(meta.path??'').slice(0,512)+'\nSHA256: '+String(meta.sha256??'')+'\n\n';
  return {text:header+source+(meta.truncated?'\n[PARTIAL SOURCE: follow structuredContent.next for the next contiguous range.]':''),
    structuredContent:meta,budget:b};
 }
 const selected=READ.test(tool)&&text.length>b.textBudget
   ?text.slice(0,b.textBudget)+(hasHandle
     ?'\n[OUTPUT TRUNCATED: contiguous prefix only. Retrieve the full local resultHandle or follow source pagination before deriving edits.]'
     :'\n[OUTPUT TRUNCATED: contiguous prefix only. Follow source pagination or request a narrower range before deriving edits.]')
   :relevant(text,b.textBudget,b.isError);
 if(!hasHandle&&!READ.test(tool))return {text:selected,structuredContent:result.structuredContent,budget:b};
 return {text:selected,structuredContent:compactStructured(result.structuredContent,tool),budget:b};
}
export function adaptiveFinalText(text:string,budget=18_000){
 return relevant(text,budget,false);
}
