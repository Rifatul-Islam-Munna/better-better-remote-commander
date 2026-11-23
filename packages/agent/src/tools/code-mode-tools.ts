/** Code Mode is one BRC MCP tool, backed by a local QuickJS/WASM worker. */
import { zodToJsonSchema } from 'zod-to-json-schema';
import type { ToolResult } from 'brc-shared';
import type { ToolDefinition } from '../core/tool-definitions.js';
import { CodeModeArgsSchema, executeCodeMode } from '../runtime/code-mode.js';
import { AgentError, failure } from '../runtime/errors.js';
import { getCallContext } from '../context.js';

type Executor=(name:string,args:Record<string,unknown>)=>Promise<ToolResult>;
let executor:Executor|undefined;
let catalog:()=>ToolDefinition[]=()=>[];

export function setCodeModeToolExecutor(fn:Executor,getCatalog:()=>ToolDefinition[]):void{
  executor=fn;
  catalog=getCatalog;
}

export const CODE_MODE_TOOL_HANDLERS:Record<string,
  (args:Record<string,unknown>)=>Promise<ToolResult>>={
  code_mode:async(args)=>{
    if(!executor)return failure(new AgentError('CODE_MODE_UNAVAILABLE','Code Mode dispatcher is not initialized.'));
    const parsed=CodeModeArgsSchema.safeParse(args);
    if(!parsed.success)return failure(new AgentError('INVALID_ARGUMENT',
      'Invalid code_mode request: '+parsed.error.message.slice(0,600)));
    return executeCodeMode(parsed.data,catalog(),executor,getCallContext()?.signal);
  }
};

export function buildCodeModeToolDefinitions():ToolDefinition[]{
  return [{
    name:'code_mode',
    description:[
      'USE WHEN: PRIMARY DEFAULT for multi-step coding, testing, debugging, project research and installed-plugin workflows. Multiple Code Mode workflows may run concurrently through an adaptive local scheduler while direct tools remain fallback.',
      'USE INSTEAD: Direct read_file/edit_block/run_command for a single simple action; spawn_job for long builds.',
      'IMPORTANT: Runs JavaScript in a bounded QuickJS/WASM sandbox ON THE PAIRED DEVICE (not the relay).',
      'The sandbox has NO Node.js process, filesystem, imports, browser, fetch or unrestricted host execution.',
      'Use tools.search/describe/call for native and local harness capabilities. Durable steps, project sessions, result handles, workflow reuse, reusable terminal/browser aliases, fibers, safe retry, specialist roles, semantic search and traces are locally discoverable.',
      'Concurrency is hardware-aware: hard ceiling 12 admitted / 8 runnable, scaled down by CPU and free RAM. Harness/session/result/fiber state shares 30 MiB; semantic cache has 64 MiB. Existing permissions, source hashes, file locks and mutation guards still apply.',
      'Provide JavaScript statements in source and finish with return. Example:',
      'const a=await tools.call("git_status",{path:"D:/repo"});',
      'return {status:a.text.slice(0,1000)};',
      'Always await tools.search, tools.describe and tools.call. Nested calls inside one workflow stay sequential; parallel work should use separate Code Mode workflows or the approved job/parallel tools. Loops and conditionals are supported.',
      'A nested tool failure throws by default; use try/catch ONLY when recovery is understood. Caught tool failures remain flagged in the final structured status.',
      'For a long build use await tools.call("spawn_job", {command:"...",cwd:"..."}) and return the jobId;',
      'then use get_job/wait_jobs in subsequent direct MCP calls. Do not claim a pending job is complete.',
      'Never automatically repeat uncertain writes, and never use Code Mode to bypass user authorization.',
      'Discovery and tool-result payloads are bounded; binary previews should use direct tools.',
    ].join(' '),
    inputSchema:zodToJsonSchema(CodeModeArgsSchema),
    annotations:{title:'Code Mode (local sandbox)',readOnlyHint:false,destructiveHint:true},
  }];
}
