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
      'USE WHEN: PRIMARY DEFAULT for multi-step coding. Independent technical workstreams MUST use tools.agent.run/specialist with role + specialty; omitted complex or multi-domain calls are auto-routed.',
      'USE INSTEAD: Direct tools only for one simple action or unavailable/unsupported Code Mode; large or truncated Code Mode output is NOT a fallback signal.',
      'IMPORTANT: Runs JavaScript in a bounded QuickJS/WASM sandbox ON THE PAIRED DEVICE (not the relay).',
      'The sandbox has NO Node.js process, filesystem, imports, browser, fetch or unrestricted host execution.',
      'Use tools.search/describe/call for local capabilities. tools.agent.specialist({role,specialty,task,steps}) is the explicit specialist API; tools.agent.roles() lists permissions. tools.parallelSafe handles safe read DAGs; worker_tasks coordinates shared coding DAGs.',
      'Concurrency is hardware-aware: hard ceiling 12 admitted / 8 runnable, continuously scaled by live CPU/RAM/event-loop pressure with per-project fairness. Safe reads coalesce, waits park/wake, transient infrastructure uses circuit breakers, and definite build/test/install/check work auto-escalates to durable jobs. Harness state shares 30 MiB; semantic cache has 64 MiB.',
      'Provide JavaScript statements in source and finish with return. Example:',
      'const a=await tools.call("git_status",{path:"D:/repo"});',
      'return {status:a.text.slice(0,1000)};',
      'Always await tool operations. Ordinary nested calls stay sequential; use tools.parallelSafe([...]) for independent safe reads, or separate Code Mode workflows / approved job tools for broader parallelism. Loops and conditionals are supported.',
      'A nested tool failure throws by default; use try/catch ONLY when recovery is understood. Caught tool failures remain flagged in the final structured status.',
      'For a long build use await tools.call("spawn_job", {command:"...",cwd:"..."}) and return the jobId;',
      'then use get_job/wait_jobs in subsequent direct MCP calls. Do not claim a pending job is complete.',
      'Never automatically repeat uncertain writes, and never use Code Mode to bypass user authorization.',
      'Discovery and tool-result payloads are bounded. If continuation/resultHandle appears, stay in Code Mode: use tools.results.search/get, semantic offset/limit/preview_chars, or a narrower query and return only compact evidence. Binary previews are the exception and should use direct tools.',
    ].join(' '),
    inputSchema:zodToJsonSchema(CodeModeArgsSchema),
    annotations:{title:'Code Mode (local sandbox)',readOnlyHint:false,destructiveHint:true},
  }];
}
