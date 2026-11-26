/** Code Mode is one BRC MCP tool, backed by a local QuickJS/WASM worker. */
import { zodToJsonSchema } from 'zod-to-json-schema';
import type { ToolResult } from 'brc-shared';
import {CODE_MODE_DISCOVERY_DESCRIPTION} from 'brc-shared';
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
    description:CODE_MODE_DISCOVERY_DESCRIPTION,
    inputSchema:zodToJsonSchema(CodeModeArgsSchema),
    annotations:{title:'Code Mode (local sandbox)',readOnlyHint:false,destructiveHint:true},
  }];
}
