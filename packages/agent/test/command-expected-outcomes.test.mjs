import {test,after} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {createServer} from 'node:http';
import {rgPath} from '@vscode/ripgrep';

const scratch=await fs.mkdtemp(path.join(os.tmpdir(),'brc-command-outcomes-'));
process.env.BRC_HOME=path.join(scratch,'home');
process.env.BRC_TOOL_HISTORY='0';process.env.BRC_ERROR_LOG='0';
const project=path.join(scratch,'project');await fs.mkdir(project);
await fs.writeFile(path.join(project,'fixture.txt'),'known_needle\n');
const {configManager}=await import('../dist/core/config-manager.js');
await configManager.setValue('allowedDirectories',[project]);
const {runCommandCore}=await import('../dist/tools/brc-tools.js');
const {executeCodeMode}=await import('../dist/runtime/code-mode.js');
const {summarizeCommandActivity}=await import('../dist/runtime/command-log-summary.js');
const run=args=>runCommandCore({cwd:project,timeout_ms:6000,raw:true,...args},{maxTimeoutMs:8000});
after(async()=>{await configManager.close();await fs.rm(scratch,{recursive:true,force:true,maxRetries:6,retryDelay:80});});

test('native rg exit 1 is a no-match result with its original exit code',async()=>{
 const result=await run({program:rgPath,args:['not_in_fixture','fixture.txt']});
 assert.notEqual(result.isError,true,JSON.stringify(result));
 assert.equal(result.structuredContent.exitCode,1);assert.equal(result.structuredContent.noMatches,true);
 assert.equal(result.structuredContent.commandSucceeded,true);assert.equal(result.structuredContent.error,undefined);
 assert.equal(result.structuredContent.processOutcome,'no_matches');
 assert.equal(summarizeCommandActivity(result).detail,'no matches (exit 1)');
});

test('rg syntax/path errors and unrelated exit 1 remain real failures',async()=>{
 for(const args of [['[','fixture.txt'],['HTTP/1.1 404[','fixture.txt'],['known_needle','missing.txt']]){
  const result=await run({program:rgPath,args});
  assert.equal(result.isError,true);assert.equal(result.structuredContent.exitCode,2);
  assert.equal(result.structuredContent.noMatches,undefined);
  assert.equal(result.structuredContent.error.code,'COMMAND_FAILED');
 }
 const result=await run({program:process.execPath,args:['-e','process.exit(1)']});
 assert.equal(result.isError,true);assert.equal(result.structuredContent.error.code,'COMMAND_FAILED');
 assert.equal(result.structuredContent.commandSucceeded,false);
});

test('native rg quiet mode with no matches remains successful',async()=>{
 const result=await run({program:rgPath,args:['--quiet','not_in_fixture','fixture.txt']});
 assert.notEqual(result.isError,true);assert.equal(result.structuredContent.noMatches,true);
});

test('PowerShell loopback HTTP failure reports status without pretending the request succeeded',{skip:process.platform!=='win32'},async()=>{
 const server=createServer((req,res)=>{res.writeHead(404);res.end('fixture missing');});
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
 try{
  const url='http://127.0.0.1:'+server.address().port+'/fixture';
  const result=await run({program:'powershell.exe',args:['-NoProfile','-NonInteractive','-Command',"$ErrorActionPreference='Stop'; Invoke-WebRequest -UseBasicParsing -Uri '"+url+"'"]});
  assert.equal(result.isError,true,JSON.stringify(result));
  assert.equal(result.structuredContent.httpStatus,404);
  assert.equal(result.structuredContent.error.code,'HTTP_REQUEST_FAILED');
  assert.equal(result.structuredContent.commandSucceeded,false);assert.equal(result.structuredContent.exitCode,1);
  const parameters={program:'powershell.exe',cwd:project,args:['-NoProfile','-NonInteractive','-Command',
   '#'+ 'x'.repeat(1200)+"\n$ErrorActionPreference='Stop'; Invoke-WebRequest -UseBasicParsing -Uri '"+url+"'"]};
  const coded=await executeCodeMode({source:'return await tools.call("run_command",'+JSON.stringify(parameters)+');',max_calls:2,max_runtime_ms:6000},
   [{name:'run_command',description:'Native command',inputSchema:{type:'object'},annotations:{readOnlyHint:false}}],
   (name,args)=>run(args));
  assert.equal(coded.structuredContent.errorCode,'HTTP_REQUEST_FAILED',JSON.stringify(coded));
  assert.match(coded.content[0].text,/HTTP_REQUEST_FAILED: The remote HTTP endpoint returned status 404/);
 }finally{server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}
});
