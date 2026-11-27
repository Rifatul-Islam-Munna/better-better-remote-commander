// Real temporary directories/junctions; never change the owner's BRC policy.
import {test,after} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {mkdtempSync,realpathSync} from 'node:fs';
const scratch=mkdtempSync(path.join(realpathSync.native(os.tmpdir()),'brc-directory-policy-'));
const project=path.join(scratch,'project'),store=path.join(scratch,'dependency-store');
process.env.BRC_HOME=path.join(scratch,'home');
process.env.BRC_LOG_LEVEL='silent';process.env.BRC_TOOL_HISTORY='0';process.env.BRC_ERROR_LOG='0';
await fs.mkdir(project);await fs.mkdir(store);
await fs.writeFile(path.join(store,'private.txt'),'external content must not be read');
const {configManager}=await import('../dist/core/config-manager.js');
await configManager.setValue('allowedDirectories',[project]);
const {callTool}=await import('../dist/dispatcher.js');
after(async()=>{await configManager.close();await fs.rm(scratch,{recursive:true,force:true,maxRetries:8,retryDelay:90});});
async function link(t,target,destination){
 try{await fs.symlink(target,destination,process.platform==='win32'?'junction':'dir');return true;}
 catch(e){if(e.code==='EPERM'||e.code==='ENOTSUP'){t.skip('Directory links unavailable: '+e.code);return false;}throw e;}
}

test('directory policy denial has a precise code and non-retrying recovery',async()=>{
 const result=await callTool('list_directory',{path:store});
 assert.equal(result.isError,true);
 assert.equal(result.structuredContent.error.code,'PATH_NOT_ALLOWED');
 assert.equal(result.structuredContent.error.retryable,false);
 assert.equal(result.structuredContent.recovery.automaticRetry,false);
 assert.match(result.structuredContent.recovery.suggestion,/symlink\/junction/);
 assert.doesNotMatch(JSON.stringify(result),/private\.txt|external content/);
 assert.deepEqual((await configManager.getConfig()).allowedDirectories,[project]);
});

test('external dependency junctions stay denied without DIRECTORY_LIST_FAILED',async t=>{
 const destination=path.join(project,'next');
 if(!await link(t,store,destination))return;
 for(const requested of [destination,path.join(destination,'missing','docs')]){
  const result=await callTool('list_directory',{path:requested,depth:3,limit:120});
  assert.equal(result.isError,true);
  assert.equal(result.structuredContent.error.code,'PATH_NOT_ALLOWED');
  assert.match(result.structuredContent.error.message,/Resolved symlink\/junction target/);
  assert.doesNotMatch(JSON.stringify(result),/private\.txt|external content/);
 }
 const parent=await callTool('list_directory',{path:project,depth:3});
 assert.notEqual(parent.isError,true);
 assert.match(parent.content[0].text,/\[LINK\] next/);
 assert.doesNotMatch(JSON.stringify(parent),/private\.txt|external content/);
});

test('junctions resolving inside the existing allowed root remain readable',async t=>{
 const target=path.join(project,'local-package');await fs.mkdir(target);
 await fs.writeFile(path.join(target,'index.js'),'export {};');
 const destination=path.join(project,'linked-package');
 if(!await link(t,target,destination))return;
 const result=await callTool('list_directory',{path:destination,depth:1});
 assert.notEqual(result.isError,true,JSON.stringify(result));
 assert.match(result.content[0].text,/\[FILE\] index\.js/);
});

test('unavailable directory policy keeps its own classified failure',async()=>{
 const original=configManager.getConfig;
 configManager.getConfig=async()=>{throw new Error('simulated policy failure');};
 try{
  const result=await callTool('list_directory',{path:project});
  assert.equal(result.isError,true);
  assert.equal(result.structuredContent.error.code,'PATH_POLICY_UNAVAILABLE');
  assert.equal(result.structuredContent.recovery.automaticRetry,false);
 }finally{configManager.getConfig=original;}
});
