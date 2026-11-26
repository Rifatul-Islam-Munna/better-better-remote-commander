import {test,after} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {createHash} from 'node:crypto';
const scratch=await fs.mkdtemp(path.join(os.tmpdir(),'brc-io-budgets-'));
process.env.BRC_HOME=path.join(scratch,'home');process.env.BRC_ERROR_LOG='0';
const root=path.join(scratch,'project');await fs.mkdir(root);
const {configManager}=await import('../dist/core/config-manager.js');
await configManager.setValue('allowedDirectories',[root]);
const {callTool}=await import('../dist/dispatcher.js');
const {projectAdaptiveToolResult,compactStructured}=await import('../dist/runtime/adaptive-result-budget.js');
const {executeWithSafeRetry}=await import('../dist/runtime/harness-vnext.js');
const hash=s=>createHash('sha256').update(s).digest('hex');
after(async()=>{await configManager.close();await fs.rm(scratch,{recursive:true,force:true,maxRetries:8,retryDelay:80});});

test('compact read_file pages preserve every line and advance without rereading earlier pages',async()=>{
 const file=path.join(root,'lines.ts'),lines=Array.from({length:140},(_,i)=>'const row'+i+' = "'+'x'.repeat(110)+'";');
 await fs.writeFile(file,lines.join('\n'));
 let args={path:file,length:140,maxChars:1000},returned=[],calls=0;
 for(;;){
  const result=await callTool('read_file',args);assert.notEqual(result.isError,true,JSON.stringify(result));
  const meta=result.structuredContent.read;
  const body=result.content[0].text.split('\n\n').slice(1).join('\n\n').replace(/\n\n\[OUTPUT TRUNCATED:[\s\S]*$/,'');
  returned.push(...body.split('\n'));assert.ok(result.content[0].text.length<1600);
  if(!meta.next)break;
  assert.equal(meta.next.tool,'read_file');assert.ok(meta.next.args.offset>args.offset||args.offset===undefined);
  args=meta.next.args;assert.ok(++calls<30);
 }
 assert.deepEqual(returned,lines);
 const defaultRead=await callTool('read_file',{path:file,length:140});
 assert.equal(defaultRead.structuredContent.read.maxCharsApplied,12000);
 assert.ok(defaultRead.content[0].text.length<13000);
});

test('source character continuation starts after the returned range, including a nonzero line offset',async()=>{
 const file=path.join(root,'long.ts'),prefix='// preceding source\n',body='X'.repeat(46000);
 await fs.writeFile(file,prefix+body);
 const first=await callTool('read_code_file',{path:file,offset:1,length:1});
 const a=first.structuredContent;assert.equal(a.text.length,12000);
 assert.equal(a.nextCharOffset,prefix.length+12000);assert.equal(a.sha256,hash(prefix+body));
 const parts=[a.text];let next=a.next;
 while(next){const r=await callTool(next.tool,next.args);parts.push(r.structuredContent.text);next=r.structuredContent.next;}
 assert.equal(parts.join(''),body);
});

test('Code Mode keeps source text/hash and moves its cursor by the visible source prefix',()=>{
 const source='Q'.repeat(40000),meta={path:'D:/fixture/long.ts',mode:'characters',charOffset:500,charLength:source.length,totalChars:50000,sha256:'a'.repeat(64),text:source,nextCharOffset:40500};
 const result={content:[{type:'text',text:source}],structuredContent:meta};
 const view=projectAdaptiveToolResult('read_code_file',result,source,true);
 assert.equal(view.structuredContent.sha256,meta.sha256);
 assert.equal(view.structuredContent.text,source.slice(0,12000));
 assert.equal(view.structuredContent.nextCharOffset,12500);
 assert.equal(view.structuredContent.next.args.charOffset,12500);
 const bounded=compactStructured({...meta,results:['z'.repeat(80000)]},'read_code_file');
 assert.equal(bounded.sha256,meta.sha256);assert.ok(bounded.omittedStructuredFields.includes('results'));
 assert.ok(JSON.stringify(bounded).length<32500);
 const lines=Array.from({length:500},(_,i)=>'line '+i+' '+'.'.repeat(100)).join('\n');
 const preview=projectAdaptiveToolResult('read_file',{content:[],structuredContent:{read:{next:{offset:20}}}},lines,true);
 assert.ok(preview.text.startsWith(lines.slice(0,1000)));
 assert.ok(!preview.text.includes('line 499'),'distant source tail must not be stitched onto an apparent contiguous page');
 assert.deepEqual(preview.structuredContent.read.next,{offset:20});
});

test('batched reads bound combined output and return cursors for unseen files',async()=>{
 const files=[];for(let i=0;i<6;i++){const file=path.join(root,'batch-'+i+'.ts');await fs.writeFile(file,Array.from({length:140},(_,n)=>'file'+i+' row'+n+' '+'.'.repeat(120)).join('\n'));files.push(file);}
 const r=await callTool('read_multiple_files',{paths:files});assert.notEqual(r.isError,true);
 assert.ok(r.content.reduce((n,c)=>n+(c.text?.length??0),0)<33000);
 assert.equal(r.structuredContent.batch.truncated,true);
 assert.ok(r.structuredContent.batch.continuations.some(x=>x.reason.includes('not returned')));
 for(const item of r.structuredContent.batch.continuations)assert.ok(item.next?.args.path);
});

test('failed multi-file edits identify exact reread window/hash and commit no staged hunks',async()=>{
 const one=path.join(root,'first.ts'),two=path.join(root,'second.ts');
 const a='export const first = 1;\n',b='export const second = 2;\n';await fs.writeFile(one,a);await fs.writeFile(two,b);
 const r=await callTool('edit_files',{files:[
  {path:one,expected_sha256:hash(a),edits:[{old_string:'first = 1',new_string:'first = 9'}]},
  {path:two,expected_sha256:hash(b),edits:[{old_string:'second = 2',new_string:'second = 8'},{old_string:'missing anchor',new_string:'replacement'}]},
 ]});
 assert.equal(r.isError,true);assert.equal(r.structuredContent.error.code,'EDIT_MISMATCH');
 assert.equal(r.structuredContent.currentSha256,hash(b));assert.equal(r.structuredContent.failedFileIndex,1);
 assert.equal(r.structuredContent.failedEditIndex,1);assert.equal(r.structuredContent.changed,false);
 assert.equal(r.structuredContent.next.args.maxChars,6000);
 assert.equal(await fs.readFile(one,'utf8'),a);assert.equal(await fs.readFile(two,'utf8'),b);
});

test('oversized writes return compact chunk guidance without echoing content or changing the file',async()=>{
 const file=path.join(root,'write.ts'),original='export const keep = 1;\n';await fs.writeFile(file,original);
 const r=await callTool('write_file',{path:file,mode:'rewrite',content:'SECRET_CHUNK_BODY\n'.repeat(2100)});
 assert.equal(r.isError,true);assert.equal(r.structuredContent.error.code,'WRITE_LINE_LIMIT');
 assert.equal(r.structuredContent.write.writeCommitted,false);
 assert.equal(r.structuredContent.write.recommendedChunkChars,12000);
 assert.ok(JSON.stringify(r).length<2200);assert.ok(!JSON.stringify(r).includes('SECRET_CHUNK_BODY'));
 assert.equal(await fs.readFile(file,'utf8'),original);
});

test('deterministic read failures are never automatically retried back to back',async()=>{
 for(const code of ['INVALID_ARGUMENT','ENOENT','CODE_SNAPSHOT_FAILED','TOOL_FAILED','FILE_READ_FAILED']){
  let calls=0;
  const r=await executeWithSafeRetry('read_file',true,async()=>{
   calls++;return {isError:true,content:[],structuredContent:{error:{code,retryable:false}}};
  },{retry:async()=>{throw new Error('deterministic errors must not enter the retry loop');}});
  assert.equal(calls,1,code);assert.equal(r.attempts,1,code);assert.equal(r.result.isError,true);
 }
});
