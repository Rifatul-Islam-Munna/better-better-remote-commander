import test from 'node:test';
import assert from 'node:assert/strict';
import {activity} from '../dist/runtime/activity.js';
import {callTool} from '../dist/dispatcher.js';

test('a real dispatched read-only call emits bounded MyMCP start and completion receipts',async()=>{
 const previous=activity.getConsoleMode();
 activity.setConsoleMode('mymcp');
 const resume=activity.pauseConsole();
 const before=activity.consoleQueue.length;
 try{
   const reply=await callTool('agent_status',{});
   assert.notEqual(reply.isError,true,JSON.stringify(reply.structuredContent?.error));
   const emitted=activity.consoleQueue.slice(before);
   assert.equal(emitted.length,2,emitted.map(x=>x.slice(0,200)).join('\n'));
   assert.match(emitted[0],/🔧 Tool call agent_status started\s+#[a-zA-Z0-9_-]+/);
   assert.match(emitted[1],/✅ Tool call agent_status completed/);
   assert.match(emitted[1],/Call #[a-zA-Z0-9_-]+/);
   assert.match(emitted[1],/Result preview/);
   assert.doesNotMatch(emitted.join(''),/metadata:|"structuredContent"|"origin":/);
   assert.ok(Buffer.byteLength(emitted.join(''),'utf8')<=4096);
 }finally{
   activity.consoleQueue.splice(before);
   activity.queuedBytes=activity.consoleQueue.reduce((n,v)=>n+Buffer.byteLength(v),0);
   resume();
   activity.setConsoleMode(previous);
 }
});
