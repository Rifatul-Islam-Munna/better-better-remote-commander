import test from 'node:test';
import assert from 'node:assert/strict';
import {fileURLToPath} from 'node:url';
import {activity} from '../dist/runtime/activity.js';
import {callTool} from '../dist/dispatcher.js';

const target=fileURLToPath(new URL('../package.json',import.meta.url));

function nestedStartLines(lines){
  return lines.filter(x=>x.includes('Tool call read_file started'));
}
function cleanup(before,previous,resume){
  activity.consoleQueue.splice(before);
  activity.queuedBytes=activity.consoleQueue.reduce((n,v)=>n+Buffer.byteLength(v),0);
  resume();
  activity.setConsoleMode(previous);
}

test('auto specialist routing is visible in nested MyMCP tool-start logs',async()=>{
  const previous=activity.getConsoleMode();
  activity.setConsoleMode('mymcp');
  const resume=activity.pauseConsole();
  const before=activity.consoleQueue.length;
  try{
    const source=[
      '// go.mod pubspec.yaml',
      'const r=await tools.call("read_file",{path:'+JSON.stringify(target)+'});',
      'return r.text.slice(0,20);',
    ].join('\n');
    const reply=await callTool('code_mode',{source,max_calls:4,max_runtime_ms:10000});
    assert.notEqual(reply.isError,true,JSON.stringify(reply.structuredContent?.error));
    const starts=nestedStartLines(activity.consoleQueue.slice(before));
    assert.equal(starts.length,1,activity.consoleQueue.slice(before).join('\n'));
    assert.match(starts[0],/Specialist: explorer\/go/);
    assert.match(starts[0],/Routing: AUTO/);
  }finally{cleanup(before,previous,resume);}
});

test('explicit specialist routing is visible in nested MyMCP tool-start logs',async()=>{
  const previous=activity.getConsoleMode();
  activity.setConsoleMode('mymcp');
  const resume=activity.pauseConsole();
  const before=activity.consoleQueue.length;
  try{
    const source=[
      'const r=await tools.agent.specialist({role:"explorer",specialty:"flutter",task:"inspect model",steps:[',
      '  {tool:"read_file",args:{path:'+JSON.stringify(target)+'}}',
      ']});',
      'return r.completed;',
    ].join('\n');
    const reply=await callTool('code_mode',{source,max_calls:4,max_runtime_ms:10000});
    assert.notEqual(reply.isError,true,JSON.stringify(reply.structuredContent?.error));
    const starts=nestedStartLines(activity.consoleQueue.slice(before));
    assert.equal(starts.length,1,activity.consoleQueue.slice(before).join('\n'));
    assert.match(starts[0],/Specialist: explorer\/flutter/);
    assert.match(starts[0],/Routing: EXPLICIT/);
  }finally{cleanup(before,previous,resume);}
});
