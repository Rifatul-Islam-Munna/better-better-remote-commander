/**
 * QuickJS/WASM Code Mode guest. This worker is trusted infrastructure; the
 * model-authored program runs ONLY inside QuickJS, with no Node globals,
 * module loader, filesystem, process, network, or native object handles.
 * The sole bridge is a JSON-only, parent-validated RPC function.
 */
import { parentPort, workerData } from 'node:worker_threads';
import { getQuickJS } from 'quickjs-emscripten';

type RpcRequest = {type:'call'|'search'|'describe'|'semantic'|'snippet'|'agent'|'harness'|'trace'|'step'|'roundtrip'|'workflow';name?:string;args?:unknown;query?:string};
type RpcReply = {ok:boolean;value?:unknown;error?:{code:string;message:string}};
type Incoming = {type:'reply';id:number;reply:RpcReply}|{type:'cancel'};
type Inputs = {source:string;deadline:number};

if (!parentPort) throw new Error('Code Mode worker requires a parent thread.');
const port=parentPort;
const input=workerData as Inputs;
const pending=new Map<number,{resolve:(value:RpcReply)=>void;reject:(error:Error)=>void}>();
// A guest can launch tools.call() without awaiting it. Never destroy the VM
// while its bridge promises are still live: JS_FreeRuntime can abort on them.
const inFlightGuestCalls=new Set<Promise<void>>();
let sequence=0;
let cancelled=false;

port.on('message',(message:Incoming)=>{
  if(message?.type==='cancel'){
    cancelled=true;
    for(const waiter of pending.values())waiter.reject(new Error('CODE_MODE_CANCELLED'));
    pending.clear();
  }else if(message?.type==='reply'){
    const waiter=pending.get(message.id);
    if(waiter){pending.delete(message.id);waiter.resolve(message.reply);}
  }
});

function rpc(request:RpcRequest):Promise<RpcReply>{
  if(cancelled||Date.now()>=input.deadline)
    return Promise.reject(new Error('CODE_MODE_CANCELLED'));
  const id=++sequence;
  return new Promise<RpcReply>((resolve,reject)=>{
    pending.set(id,{resolve,reject});
    port.postMessage({type:'rpc',id,request});
  });
}

function safeError(error:unknown):string{
  const value=error instanceof Error?error.message:String(error);
  return value.slice(0,900);
}

async function execute():Promise<unknown>{
  const vm=(await getQuickJS()).newContext();
  vm.runtime.setMemoryLimit(32*1024*1024);
  vm.runtime.setMaxStackSize(512*1024);
  vm.runtime.setInterruptHandler(()=>cancelled||Date.now()>=input.deadline);
  try {
    const bridge=vm.newFunction('__brcCodeModeRpc',requestHandle=>{
      // Extract the string BEFORE returning from the callback: QuickJS handles
      // provided as callback parameters are no longer valid asynchronously.
      const raw=vm.getString(requestHandle);
      const pendingResult=vm.newPromise();
      const settle=(reply:RpcReply)=>{
        if(!vm.alive)return;
        const handle=vm.newString(JSON.stringify(reply));
        pendingResult.resolve(handle);
        handle.dispose();
      };
      if(raw.length>512*1024){
        settle({ok:false,error:{code:'CODE_MODE_INPUT_LIMIT',message:'Tool arguments exceed 512 KiB.'}});
      }else{
        try{
          const request=JSON.parse(raw) as RpcRequest;
          const operation=rpc(request).then(settle,error=>settle({ok:false,error:{
            code:'CODE_MODE_BRIDGE_FAILED',message:safeError(error)}}));
          inFlightGuestCalls.add(operation);
          // Observe both outcomes without creating an unhandled rejection.
          void operation.then(()=>inFlightGuestCalls.delete(operation),
            ()=>inFlightGuestCalls.delete(operation));
        }catch(error){
          settle({ok:false,error:{code:'CODE_MODE_BRIDGE_FAILED',message:safeError(error)}});
        }
      }
      pendingResult.settled.then(()=>{
        if(vm.alive)vm.runtime.executePendingJobs();
      }).catch(()=>undefined);
      return pendingResult.handle;
    });
    bridge.consume(fn=>vm.setProp(vm.global,'__brcCodeModeRpc',fn));

    // Tools return a QuickJS Promise. The bridge resumes guest microtasks
    // after a local BRC tool completes; generated code uses await tools.call.
    const prelude=[
      'const __rpc = async function(request) {',
      ' const response = JSON.parse(await __brcCodeModeRpc(JSON.stringify(request)));',
      ' if (!response.ok) {',
      '   const e = new Error((response.error && response.error.code || "CODE_MODE_ERROR") + ": " + (response.error && response.error.message || "Tool operation failed"));',
      '   e.code = response.error && response.error.code || "CODE_MODE_ERROR";',
      '   throw e;',
      ' }',
      ' return response.value;',
      '};',
      'const __workflowRun = async function(query,context={}) {',
      ' const match = await __rpc({type:"workflow",name:"match",query});',
      ' if (!match || !match.found) return {matched:false};',
      ' const AsyncFunction = Object.getPrototypeOf(async function(){}).constructor;',
      ' const fn = new AsyncFunction("tools","context",match.source);',
      ' const value = await fn(tools,context);',
      ' return {matched:true,id:match.id,name:match.name,score:match.score,value};',
      '};',
      'const tools = Object.freeze({',
      ' search(query) { return __rpc({type:"search",query}); },',
      ' describe(name) { return __rpc({type:"describe",name}); },',
      ' call(name,args={}) { return name==="saved_workflow_run" ? __workflowRun(args.query,args.context||{}) : __rpc({type:"call",name,args}); },',
      ' step: async function(name,fn) {',
      '   if (typeof name !== "string" || typeof fn !== "function") throw new Error("tools.step(name, asyncFn) requires a name and function");',
      '   const existing = await __rpc({type:"step",name,args:{op:"get"}});',
      '   if (existing && existing.found) return existing.value;',
      '   const value = await fn();',
      '   await __rpc({type:"step",name,args:{op:"save",value}});',
      '   return value;',
      ' },',
      ' semantic: Object.freeze({ search(args) { return __rpc({type:"semantic",name:"search",args}); } }),',
      ' snippets: Object.freeze({',
      '   search(query,limit=10) { return __rpc({type:"snippet",name:"search",query,args:{limit}}); },',
      '   get(id) { return __rpc({type:"snippet",name:"get",args:{id}}); },',
      '   save(args) { return __rpc({type:"snippet",name:"save",args}); }',
      ' }),',
      ' sessions: Object.freeze({',
      '   open(args) { return __rpc({type:"roundtrip",name:"session_open",args}); },',
      '   get(id) { return __rpc({type:"roundtrip",name:"session_get",args:{id}}); },',
      '   update(id,patch) { return __rpc({type:"roundtrip",name:"session_update",args:{id,patch}}); },',
      '   list(limit=20) { return __rpc({type:"roundtrip",name:"session_list",args:{limit}}); }',
      ' }),',
      ' results: Object.freeze({',
      '   store(value,label="result") { return __rpc({type:"roundtrip",name:"result_store",args:{value,label}}); },',
      '   get(id,args={}) { return __rpc({type:"roundtrip",name:"result_get",args:{id,...args}}); },',
      '   drop(id) { return __rpc({type:"roundtrip",name:"result_drop",args:{id}}); }',
      ' }),',
      ' terminal: Object.freeze({ use(args) { return __rpc({type:"roundtrip",name:"terminal",args}); } }),',
      ' browser: Object.freeze({ use(args) { return __rpc({type:"roundtrip",name:"browser",args}); } }),',
      ' fibers: Object.freeze({ run(args) { return __rpc({type:"roundtrip",name:"fiber",args}); } }),',
      ' workflows: Object.freeze({',
      '   match(query) { return __rpc({type:"workflow",name:"match",query}); },',
      '   run(query,context={}) { return __workflowRun(query,context); }',
      ' }),',
      ' agent: Object.freeze({',
      '   run(args) { return __rpc({type:"agent",name:"run",args}); },',
      '   roles() { return __rpc({type:"agent",name:"roles",args:{}}); }',
      ' }),',
      ' harness: Object.freeze({ status() { return __rpc({type:"harness",name:"status",args:{}}); } }),',
      ' trace: Object.freeze({ current() { return __rpc({type:"trace",name:"current",args:{}}); } })',
      '});',
    ].join('\n');

    const program='(async function(){\n"use strict";\n'+prelude+'\n'+input.source+'\n})()';
    const evaluation=vm.evalCode(program,'brc-code-mode.js');
    if(evaluation.error){
      const detail=vm.dump(evaluation.error) as {message?:unknown}|string;
      evaluation.error.dispose();
      throw new Error(typeof detail==='string'?detail:String(detail?.message??'Sandbox evaluation failed.'));
    }
    const promiseHandle=evaluation.value;
    try{
      // The bridge's deferred promises trigger executePendingJobs as each
      // host operation settles. resolvePromise then awaits the real final
      // guest value without polling or blocking the event loop.
      const awaiting=vm.resolvePromise(promiseHandle);
      // Start the guest async function even when no host RPC is called.
      // Later RPC completions wake the remaining microtasks automatically.
      vm.runtime.executePendingJobs();
      const settled=await awaiting;
      const unawaited=inFlightGuestCalls.size;
      if(unawaited){
        // A detached tools.call() promise can outlive the guest script. Drain
        // every already-started host call and any guest continuation it wakes
        // before disposing QuickJS. Once this loop ends there is no unresolved
        // bridge work, so the parent can report a completed workflow with a
        // warning instead of a synthetic harness failure.
        while(inFlightGuestCalls.size){
          await Promise.allSettled([...inFlightGuestCalls]);
          if(vm.alive)vm.runtime.executePendingJobs();
        }
      }
      if(settled.error){
        const detail=vm.dump(settled.error) as {message?:unknown}|string;
        settled.error.dispose();
        throw new Error(typeof detail==='string'?detail:String(detail?.message??'Sandbox promise rejected.'));
      }
      try{return {value:vm.dump(settled.value),unawaitedCalls:unawaited};}
      finally{settled.value.dispose();}
    }finally{promiseHandle.dispose();}
  }finally{
    vm.dispose();
  }
}

void execute().then(value=>{
  port.postMessage({type:'done',value});
}).catch(error=>{
  port.postMessage({type:'error',message:safeError(error)});
});
