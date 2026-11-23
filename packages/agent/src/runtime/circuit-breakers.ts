/** Small process-local circuit breakers for infrastructure-style dependencies.
 * They stop repeated transient failures across concurrent Code Mode workflows,
 * but never trip on ordinary file/build/business-logic errors.
 */
import {AgentError} from './errors.js';

const FAILURE_WINDOW_MS=30_000;
const THRESHOLD=3;
const BASE_OPEN_MS=10_000;
const MAX_OPEN_MS=60_000;
const MAX_STATES=64;
const TRANSIENT_CODES=new Set([
  'PLUGIN_BUSY','PLUGIN_START_FAILED','PLUGIN_DISCONNECTED','PLUGIN_CONNECTION_FAILED','PLUGIN_UNAVAILABLE',
  'LSP_BUSY','LSP_START_FAILED','LSP_UNAVAILABLE','PARSER_BUSY','PARSER_START_FAILED','PARSER_WORKER_FAILED',
  'SEARCH_START_FAILED','SEARCH_TIMEOUT','RESOURCE_ADMISSION_BUSY','RESOURCE_ADMISSION_TIMEOUT',
  'TEMPORARY_FAILURE','ECONNREFUSED','ECONNRESET','ETIMEDOUT','EPIPE',
]);
const TRANSIENT_MESSAGE=/(?:econnrefused|econnreset|socket hang up|connection (?:closed|reset|refused)|temporar(?:y|ily) unavailable|service unavailable|timed? out)/i;

interface State {
  key:string;
  failures:number[];
  openUntil:number;
  opens:number;
  halfOpenInFlight:boolean;
  lastFailureCode?:string;
  lastFailureAt?:number;
  lastSuccessAt?:number;
}
export interface CircuitPermit {key?:string;probe?:boolean}

function family(tool:string,args:Record<string,unknown>){
  if(tool==='call_plugin_tool'){
    const plugin=typeof args.plugin==='string'?args.plugin:'unknown';
    return 'plugin:'+plugin.slice(0,120);
  }
  if(tool==='lsp_query'){
    const lang=typeof args.language==='string'?args.language:'default';
    return 'lsp:'+lang.slice(0,60);
  }
  if(tool==='start_search'||tool==='get_more_search_results')return 'search';
  if(['ast_search','find_symbols','find_references','code_graph'].includes(tool))return 'parser';
  if(tool==='semantic_project_search')return 'semantic';
  return undefined;
}
function transient(code?:string,message?:string){
  return !!(code&&TRANSIENT_CODES.has(code))||!!(message&&TRANSIENT_MESSAGE.test(message));
}

class CircuitBreakers {
  private states=new Map<string,State>();
  private blocked=0;
  private opened=0;
  private closed=0;
  private probes=0;
  private transientFailures=0;

  private state(key:string){
    let state=this.states.get(key);
    if(!state){
      if(this.states.size>=MAX_STATES){
        const oldest=[...this.states.values()].sort((a,b)=>
          (a.lastFailureAt??a.lastSuccessAt??0)-(b.lastFailureAt??b.lastSuccessAt??0))[0];
        if(oldest)this.states.delete(oldest.key);
      }
      state={key,failures:[],openUntil:0,opens:0,halfOpenInFlight:false};
      this.states.set(key,state);
    }
    return state;
  }
  before(tool:string,args:Record<string,unknown>):CircuitPermit{
    const key=family(tool,args);
    if(!key)return {};
    const state=this.state(key),now=Date.now();
    state.failures=state.failures.filter(t=>now-t<=FAILURE_WINDOW_MS);
    if(state.openUntil>now){
      this.blocked++;
      throw new AgentError('CIRCUIT_OPEN',
        'Circuit '+key+' is temporarily open after repeated transient failures. Retry after '+Math.max(1,state.openUntil-now)+' ms.',true);
    }
    if(state.openUntil>0){
      if(state.halfOpenInFlight){
        this.blocked++;
        throw new AgentError('CIRCUIT_HALF_OPEN',
          'Circuit '+key+' is testing recovery with another workflow; retry shortly.',true);
      }
      state.halfOpenInFlight=true;
      this.probes++;
      return {key,probe:true};
    }
    return {key};
  }
  success(permit:CircuitPermit){
    if(!permit.key)return;
    const state=this.states.get(permit.key);if(!state)return;
    const wasOpen=state.openUntil>0||state.halfOpenInFlight;
    state.failures=[];state.openUntil=0;state.halfOpenInFlight=false;state.lastSuccessAt=Date.now();
    if(wasOpen){state.opens=0;this.closed++;}
  }
  failure(permit:CircuitPermit,code?:string,message?:string){
    if(!permit.key)return;
    const state=this.state(permit.key),now=Date.now();
    if(!transient(code,message)){
      // A deterministic/ordinary tool error proves the dependency responded.
      if(permit.probe)this.success(permit);
      return;
    }
    this.transientFailures++;
    state.lastFailureCode=code;state.lastFailureAt=now;state.halfOpenInFlight=false;
    state.failures=state.failures.filter(t=>now-t<=FAILURE_WINDOW_MS);
    state.failures.push(now);
    if(permit.probe||state.failures.length>=THRESHOLD){
      state.opens++;
      const cooldown=Math.min(MAX_OPEN_MS,BASE_OPEN_MS*Math.pow(2,Math.max(0,state.opens-1)));
      state.openUntil=now+cooldown;
      state.failures=[];
      this.opened++;
    }
  }
  status(){
    const now=Date.now();
    return {
      enabled:true,threshold:THRESHOLD,failureWindowMs:FAILURE_WINDOW_MS,
      baseOpenMs:BASE_OPEN_MS,maxOpenMs:MAX_OPEN_MS,blocked:this.blocked,opened:this.opened,
      closed:this.closed,probes:this.probes,transientFailures:this.transientFailures,
      circuits:[...this.states.values()].map(s=>({
        key:s.key,state:s.openUntil>now?'open':s.openUntil>0?'half-open':'closed',
        retryAfterMs:s.openUntil>now?s.openUntil-now:0,opens:s.opens,
        recentFailures:s.failures.length,lastFailureCode:s.lastFailureCode??null,
        lastFailureAt:s.lastFailureAt??null,lastSuccessAt:s.lastSuccessAt??null,
      })),
      policy:'Only infrastructure-like transient failures count. Ordinary command/build/file/tool errors do not open circuits.',
    };
  }
}
export const circuitBreakers=new CircuitBreakers();
