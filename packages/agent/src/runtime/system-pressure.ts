/** Live, process-local machine pressure sampling.
 * No user data, paths, commands, or outputs are stored. Samples stay in RAM only.
 */
import os from 'node:os';
import {pressureConcurrencyLimit} from './parallel-capacity.js';

type PressureLevel='normal'|'warm'|'high'|'critical';
interface CpuTotals {idle:number;total:number}

const SAMPLE_MS=1000;
const MAX_LAG_SAMPLES=24;
let cpuEwma=0;
let lastCpu:CpuTotals|undefined;
let expected=Date.now()+SAMPLE_MS;
let lagSamples:number[]=[];
let lastSampleAt=0;

function totals():CpuTotals{
  let idle=0,total=0;
  for(const cpu of os.cpus()){
    idle+=cpu.times.idle;
    total+=cpu.times.idle+cpu.times.user+cpu.times.sys+cpu.times.nice+cpu.times.irq;
  }
  return {idle,total};
}
function percentile(values:number[],p:number){
  if(!values.length)return 0;
  const sorted=[...values].sort((a,b)=>a-b);
  return sorted[Math.min(sorted.length-1,Math.max(0,Math.ceil(sorted.length*p)-1))]??0;
}
function sample(){
  const now=Date.now();
  const current=totals();
  if(lastCpu){
    const total=Math.max(1,current.total-lastCpu.total);
    const idle=Math.max(0,current.idle-lastCpu.idle);
    const utilization=Math.max(0,Math.min(1,1-idle/total));
    cpuEwma=cpuEwma===0?utilization:cpuEwma*0.72+utilization*0.28;
  }
  lastCpu=current;
  const lag=Math.max(0,now-expected);
  lagSamples.push(lag);
  if(lagSamples.length>MAX_LAG_SAMPLES)lagSamples.shift();
  expected=now+SAMPLE_MS;
  lastSampleAt=now;
}
lastCpu=totals();
const timer=setInterval(sample,SAMPLE_MS);
timer.unref?.();

function levelFor(cpu:number,freeRatio:number,freeBytes:number,lagP95:number):PressureLevel{
  if(freeBytes<768*1024*1024||freeRatio<0.06||(cpu>0.94&&lagP95>120)||lagP95>250)return 'critical';
  if(freeBytes<1536*1024*1024||freeRatio<0.10||cpu>0.90||lagP95>120)return 'high';
  if(freeBytes<3*1024*1024*1024||freeRatio<0.18||cpu>0.75||lagP95>55)return 'warm';
  return 'normal';
}
function scaleFor(level:PressureLevel){
  if(level==='critical')return 0.28;
  if(level==='high')return 0.50;
  if(level==='warm')return 0.75;
  return 1;
}
export function systemPressureStatus(){
  const totalBytes=os.totalmem(),freeBytes=os.freemem();
  const freeRatio=totalBytes?freeBytes/totalBytes:1;
  const lagP95Ms=Math.round(percentile(lagSamples,0.95));
  const cpuUtilization=Math.max(0,Math.min(1,cpuEwma));
  const level=levelFor(cpuUtilization,freeRatio,freeBytes,lagP95Ms);
  const scale=scaleFor(level);
  return {
    level,scale,
    cpuUtilization:Math.round(cpuUtilization*1000)/1000,
    cpuPercent:Math.round(cpuUtilization*1000)/10,
    memory:{totalBytes,freeBytes,freeRatio:Math.round(freeRatio*1000)/1000},
    eventLoop:{lagP95Ms,samples:lagSamples.length},
    lastSampleAt,
    suggestedSafeParallelism:pressureConcurrencyLimit(level),
    policy:'RAM-only pressure sampler. Capacity shrinks under sustained CPU use, low free memory, or event-loop lag; it grows again automatically when pressure clears.',
  };
}
