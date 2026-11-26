/** Hardware-aware lightweight job capacity. No state, I/O or model calls. */
import os from 'node:os';

export const MAX_LIGHTWEIGHT_PARALLELISM=6;
export type ParallelPressure='normal'|'warm'|'high'|'critical';
const GiB=1024**3;

export function lightweightHardwareLimit(cpuParallelism:number,totalMemoryBytes:number):number {
  const cpus=Number.isFinite(cpuParallelism)?Math.max(1,Math.floor(cpuParallelism)):1;
  const memory=Number.isFinite(totalMemoryBytes)?Math.max(0,totalMemoryBytes):0;
  // Leave CPU headroom and avoid assigning desktop-sized pools to small devices.
  return Math.max(1,Math.min(MAX_LIGHTWEIGHT_PARALLELISM,
    Math.max(1,Math.floor(cpus/2)),Math.max(1,Math.floor(memory/(2*GiB)))));
}
export function lightweightConcurrencyLimit():number {
  const cpus=typeof os.availableParallelism==='function'?os.availableParallelism():os.cpus().length;
  return lightweightHardwareLimit(cpus,os.totalmem());
}
export function pressureConcurrencyLimit(level:ParallelPressure,hardwareLimit=lightweightConcurrencyLimit()):number {
  const cap=level==='normal'?MAX_LIGHTWEIGHT_PARALLELISM:level==='warm'?3:level==='high'?2:1;
  return Math.max(1,Math.min(hardwareLimit,cap));
}
