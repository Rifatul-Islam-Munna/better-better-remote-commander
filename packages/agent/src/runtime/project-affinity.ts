/** Lightweight project-affinity detection for Code Mode fairness.
 * Uses only local filesystem metadata and a small in-memory cache.
 */
import fs from 'node:fs/promises';
import path from 'node:path';

const CACHE_TTL_MS=5*60_000;
const CACHE_MAX=256;
const MARKERS=['.git','pnpm-workspace.yaml','package.json','pyproject.toml','Cargo.toml','go.mod','deno.json','composer.json'];
const cache=new Map<string,{key:string;at:number}>();

function normalize(value:string){
  const resolved=path.resolve(value);
  return process.platform==='win32'?resolved.toLowerCase():resolved;
}
function remember(dir:string,key:string){
  cache.set(dir,{key,at:Date.now()});
  if(cache.size>CACHE_MAX){
    const oldest=[...cache.entries()].sort((a,b)=>a[1].at-b[1].at).slice(0,cache.size-CACHE_MAX);
    for(const [k] of oldest)cache.delete(k);
  }
}
async function isFileLike(candidate:string){
  try{return (await fs.stat(candidate)).isFile();}
  catch{return path.extname(candidate).length>0;}
}
async function hasMarker(dir:string){
  for(const marker of MARKERS){
    try{await fs.access(path.join(dir,marker));return true;}catch{}
  }
  return false;
}
async function findRoot(candidate:string){
  let dir=(await isFileLike(candidate))?path.dirname(candidate):candidate;
  dir=path.resolve(dir);
  const cacheKey=normalize(dir),saved=cache.get(cacheKey);
  if(saved&&Date.now()-saved.at<CACHE_TTL_MS)return saved.key;
  const visited:string[]=[];
  let current=dir;
  for(let i=0;i<9;i++){
    const key=normalize(current);
    const hit=cache.get(key);
    if(hit&&Date.now()-hit.at<CACHE_TTL_MS){
      for(const v of visited)remember(v,hit.key);
      return hit.key;
    }
    visited.push(key);
    if(await hasMarker(current)){
      const root=normalize(current);
      for(const v of visited)remember(v,root);
      return root;
    }
    const parent=path.dirname(current);
    if(parent===current)break;
    current=parent;
  }
  const fallback=normalize(dir);
  for(const v of visited)remember(v,fallback);
  return fallback;
}
function candidates(args:Record<string,unknown>){
  const out:string[]=[];
  for(const key of ['cwd','root','project','repo','path','file_path']){
    const value=args[key];
    if(typeof value==='string'&&path.isAbsolute(value))out.push(value);
  }
  if(Array.isArray(args.paths)){
    for(const value of args.paths)if(typeof value==='string'&&path.isAbsolute(value))out.push(value);
  }
  return out;
}
export async function inferProjectAffinity(args:Record<string,unknown>){
  const found=candidates(args);
  if(!found.length)return undefined;
  return findRoot(found[0]);
}
export function projectAffinityStatus(){
  return {cachedDirectories:cache.size,cacheMax:CACHE_MAX,ttlMs:CACHE_TTL_MS,markers:MARKERS};
}
