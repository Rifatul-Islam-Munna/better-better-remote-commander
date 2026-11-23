/** Bounded local hybrid semantic/lexical source index.
 * The cache is local-only, excludes sensitive/binary/dependency paths, and is
 * globally capped at 64 MiB by semanticCacheStore with oldest-first eviction.
 */
import fs from 'node:fs/promises';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {rootPath,listProjectFiles,projectPath,sensitive,inside,slash} from './workspace.js';
import {sourceCandidate} from './languages.js';
import {readBounded,decodeCode} from './file-safety.js';
import {checkCancelled} from '../context.js';
import {AgentError} from './errors.js';
import {semanticCacheStore} from './bounded-local-store.js';

const VERSION=1;
const MAX_FILES=1600;
const MAX_SOURCE_BYTES=24*1024*1024;
const MAX_FILE_BYTES=512*1024;
const MAX_CHUNKS=7000;
const CHUNK_LINES=32;
const OVERLAP_LINES=6;
const MAX_PREVIEW=1200;
const MAX_TERMS=96;
const MAX_RECORD_TARGET=18*1024*1024;
const FRESH_MS=5*60_000;

const STOP=new Set([
  'the','and','for','with','from','this','that','into','return','const','let','var',
  'function','async','await','class','interface','type','import','export','default',
  'true','false','null','undefined','new','string','number','boolean','object','void',
  'public','private','protected','static','get','set','use','using','where','when',
  'what','why','how','does','have','has','are','was','were','then','than','not',
]);

interface Chunk {
  path:string;
  lineStart:number;
  lineEnd:number;
  preview:string;
  terms:string[];
  symbols:string[];
}
interface SemanticRecord {
  version:1;
  root:string;
  builtAt:number;
  accessedAt:number;
  files:number;
  sourceBytes:number;
  truncated:boolean;
  chunks:Chunk[];
}
const dirtyRoots=new Set<string>();
let invalidateAll=false;

function keyFor(root:string){
  return createHash('sha256').update(process.platform==='win32'?root.toLowerCase():root).digest('hex');
}
function splitCamel(value:string){
  return value.replace(/([a-z0-9])([A-Z])/g,'$1 $2').replace(/[_\-.\/\\:]+/g,' ');
}
function tokens(text:string,max=MAX_TERMS):string[]{
  const cleaned=splitCamel(text)
    .replace(/(['"`])(?:\\.|(?!\1).){24,}\1/g,' ')
    .toLowerCase();
  const out:string[]=[];
  const seen=new Set<string>();
  for(const raw of cleaned.match(/[a-z][a-z0-9]{1,39}/g)??[]){
    if(STOP.has(raw)||seen.has(raw))continue;
    seen.add(raw);out.push(raw);
    if(out.length>=max)break;
  }
  return out;
}
function symbols(text:string):string[]{
  const out:string[]=[];
  const seen=new Set<string>();
  const patterns=[
    /\b(?:class|interface|type|enum|function|def|fn|func|struct|trait|record)\s+([A-Za-z_$][\w$]*)/g,
    /\b(?:const|let|var|final|val)\s+([A-Za-z_$][\w$]*)\s*[=:]/g,
    /\b([A-Za-z_$][\w$]{2,})\s*\([^\n)]{0,160}\)\s*(?:=>|\{|:)/g,
  ];
  for(const re of patterns){
    let match:RegExpExecArray|null;
    while((match=re.exec(text))&&out.length<24){
      const name=match[1];
      if(!seen.has(name)){seen.add(name);out.push(name);}
    }
  }
  return out;
}
function normalizePreview(text:string):string{
  const value=text.replace(/\r/g,'').trim();
  return value.length<=MAX_PREVIEW?value:value.slice(0,MAX_PREVIEW)+'\n…';
}
function scoreChunk(chunk:Chunk,query:string[],rawQuery:string):number{
  if(!query.length)return 0;
  const termSet=new Set(chunk.terms);
  const symbolTokens=new Set(chunk.symbols.flatMap(s=>tokens(s,12)));
  const pathTokens=new Set(tokens(chunk.path,48));
  let score=0;
  for(const q of query){
    if(termSet.has(q))score+=4;
    if(symbolTokens.has(q))score+=7;
    if(pathTokens.has(q))score+=5;
    for(const term of chunk.terms){
      if(term!==q&&term.startsWith(q)&&q.length>=3){score+=0.7;break;}
    }
  }
  const lower=chunk.preview.toLowerCase();
  const phrase=rawQuery.trim().toLowerCase();
  if(phrase.length>=4&&lower.includes(phrase))score+=12;
  if(query.every(q=>termSet.has(q)||symbolTokens.has(q)||pathTokens.has(q)))score+=8;
  return score;
}
async function fitRecord(record:SemanticRecord):Promise<SemanticRecord>{
  let encoded=Buffer.byteLength(JSON.stringify(record),'utf8');
  while(encoded>MAX_RECORD_TARGET&&record.chunks.length>100){
    record.chunks.length=Math.max(100,Math.floor(record.chunks.length*0.82));
    record.truncated=true;
    encoded=Buffer.byteLength(JSON.stringify(record),'utf8');
  }
  if(encoded>MAX_RECORD_TARGET)
    throw new AgentError('SEMANTIC_INDEX_TOO_LARGE','Semantic index could not fit the bounded local cache; narrow the project root.');
  return record;
}
async function build(rawRoot:string):Promise<SemanticRecord>{
  const root=await rootPath(rawRoot);
  const listing=await listProjectFiles(root,MAX_FILES,{fresh:true});
  const chunks:Chunk[]=[];
  let sourceBytes=0,files=0,truncated=listing.truncated;
  for(const relative of listing.files){
    checkCancelled();
    if(chunks.length>=MAX_CHUNKS||sourceBytes>=MAX_SOURCE_BYTES){truncated=true;break;}
    if(sensitive(relative)||!sourceCandidate(relative))continue;
    const file=await projectPath(root,relative);
    let stat:import('node:fs').Stats;
    try{stat=await fs.stat(file);}catch{continue;}
    if(!stat.isFile()||stat.size===0||stat.size>MAX_FILE_BYTES)continue;
    let data:Buffer;
    try{data=await readBounded(file,MAX_FILE_BYTES);}catch{continue;}
    sourceBytes+=data.length;files++;
    const text=decodeCode(data);
    const lines=text.split(/\r?\n/);
    const step=Math.max(1,CHUNK_LINES-OVERLAP_LINES);
    for(let start=0;start<lines.length;start+=step){
      if(chunks.length>=MAX_CHUNKS){truncated=true;break;}
      const end=Math.min(lines.length,start+CHUNK_LINES);
      const body=lines.slice(start,end).join('\n');
      const chunkTerms=tokens(body);
      const syms=symbols(body);
      if(!chunkTerms.length&&!syms.length)continue;
      chunks.push({
        path:slash(relative),lineStart:start+1,lineEnd:end,
        preview:normalizePreview(body),terms:chunkTerms,symbols:syms,
      });
      if(end===lines.length)break;
    }
  }
  const now=Date.now();
  return fitRecord({version:VERSION,root,builtAt:now,accessedAt:now,files,sourceBytes,truncated,chunks});
}
async function load(rawRoot:string,refresh=false):Promise<{record:SemanticRecord;rebuilt:boolean}>{
  const root=await rootPath(rawRoot),id=keyFor(root);
  let record=refresh||dirtyRoots.has(root)?null:await semanticCacheStore.get<SemanticRecord>('project',id);
  const valid=record?.version===VERSION&&record.root===root&&Array.isArray(record.chunks);
  if(!valid||Date.now()-(record?.builtAt??0)>FRESH_MS){
    record=await build(root);
    await semanticCacheStore.put('project',id,record);
    dirtyRoots.delete(root);
    return {record,rebuilt:true};
  }
  record!.accessedAt=Date.now();
  return {record:record!,rebuilt:false};
}

export async function semanticProjectSearch(args:{
  path:string;query:string;limit?:number;refresh?:boolean;
}){
  if(typeof args.query!=='string'||args.query.trim().length<2||args.query.length>500)
    throw new AgentError('INVALID_ARGUMENT','semantic search query must be 2-500 characters.');
  const limit=Math.max(1,Math.min(30,args.limit??10));
  const {record,rebuilt}=await load(args.path,args.refresh===true);
  const query=tokens(args.query,32);
  const ranked=record.chunks.map(chunk=>({chunk,score:scoreChunk(chunk,query,args.query)}))
    .filter(x=>x.score>0)
    .sort((a,b)=>b.score-a.score||a.chunk.path.localeCompare(b.chunk.path)||a.chunk.lineStart-b.chunk.lineStart)
    .slice(0,limit)
    .map(({chunk,score})=>({
      path:chunk.path,lineStart:chunk.lineStart,lineEnd:chunk.lineEnd,
      score:Math.round(score*100)/100,symbols:chunk.symbols.slice(0,12),preview:chunk.preview,
    }));
  return {
    root:record.root,query:args.query,results:ranked,index:{
      rebuilt,builtAt:record.builtAt,files:record.files,chunks:record.chunks.length,
      sourceBytes:record.sourceBytes,truncated:record.truncated,persistent:true,
      cacheLimitBytes:64*1024*1024,
    },
    note:'Local hybrid semantic search uses code symbols, path terms and lexical relevance. It is not a remote embedding service; use LSP/code_graph for compiler-grade semantics.',
  };
}
export function invalidateSemanticProject(file?:string){
  if(!file){invalidateAll=true;return;}
  const absolute=path.resolve(file);
  for(const root of knownRoots)if(inside(root,absolute))dirtyRoots.add(root);
}
const knownRoots=new Set<string>();
export async function registerSemanticRoot(rawRoot:string){
  const root=await rootPath(rawRoot);knownRoots.add(root);return root;
}
export async function semanticSearch(args:{path:string;query:string;limit?:number;refresh?:boolean}){
  const root=await registerSemanticRoot(args.path);
  const refresh=args.refresh===true||invalidateAll;
  const result=await semanticProjectSearch({...args,path:root,refresh});
  if(invalidateAll){invalidateAll=false;dirtyRoots.clear();}
  return result;
}
export async function semanticSearchStatus(){
  const store=await semanticCacheStore.status();
  const records=await semanticCacheStore.list<SemanticRecord>('project',50);
  return {
    store,
    projects:records.map(r=>({
      root:r.value.root,builtAt:r.value.builtAt,files:r.value.files,
      chunks:r.value.chunks?.length??0,sourceBytes:r.value.sourceBytes,
      bytes:r.bytes,truncated:r.value.truncated,
    })),
    freshnessMs:FRESH_MS,maxFilesPerBuild:MAX_FILES,maxChunksPerBuild:MAX_CHUNKS,
  };
}
