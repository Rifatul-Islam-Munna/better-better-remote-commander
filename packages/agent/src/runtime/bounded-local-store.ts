/** Agent-owned bounded local JSON stores with oldest-first eviction.
 * Never touches project files, credentials, relay state, or arbitrary paths.
 */
import fs from 'node:fs/promises';
import path from 'node:path';
import {randomBytes} from 'node:crypto';
import {homeDir} from '../transport/pairing.js';
import {AgentError} from './errors.js';

const OWNER='brc-bounded-store-v1\n';
const NAME=/^[a-z][a-z0-9-]{0,31}$/;
const ID=/^[a-zA-Z0-9._-]{1,128}$/;
const MAX_SCAN=4096;

interface Entry {
  file:string;
  name:string;
  bytes:number;
  mtimeMs:number;
}
export interface StoreStatus {
  name:string;
  root:string;
  maxBytes:number;
  bytes:number;
  records:number;
  lastPruned:number;
  removedRecords:number;
  removedBytes:number;
  scanLimitHit:boolean;
}

export class BoundedLocalStore {
  private lane:Promise<void>=Promise.resolve();
  private lastPruned=0;
  private removedRecords=0;
  private removedBytes=0;
  private scanLimitHit=false;
  constructor(
    readonly name:string,
    readonly maxBytes:number,
    readonly maxRecordBytes:number,
  ){
    if(!NAME.test(name))throw new Error('Invalid bounded store name.');
    if(!Number.isFinite(maxBytes)||maxBytes<1024*1024)throw new Error('Invalid bounded store quota.');
    if(!Number.isFinite(maxRecordBytes)||maxRecordBytes<1024||maxRecordBytes>maxBytes)
      throw new Error('Invalid bounded store record limit.');
  }
  root(){return path.join(homeDir(),this.name);}
  private marker(){return path.join(this.root(),'.brc-owned');}
  private file(category:string,id:string){
    if(!NAME.test(category)||!ID.test(id))throw new AgentError('INVALID_ARGUMENT','Invalid local store record identifier.');
    return path.join(this.root(),category+'--'+id+'.json');
  }
  private async ensure():Promise<string>{
    const root=this.root();
    const st=await fs.lstat(root).catch((e:NodeJS.ErrnoException)=>{
      if(e.code==='ENOENT')return null;
      throw e;
    });
    if(!st){
      await fs.mkdir(root,{recursive:true,mode:0o700});
      await fs.writeFile(this.marker(),OWNER,{flag:'wx',mode:0o600}).catch(async e=>{
        if((e as NodeJS.ErrnoException).code!=='EEXIST')throw e;
      });
    }else if(!st.isDirectory()||st.isSymbolicLink()){
      throw new AgentError('UNSAFE_CACHE_ROOT','Local agent store must be a real directory, not a symlink.');
    }
    const marker=await fs.readFile(this.marker(),'utf8').catch(()=>'');
    if(marker!==OWNER)throw new AgentError('UNOWNED_CACHE_ROOT','Refusing to modify an unrecognized local agent store.');
    return root;
  }
  private async entries(root:string):Promise<Entry[]>{
    const out:Entry[]=[];
    this.scanLimitHit=false;
    let count=0;
    for(const dirent of await fs.readdir(root,{withFileTypes:true})){
      if(dirent.name==='.brc-owned')continue;
      if(++count>MAX_SCAN){this.scanLimitHit=true;break;}
      if(!dirent.isFile()||dirent.isSymbolicLink()||!dirent.name.endsWith('.json'))continue;
      const file=path.join(root,dirent.name);
      try{
        const st=await fs.lstat(file);
        if(st.isFile()&&!st.isSymbolicLink())out.push({file,name:dirent.name,bytes:st.size,mtimeMs:st.mtimeMs});
      }catch{}
    }
    return out;
  }
  private async pruneLocked(protect?:string):Promise<{bytes:number;records:number;removed:number;removedBytes:number}>{
    const root=await this.ensure();
    const entries=await this.entries(root);
    if(this.scanLimitHit){
      const bytes=entries.reduce((n,e)=>n+e.bytes,0);
      return {bytes,records:entries.length,removed:0,removedBytes:0};
    }
    entries.sort((a,b)=>a.mtimeMs-b.mtimeMs||a.name.localeCompare(b.name));
    let bytes=entries.reduce((n,e)=>n+e.bytes,0),removed=0,removedBytes=0;
    for(const entry of entries){
      if(bytes<=this.maxBytes)break;
      if(protect&&path.resolve(entry.file)===path.resolve(protect))continue;
      try{
        const current=await fs.lstat(entry.file);
        if(!current.isFile()||current.isSymbolicLink()||current.size!==entry.bytes||current.mtimeMs!==entry.mtimeMs)continue;
        await fs.unlink(entry.file);
        bytes-=entry.bytes;removed++;removedBytes+=entry.bytes;
      }catch{}
    }
    this.lastPruned=Date.now();
    this.removedRecords+=removed;
    this.removedBytes+=removedBytes;
    return {bytes,records:Math.max(0,entries.length-removed),removed,removedBytes};
  }
  private serial<T>(fn:()=>Promise<T>):Promise<T>{
    const run=this.lane.catch(()=>{}).then(fn);
    this.lane=run.then(()=>{},()=>{});
    return run;
  }
  async put(category:string,id:string,value:unknown):Promise<{bytes:number;pruned:number}>{
    const json=JSON.stringify(value);
    const bytes=Buffer.from(json,'utf8');
    if(bytes.length>this.maxRecordBytes)
      throw new AgentError('LOCAL_STORE_RECORD_TOO_LARGE',
        'Local agent record exceeds '+this.maxRecordBytes+' bytes; reduce the workflow/index payload.');
    return this.serial(async()=>{
      const root=await this.ensure();
      const dest=this.file(category,id);
      const temp=dest+'.'+process.pid+'.'+randomBytes(5).toString('hex')+'.tmp';
      try{
        await fs.writeFile(temp,bytes,{flag:'wx',mode:0o600});
        // Windows scanners/readers can briefly hold the existing record.
        // Retry only publication of this already-written agent-owned snapshot;
        // never replay a tool, remove the destination or bypass quota checks.
        for(let attempt=0;;attempt++){
          try{await fs.rename(temp,dest);break;}
          catch(error){
            if(attempt>=4||!['EPERM','EACCES','EBUSY'].includes((error as NodeJS.ErrnoException).code??''))throw error;
            await new Promise(resolve=>setTimeout(resolve,25*(attempt+1)));
          }
        }
      }finally{await fs.unlink(temp).catch(()=>{});}
      const pruned=await this.pruneLocked(dest);
      const current=await fs.stat(dest).catch(()=>null);
      if(!current){
        throw new AgentError('LOCAL_STORE_QUOTA','Record could not be retained inside the local storage quota.');
      }
      if(pruned.bytes>this.maxBytes){
        await fs.unlink(dest).catch(()=>{});
        await this.pruneLocked();
        throw new AgentError('LOCAL_STORE_QUOTA','Local agent storage is full and older records could not be safely evicted.');
      }
      return {bytes:current.size,pruned:pruned.removed};
    });
  }
  async get<T=unknown>(category:string,id:string):Promise<T|null>{
    const file=this.file(category,id);
    try{
      const st=await fs.lstat(file);
      if(!st.isFile()||st.isSymbolicLink()||st.size>this.maxRecordBytes)return null;
      const raw=await fs.readFile(file,'utf8');
      const value=JSON.parse(raw) as T;
      // Finish the metadata handle before a caller immediately replaces this
      // record; fire-and-forget touches can race Windows atomic publication.
      await fs.utimes(file,new Date(),new Date()).catch(()=>{});
      return value;
    }catch{return null;}
  }
  async remove(category:string,id:string):Promise<void>{
    await this.serial(async()=>{await fs.unlink(this.file(category,id)).catch(()=>{});});
  }
  async list<T=unknown>(category:string,limit=100):Promise<{id:string;value:T;mtimeMs:number;bytes:number}[]>{
    if(!NAME.test(category))throw new AgentError('INVALID_ARGUMENT','Invalid local store category.');
    const root=await this.ensure();
    const prefix=category+'--';
    const entries=(await this.entries(root)).filter(e=>e.name.startsWith(prefix))
      .sort((a,b)=>b.mtimeMs-a.mtimeMs).slice(0,Math.max(1,Math.min(limit,500)));
    const out:{id:string;value:T;mtimeMs:number;bytes:number}[]=[];
    for(const entry of entries){
      const id=entry.name.slice(prefix.length,-5);
      if(!ID.test(id))continue;
      try{
        const value=JSON.parse(await fs.readFile(entry.file,'utf8')) as T;
        out.push({id,value,mtimeMs:entry.mtimeMs,bytes:entry.bytes});
      }catch{}
    }
    return out;
  }
  async prune():Promise<{bytes:number;records:number;removed:number;removedBytes:number}>{
    return this.serial(()=>this.pruneLocked());
  }
  async status():Promise<StoreStatus>{
    const root=await this.ensure();
    const entries=await this.entries(root);
    return {
      name:this.name,root,maxBytes:this.maxBytes,
      bytes:entries.reduce((n,e)=>n+e.bytes,0),records:entries.length,
      lastPruned:this.lastPruned,removedRecords:this.removedRecords,
      removedBytes:this.removedBytes,scanLimitHit:this.scanLimitHit
    };
  }
}

export const HARNESS_STATE_BYTES=30*1024*1024;
export const SEMANTIC_CACHE_BYTES=64*1024*1024;
export const harnessStateStore=new BoundedLocalStore('harness-vnext',HARNESS_STATE_BYTES,8*1024*1024);
export const semanticCacheStore=new BoundedLocalStore('semantic-cache-v1',SEMANTIC_CACHE_BYTES,20*1024*1024);
