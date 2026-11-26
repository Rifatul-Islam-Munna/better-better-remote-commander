import {test,after,mock} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
const scratch=await fs.mkdtemp(path.join(os.tmpdir(),'brc-store-publication-'));
process.env.BRC_HOME=scratch;
const {BoundedLocalStore}=await import('../dist/runtime/bounded-local-store.js');
after(async()=>{mock.restoreAll();await fs.rm(scratch,{recursive:true,force:true,maxRetries:8,retryDelay:80});});

test('transient Windows publication conflicts retry the same snapshot without replaying work',async()=>{
 const store=new BoundedLocalStore('retry-fixture',1024*1024,1024);
 await store.put('row','one',{version:1});
 const rename=fs.rename;let calls=0;
 const stub=mock.method(fs,'rename',async(...args)=>{
  if(++calls<3)throw Object.assign(new Error('test sharing conflict'),{code:'EPERM'});
  return rename(...args);
 });
 try{await store.put('row','one',{version:2});assert.equal(calls,3);assert.deepEqual(await store.get('row','one'),{version:2});}
 finally{stub.mock.restore();}
 assert.ok(!(await fs.readdir(store.root())).some(x=>x.endsWith('.tmp')));
});

test('permanent publication failure stays bounded and preserves the last complete record',async()=>{
 const store=new BoundedLocalStore('failure-fixture',1024*1024,1024);
 await store.put('row','one',{version:1});let calls=0;
 const stub=mock.method(fs,'rename',async()=>{calls++;throw Object.assign(new Error('permanent conflict'),{code:'EACCES'});});
 try{await assert.rejects(store.put('row','one',{version:2}),{code:'EACCES'});assert.equal(calls,5);}
 finally{stub.mock.restore();}
 assert.deepEqual(await store.get('row','one'),{version:1});
 assert.ok(!(await fs.readdir(store.root())).some(x=>x.endsWith('.tmp')));
});
