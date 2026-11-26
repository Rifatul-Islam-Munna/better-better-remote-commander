/** Bounded executable onboarding; no commands, model calls or state writes. */
import {CODE_MODE_MAX_ADMITTED,CODE_MODE_MAX_RUNNABLE} from 'brc-shared';

const parallelism = {
  default:'Prefer parallel Code Mode for independent coding work: parallelSafe for eligible reads/searches and explicit run_command calls, concurrent outer code_mode calls for independent workflows when supported by the host. Await dependencies and serialize overlapping writes.',
  safeReads:'await tools.parallelSafe([{tool:"git_status",args:{path:root}},{tool:"git_diff",args:{path:root}}],{max_concurrency:3})',
  searches:'await tools.parallelSafe([{tool:"start_search",args:{path:root,pattern:"TODO",searchType:"content",wait:true,maxResults:20}},{tool:"start_search",args:{path:root,pattern:"test",searchType:"files",wait:true,maxResults:20}}],{max_concurrency:3})',
  results:'parallelSafe returns an envelope: reply.results contains bounded replies in input order (text, structuredContent, isError, pending, jobId). reply.map(fn) maps those replies too. Read source from item.structuredContent.text; check outputTruncated/resultUnavailable and follow source next or get_job. Summary remains in reply.structuredContent. This is not an array of full unbounded results.',
  commands:'Explicit {tool:"run_command",args:{program,args,cwd}} calls also work in parallelSafe and route through normal run_parallel command/path checks and resource limits. Commands may mutate; choose independent work and serialize competing writes. Direct run_parallel also supports commands and depends_on DAGs.',
  workflows:'The calling AI host may submit independent code_mode requests concurrently. Each has an isolated QuickJS worker, ledger and cancellation signal. BRC never spawns another AI model.',
  limits:{maxAdmitted:CODE_MODE_MAX_ADMITTED,maxRunnable:CODE_MODE_MAX_RUNNABLE,maxRunnablePerProject:3,maxSafeReadFanout:3,adaptive:true},
  ordering:'Ordinary tools.call and specialist steps stay sequential. Do not Promise.all them inside one workflow. parallelSafe accepts independent verified reads, blocking start_search (omitted wait defaults to true) and explicit run_command calls; it rejects explicit wait:false, direct file mutations, search cursors and job-control tools before starting any calls. depends_on:[0] refers to an earlier call.',
  safety:'Await dependent reads before edits; serialize overlapping writes. Reduce fanout under pressure. Capacity is not a target worker count.',
  completion:'Inspect response.structuredContent, pending and actual exitCode. Follow get_job/wait_jobs for pending work; never rerun completed mutations to retrieve output.',
};
const features = [
  {api:'tools.search(query), tools.describe(name), tools.call(name,args)',use:'Discover exact schemas for permitted native and installed-plugin tools. Replies expose text, structuredContent, isError and pending.'},
  {api:'tools.call("read_code_file",{path,offset,length,maxChars}), tools.call("read_multiple_files",{paths,maxChars})',use:'Compact defaults: 12000 source characters / 32000 combined batch text. Source lives in structuredContent.text with sha256 and exact next cursor; follow next without restarting. Explicit larger budgets may produce result handles. Return only needed source/context, not duplicate full replies.'},
  {api:'tools.parallelSafe(calls,options)',use:'Bounded read/command DAGs with ordinary tool policies; see tools.help("parallelism").'},
  {api:'tools.semantic.search({path,query,offset,limit,preview_chars})',use:'Page local semantic/hybrid project retrieval; follow continuation.next. Retrieval is not compiler-semantic proof.'},
  {api:'tools.workflows.match(query), tools.workflows.run(query,context)',use:'Match/reuse an unambiguous saved workflow; inspect unfamiliar source via snippets.get. Saved code grants no permission.'},
  {api:'tools.snippets.search(query), tools.snippets.get(id), tools.snippets.save({name,source,...})',use:'Find/read/save bounded local reusable workflows.'},
  {api:'tools.sessions.open({name,root,state}), tools.sessions.get(id), tools.sessions.update(id,patch), tools.sessions.list(limit)',use:'Reuse project context/session state across calls.'},
  {api:'tools.results.store(value,label), tools.results.search(id,query,options), tools.results.get(id,{offset,length}), tools.results.drop(id)',use:'Filter/page large local results. resultHandle/continuation means stay in Code Mode, not direct fallback.'},
  {api:'tools.step(name,async()=>...)',use:'Reuse a recorded named step within a matching durable workflow. Inspect recovery status; never replay uncertain mutations.'},
  {api:'tools.terminal.use({op,name,...}), tools.browser.use({op,name,...})',use:'Reuse terminal or installed browser-plugin sessions. Describe reusable_terminal/reusable_browser for operations. Browser actions need an authorized browser task.'},
  {api:'tools.fibers.run({op,name,steps,...})',use:'Create/start/status/wait/resume named job-backed fibers. Describe durable_fiber first; pending means unfinished.'},
  {api:'tools.agent.roles(), tools.agent.specialist({role,specialty,task,steps})',use:'Permission-scoped explorer/reviewer/tester/coder execution of parent-authored sequential steps; complex/multi-domain calls can auto-route. These are not concurrent AI workers.'},
  {api:'tools.call("worker_tasks",...), tools.call("worker_patch",...), tools.call("worker_integrate",...)',use:'Coordinate genuine host-native AI workers when available: scoped claims, proposed patches, parent preview/apply/verify. Otherwise use parallelSafe/run_parallel/concurrent workflows.'},
  {api:'tools.harness.status(), tools.trace.current()',use:'Inspect scheduler active/parked/queued/peak counters, pressure, cache/retry health and current bounded trace.'},
  {api:'tools.call("spawn_job",...), tools.call("get_job",...), tools.call("wait_jobs",...)',use:'Start/follow long work; inspect actual completion. Recovery receipts do not authorize replay. Describe exact job schemas.'},
];
const errors = {
  PARALLEL_SAFE_TOOL_DENIED:'parallelSafe accepts blocking start_search and explicit run_command calls. Keep direct file mutations and cursor/job-control calls separate. No calls in the rejected batch were started; see tools.help("parallelism").',
  CODE_MODE_NESTED_PARALLEL_DENIED:'Use parallelSafe for eligible reads, run_parallel for independent operations, or concurrent outer code_mode calls. Ordinary tools.call/specialist runs stay sequential.',
  CODE_MODE_SCRIPT_FAILED:'Inspect returned JavaScript error and exact schemas. Replies use .text / .structuredContent; do not assume arbitrary fields.',
  EDIT_MISMATCH:'Re-read exact current source/hash. Correct unique anchors and whitespace; do not relax guards.',
  FILE_CONFLICT:'Re-read current source/hash. Serialize competing writes; never retry uncertain mutations blindly.',
  PATCH_SYNTAX:'Describe apply_patch; use exact patch grammar without Markdown fences. Re-read affected source for PATCH_CONTEXT_MISMATCH.',
  COMMAND_FAILED:'Inspect stderr/exitCode. Prefer run_command program + literal args + cwd. JavaScript passed to node -e has another quoting layer: escape literal backslashes/newlines or use a file. Never repair/re-execute uncertain commands automatically.',
  HTTP_REQUEST_FAILED:'Inspect httpStatus and stderr. Verify the endpoint/ref for 404, authentication or rate limits for 401/403/429. The request failed; no automatic command replay.',
  ENOENT:'Verify exact allowed path with list_directory/start_search. Do not invent files or change allowedDirectories.',
  SEARCH_FAILED:'Inspect search error and verify root/globs. Use literalSearch:true for literal text; narrow scope for timeouts. Do not loop over identical invalid arguments.',
};
const editing={
  tools:['read_code_file','edit_files','edit_block','apply_patch'],
  read:'Read current source and SHA-256 with read_code_file before editing; follow source pagination for unseen lines.',
  edit:'Describe edit_files for exact multi-file edits using expected_sha256 and unique old_string anchors. Use dryRun:true to preview. Describe edit_block or apply_patch for their exact schemas.',
  recovery:'EDIT_MISMATCH or FILE_CONFLICT requires fresh source/hash and corrected exact anchors. Candidate lines never authorize approximate edits; uncertain writes are not replayed.',
};
export const CODE_MODE_HELP_TOPICS=['overview','parallelism','features','errors','editing'] as const;
const aliases:Record<string,string>={parallel:'parallelism',parallel_safe:'parallelism',parallelsafe:'parallelism',concurrency:'parallelism',tools:'features',api:'features',capabilities:'features',edit:'editing',edits:'editing',error:'errors',troubleshooting:'errors',recovery:'errors',help:'overview'};
export function codeModeHelp(topic:string='overview') {
  const requestedTopic=topic;
  topic=topic.trim().toLowerCase();topic=aliases[topic]??topic;
  if(topic==='parallelism')return parallelism;
  if(topic==='features')return {features};
  if(topic==='errors')return errors;
  if(topic==='editing')return editing;
  return {version:1,topics:CODE_MODE_HELP_TOPICS,parallelism,features,errors,editing,
    ...(!CODE_MODE_HELP_TOPICS.some(t=>t===topic)?{requestedTopic,topicFound:false}:{}),
    start:'Read parallelism for real fanout, features for the full API map, errors for recovery. Discover exact schemas with tools.search/describe.',
    limits:{sourceChars:50_000,defaultCalls:48,maxCalls:96,defaultRuntimeMs:85_000,maxRuntimeMs:105_000},
    sandbox:'QuickJS on the paired device. No Node/fs/process/require/fetch/browser globals. Bridged calls retain permissions, path/hash checks, cancellation and structured errors.'};
}
