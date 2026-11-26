/** Public onboarding shared by the agent and relay, including offline catalogs. */
export const CODE_MODE_MAX_ADMITTED = 12;
export const CODE_MODE_MAX_RUNNABLE = 8;
export const CODE_MODE_TOOL_CHOICE = {
  use: 'PRIMARY DEFAULT for multi-step work. Use tools.parallelSafe for independent safe reads; send independent code_mode workflows concurrently when the host supports concurrent calls.',
  instead: 'Use direct tools only for one simple action or unavailable/unsupported Code Mode; large or truncated Code Mode output is NOT a fallback signal.',
  caveat: 'Start unfamiliar work with await tools.help(). Ordinary tools.call operations stay sequential. Specialists are local permission scopes, not extra LLMs. Stay in Code Mode for continuations; never replay uncertain mutations.',
};
export const CODE_MODE_WORKFLOW_GUIDANCE = [
  'START: const help=await tools.help(); discover exact schemas with tools.search/describe.',
  'PARALLEL: await tools.parallelSafe([{tool:"git_status",args:{path:root}},{tool:"git_diff",args:{path:root}}],{max_concurrency:3}); inspect structuredContent and pending jobs.',
  'For independent commands/DAGs discover run_parallel. For independent multi-step work submit separate code_mode calls concurrently: up to 12 admitted / 8 runnable globally, 1-3 runnable per project, adapted to CPU/RAM pressure. These are limits, not a target worker count.',
  'DEPENDENCIES: await reads before edits; serialize overlapping writes. Never Promise.all ordinary tools.call or specialist runs inside one workflow.',
  'FEATURES: tools.help("features") covers semantic search, saved workflows/snippets, project sessions, result handles, durable steps/resume, reusable terminal/browser sessions, fibers, specialists, native-worker coordination, live status/traces and job recovery.',
  'NEXT: follow local result handles/pages; inspect get_job/wait_jobs for unfinished work. Await every operation; return compact verified evidence.',
].join(' ');
export const CODE_MODE_DISCOVERY_DESCRIPTION = 'USE WHEN: '+CODE_MODE_TOOL_CHOICE.use+
  '\nUSE INSTEAD: '+CODE_MODE_TOOL_CHOICE.instead+
  '\nIMPORTANT: '+CODE_MODE_TOOL_CHOICE.caveat+
  '\nHOW / NEXT: '+CODE_MODE_WORKFLOW_GUIDANCE;
