/** Three optional coordination tools. Native model workers belong to the MCP host, never this server. */
import { z } from 'zod';
import { zodToJsonSchema } from 'zod-to-json-schema';
import type { ToolDefinition } from '../core/tool-definitions.js';
import type { ToolResult } from 'brc-shared';
import { WorkspaceChangeSchema } from './workspace-batch.js';
import { workerCoordinator, WORKER_LIMITS } from '../runtime/worker-coordination.js';
import { AgentError, result } from '../runtime/errors.js';
const Id = z.string().regex(/^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$/);
const Key = z.string().min(20).max(128);
const Check = z.string().regex(/^[A-Za-z0-9_.:-]{1,100}$/);
const Task = z.object({ id: Id, goal: z.string().min(1).max(2000), scope: z.array(z.string().min(1).max(500)).min(1).max(16),
    dependsOn: z.array(Id).max(31).default([]), readOnly: z.boolean().default(false),
    priority:z.number().int().min(0).max(100).optional(),supersedes:z.array(Id).max(31).optional() }).strict();
export const WorkerTasksSchema = z.object({
    action: z.enum(['create', 'status', 'resume', 'claim', 'renew', 'release', 'reset', 'set_checks', 'cancel_obsolete', 'capture_worktree', 'cancel']),
    path: z.string().min(1).max(2000).optional(), goal: z.string().min(1).max(2000).optional(), tasks: z.array(Task).min(1).max(32).optional(),
    hostMode: z.enum(['native_workers', 'parallel_tools', 'single']).default('single'),
    maxWorkers: z.number().int().min(1).max(10).default(3), requiredChecks: z.array(Check).max(5).optional(),
    planId: z.string().uuid().optional(), taskId: Id.optional(), workerId: z.string().min(1).max(80).regex(/^[^\x00-\x1f\x7f]+$/).optional(),
    coordinatorKey: Key.optional(), claimKey: Key.optional(), leaseSeconds: z.number().int().min(30).max(3600).default(900),
    summary:z.string().min(1).max(4000).optional(),
    reads: z.array(z.object({ path: z.string().min(1).max(500), offset: z.number().int().min(0).default(0), length: z.number().int().min(1).max(400).default(120) }).strict()).max(16).optional(),
}).strict();
export const WorkerPatchSchema = z.object({ planId: z.string().uuid(), taskId: Id, claimKey: Key, submissionId: Id,
    summary: z.string().min(1).max(4000), changes: z.array(WorkspaceChangeSchema).max(16).default([]) }).strict();
export const WorkerIntegrateSchema = z.object({ action: z.enum(['preview', 'apply', 'verify', 'rollback', 'finish']).default('preview'),
    planId: z.string().uuid(), coordinatorKey: Key, taskIds: z.array(Id).min(1).max(32).optional(),
    review_sha256: z.string().regex(/^[a-f0-9]{64}$/).optional(),rollback_id:z.string().regex(/^[a-f0-9]{64}$/).optional(),
    runChecks: z.boolean().default(false) }).strict();
export const WORKER_TOOL_HANDLERS: Record<string, (raw: Record<string, unknown>) => Promise<ToolResult>> = {
    worker_tasks: async (raw) => {
        const a = WorkerTasksSchema.parse(raw);
        if (a.action === 'create') {
            if (!a.path || !a.goal || !a.tasks)
                throw new AgentError('INVALID_ARGUMENT', 'create requires path, goal and tasks.');
            if (Buffer.byteLength(JSON.stringify(a)) > 512 * 1024)
                throw new AgentError('PLAN_TOO_LARGE', 'Plan metadata exceeds 512 KiB. Use fewer/smaller tasks.');
            return result(await workerCoordinator.create({ ...a, path: a.path, goal: a.goal, tasks: a.tasks }));
        }
        if (!a.planId)
            throw new AgentError('INVALID_ARGUMENT', 'planId is required.');
        return result(await workerCoordinator.manage({ ...a, planId: a.planId }));
    },
    worker_patch: async (raw) => result(await workerCoordinator.submit(WorkerPatchSchema.parse(raw))),
    worker_integrate: async (raw) => workerCoordinator.integrate(WorkerIntegrateSchema.parse(raw)),
};
export function buildWorkerToolDefinitions(): ToolDefinition[] {
    return [
        { name: 'worker_tasks', schema: WorkerTasksSchema,
            description: 'Global coding DAG. create tasks with scope/dependsOn/priority/supersedes; claim steals the highest-priority ready node. Only 2+ independent writers on a clean Git root can get temporary worktreePath; edit only it, then capture_worktree. Worktrees are deleted after capture/release/cancel/expiry/finish and crash leftovers are swept on startup. Applied superseding nodes cancel obsolete downstream branches. Parent previews/applies patches and runs incremental then final checks. No model/API spawning; scope is not an OS sandbox.' },
        { name: 'worker_patch', schema: WorkerPatchSchema,
            description: 'Submit a claimed task\'s staged UTF-8 patch with exact hashes; writes no shared files. Eligible worktrees should use worker_tasks capture_worktree, which converts the diff to the same staged-patch path and deletes the checkout. Read-only tasks submit findings only; replays are idempotent; 2 MiB/patch.' },
        { name: 'worker_integrate', schema: WorkerIntegrateSchema,
            description: 'Parent preview/apply/verify/rollback/finish. Apply returns a bounded hash-guarded rollbackSnapshotId. rollback refuses changed files or later dependent applied tasks. Final verify/finish requires fresh checks.' },
    ].map(d => ({ name: d.name, description: d.description, inputSchema: zodToJsonSchema(d.schema),
        annotations: { title: d.name, readOnlyHint: false, destructiveHint: d.name === 'worker_integrate' } }));
}
export { WORKER_LIMITS };
