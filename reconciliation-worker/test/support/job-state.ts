/**
 * @file Reads persisted update jobs only for isolated database-test observations.
 * Borrows a test-owned PostgreSQL pool, performs one read-only query, and neither claims
 * work nor exposes a production inspection API. The caller validates and closes its pool.
 */
import type { Pool } from 'pg';
import type { Diagnostic } from '../../src/model/diagnostic.js';
import type { Job, JobStatus } from '../../src/model/job.js';
import type { TargetOutcome } from '../../src/model/reconciliation.js';

/** Existing typed columns under the externally initialized test schema and default pg date parsers. */
type UpdateJobRow = {
  job_id: string;
  kind: 'update';
  product_id: string;
  source_revision: number | null;
  approved_payload: unknown;
  status: JobStatus;
  attempt_count: number;
  next_attempt_at: Date | null;
  lease_token: string | null;
  lease_expires_at: Date | null;
  sql_outcome: TargetOutcome;
  mongo_outcome: TargetOutcome;
  last_error: Diagnostic | null;
  conflict_details: Record<string, unknown> | null;
  created_at: Date;
  updated_at: Date;
  completed_at: Date | null;
};

/**
 * Observe a stored update for assertions without adding inspection behavior to the worker.
 * @param pool - Caller-owned isolated test pool, already validated and configured with query timeouts.
 * @param id - Exact job UUID bound as a query parameter; other job kinds are excluded.
 * @returns An internal job with explicit ISO date projections, or null after confirmed absence.
 * @throws If the read fails or the expected test schema/date parser is incompatible; failure is not absence.
 */
export async function readUpdateJob(pool: Pool, id: string): Promise<Job | null> {
  const result = await pool.query<UpdateJobRow>(
    `SELECT job_id,kind,product_id,source_revision,approved_payload,status,attempt_count,
    next_attempt_at,lease_token,lease_expires_at,sql_outcome,mongo_outcome,last_error,conflict_details,
    created_at,updated_at,completed_at FROM public.reconciliation_jobs WHERE job_id=$1 AND kind='update'`,
    [id],
  );
  const row = result.rows[0];
  if (!row) return null;
  return {
    jobId: row.job_id,
    kind: row.kind,
    productId: row.product_id,
    revision: row.source_revision,
    approvedPayload: row.approved_payload,
    status: row.status,
    attemptCount: row.attempt_count,
    leaseToken: row.lease_token,
    leaseExpiresAt: row.lease_expires_at?.toISOString() ?? null,
    nextAttemptAt: row.next_attempt_at?.toISOString() ?? null,
    createdAt: row.created_at.toISOString(),
    updatedAt: row.updated_at.toISOString(),
    completedAt: row.completed_at?.toISOString() ?? null,
    outcomes: { sql: row.sql_outcome, mongo: row.mongo_outcome },
    lastError: row.last_error,
    conflictDetails: row.conflict_details,
  };
}
