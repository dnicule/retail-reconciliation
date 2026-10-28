/**
 * @file Claims durable approved-update jobs and records fenced results through PostgreSQL.
 * Borrows a caller-owned pool, commits before returning claims, and never writes products.
 * Other retained job kinds stay outside all reads and writes. Lease tokens remain
 * internal and commit uncertainty is left for lease recovery.
 */
import { randomUUID } from 'node:crypto';
import type { Pool } from 'pg';
import { InvalidSnapshotError, SystemicError } from './model/diagnostic.js';
import { validateSnapshot } from './model/product.js';
import type { AttemptResult, Claim, Job, JobStatus } from './model/job.js';
import type { Diagnostic } from './model/diagnostic.js';
import type { JobRepository, Logger } from './model/contracts.js';
import type { Snapshot } from './model/product.js';
import type { TargetOutcome } from './model/reconciliation.js';
import { query, withClient } from './postgres.js';
import { assertPostgresReady } from './readiness.js';
import { beginTransactionSql, commitTransactionSql } from './resources/postgres/connection.js';
import {
  selectEligibleJobSql,
  markJobExhaustedSql,
  claimJobSql,
  selectLatestApprovedSnapshotSql,
  lockJobForFinalizationSql,
  finalizeJobSql,
} from './resources/postgres/jobs.js';

/** The continuous service adds claiming and readiness to the processor's minimal repository. */
export interface WorkerJobRepository extends JobRepository {
  /** Commit one eligible approved-update claim, or an exhausted transition, by the absolute millisecond deadline. */
  claim(deadline: number, jobId?: string): Promise<Claim>;
  /** Inspect expected PostgreSQL structures read-only; throws for unavailable/incompatible dependencies. */
  ready(deadline: number): Promise<void>;
}

/**
 * Reject a structurally incompatible database row without leaking SQL or stored values.
 * @returns A sanitized systemic error so claims stop instead of repeatedly consuming work.
 */
function incompatible(): SystemicError {
  return new SystemicError(
    'INCOMPATIBLE_JOB_SCHEMA',
    'Stored job fields do not match the reconciliation job contract.',
  );
}

/** Read a required text field from the driver's untyped row; reject incompatible storage. */
function string(value: unknown): string {
  if (typeof value !== 'string') throw incompatible();
  return value;
}

/** Read an optional text field without treating an invalid type as absent. */
function optionalString(value: unknown): string | null {
  return value === null ? null : string(value);
}

/** Serialize database dates as ISO time while rejecting absent or malformed required timestamps. */
function date(value: unknown): string {
  if (!(value instanceof Date) && typeof value !== 'string') throw incompatible();
  const parsed = value instanceof Date ? value : new Date(value);
  if (!Number.isFinite(parsed.getTime())) throw incompatible();
  return parsed.toISOString();
}

/** Preserve SQL NULL scheduling fields and serialize every present timestamp consistently. */
function optionalDate(value: unknown): string | null {
  return value === null ? null : date(value);
}

/** Check the supported durable state vocabulary rather than asserting arbitrary strings. */
function status(value: unknown): JobStatus {
  if (
    value === 'pending' ||
    value === 'processing' ||
    value === 'completed' ||
    value === 'conflict' ||
    value === 'needs_attention'
  )
    return value;
  throw incompatible();
}

/** Check each target's recorded result vocabulary before projecting a database row. */
function outcome(value: unknown): TargetOutcome {
  if (
    value === 'unknown' ||
    value === 'satisfied' ||
    value === 'retry' ||
    value === 'conflict' ||
    value === 'failed'
  )
    return value;
  throw incompatible();
}

/** Validate nullable JSON object shape without altering diagnostic or conflicting-field content. */
function object(value: unknown): Record<string, unknown> | null {
  if (value === null) return null;
  if (typeof value !== 'object' || Array.isArray(value)) throw incompatible();
  return value as Record<string, unknown>;
}

/**
 * Project database errors while retaining legacy JSON objects that lack code/message fields.
 * @param value - Nullable diagnostic JSON supplied by PostgreSQL.
 * @returns Diagnostic with an explicit fallback description and all existing safe fields.
 */
function diagnostic(value: unknown): Diagnostic | null {
  const record = object(value);
  if (record === null) return null;
  return {
    ...record,
    code: typeof record.code === 'string' ? record.code : 'RECORDED_ERROR',
    message:
      typeof record.message === 'string' ? record.message : 'Inspect the recorded job diagnostics.',
  };
}

/**
 * Convert one persisted row to the internal camel-case job projection.
 * @param row - PostgreSQL driver row; immutable JSON remains untrusted until decision validation.
 * @returns Job with ISO timestamps and a private lease token for attempt fencing.
 * @throws SystemicError when ordinary storage columns have an incompatible shape.
 */
function job(row: Record<string, unknown>): Job {
  if (row.kind !== 'update') throw incompatible();
  if (
    typeof row.attempt_count !== 'number' ||
    !Number.isInteger(row.attempt_count) ||
    row.attempt_count < 0
  )
    throw incompatible();
  if (
    row.source_revision !== null &&
    (typeof row.source_revision !== 'number' || !Number.isInteger(row.source_revision))
  )
    throw incompatible();
  return {
    jobId: string(row.job_id),
    kind: row.kind,
    productId: string(row.product_id),
    revision: row.source_revision,
    approvedPayload: row.approved_payload,
    status: status(row.status),
    attemptCount: row.attempt_count,
    leaseToken: optionalString(row.lease_token),
    leaseExpiresAt: optionalDate(row.lease_expires_at),
    nextAttemptAt: optionalDate(row.next_attempt_at),
    createdAt: date(row.created_at),
    updatedAt: date(row.updated_at),
    completedAt: optionalDate(row.completed_at),
    outcomes: { sql: outcome(row.sql_outcome), mongo: outcome(row.mongo_outcome) },
    lastError: diagnostic(row.last_error),
    conflictDetails: object(row.conflict_details),
  };
}

/**
 * Require a returned row after a mutation that owns the selected row's transaction lock.
 * @param rows - Acknowledged RETURNING rows from the locked mutation.
 * @returns The single projected job.
 * @throws On an unexpected zero-row response, preventing an unconfirmed success report.
 */
function requiredJob(rows: Record<string, unknown>[]): Job {
  const row = rows[0];
  if (!row) throw new Error('The locked job mutation returned no confirmed job.');
  return job(row);
}

/**
 * Build a lazy job repository; the caller retains responsibility for closing the pool.
 * @param pool - PostgreSQL pool used only through bounded borrowed connections.
 * @param log - Structured operation sink; no token, credential, or full approved payload is emitted.
 * @returns Claim, retained-approval, fenced-finalization, and readiness methods. Rejected writes may have committed.
 */
export function createJobRepository(pool: Pool, log: Logger): WorkerJobRepository {
  return {
    /**
     * Lock one oldest eligible approved-update row and commit its new lease before exposing it to processing.
     * @param deadline - Absolute Unix milliseconds bounding acquisition and all transaction statements.
     * @param jobId - Optional specific update identity to claim; ordinary queue scans omit it and eligibility is always checked.
     * @returns None, a committed claim, or a confirmed exhausted transition without another attempt.
     * @throws If acquisition/query/commit acknowledgment fails; callers must not process an uncertain claim.
     */
    async claim(deadline, jobId) {
      const started = Date.now();
      const result = await withClient(
        pool,
        deadline,
        /** Keep selection and mutation on one connection; failed transactions are discarded. */ async (
          client,
        ): Promise<Claim> => {
          await client.query(beginTransactionSql);
          const selected = await client.query(selectEligibleJobSql, [jobId ?? null]);
          const row = selected.rows[0];
          if (!row) {
            await client.query(commitTransactionSql);
            return { kind: 'none' };
          }
          const previous = job(row);
          if (previous.attemptCount >= 8) {
            const result = await client.query(markJobExhaustedSql, [
              previous.jobId,
              {
                code: 'RETRY_EXHAUSTED',
                message: 'Eight attempts have started; this job requires attention.',
                previousError: previous.lastError,
              },
            ]);
            const exhausted = requiredJob(result.rows);
            await client.query(commitTransactionSql);
            return { kind: 'exhausted', job: exhausted };
          }
          const updated = await client.query(claimJobSql, [previous.jobId, randomUUID()]);
          const claimed = requiredJob(updated.rows);
          await client.query(commitTransactionSql);
          return { kind: 'claimed', job: claimed };
        },
      );
      if (result.kind !== 'none')
        log(result.kind === 'claimed' ? 'job_claimed' : 'job_retry_exhausted', {
          level: result.kind === 'claimed' ? 'INFO' : 'WARN',
          outcome: result.kind,
          jobId: result.job.jobId,
          productId: result.job.productId,
          attemptCount: result.job.attemptCount,
          leaseExpiresAt: result.job.leaseExpiresAt,
          durationMs: Date.now() - started,
        });
      return result;
    },

    /**
     * Read the highest retained approval without filtering its mutable status.
     * @param productId - Product whose immutable approved history is required.
     * @param deadline - Absolute Unix millisecond operation deadline.
     * @returns Validated complete snapshot, or null only after a successful empty query.
     * @throws InvalidSnapshotError for malformed evidence; driver/systemic errors remain errors, never absence.
     */
    async latest(productId, deadline) {
      const result = await query(pool, selectLatestApprovedSnapshotSql, [productId], deadline);
      const row = result.rows[0];
      if (!row) return null;
      const candidate = {
        productId: row.product_id,
        revision: row.source_revision,
        product: row.approved_payload,
      };
      const failure = validateSnapshot(candidate, productId);
      if (failure)
        throw new InvalidSnapshotError(
          'INVALID_APPROVED_SNAPSHOT',
          'The highest retained immutable approved snapshot is invalid.',
        );
      // Validation proves the complete exact-field shape; the pg boundary otherwise exposes unknown JSON.
      return candidate as Snapshot;
    },

    /**
     * Save a proposed result only while this exact processing lease remains unexpired.
     * @param jobId - Durable update identity; immutable columns are never changed.
     * @param leaseToken - Internal attempt token, never logged.
     * @param result - Proposed terminal or scheduled state from completed observations.
     * @param deadline - Absolute Unix milliseconds bounding the result transaction.
     * @returns Confirmed saved update, or null when the update or current unexpired ownership is absent.
     * @throws If acknowledgment fails; callers must leave recovery to the durable state.
     */
    async finalize(jobId, leaseToken, result, deadline) {
      const started = Date.now();
      const saved = await withClient(
        pool,
        deadline,
        /** Lock first so expiry is evaluated only after any wait for an unchanged row. */ async (
          client,
        ) => {
          await client.query(beginTransactionSql);
          await client.query(lockJobForFinalizationSql, [jobId]);
          const updated = await client.query(finalizeJobSql, [
            jobId,
            leaseToken,
            result.status,
            result.outcomes.sql,
            result.outcomes.mongo,
            result.lastError,
            result.conflictDetails,
            result.retryDelayMs,
          ]);
          const value = updated.rows[0] ? job(updated.rows[0]) : null;
          await client.query(commitTransactionSql);
          return value;
        },
      );
      if (!saved)
        log('job_ownership_lost', {
          level: 'WARN',
          jobId,
          outcome: 'not_recorded',
          durationMs: Date.now() - started,
        });
      else
        log(saved.status === 'pending' ? 'job_retry_scheduled' : 'job_finalized', {
          level:
            saved.status === 'conflict' || saved.status === 'needs_attention' ? 'WARN' : 'INFO',
          jobId,
          productId: saved.productId,
          outcome: saved.status,
          attemptCount: saved.attemptCount,
          outcomes: saved.outcomes,
          nextAttemptAt: saved.nextAttemptAt,
          durationMs: Date.now() - started,
        });
      return saved;
    },

    /** Delegate the package's read-only structure probe without importing database initialization code. */
    async ready(deadline) {
      await assertPostgresReady(pool, deadline);
    },
  };
}
