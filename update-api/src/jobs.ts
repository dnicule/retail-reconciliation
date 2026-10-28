/**
 * @file Persists immutable approved update jobs and records lease-guarded attempt outcomes.
 * Resolves admission identity, selects retained approved evidence, exposes read-only job
 * diagnostics, and checks admission prerequisites through the PostgreSQL catalog.
 * Product delivery, retries, lease recovery, and pool shutdown belong to other components.
 */

import type { Pool } from 'pg';
import { randomUUID } from 'node:crypto';
import type {
  Admission,
  ApprovedUpdate,
  AttemptResult,
  JobRepository,
  JobStatus,
  JobView,
  ObservedJob,
  Outcome,
  SharedProduct,
  Snapshot,
} from './model.js';
import { query, withClient } from './postgres.js';

// Explicit projection prevents ownership credentials from entering public job views.
const columns = `job_id, kind, product_id, source_revision, approved_payload, status, attempt_count,
  next_attempt_at, lease_expires_at, sql_outcome, mongo_outcome, last_error, conflict_details,
  created_at, updated_at, completed_at`;
const returned = `${columns}, clock_timestamp() AS observed_at`;

/**
 * Reject an incompatible database value with a safe, content-free error.
 * @returns Never returns; callers use this in validation branches.
 * @throws Always, without including row contents or ownership credentials.
 */
function invalid(): never {
  throw new Error('Unexpected reconciliation job data');
}

/**
 * Validate a required textual database value without coercion.
 * @param value - Driver-decoded column or JSON field; null is not accepted.
 * @returns The original string, including its existing whitespace and casing.
 * @throws If the value is not a string.
 */
function string(value: unknown): string {
  return typeof value === 'string' ? value : invalid();
}

/**
 * Validate an integer decoded by the PostgreSQL driver.
 * @param value - Numeric column value; numeric strings and null are rejected.
 * @returns The unchanged safe integer when it is zero or greater.
 * @throws If the value is not a nonnegative safe integer.
 */
function integer(value: unknown): number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : invalid();
}

/**
 * Serialize a required database timestamp as an ISO UTC timestamp.
 * @param value - Driver Date instance or a timestamp string accepted by Date.
 * @returns The timestamp in ISO format without mutating a supplied Date instance.
 * @throws If the value has an unsupported type or cannot represent a valid date.
 */
function date(value: unknown): string {
  const parsed = value instanceof Date ? value : new Date(string(value));
  return Number.isFinite(parsed.getTime()) ? parsed.toISOString() : invalid();
}

/**
 * Preserve SQL null while serializing a populated timestamp.
 * @param value - Nullable timestamp column decoded by the driver.
 * @returns Null for SQL null, otherwise an ISO UTC timestamp.
 * @throws If a non-null value is not a valid Date or timestamp string.
 */
function nullableDate(value: unknown): string | null {
  return value === null ? null : date(value);
}

/**
 * Validate the outer shape of a JSON object returned by PostgreSQL.
 * @param value - Decoded JSON value; arrays and null are not accepted.
 * @returns A shallow object copy; nested diagnostic values are preserved as received.
 * @throws If the value is not a non-null, non-array object.
 */
function object(value: unknown): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return invalid();
  return Object.fromEntries(Object.entries(value));
}

/**
 * Preserve SQL null while copying a populated diagnostic object.
 * @param value - Nullable JSON diagnostic column decoded by the driver.
 * @returns Null for SQL null, otherwise a shallow object copy.
 * @throws If a non-null value is not a non-array object.
 */
function nullableObject(value: unknown): Record<string, unknown> | null {
  return value === null ? null : object(value);
}

/**
 * Validate and copy the three fields of a retained approved product payload.
 * String limits count Unicode code points, and accepted strings are not normalized.
 * @param value - Decoded JSON payload expected to contain exactly the shared product fields.
 * @returns A new snapshot of name, description, and category with their exact stored values.
 * @throws If the shape, field types, or documented character limits are invalid.
 */
function product(value: unknown): SharedProduct {
  const record = object(value);
  const name = string(record.name),
    description = string(record.description),
    category = string(record.category);
  if (
    Object.keys(record).length !== 3 ||
    [...name].length < 1 ||
    [...name].length > 200 ||
    [...description].length > 4000 ||
    [...category].length < 1 ||
    [...category].length > 100
  )
    return invalid();
  return { name, description, category };
}

/**
 * Narrow a database value to one of the caller's explicitly allowed string values.
 * @param value - Decoded database value to compare without coercion.
 * @param choices - Complete allowed values for the field being read.
 * @returns The matching choice.
 * @throws If no allowed choice exactly matches the value.
 */
function enumeration<T extends string>(value: unknown, choices: readonly T[]): T {
  for (const choice of choices) if (value === choice) return choice;
  return invalid();
}

/**
 * Build the public job projection from a selected database row.
 * @param row - Job columns decoded by the driver; lease tokens are never projected.
 * @returns A new diagnostic view with validated field shapes and ISO timestamps.
 * @throws If a projected field has an incompatible type, enum, payload, or date.
 */
function view(row: Record<string, unknown>): JobView {
  return {
    jobId: string(row.job_id),
    kind: enumeration(row.kind, ['update', 'audit']),
    productId: string(row.product_id),
    revision: row.source_revision === null ? null : integer(row.source_revision),
    approvedPayload: row.approved_payload === null ? null : product(row.approved_payload),
    status: enumeration<JobStatus>(row.status, [
      'pending',
      'processing',
      'completed',
      'conflict',
      'needs_attention',
    ]),
    attemptCount: integer(row.attempt_count),
    outcomes: {
      sql: enumeration<Outcome>(row.sql_outcome, [
        'unknown',
        'satisfied',
        'retry',
        'conflict',
        'failed',
      ]),
      mongo: enumeration<Outcome>(row.mongo_outcome, [
        'unknown',
        'satisfied',
        'retry',
        'conflict',
        'failed',
      ]),
    },
    nextAttemptAt: nullableDate(row.next_attempt_at),
    leaseExpiresAt: nullableDate(row.lease_expires_at),
    lastError: nullableObject(row.last_error),
    conflictDetails: nullableObject(row.conflict_details),
    createdAt: date(row.created_at),
    updatedAt: date(row.updated_at),
    completedAt: nullableDate(row.completed_at),
  };
}

/**
 * Attach the query's PostgreSQL observation time to a validated job view.
 * @param row - Job projection containing the additional observed_at column.
 * @returns The public job and the database timestamp when that row was observed.
 * @throws If the job projection or observation timestamp is invalid.
 */
function observed(row: Record<string, unknown>): ObservedJob {
  return { job: view(row), observedAt: date(row.observed_at) };
}

/**
 * Decode a complete approved snapshot selected from retained update history.
 * The caller must restrict its query to update jobs; audit payloads are not approved evidence.
 * @param row - Product ID, source revision, and approved payload from an update job.
 * @returns A snapshot with a positive PostgreSQL integer revision and validated shared fields.
 * @throws If a field is malformed or the revision is outside 1 through 2,147,483,647.
 */
function snapshot(row: Record<string, unknown>): Snapshot {
  const revision = integer(row.source_revision);
  if (revision < 1 || revision > 2147483647) return invalid();
  return { productId: string(row.product_id), revision, product: product(row.approved_payload) };
}

/**
 * Check whether an existing event row represents exactly the submitted approved update.
 * @param row - Existing row found by the submitted event ID, which may instead be an audit.
 * @param update - Validated submission whose immutable product identity and content are compared.
 * @returns True only for an update with matching product ID, revision, and all shared strings.
 * @throws If identity matches but the stored approved payload is malformed.
 */
function sameUpdate(row: Record<string, unknown>, update: ApprovedUpdate): boolean {
  if (
    row.kind !== 'update' ||
    row.product_id !== update.productId ||
    row.source_revision !== update.revision
  )
    return false;
  const stored = product(row.approved_payload);
  return (
    stored.name === update.product.name &&
    stored.description === update.product.description &&
    stored.category === update.product.category
  );
}

/**
 * Commit one immutable update and its initial lease before authorizing destination delivery.
 * Uses one transaction connection and at most three seconds of the remaining budget.
 * Duplicate events and identity collisions are read without dispatching or changing the
 * existing job. A new admission also selects the highest retained approved snapshot.
 * @param pool - Caller-owned pool used for the short admission transaction.
 * @param update - Validated event with source-assigned revision and exact approved strings.
 * @param deadline - Absolute Unix timestamp in milliseconds limiting admission.
 * @returns Accepted with an acknowledged commit, lease token, and selected snapshot;
 * duplicate/collision with confirmed identity; or unavailable with definite nonacceptance
 * before commit begins and unknown acceptance after it begins. Operational failures are
 * represented in the result rather than rejected promises.
 */
async function admit(pool: Pool, update: ApprovedUpdate, deadline: number): Promise<Admission> {
  let commitStarted = false;
  let confirmedExisting: Extract<Admission, { kind: 'duplicate' | 'collision' }> | undefined;
  try {
    return await withClient(
      pool,
      Math.min(deadline, Date.now() + 3000),
      /** Keeps the transaction and all identity decisions on this one client. */ async (
        client,
      ): Promise<Admission> => {
        await client.query('BEGIN');
        const leaseToken = randomUUID();
        const insertion = await client.query(
          `INSERT INTO public.reconciliation_jobs
        (job_id, kind, product_id, source_revision, approved_payload, status, attempt_count,
         next_attempt_at, lease_token, lease_expires_at, sql_outcome, mongo_outcome)
        VALUES ($1, 'update', $2, $3, $4::jsonb, 'processing', 1,
                NULL, $5, clock_timestamp() + interval '30 seconds', 'unknown', 'unknown')
        ON CONFLICT DO NOTHING RETURNING ${returned}`,
          [
            update.eventId,
            update.productId,
            update.revision,
            JSON.stringify(update.product),
            leaseToken,
          ],
        );
        const inserted = insertion.rows[0];
        if (!inserted) {
          // A conflicting insert may wait for another transaction. READ COMMITTED gives these
          // subsequent statements a fresh snapshot that includes its acknowledged identity.
          const byId = await client.query(
            `SELECT ${returned} FROM public.reconciliation_jobs WHERE job_id = $1`,
            [update.eventId],
          );
          const existing = byId.rows[0];
          if (existing) {
            const duplicate = sameUpdate(existing, update);
            const state = observed(existing);
            confirmedExisting = duplicate
              ? { kind: 'duplicate', ...state }
              : { kind: 'collision', code: 'EVENT_ID_COLLISION', canonicalJobId: state.job.jobId };
            await client.query('ROLLBACK');
            return confirmedExisting;
          }
          const byRevision = await client.query(
            `SELECT ${returned} FROM public.reconciliation_jobs
          WHERE kind = 'update' AND product_id = $1 AND source_revision = $2`,
            [update.productId, update.revision],
          );
          const canonical = byRevision.rows[0];
          if (!canonical) throw new Error('Conflicting job identity could not be confirmed');
          confirmedExisting = {
            kind: 'collision',
            code: 'REVISION_ALREADY_REGISTERED',
            canonicalJobId: string(canonical.job_id),
          };
          await client.query('ROLLBACK');
          return confirmedExisting;
        }
        // Select approved evidence before commit, so PostgreSQL outage after admission cannot block Mongo delivery.
        const latest = await client.query(
          `SELECT product_id, source_revision, approved_payload
        FROM public.reconciliation_jobs WHERE kind = 'update' AND product_id = $1
        ORDER BY source_revision DESC LIMIT 1`,
          [update.productId],
        );
        const selectedRow = latest.rows[0];
        if (!selectedRow) return invalid();
        const selected = snapshot(selectedRow);
        const state = observed(inserted);
        commitStarted = true;
        await client.query('COMMIT');
        // Returning accepted is the only point where delivery becomes authorized.
        return { kind: 'accepted', ...state, leaseToken, selected };
      },
    );
  } catch {
    // Lost rollback acknowledgment does not erase a previously confirmed immutable identity.
    // withClient already discarded the connection, so its transaction cannot leak into the pool.
    if (confirmedExisting) return confirmedExisting;
    // Destroying a connection rolls back uncommitted work; a sent COMMIT can remain uncertain.
    return { kind: 'unavailable', acceptance: commitStarted ? 'unknown' : 'not_accepted' };
  }
}

/**
 * Build persisted diagnostics from each destination's semantic attempt outcome.
 * @param result - Combined attempt result whose adapter diagnostics are already sanitized.
 * @returns Non-satisfied target outcomes, actions, and optional safe diagnostics; null when
 * both targets are satisfied. Raw driver errors and successful target payloads are omitted.
 */
function diagnostics(result: AttemptResult): Record<string, unknown> | null {
  const details: Record<string, unknown> = {};
  for (const target of ['sql', 'mongo'] as const) {
    const outcome = result[target];
    if (outcome.outcome !== 'satisfied')
      details[target] = {
        outcome: outcome.outcome,
        action: outcome.action,
        ...(outcome.diagnostic
          ? { code: outcome.diagnostic.code, message: outcome.diagnostic.message }
          : {}),
      };
  }
  return Object.keys(details).length ? details : null;
}

/**
 * Record both destination outcomes only while the attempt still owns the processing row.
 * The row is locked before checking PostgreSQL time. A confirmed transition clears the
 * lease, sets retry or completion timestamps as appropriate, and stores safe diagnostics;
 * the database trigger updates the modification time and publishes any pending wakeup.
 * @param pool - Caller-owned pool used for one atomic guarded statement.
 * @param jobId - Durable job whose mutable attempt results are being recorded.
 * @param leaseToken - Private ownership token returned by this attempt's admission.
 * @param result - Proposed state, per-target outcomes, and retry delay in milliseconds.
 * @param deadline - Absolute Unix timestamp in milliseconds limiting bookkeeping.
 * @returns The confirmed updated job and observation time, or null if the row is absent
 * or its processing status, lease token, or lease expiry no longer permits this attempt.
 * @throws If the database result cannot be confirmed or decoded; a rejected write may
 * already have committed and must not be reported as a confirmed failure to persist.
 */
async function finalize(
  pool: Pool,
  jobId: string,
  leaseToken: string,
  result: AttemptResult,
  deadline: number,
): Promise<ObservedJob | null> {
  const details = diagnostics(result);
  // Lock first, then evaluate the database clock from the materialized row. A plain UPDATE
  // predicate can evaluate expiry before waiting for an unchanged row lock, admitting stale work.
  const saved = await query(
    pool,
    `WITH locked AS MATERIALIZED (
    SELECT job_id AS locked_job_id, status AS locked_status, lease_token AS locked_lease_token,
      lease_expires_at AS locked_lease_expires_at
    FROM public.reconciliation_jobs WHERE job_id = $1 FOR UPDATE
  ) UPDATE public.reconciliation_jobs SET
    status = $3, sql_outcome = $4, mongo_outcome = $5,
    next_attempt_at = CASE WHEN $3 = 'pending' THEN clock_timestamp() + ($6 * interval '1 millisecond') ELSE NULL END,
    completed_at = CASE WHEN $3 = 'completed' THEN clock_timestamp() ELSE NULL END,
    lease_token = NULL, lease_expires_at = NULL, last_error = $7::jsonb, conflict_details = $8::jsonb
    FROM locked WHERE job_id = locked.locked_job_id AND locked.locked_status = 'processing'
      AND locked.locked_lease_token = $2 AND locked.locked_lease_expires_at > clock_timestamp()
    RETURNING ${returned}`,
    [
      jobId,
      leaseToken,
      result.status,
      result.sql.outcome,
      result.mongo.outcome,
      result.retryDelayMs,
      details === null ? null : JSON.stringify(details),
      result.status === 'conflict' ? JSON.stringify(details ?? {}) : null,
    ],
    deadline,
  );
  return saved.rows[0] ? observed(saved.rows[0]) : null;
}

/**
 * Construct the job repository without opening connections or changing database state.
 * Repository operations borrow bounded connections; the root process owns pool shutdown.
 * @param pool - Shared PostgreSQL pool, which remains caller-owned for the repository's lifetime.
 * @returns Admission, lookup, finalization, and read-only readiness operations plus a no-op close.
 * @example
 * const jobs = createJobRepository(pool);
 * const observed = await jobs.get(jobId, Date.now() + 3000);
 * // Null means a successful lookup proved absence; a failed lookup rejects.
 */
export function createJobRepository(pool: Pool): JobRepository {
  return {
    /**
     * Admit an immutable event or identify its existing durable identity.
     * @param update - Validated approved event; retries must keep the event and content unchanged.
     * @param deadline - Absolute Unix timestamp in milliseconds limiting admission.
     * @returns Accepted only after commit confirmation, duplicate/collision after identity
     * confirmation, or unavailable with definite/unknown acceptance after an operational failure.
     */
    admit(update, deadline) {
      return admit(pool, update, deadline);
    },
    /**
     * Look up a job without claiming, retrying, or changing it.
     * @param jobId - Validated durable job UUID to retrieve.
     * @param deadline - Absolute Unix timestamp in milliseconds limiting the lookup.
     * @returns The observed public job, or null only after a successful lookup proves absence.
     * @throws If the query fails, times out, or returns an incompatible row.
     */
    async get(jobId, deadline) {
      const found = await query(
        pool,
        `SELECT ${returned} FROM public.reconciliation_jobs WHERE job_id = $1`,
        [jobId],
        deadline,
      );
      return found.rows[0] ? observed(found.rows[0]) : null;
    },
    /**
     * Read the latest job history for one product without changing scheduling or state.
     * @param productId - Validated product identifier to match exactly.
     * @param deadline - Absolute Unix timestamp in milliseconds limiting the history query.
     * @returns At most ten public jobs ordered by creation time, then job ID, descending;
     * an empty list means the successful query found no jobs.
     * @throws If the query fails, times out, or returns an incompatible row.
     */
    async recent(productId, deadline) {
      const found = await query(
        pool,
        `SELECT ${columns} FROM public.reconciliation_jobs
        WHERE product_id = $1 ORDER BY created_at DESC, job_id DESC LIMIT 10`,
        [productId],
        deadline,
      );
      return found.rows.map(view);
    },
    /**
     * Save the attempt result only under its live processing lease.
     * @param jobId - Durable job to finalize.
     * @param leaseToken - Private token identifying the attempt allowed to record results.
     * @param result - Proposed status, target outcomes, and retry delay in milliseconds.
     * @param deadline - Absolute Unix timestamp in milliseconds limiting the guarded update.
     * @returns The confirmed observation, or null when the row is absent or ownership is invalid.
     * @throws If finalization cannot be confirmed or decoded; the write may have committed.
     */
    finalize(jobId, leaseToken, result, deadline) {
      return finalize(pool, jobId, leaseToken, result, deadline);
    },
    /**
     * Check admission privileges and required job schema using catalog reads only.
     * @param deadline - Absolute Unix timestamp in milliseconds limiting the probe.
     * @returns Available after confirming the expected schema and privileges; otherwise
     * unavailable with a safe schema or availability diagnostic. Query failures resolve
     * as unavailable rather than rejecting, and no objects or reference rows are created.
     */
    async ready(deadline) {
      try {
        const checked = await query(pool, readinessSql, [], deadline);
        return checked.rows[0]?.ready === true
          ? { available: true }
          : {
              available: false,
              diagnostic: {
                code: 'POSTGRES_SCHEMA_UNAVAILABLE',
                message: 'Required job schema or admission privileges are unavailable.',
              },
            };
      } catch {
        return {
          available: false,
          diagnostic: {
            code: 'POSTGRES_UNAVAILABLE',
            message: 'PostgreSQL readiness could not be confirmed.',
          },
        };
      }
    },
    /**
     * Complete repository cleanup without closing the caller-owned pool.
     * @returns A resolved promise; the root process closes the pool after adapters drain.
     */
    async close() {},
  };
}

// Catalog checks inspect the definitions that admission/finalization require; no verification writes occur.
const readinessSql = `WITH expected(name, type_name, required) AS (VALUES
  ('job_id','uuid',true), ('kind','text',true), ('product_id','text',true), ('source_revision','integer',false),
  ('approved_payload','jsonb',false), ('status','text',true), ('attempt_count','integer',true),
  ('next_attempt_at','timestamp with time zone',false), ('lease_token','uuid',false),
  ('lease_expires_at','timestamp with time zone',false), ('sql_outcome','text',true), ('mongo_outcome','text',true),
  ('last_error','jsonb',false), ('conflict_details','jsonb',false), ('created_at','timestamp with time zone',true),
  ('updated_at','timestamp with time zone',true), ('completed_at','timestamp with time zone',false)
), relation AS (SELECT to_regclass('public.reconciliation_jobs') AS id)
SELECT COALESCE(
  has_schema_privilege('public', 'USAGE')
  AND has_table_privilege(relation.id, 'SELECT') AND has_table_privilege(relation.id, 'INSERT')
  AND has_table_privilege(relation.id, 'UPDATE')
  AND NOT EXISTS (SELECT 1 FROM expected e LEFT JOIN pg_attribute a ON a.attrelid = relation.id
      AND a.attname = e.name AND NOT a.attisdropped
      WHERE a.attname IS NULL OR format_type(a.atttypid, a.atttypmod) <> e.type_name OR a.attnotnull <> e.required)
  AND (SELECT count(*) FROM pg_attribute a JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
      WHERE a.attrelid = relation.id AND a.attname IN ('created_at', 'updated_at')
        AND pg_get_expr(d.adbin, d.adrelid) = 'clock_timestamp()') = 2
  AND EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conrelid = relation.id AND c.contype = 'p'
      AND pg_get_constraintdef(c.oid) = 'PRIMARY KEY (job_id)')
  AND EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = relation.id AND i.indisvalid AND i.indisunique
      AND pg_get_indexdef(i.indexrelid) = 'CREATE UNIQUE INDEX uq_jobs_update_revision ON public.reconciliation_jobs USING btree (product_id, source_revision) WHERE (kind = ''update''::text)')
  AND (SELECT count(*) FROM pg_constraint c WHERE c.conrelid = relation.id AND c.convalidated AND c.contype = 'c'
      AND c.conname IN ('jobs_kind_check','jobs_product_id_check','jobs_payload_check','jobs_status_check',
      'jobs_sql_outcome_check','jobs_mongo_outcome_check','jobs_lease_check','jobs_next_attempt_check',
      'jobs_completed_check','jobs_attempt_count_check','jobs_last_error_check','jobs_conflict_details_check',
      'jobs_conflict_requires_details_check')) = 13
  AND (SELECT count(*) FROM pg_trigger t JOIN pg_proc p ON p.oid = t.tgfoid JOIN pg_namespace n ON n.oid = p.pronamespace
      WHERE t.tgrelid = relation.id AND t.tgenabled IN ('O','A') AND NOT t.tgisinternal AND n.nspname = 'public'
      AND ((t.tgname = 'reconciliation_jobs_before_update' AND p.proname = 'enforce_reconciliation_job_update')
       OR (t.tgname = 'reconciliation_jobs_ready' AND p.proname = 'notify_reconciliation_ready'))) = 2
  AND EXISTS (SELECT 1 FROM public.schema_migrations WHERE version = 1 AND name = 'initial_reconciliation_schema'),
  false) AS ready FROM relation`;
