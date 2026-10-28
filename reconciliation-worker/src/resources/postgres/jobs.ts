/**
 * @file Defines parameterized PostgreSQL statements for durable update-job processing.
 * This module contains SQL only; adapters own bindings, transactions, row validation,
 * and execution. No external values are interpolated into these statements.
 */

/** Projection shared by locked reads and returned job mutations; contains the private lease token. */
const columns: string =
  'job_id,kind,product_id,source_revision,approved_payload,status,attempt_count,next_attempt_at,lease_token,lease_expires_at,sql_outcome,mongo_outcome,last_error,conflict_details,created_at,updated_at,completed_at';

/** Eligibility uses the current statement clock for due pending updates and expired processing leases. */
const eligible: string =
  "((status='pending' AND next_attempt_at<=statement_timestamp()) OR (status='processing' AND lease_expires_at<=statement_timestamp()))";

/**
 * Lock the oldest eligible update while allowing other workers to skip owned rows.
 * Parameters: $1 = optional job UUID, or null for an unrestricted eligible-job scan.
 */
export const selectEligibleJobSql: string = `SELECT ${columns} FROM public.reconciliation_jobs WHERE kind='update' AND ${eligible} AND ($1::uuid IS NULL OR job_id=$1) ORDER BY COALESCE(next_attempt_at,lease_expires_at),job_id FOR UPDATE SKIP LOCKED LIMIT 1`;

/**
 * Move the already locked exhausted update to attention without starting another attempt.
 * Parameters: $1 = job UUID; $2 = retry-exhaustion diagnostic JSON.
 */
export const markJobExhaustedSql: string = `UPDATE public.reconciliation_jobs SET status='needs_attention',next_attempt_at=NULL,lease_token=NULL,lease_expires_at=NULL,completed_at=NULL,last_error=$2,conflict_details=NULL WHERE job_id=$1 AND kind='update' RETURNING ${columns}`;

/**
 * Install a fresh thirty-second lease and increment the locked update attempt count.
 * Parameters: $1 = job UUID; $2 = fresh lease-token UUID.
 */
export const claimJobSql: string = `UPDATE public.reconciliation_jobs SET status='processing',attempt_count=attempt_count + 1,next_attempt_at=NULL,lease_token=$2,lease_expires_at=clock_timestamp()+interval '30 seconds',sql_outcome='unknown',mongo_outcome='unknown',last_error=NULL,conflict_details=NULL,completed_at=NULL WHERE job_id=$1 AND kind='update' RETURNING ${columns}`;

/**
 * Read the highest retained approved snapshot regardless of its mutable delivery status.
 * Parameters: $1 = product identifier.
 */
export const selectLatestApprovedSnapshotSql: string =
  "SELECT product_id,source_revision,approved_payload FROM public.reconciliation_jobs WHERE product_id=$1 AND kind='update' ORDER BY source_revision DESC LIMIT 1";

/**
 * Acquire the update row lock before the following statement evaluates lease expiry.
 * Parameters: $1 = job UUID.
 */
export const lockJobForFinalizationSql: string =
  "SELECT job_id FROM public.reconciliation_jobs WHERE job_id=$1 AND kind='update' FOR UPDATE";

/**
 * Persist attempt results only under the current, unexpired update-processing lease.
 * Parameters: $1 = job UUID; $2 = lease-token UUID; $3 = proposed status;
 * $4 = SQL outcome; $5 = MongoDB outcome; $6 = last-error JSON or null;
 * $7 = conflict-details JSON or null; $8 = retry delay in milliseconds.
 */
export const finalizeJobSql: string = `UPDATE public.reconciliation_jobs SET status=$3,sql_outcome=$4,mongo_outcome=$5,last_error=$6,conflict_details=$7,next_attempt_at=CASE WHEN $3='pending' THEN clock_timestamp()+($8::double precision * interval '1 millisecond') ELSE NULL END,completed_at=CASE WHEN $3='completed' THEN clock_timestamp() ELSE NULL END,lease_token=NULL,lease_expires_at=NULL WHERE job_id=$1 AND kind='update' AND lease_token=$2 AND status='processing' AND lease_expires_at > clock_timestamp() RETURNING ${columns}`;
