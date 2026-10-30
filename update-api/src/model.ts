/**
 * @file Contracts shared by HTTP handling, durable jobs, and product destination adapters.
 * Describes source authority, confirmed observations, uncertain outcomes, and resource
 * ownership without coupling those responsibilities to a driver or opening connections.
 */

/** Complete managed content; accepted strings are compared exactly, without normalization. */
export type SharedProduct = { name: string; description: string; category: string };
/** Source evidence treated as immutable; revision ordering never comes from application timestamps. */
export type Snapshot = { productId: string; revision: number; product: SharedProduct };
/** A source event's identity and complete snapshot; HTTP admission canonicalizes the UUID. */
export type ApprovedUpdate = Snapshot & { eventId: string };
/** Persisted processing state; completed satisfies an event, not permanent agreement between stores. */
export type JobStatus = 'pending' | 'processing' | 'completed' | 'conflict' | 'needs_attention';
/** Last recorded target result; satisfied includes a valid newer revision superseding the event. */
export type Outcome = 'unknown' | 'satisfied' | 'retry' | 'conflict' | 'failed';
/** Controlled diagnostic code and explanation suitable for responses; never a raw driver error. */
export type Diagnostic = { code: string; message: string };
/**
 * Public projection of a stored job with ISO timestamps and no lease token.
 * Audit revision/payload are null; update snapshots remain the original submitted content
 * even when delivery selects newer retained history. Outcomes retain the last recorded target
 * state, or unknown before results are confirmed.
 */
export type JobView = {
  jobId: string;
  kind: 'update' | 'audit';
  productId: string;
  revision: number | null;
  approvedPayload: SharedProduct | null;
  status: JobStatus;
  attemptCount: number;
  outcomes: { sql: Outcome; mongo: Outcome };
  nextAttemptAt: string | null;
  leaseExpiresAt: string | null;
  lastError: Record<string, unknown> | null;
  conflictDetails: Record<string, unknown> | null;
  createdAt: string;
  updatedAt: string;
  completedAt: string | null;
};
/** Confirmed job state paired with its database observation time, which survives stale-response fallback. */
export type ObservedJob = { job: JobView; observedAt: string };
/**
 * Admission decision: only accepted authorizes a new initial delivery after acknowledged commit.
 * Its selected snapshot may be newer than the immutable job snapshot. The lease token is
 * internal ownership evidence. Duplicate/collision never redispatch; unknown acceptance
 * requires replaying the original event because COMMIT may have reached PostgreSQL.
 */
export type Admission =
  | { kind: 'accepted'; job: JobView; observedAt: string; leaseToken: string; selected: Snapshot }
  | { kind: 'duplicate'; job: JobView; observedAt: string }
  | {
      kind: 'collision';
      code: 'EVENT_ID_COLLISION' | 'REVISION_ALREADY_REGISTERED';
      canonicalJobId: string;
    }
  | { kind: 'unavailable'; acceptance: 'not_accepted' | 'unknown' };
/** Semantic destination result; retry may hide an applied write and does not prove non-delivery. */
export type TargetResult = {
  outcome: Exclude<Outcome, 'unknown'>;
  action:
    | 'inserted'
    | 'updated'
    | 'unchanged'
    | 'superseded'
    | 'unavailable'
    | 'invalid'
    | 'conflict'
    | 'contention';
  diagnostic?: Diagnostic;
};
/** Proposed bookkeeping, not persisted state; retryDelayMs is applied using PostgreSQL time only if pending. */
export type AttemptResult = {
  status: Exclude<JobStatus, 'processing'>;
  sql: TargetResult;
  mongo: TargetResult;
  retryDelayMs: number;
};
/**
 * One independent destination read, with the local ISO observation time.
 * Present retains stored diagnostic/local fields alongside the validated shared snapshot;
 * missing requires a successful empty read. Unavailable and invalid are never repair evidence.
 */
export type ProductObservation =
  | {
      state: 'present';
      observedAt: string;
      snapshot: Snapshot;
      stored: Record<string, unknown>;
    }
  | { state: 'missing'; observedAt: string }
  | { state: 'unavailable' | 'invalid'; observedAt: string; diagnostic: Diagnostic };
/** Read-only dependency assessment; false means readiness was not confirmed, with optional safe detail. */
export type Readiness = { available: boolean; diagnostic?: Diagnostic };
/** Durable job operations; every deadline is an absolute Unix epoch time in milliseconds. */
export interface JobRepository {
  /**
   * Commit one immutable event and initial lease, or report the existing identity/failure.
   * @param update - Validated source envelope with a canonical lowercase event UUID.
   * @param deadline - Latest permitted operation time; the implementation also caps admission at three seconds.
   * @returns Accepted only after COMMIT acknowledgment; otherwise a duplicate, collision, or
   * unavailable decision distinguishing definite non-acceptance from uncertain commit.
   */
  admit(update: ApprovedUpdate, deadline: number): Promise<Admission>;
  /**
   * Read one job without claiming or updating it.
   * @param jobId - Validated event/audit UUID identifying the durable row.
   * @param deadline - Absolute upper bound for acquisition and lookup.
   * @returns Confirmed public state and observation time, or null after a successful empty lookup.
   * @throws If the lookup is unavailable, times out, or returns incompatible stored data.
   */
  get(jobId: string, deadline: number): Promise<ObservedJob | null>;
  /**
   * Read bounded product history without changing scheduling or attempt counts.
   * @param productId - Case-sensitive product identifier whose history is requested.
   * @param deadline - Absolute upper bound for acquisition and the history query.
   * @returns Up to ten jobs in descending creation order; an empty list proves no retained history.
   * @throws If the history query or stored-data validation fails; failure is not an empty history.
   */
  recent(productId: string, deadline: number): Promise<JobView[]>;
  /**
   * Persist attempt results only after acquiring the row lock and confirming current ownership.
   * A saved result clears lease fields and sets scheduling/completion dates for the chosen state.
   * @param jobId - Job being finalized; its immutable source fields remain unchanged.
   * @param leaseToken - Internal token for the attempt; never expose it in responses or logs.
   * @param result - Both target outcomes, proposed status, and pending retry delay in milliseconds.
   * @param deadline - Absolute upper bound for lock acquisition and result recording.
   * @returns Confirmed stored state, or null when the row is absent or no longer owned and unexpired.
   * @throws If finalization cannot be confirmed; a rejected promise may hide a committed update.
   */
  finalize(
    jobId: string,
    leaseToken: string,
    result: AttemptResult,
    deadline: number,
  ): Promise<ObservedJob | null>;
  /**
   * Inspect expected job schema and admission privileges without creating verification objects.
   * @param deadline - Absolute upper bound for the catalog/privilege query.
   * @returns Available only when the checks pass; database failures resolve as unavailable diagnostics.
   */
  ready(deadline: number): Promise<Readiness>;
  /**
   * Complete adapter-owned cleanup after in-flight work drains.
   * @returns A resolved promise for the current implementation; the runtime owns and closes the shared pool.
   */
  close(): Promise<void>;
}
/** Product I/O contract; database/schema provisioning and later worker recovery remain outside adapters. */
export interface ProductDestination {
  /**
   * Read and validate one product without repair or timestamp changes.
   * @param productId - Case-sensitive identifier shared by both destinations.
   * @param deadline - Absolute epoch-millisecond limit, including connection acquisition.
   * @returns Present, confirmed missing, invalid stored data, or unavailable with sanitized diagnostics.
   * Database failures are converted to observations rather than rejected as false absence.
   */
  read(productId: string, deadline: number): Promise<ProductObservation>;
  /**
   * Insert a missing copy or advance a lower revision, reevaluating races for at most three rounds.
   * Updates preserve local fields; equal matching and valid newer records remain unchanged.
   * @param snapshot - Selected complete approved evidence; the adapter never invents a revision.
   * @param deadline - Absolute epoch-millisecond limit for this entire delivery attempt.
   * @returns A semantic satisfied/retry/conflict/failed result; database failures become diagnostics,
   * and retry after an uncertain write does not prove the write was rejected.
   */
  deliver(snapshot: Snapshot, deadline: number): Promise<TargetResult>;
  /**
   * Probe the existing destination without creating or modifying database objects.
   * @param deadline - Absolute epoch-millisecond limit for the read-only probe.
   * @returns Availability with a safe diagnostic when the probe cannot confirm readiness.
   */
  ready(deadline: number): Promise<Readiness>;
  /**
   * Complete destination-owned cleanup after requests drain.
   * @returns A resolved promise for current adapters; the runtime closes their shared database clients.
   */
  close(): Promise<void>;
}
