/**
 * @file Reconciles one claimed job and records a result through fenced ownership checks.
 * Borrowed adapters own database clients; transient failures never establish absence.
 */
import { decide } from './decision.js';
import { InvalidSnapshotError, SystemicError } from './model/diagnostic.js';
import type { AttemptResult, Job, JobStatus } from './model/job.js';
import type { Diagnostic } from './model/diagnostic.js';
import type { JobRepository, Logger, ProductStore } from './model/contracts.js';
import type { Observation, RepairResult, Target, TargetOutcomes } from './model/reconciliation.js';
import type { Snapshot } from './model/product.js';
export type ProcessorDependencies = {
  jobs: JobRepository;
  sql: ProductStore;
  mongo: ProductStore;
  log: Logger;
  random?: () => number;
};
export type ProcessResult = { status: JobStatus | 'lost' | 'unknown'; job: Job | null };
type Round = {
  sql: Observation;
  mongo: Observation;
  latest: Snapshot | null;
  failure: Diagnostic | null;
  permanent: boolean;
};
const targets: readonly Target[] = ['sql', 'mongo'];

/**
 * Read all required sources independently, awaiting every result before interpreting failures.
 * @param job - Claimed update whose retained approved history must be refreshed.
 * @param dependencies - Borrowed stores and job repository.
 * @param deadline - Absolute Unix millisecond cutoff for product work.
 * @returns Fresh observations and an explicit history failure when history could not be read.
 * @throws SystemicError for infrastructure faults that should stop further claims.
 */
async function readRound(
  job: Job,
  dependencies: ProcessorDependencies,
  deadline: number,
): Promise<Round> {
  const [sql, mongo, history] = await Promise.allSettled([
    dependencies.sql.read(job.productId, deadline),
    dependencies.mongo.read(job.productId, deadline),
    dependencies.jobs.latest(job.productId, deadline),
  ]);
  for (const result of [sql, mongo, history]) {
    if (result.status === 'rejected' && result.reason instanceof SystemicError) throw result.reason;
  }
  const unavailable: Observation = {
    state: 'unavailable',
    observedAt: new Date().toISOString(),
    diagnostic: {
      code: 'READ_UNAVAILABLE',
      message: 'Required destination read could not be confirmed.',
    },
  };
  const historyError: unknown = history.status === 'rejected' ? history.reason : null;
  return {
    sql: sql.status === 'fulfilled' ? sql.value : unavailable,
    mongo: mongo.status === 'fulfilled' ? mongo.value : unavailable,
    latest: history.status === 'fulfilled' ? history.value : null,
    failure:
      history.status === 'fulfilled'
        ? null
        : historyError instanceof InvalidSnapshotError
          ? historyError.diagnostic
          : {
              code: 'HISTORY_UNAVAILABLE',
              message: 'Highest retained approval could not be read.',
            },
    permanent: historyError instanceof InvalidSnapshotError,
  };
}

/**
 * Choose bounded exponential jitter for a started attempt.
 * @param attemptCount - Started attempts in this cycle, including API delivery.
 * @param random - Caller-supplied unit-interval sampler, defaulting to Math.random.
 * @returns An integer duration in milliseconds within half to all of the ceiling.
 */
function retryDelay(attemptCount: number, random: () => number): number {
  const ceiling = Math.min(30_000, 1_000 * 2 ** Math.max(0, attemptCount - 1));
  return Math.round(ceiling * (0.5 + 0.5 * Math.min(1, Math.max(0, random()))));
}

/**
 * Confirm an attempt proposal using the repository's lease fence, never forcing a fallback write.
 * @param job - Claimed work with its original private ownership token.
 * @param proposal - Proposed status and per-target outcomes; not yet durable.
 * @param dependencies - Borrowed repository and sanitized logger.
 * @param deadline - Absolute Unix millisecond deadline reserved for finalization.
 * @param startedAt - Unix milliseconds used only for diagnostic duration.
 * @returns Confirmed persisted state, lost ownership, or unknown acknowledgment.
 * @throws SystemicError when a configuration/authentication/schema fault must stop claims.
 */
async function finalize(
  job: Job,
  proposal: AttemptResult,
  dependencies: ProcessorDependencies,
  deadline: number,
  startedAt: number,
): Promise<ProcessResult> {
  if (job.leaseToken === null)
    throw new Error('Processing requires a claimed job with an ownership token.');
  const fields = { jobId: job.jobId, productId: job.productId, attemptCount: job.attemptCount };
  try {
    const saved = await dependencies.jobs.finalize(job.jobId, job.leaseToken, proposal, deadline);
    if (saved === null) {
      dependencies.log('job_ownership_lost', {
        ...fields,
        outcome: 'lost',
        durationMs: Date.now() - startedAt,
      });
      return { status: 'lost', job: null };
    }
    const event =
      saved.status === 'completed'
        ? 'job_completed'
        : saved.status === 'pending'
          ? 'job_retry_scheduled'
          : saved.status === 'conflict'
            ? 'job_conflict'
            : 'job_needs_attention';
    dependencies.log(event, {
      ...fields,
      outcome: saved.status,
      outcomes: saved.outcomes,
      nextAttemptAt: saved.nextAttemptAt,
      code: saved.lastError?.code,
      durationMs: Date.now() - startedAt,
      level:
        saved.status === 'needs_attention'
          ? 'error'
          : saved.status === 'completed'
            ? 'info'
            : 'warn',
    });
    return { status: saved.status, job: saved };
  } catch (error) {
    if (error instanceof SystemicError) throw error;
    // The repository write may have committed: returning unknown leaves lease recovery authoritative.
    dependencies.log('job_finalization_uncertain', {
      ...fields,
      outcome: 'unknown',
      durationMs: Date.now() - startedAt,
    });
    return { status: 'unknown', job: null };
  }
}

/**
 * Reconcile a single acknowledged claim within three rounds, reserving finalization time.
 * @param job - Current processing claim with immutable source evidence and a private lease token.
 * @param dependencies - Borrowed database adapters, repository, logger, and optional deterministic jitter sampler.
 * @param deadline - Absolute Unix milliseconds; defaults to 15 seconds from invocation.
 * @returns The confirmed recorded status, lost ownership, or unknown finalization; never an invented durable success.
 * @throws SystemicError for infrastructure faults, or Error when supplied work is not a valid claim.
 */
export async function processJob(
  job: Job,
  dependencies: ProcessorDependencies,
  deadline = Date.now() + 15_000,
): Promise<ProcessResult> {
  if (
    job.status !== 'processing' ||
    job.leaseToken === null ||
    job.attemptCount < 1 ||
    job.attemptCount > 8
  ) {
    throw new Error('Processing requires an acknowledged claim within its attempt limit.');
  }
  const startedAt = Date.now();
  const productDeadline = deadline - 3_000;
  const outcomes: TargetOutcomes = { sql: 'unknown', mongo: 'unknown' };
  let status: AttemptResult['status'] = 'pending';
  let lastError: Diagnostic | null = {
    code: 'VERIFICATION_INCOMPLETE',
    message: 'Fresh agreement was not verified within this attempt.',
  };
  let conflictDetails: Record<string, unknown> | null = null;
  const fields = { jobId: job.jobId, productId: job.productId, attemptCount: job.attemptCount };
  for (let round = 1; round <= 3 && Date.now() < productDeadline; round += 1) {
    const observations = await readRound(job, dependencies, productDeadline);
    if (observations.failure !== null) {
      status = observations.permanent ? 'needs_attention' : 'pending';
      lastError = observations.failure;
      break;
    }
    const decision = decide({ job, ...observations });
    for (const target of targets) {
      // Preserve acknowledged partial repairs when a later required read is unavailable.
      if (
        decision.outcomes[target] !== 'unknown' &&
        !(decision.outcomes[target] === 'retry' && outcomes[target] === 'satisfied')
      ) {
        outcomes[target] = decision.outcomes[target];
      }
    }
    if (decision.kind === 'conflict') {
      status = 'conflict';
      lastError = decision.diagnostic;
      conflictDetails = decision.conflictDetails;
      break;
    }
    if (decision.kind === 'invalid' || decision.kind === 'retry') {
      status = decision.kind === 'invalid' ? 'needs_attention' : 'pending';
      lastError = decision.diagnostic;
      break;
    }
    const winner = decision.winner;
    if (winner === null)
      throw new Error('A repair or completion decision requires a valid winner.');
    dependencies.log('snapshot_selected', {
      ...fields,
      revision: winner.revision,
      round,
      outcome: decision.kind,
    });
    for (const target of targets) {
      if (decision.outcomes[target] === 'satisfied') {
        dependencies.log('product_noop', {
          ...fields,
          target,
          revision: winner.revision,
          outcome: 'unchanged',
        });
      }
    }
    if (decision.kind === 'complete') {
      status = 'completed';
      lastError = null;
      break;
    }
    if (Date.now() >= productDeadline) break;
    const repairStartedAt = Date.now();
    const repairs = await Promise.allSettled(
      decision.repairs.map(
        /** Dispatch independent target repairs against this round's exact observations. */ (
          target,
        ) => dependencies[target].repair(winner, observations[target], productDeadline),
      ),
    );
    let systemicFailure: SystemicError | null = null;
    for (let index = 0; index < repairs.length; index += 1) {
      const target = decision.repairs[index];
      const settled = repairs[index];
      if (target === undefined || settled === undefined)
        throw new Error('Repair result lost its target mapping.');
      if (settled.status === 'rejected' && settled.reason instanceof SystemicError)
        systemicFailure = settled.reason;
      const result: RepairResult =
        settled.status === 'fulfilled'
          ? settled.value
          : {
              outcome: 'retry',
              diagnostic: {
                code: 'WRITE_UNCONFIRMED',
                message: 'Repair acknowledgment was lost; its effect is unknown.',
              },
            };
      outcomes[target] =
        result.outcome === 'applied' || result.outcome === 'noop'
          ? 'satisfied'
          : result.outcome === 'failed'
            ? 'failed'
            : 'retry';
      if (result.outcome === 'failed') {
        status = 'needs_attention';
        lastError = result.diagnostic ?? {
          code: 'REPAIR_FAILED',
          message: 'The destination rejected this product repair.',
        };
      }
      dependencies.log('product_repair_result', {
        ...fields,
        target,
        revision: winner.revision,
        outcome: result.outcome,
        code: result.diagnostic?.code,
        durationMs: Date.now() - repairStartedAt,
        level:
          result.outcome === 'failed'
            ? 'error'
            : result.outcome === 'retry' || result.outcome === 'race'
              ? 'warn'
              : 'info',
      });
    }
    if (systemicFailure) throw systemicFailure;
    if (status === 'needs_attention') break;
    // Applied writes, duplicate inserts, CAS races, and lost acknowledgments all require fresh reads.
  }
  if (status === 'pending' && job.attemptCount >= 8) {
    status = 'needs_attention';
    lastError = {
      code: 'RETRY_EXHAUSTED',
      message:
        'Eight started attempts were used; an external owner must resolve the fault and requeue a new retry cycle.',
      cause: lastError,
    };
  }
  const proposal: AttemptResult = {
    status,
    outcomes,
    lastError,
    conflictDetails,
    retryDelayMs:
      status === 'pending' ? retryDelay(job.attemptCount, dependencies.random ?? Math.random) : 0,
  };
  return finalize(job, proposal, dependencies, deadline, startedAt);
}
