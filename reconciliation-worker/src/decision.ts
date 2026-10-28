/**
 * @file Selects immutable shared snapshots from successful destination observations.
 * Decisions are pure proposals; repair and durable bookkeeping are owned elsewhere.
 */
import { sameProduct, validateSnapshot } from './model/product.js';
import type { Diagnostic } from './model/diagnostic.js';
import type { Job } from './model/job.js';
import type { Observation, Target, TargetOutcomes } from './model/reconciliation.js';
import type { Snapshot } from './model/product.js';
export type Decision = {
  kind: 'repair' | 'complete' | 'retry' | 'invalid' | 'conflict';
  winner: Snapshot | null;
  repairs: Target[];
  outcomes: TargetOutcomes;
  diagnostic: Diagnostic | null;
  conflictDetails: Record<string, unknown> | null;
};
export type DecisionInput = {
  job: Job;
  sql: Observation;
  mongo: Observation;
  latest: Snapshot | null;
};
type Candidate = { source: string; snapshot: Snapshot; observedAt: string | null };
const targets: readonly Target[] = ['sql', 'mongo'];

/**
 * Select a complete highest revision after validating all required observations.
 * @param input - Claimed job, both fresh destination reads, and refreshed update history.
 * @returns A side-effect-free decision; missing reads never produce a winner.
 */
export function decide(input: DecisionInput): Decision {
  const result: Decision = {
    kind: 'retry',
    winner: null,
    repairs: [],
    outcomes: { sql: 'unknown', mongo: 'unknown' },
    diagnostic: null,
    conflictDetails: null,
  };
  const candidates: Candidate[] = [];
  for (const target of targets) {
    const observation = input[target];
    if (observation.state === 'invalid') {
      result.kind = 'invalid';
      result.outcomes[target] = 'failed';
      result.diagnostic = observation.diagnostic;
    } else if (observation.state === 'unavailable') {
      result.outcomes[target] = 'retry';
      result.diagnostic ??= observation.diagnostic;
    } else if (observation.state === 'present') {
      const invalid = validateSnapshot(observation.snapshot, input.job.productId);
      if (invalid) {
        result.kind = 'invalid';
        result.outcomes[target] = 'failed';
        result.diagnostic = invalid;
      } else {
        candidates.push({
          source: target,
          snapshot: observation.snapshot,
          observedAt: observation.observedAt,
        });
      }
    }
  }
  const original = {
    productId: input.job.productId,
    revision: input.job.revision,
    product: input.job.approvedPayload,
  };
  const invalid = validateSnapshot(original, input.job.productId);
  if (invalid) return { ...result, kind: 'invalid', diagnostic: { ...invalid, source: 'job' } };
  // Every update supplies immutable evidence, including when both destination copies are absent.
  candidates.push({
    source: 'job',
    snapshot: original as Snapshot,
    observedAt: input.job.createdAt,
  });
  if (input.latest !== null) {
    const invalidHistory = validateSnapshot(input.latest, input.job.productId);
    if (invalidHistory)
      return { ...result, kind: 'invalid', diagnostic: { ...invalidHistory, source: 'history' } };
    candidates.push({ source: 'history', snapshot: input.latest, observedAt: null });
  }
  // Even valid history cannot authorize choosing a winner from a failed required destination read.
  if (
    result.kind === 'invalid' ||
    result.outcomes.sql === 'retry' ||
    result.outcomes.mongo === 'retry'
  )
    return result;
  let highest = 0;
  for (const candidate of candidates) highest = Math.max(highest, candidate.snapshot.revision);
  const leaders = candidates.filter(
    /** Keep only candidates whose revision can currently govern the full snapshot. */ (
      candidate,
    ) => candidate.snapshot.revision === highest,
  );
  const leader = leaders[0];
  if (!leader) throw new Error('Validated candidate selection unexpectedly produced no leader.');
  const fields = (['name', 'description', 'category'] as const).filter(
    /** Detect exact field disagreements among the highest revision candidates. */ (field) =>
      leaders.some(
        /** Compare each candidate against the first without assigning authority by order. */ (
          candidate,
        ) => candidate.snapshot.product[field] !== leader.snapshot.product[field],
      ),
  );
  if (fields.length > 0) {
    for (const target of targets) {
      const observation = input[target];
      if (observation.state === 'present' && observation.snapshot.revision === highest)
        result.outcomes[target] = 'conflict';
    }
    return {
      ...result,
      kind: 'conflict',
      diagnostic: {
        code: 'HIGHEST_REVISION_CONFLICT',
        message: 'Highest revision candidates disagree on shared content.',
      },
      conflictDetails: { revision: highest, fields, candidates: leaders },
    };
  }
  result.winner = leader.snapshot;
  for (const target of targets) {
    const observation = input[target];
    if (
      observation.state === 'present' &&
      observation.snapshot.revision === highest &&
      sameProduct(observation.snapshot.product, leader.snapshot.product)
    ) {
      result.outcomes[target] = 'satisfied';
    } else {
      result.repairs.push(target);
    }
  }
  result.kind = result.repairs.length === 0 ? 'complete' : 'repair';
  return result;
}
