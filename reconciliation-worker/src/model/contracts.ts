/**
 * @file Defines bounded storage and logging contracts used by the worker processor.
 * Adapters own I/O and resource cleanup; callers provide deadlines and sanitized log fields.
 */
import type { AttemptResult, Job } from './job.js';
import type { Snapshot } from './product.js';
import type { Observation, RepairResult } from './reconciliation.js';

export interface ProductStore {
  /** Read current shared fields by an absolute Unix millisecond deadline; failure is not absence. */
  read(productId: string, deadline: number): Promise<Observation>;
  /** Conditionally insert or advance using the observed revision, preserving local fields; acknowledgment may be unknown. */
  repair(snapshot: Snapshot, observed: Observation, deadline: number): Promise<RepairResult>;
  /** Inspect compatibility without writing, bounded by an absolute Unix millisecond deadline. */
  ready(deadline: number): Promise<void>;
}

export interface JobRepository {
  /** Read highest retained approval regardless of status; null means no retained update. */
  latest(productId: string, deadline: number): Promise<Snapshot | null>;
  /** Persist a proposal only under current unexpired processing ownership; null means ownership lost. */
  finalize(
    jobId: string,
    leaseToken: string,
    result: AttemptResult,
    deadline: number,
  ): Promise<Job | null>;
}

/** Log sanitized event fields; callers must never include credentials or lease tokens. */
export type Logger = (event: string, fields: Record<string, unknown>) => void;
export type Log = Logger;
