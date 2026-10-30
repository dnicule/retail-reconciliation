/**
 * @file Defines durable update jobs, lease claims, and proposed attempt bookkeeping.
 * Repository implementations alone persist these results under the current unexpired lease.
 */
import type { Diagnostic } from './diagnostic.js';
import type { TargetOutcomes } from './reconciliation.js';

export type JobStatus = 'pending' | 'processing' | 'completed' | 'conflict' | 'needs_attention';

/** Lease tokens remain internal; an update's revision and payload are immutable history. */
export type Job = {
  jobId: string;
  kind: 'update';
  productId: string;
  revision: number | null;
  approvedPayload: unknown;
  status: JobStatus;
  attemptCount: number;
  leaseToken: string | null;
  leaseExpiresAt: string | null;
  nextAttemptAt: string | null;
  createdAt: string;
  updatedAt: string;
  completedAt: string | null;
  outcomes: TargetOutcomes;
  lastError: Diagnostic | null;
  conflictDetails: Record<string, unknown> | null;
};

export type ClaimResult =
  | { kind: 'claimed'; job: Job }
  | { kind: 'exhausted'; job: Job }
  | { kind: 'none' };
export type Claim = ClaimResult;

/** A proposal becomes durable only after fenced repository finalization acknowledges it. */
export type AttemptResult = {
  status: Exclude<JobStatus, 'processing'>;
  outcomes: TargetOutcomes;
  lastError: Diagnostic | null;
  conflictDetails: Record<string, unknown> | null;
  retryDelayMs: number;
};
