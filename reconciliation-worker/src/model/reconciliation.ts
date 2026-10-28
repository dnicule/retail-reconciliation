/**
 * @file Defines destination observations and outcomes shared by reconciliation decisions and adapters.
 * Read uncertainty remains distinct from confirmed absence, and repairs report explicit acknowledgments.
 */
import type { Diagnostic } from './diagnostic.js';
import type { Snapshot } from './product.js';

export type Target = 'sql' | 'mongo';
export type TargetOutcome = 'unknown' | 'satisfied' | 'retry' | 'conflict' | 'failed';
export type TargetOutcomes = Record<Target, TargetOutcome>;

/** Unavailable is an unknown read; only missing confirms a successful absence lookup. */
export type Observation =
  | { state: 'present'; snapshot: Snapshot; observedAt: string }
  | { state: 'missing'; observedAt: string }
  | { state: 'invalid' | 'unavailable'; diagnostic: Diagnostic; observedAt: string };

export type RepairResult = {
  outcome: 'applied' | 'race' | 'noop' | 'retry' | 'failed';
  diagnostic?: Diagnostic;
};
