/**
 * @file Exercises bounded attempt processing using deterministic in-memory database substitutes.
 * Substitutes stage read failures and concurrent revisions; no application records are touched.
 */
import assert from 'node:assert/strict';
import test from 'node:test';
import { processJob } from '../../src/processor.js';
import type { ProcessorDependencies } from '../../src/processor.js';
import { InvalidSnapshotError, SystemicError } from '../../src/model/diagnostic.js';
import type { AttemptResult, Job } from '../../src/model/job.js';
import type { JobRepository, ProductStore } from '../../src/model/contracts.js';
import type { Observation, RepairResult } from '../../src/model/reconciliation.js';
import type { Snapshot } from '../../src/model/product.js';
const timestamp = '2026-10-24T10:00:00.000Z';
/** Build a valid immutable snapshot whose name can expose concurrent disagreement. */
function snapshot(revision: number, name = 'Notebook'): Snapshot {
  return {
    productId: 'SKU-PROCESS-001',
    revision,
    product: { name, description: '', category: 'Stationery' },
  };
}
/** Construct fresh observation evidence from a snapshot or confirmed absence. */
function observed(value: Snapshot | null): Observation {
  return value === null
    ? { state: 'missing', observedAt: timestamp }
    : { state: 'present', snapshot: value, observedAt: timestamp };
}
/** Build claimed work with a realistic future lease and an immutable revision one. */
function claimed(): Job {
  return {
    jobId: '30000000-0000-4000-8000-000000000002',
    kind: 'update',
    productId: 'SKU-PROCESS-001',
    revision: 1,
    approvedPayload: snapshot(1).product,
    status: 'processing',
    attemptCount: 2,
    leaseToken: '40000000-0000-4000-8000-000000000002',
    leaseExpiresAt: new Date(Date.now() + 30_000).toISOString(),
    nextAttemptAt: null,
    createdAt: timestamp,
    updatedAt: timestamp,
    completedAt: null,
    outcomes: { sql: 'unknown', mongo: 'unknown' },
    lastError: null,
    conflictDetails: null,
  };
}
/** Simulate one target while recording actual conditional repairs and read counts. */
class MemoryStore implements ProductStore {
  reads = 0;
  writes = 0;
  beforeRepair: (() => void) | null = null;
  failure: Observation | Error | null = null;
  repairFailure: RepairResult | Error | null = null;
  /** Keep a caller-owned initial snapshot for observable state changes. */
  constructor(public value: Snapshot | null) {}
  /** Return current evidence or the deliberately staged error. */
  async read(): Promise<Observation> {
    this.reads += 1;
    if (this.failure instanceof Error) throw this.failure;
    return this.failure ?? observed(this.value);
  }
  /** Honor the expected revision, permitting hooks to insert a concurrent winner. */
  async repair(next: Snapshot, prior: Observation): Promise<RepairResult> {
    this.beforeRepair?.();
    if (this.repairFailure instanceof Error) throw this.repairFailure;
    if (this.repairFailure !== null) return this.repairFailure;
    const expected = prior.state === 'present' ? prior.snapshot.revision : null;
    if ((this.value?.revision ?? null) !== expected) return { outcome: 'race' };
    this.value = next;
    this.writes += 1;
    return { outcome: 'applied' };
  }
  /** The in-memory substitute has no infrastructure to inspect. */
  async ready(): Promise<void> {}
}
/** Provide mutable retained history plus observable finalization proposals. */
class MemoryJobs implements JobRepository {
  latestReads = 0;
  history: Snapshot | null = snapshot(1);
  historyFailure: Error | null = null;
  finalizeFailure: Error | null = null;
  lost = false;
  results: AttemptResult[] = [];
  /** Retain the claimed fixture for producing confirmed final states. */
  constructor(private readonly original: Job) {}
  /** Read the currently staged approval on each round. */
  async latest(): Promise<Snapshot | null> {
    this.latestReads += 1;
    if (this.historyFailure) throw this.historyFailure;
    return this.history;
  }
  /** Record the proposal, optionally model lost ownership or lost acknowledgment. */
  async finalize(_id: string, _token: string, result: AttemptResult): Promise<Job | null> {
    this.results.push(result);
    if (this.finalizeFailure) throw this.finalizeFailure;
    if (this.lost) return null;
    return { ...this.original, ...result, leaseToken: null, leaseExpiresAt: null };
  }
}
/** Build one isolated harness with retained event evidence and captured operation events. */
function harness(
  sqlValue: Snapshot | null,
  mongoValue: Snapshot | null,
): {
  job: Job;
  jobs: MemoryJobs;
  sql: MemoryStore;
  mongo: MemoryStore;
  events: string[];
  dependencies: ProcessorDependencies;
} {
  const job = claimed();
  const jobs = new MemoryJobs(job);
  const sql = new MemoryStore(sqlValue);
  const mongo = new MemoryStore(mongoValue);
  const events: string[] = [];
  return {
    job,
    jobs,
    sql,
    mongo,
    events,
    dependencies: {
      jobs,
      sql,
      mongo,
      log: /** Capture event names while excluding payload noise from behavior assertions. */ (
        event,
      ) => {
        events.push(event);
      },
      random: /** Choose deterministic minimum jitter without wall-clock guessing. */ () => 0,
    },
  };
}

test('repairs and rereads both destinations before confirmed completion', /** A successful repair acknowledgment alone must never mark completion. */ async () => {
  const h = harness(snapshot(5), snapshot(4));
  const result = await processJob(h.job, h.dependencies);
  assert.equal(result.status, 'completed');
  assert.equal(h.mongo.value?.revision, 5);
  assert.equal(h.sql.writes, 0);
  assert.equal(h.sql.reads, 2);
  assert.equal(h.mongo.reads, 2);
  assert.equal(h.jobs.latestReads, 2);
  assert.ok(h.events.includes('job_completed'));
});
test('refreshes retained approval after racing repair', /** A newly retained event must supersede the old job during verification. */ async () => {
  const h = harness(null, null);
  h.sql.beforeRepair = /** Admit a new snapshot before destination repair finishes. */ () => {
    h.jobs.history = snapshot(6);
  };
  const result = await processJob(h.job, h.dependencies);
  assert.equal(result.status, 'completed');
  assert.equal(h.sql.value?.revision, 6);
  assert.equal(h.mongo.value?.revision, 6);
});
test('concurrent newer destination survives and is propagated after CAS rejection', /** The expected-value race exposes a new revision instead of overwriting it. */ async () => {
  const h = harness(snapshot(2), snapshot(1));
  h.mongo.beforeRepair = /** Install a competing newer revision once. */ () => {
    h.mongo.value = snapshot(7);
    h.mongo.beforeRepair = null;
  };
  assert.equal((await processJob(h.job, h.dependencies)).status, 'completed');
  assert.equal(h.sql.value?.revision, 7);
  assert.equal(h.mongo.value?.revision, 7);
});
test('unavailable destination prevents all repairs and schedules jittered retry', /** An available destination does not imply the other is absent. */ async () => {
  const h = harness(null, null);
  h.mongo.failure = {
    state: 'unavailable',
    observedAt: timestamp,
    diagnostic: { code: 'TIMEOUT', message: 'Unavailable.' },
  };
  const result = await processJob(h.job, h.dependencies);
  assert.equal(result.status, 'pending');
  assert.equal(h.sql.writes + h.mongo.writes, 0);
  assert.equal(h.jobs.results[0]?.retryDelayMs, 1_000);
  assert.equal(h.jobs.results[0]?.outcomes.mongo, 'retry');
});
test('retains one successful repair when the other fails permanently', /** Independent writes have independent results even when the overall job blocks. */ async () => {
  const h = harness(null, null);
  h.mongo.repairFailure = {
    outcome: 'failed',
    diagnostic: { code: 'INVALID_RECORD', message: 'Malformed record.' },
  };
  assert.equal((await processJob(h.job, h.dependencies)).status, 'needs_attention');
  assert.deepEqual(h.jobs.results[0]?.outcomes, { sql: 'satisfied', mongo: 'failed' });
});
test('records conflict found after an earlier valid repair', /** Preserve acknowledged writes when later observations reveal competing equal authority. */ async () => {
  const h = harness(snapshot(2), snapshot(1));
  h.mongo.beforeRepair = /** Expose a same-revision conflicting SQL write during repair. */ () => {
    h.sql.value = snapshot(2, 'Conflicting');
  };
  assert.equal((await processJob(h.job, h.dependencies)).status, 'conflict');
  assert.equal(h.mongo.writes, 1);
  assert.equal(h.mongo.value?.product.name, 'Notebook');
});
test('eighth transient attempt requires attention', /** The API initial attempt is already represented in this cycle count. */ async () => {
  const h = harness(null, null);
  h.job.attemptCount = 8;
  h.mongo.failure = new Error('Disconnected');
  assert.equal((await processJob(h.job, h.dependencies)).status, 'needs_attention');
  assert.equal(h.jobs.results[0]?.lastError?.code, 'RETRY_EXHAUSTED');
});
test('lost ownership cannot force a second finalization', /** A zero-row guarded update belongs to lease recovery, not a fallback write. */ async () => {
  const h = harness(snapshot(1), snapshot(1));
  h.jobs.lost = true;
  assert.equal((await processJob(h.job, h.dependencies)).status, 'lost');
  assert.equal(h.jobs.results.length, 1);
});
test('uncertain finalization never reports in-memory completion', /** The write may have committed, so leave authoritative recovery to the database. */ async () => {
  const h = harness(snapshot(1), snapshot(1));
  h.jobs.finalizeFailure = new Error('Acknowledgment lost');
  const result = await processJob(h.job, h.dependencies);
  assert.equal(result.status, 'unknown');
  assert.equal(result.job, null);
  assert.equal(h.jobs.results.length, 1);
  assert.ok(h.events.includes('job_finalization_uncertain'));
});
test('systemic product errors stop the process without consuming a retry result', /** Authentication or schema faults must reach lifecycle control. */ async () => {
  const h = harness(snapshot(1), snapshot(1));
  h.mongo.failure = new SystemicError('AUTHENTICATION_FAILED', 'Authentication failed.');
  await assert.rejects(processJob(h.job, h.dependencies), SystemicError);
  assert.equal(h.jobs.results.length, 0);
});
test('malformed retained approval blocks only the affected job', /** Invalid historical data differs from infrastructure configuration faults. */ async () => {
  const h = harness(snapshot(1), snapshot(1));
  h.jobs.historyFailure = new InvalidSnapshotError(
    'INVALID_APPROVED_SNAPSHOT',
    'Stored approval is invalid.',
  );
  assert.equal((await processJob(h.job, h.dependencies)).status, 'needs_attention');
  assert.equal(h.jobs.results[0]?.lastError?.code, 'INVALID_APPROVED_SNAPSHOT');
});
test('reserved finalization budget prevents late product work', /** An exhausted repair budget should still allow recording a retry. */ async () => {
  const h = harness(null, null);
  assert.equal((await processJob(h.job, h.dependencies, Date.now() + 2_500)).status, 'pending');
  assert.equal(h.sql.reads + h.mongo.reads, 0);
  assert.equal(h.jobs.results.length, 1);
});
