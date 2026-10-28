/**
 * @file Checks pure reconciliation choices using explicit observed and approved snapshots.
 * These tests model authority and failures without opening database connections.
 */
import assert from 'node:assert/strict';
import test from 'node:test';
import { decide } from '../../src/decision.js';
import { validProductId } from '../../src/model/product.js';
import type { Job } from '../../src/model/job.js';
import type { Observation } from '../../src/model/reconciliation.js';
import type { SharedProduct, Snapshot } from '../../src/model/product.js';
const timestamp = '2026-10-24T10:00:00.000Z';
const product: SharedProduct = { name: 'Notebook', description: '', category: 'Stationery' };
/** Construct a complete candidate at a supplied source revision. */
function snapshot(revision: number, name = 'Notebook'): Snapshot {
  return { productId: 'SKU-UNIT-001', revision, product: { ...product, name } };
}
/** Wrap a stored snapshot in a successful observation. */
function present(revision: number, name = 'Notebook'): Observation {
  return { state: 'present', snapshot: snapshot(revision, name), observedAt: timestamp };
}
/** Construct a claimed immutable update with complete approved evidence. */
function job(): Job {
  return {
    jobId: '30000000-0000-4000-8000-000000000001',
    kind: 'update',
    productId: 'SKU-UNIT-001',
    revision: 1,
    approvedPayload: product,
    status: 'processing',
    attemptCount: 1,
    leaseToken: '40000000-0000-4000-8000-000000000001',
    leaseExpiresAt: timestamp,
    nextAttemptAt: null,
    createdAt: timestamp,
    updatedAt: timestamp,
    completedAt: null,
    outcomes: { sql: 'unknown', mongo: 'unknown' },
    lastError: null,
    conflictDetails: null,
  };
}
const missing: Observation = { state: 'missing', observedAt: timestamp };
test('selects a newer destination and repairs the older one', /** Older authority must not downgrade newer information. */ () => {
  const result = decide({ job: job(), sql: present(5), mongo: present(4), latest: snapshot(1) });
  assert.equal(result.kind, 'repair');
  assert.equal(result.winner?.revision, 5);
  assert.deepEqual(result.repairs, ['mongo']);
});
test('restores both absent copies from the highest retained approval', /** Retained snapshot authority is independent of historical status. */ () => {
  const result = decide({ job: job(), sql: missing, mongo: missing, latest: snapshot(6) });
  assert.equal(result.winner?.revision, 6);
  assert.deepEqual(result.repairs, ['sql', 'mongo']);
});
test('highest revision disagreement records values without arbitrary repair', /** Exact string equality includes leading whitespace. */ () => {
  const result = decide({
    job: job(),
    sql: present(5),
    mongo: present(5, ' Notebook'),
    latest: null,
  });
  assert.equal(result.kind, 'conflict');
  assert.deepEqual(result.repairs, []);
  assert.deepEqual(result.conflictDetails?.fields, ['name']);
  assert.ok(JSON.stringify(result.conflictDetails).includes(' Notebook'));
});
test('higher approved content supersedes older disagreement', /** Only maximum-revision disagreement blocks a repair. */ () => {
  const result = decide({
    job: job(),
    sql: present(2, 'A'),
    mongo: present(2, 'B'),
    latest: snapshot(3),
  });
  assert.equal(result.kind, 'repair');
  assert.equal(result.winner?.revision, 3);
});
test('restores absent copies from the original approval when no other history is returned', /** The immutable update remains complete evidence for both destinations. */ () => {
  const result = decide({ job: job(), sql: missing, mongo: missing, latest: null });
  assert.equal(result.kind, 'repair');
  assert.equal(result.winner?.revision, 1);
  assert.deepEqual(result.repairs, ['sql', 'mongo']);
});
test('failed read prevents winner selection even with an approval', /** Transport failure cannot establish absence. */ () => {
  const mongo: Observation = {
    state: 'unavailable',
    observedAt: timestamp,
    diagnostic: { code: 'TIMEOUT', message: 'Read timed out.' },
  };
  const result = decide({ job: job(), sql: present(1), mongo, latest: snapshot(3) });
  assert.equal(result.kind, 'retry');
  assert.equal(result.winner, null);
  assert.equal(result.outcomes.mongo, 'retry');
  assert.deepEqual(result.repairs, []);
});
test('malformed lower-revision data requires attention', /** A bad record cannot be silently normalized using a higher candidate. */ () => {
  const sql = present(1);
  if (sql.state !== 'present') throw new Error('Fixture expected present observation.');
  sql.snapshot.product.name = '';
  const result = decide({ job: job(), sql, mongo: present(4), latest: snapshot(4) });
  assert.equal(result.kind, 'invalid');
  assert.equal(result.outcomes.sql, 'failed');
});
test('matching newer stores complete without repairs', /** Superseding the old job alone is insufficient: both stores must agree. */ () => {
  const result = decide({ job: job(), sql: present(6), mongo: present(6), latest: snapshot(4) });
  assert.equal(result.kind, 'complete');
  assert.deepEqual(result.outcomes, { sql: 'satisfied', mongo: 'satisfied' });
});
test('identifier with a final newline cannot become revision authority', /** Stored identities must satisfy the exact ASCII contract without normalization. */ () => {
  const sql = present(1);
  if (sql.state !== 'present') throw new Error('Fixture expected present observation.');
  sql.snapshot.productId += '\n';
  const original = job();
  original.productId += '\n';
  const result = decide({ job: original, sql, mongo: missing, latest: null });
  assert.equal(result.kind, 'invalid');
});

test('product identifiers accept only exact bounded ASCII values', /** Keep identifier acceptance aligned with both database validators without trimming input. */ () => {
  for (const valid of ['A', 'a'.repeat(64), 'SKU.a_B-9']) assert.equal(validProductId(valid), true);
  for (const invalid of [
    '',
    'a'.repeat(65),
    '-SKU',
    'SKU X',
    'SKU-X\n',
    'SKU-X\r',
    'SKU-X\u2028',
    'SKU-X\u2029',
    12,
    null,
  ])
    assert.equal(validProductId(invalid), false);
});
