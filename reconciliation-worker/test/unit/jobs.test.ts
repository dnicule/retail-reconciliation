/**
 * @file Exercises claim acknowledgment and bookkeeping fencing with deterministic driver substitutes.
 * These tests model protocol uncertainty only; real PostgreSQL locking is checked separately.
 */
import assert from 'node:assert/strict';
import { test } from 'node:test';
import type { Pool } from 'pg';
import { createJobRepository } from '../../src/jobs.js';
import type { AttemptResult } from '../../src/model/job.js';

const id = '10000000-0000-4000-8000-000000000001';
const token = '20000000-0000-4000-8000-000000000001';

/** Build the immutable approved update identity and mutable fields returned by PostgreSQL. */
function row(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    job_id: id,
    kind: 'update',
    product_id: 'JOB-TEST',
    source_revision: 1,
    approved_payload: { name: 'Job test', description: '', category: 'Tests' },
    status: 'processing',
    attempt_count: 1,
    next_attempt_at: null,
    lease_token: token,
    lease_expires_at: new Date(Date.now() + 30000),
    sql_outcome: 'unknown',
    mongo_outcome: 'unknown',
    last_error: null,
    conflict_details: null,
    created_at: new Date(),
    updated_at: new Date(),
    completed_at: null,
    ...overrides,
  };
}

/**
 * Substitute only the pg pool/client boundary; script each server response explicitly.
 * @param respond - Returns row data or rejects for the submitted SQL statement.
 * @returns A borrowed pool facade plus observed statements and client disposal state.
 */
function database(
  respond: (sql: string, values: unknown[]) => Promise<Record<string, unknown>[]>,
): { pool: Pool; statements: string[]; destroyed: () => boolean | undefined } {
  const statements: string[] = [];
  let discarded: boolean | undefined;
  const client = {
    /** Record the submitted statement before returning the scripted server acknowledgment. */
    async query(sql: string, values: unknown[] = []) {
      statements.push(sql);
      const rows = await respond(sql, values);
      return { rows, rowCount: rows.length, command: '', oid: 0, fields: [] };
    },
    /** Observe whether protocol uncertainty causes the borrowed connection to be discarded. */
    release(destroy?: boolean) {
      discarded = destroy;
    },
  };
  // The adapter uses only connect/query/release; this deliberate test boundary avoids a real socket.
  const pool = {
    /** Resolve the sole deterministic connection. */ async connect() {
      return client;
    },
  } as unknown as Pool;
  return {
    pool,
    statements,
    /** Return the final release decision without exposing mutable state. */ destroyed: () =>
      discarded,
  };
}

/** Produce the caller's proposed verified outcome; the repository must still fence its persistence. */
function completed(): AttemptResult {
  return {
    status: 'completed',
    outcomes: { sql: 'satisfied', mongo: 'satisfied' },
    lastError: null,
    conflictDetails: null,
    retryDelayMs: 0,
  };
}

test('claim acknowledgment failure never exposes a claimed job or successful claim log', /** Lost COMMIT acknowledgment leaves durable recovery in charge. */ async () => {
  const events: string[] = [];
  const db = database(
    /** Return a selected and updated row, then hide commit acknowledgment. */ async (sql) => {
      if (sql === 'COMMIT') throw new Error('connection lost');
      if (sql.includes('SELECT') || sql.includes('RETURNING')) return [row()];
      return [];
    },
  );
  const jobs = createJobRepository(
    db.pool,
    /** Collect event names without logging private row data. */ (event) => {
      events.push(event);
    },
  );
  await assert.rejects(jobs.claim(Date.now() + 1000), /connection lost/);
  assert.equal(events.includes('job_claimed'), false);
  assert.equal(db.destroyed(), true);
});

test('finalization locks before checking current database lease and does not force a lost result', /** An unchanged locked row can outlive the lease while this attempt waits. */ async () => {
  let checkedLock = false;
  const events: string[] = [];
  const db = database(
    /** The lock succeeds but the server finds the lease expired on the following statement. */ async (
      sql,
    ) => {
      if (sql.includes('FOR UPDATE')) {
        checkedLock = true;
        return [row()];
      }
      if (sql.includes('UPDATE public.reconciliation_jobs')) {
        assert.equal(checkedLock, true);
        assert.match(sql, /lease_expires_at\s*>\s*clock_timestamp\(\)/);
        assert.match(sql, /lease_token\s*=\s*\$2/);
        assert.match(sql, /status\s*=\s*'processing'/);
        return [];
      }
      return [];
    },
  );
  const jobs = createJobRepository(
    db.pool,
    /** Collect lost-ownership evidence. */ (event) => {
      events.push(event);
    },
  );
  assert.equal(await jobs.finalize(id, token, completed(), Date.now() + 1000), null);
  assert.equal(
    db.statements.filter(
      /** Count only mutations, not the lock statement. */ (sql) =>
        sql.includes('UPDATE public.reconciliation_jobs'),
    ).length,
    1,
  );
  assert.ok(events.includes('job_ownership_lost'));
});

test('expired eighth attempt moves to attention without a ninth claim', /** Crash recovery consumes no additional attempt once its durable count reaches eight. */ async () => {
  const db = database(
    /** Supply the exhausted row and its confirmed terminal transition. */ async (sql) => {
      if (sql.includes('FOR UPDATE')) return [row({ attempt_count: 8 })];
      if (sql.includes('RETURNING'))
        return [
          row({
            status: 'needs_attention',
            attempt_count: 8,
            lease_token: null,
            lease_expires_at: null,
            last_error: { code: 'RETRY_EXHAUSTED', message: 'No automatic attempts remain.' },
          }),
        ];
      return [];
    },
  );
  const jobs = createJobRepository(db.pool, /** Suppress logs in this unit scenario. */ () => {});
  const claim = await jobs.claim(Date.now() + 1000);
  assert.equal(claim.kind, 'exhausted');
  assert.equal(claim.job.attemptCount, 8);
  assert.equal(
    db.statements.some(
      /** Detect accidental increment in the exhausted transition. */ (sql) =>
        sql.includes('attempt_count + 1'),
    ),
    false,
  );
});

test('latest approved snapshot rejects malformed immutable payloads', /** Unknown properties must not silently become authoritative evidence. */ async () => {
  const db = database(
    /** Return a payload that violates the exact three-field source contract. */ async () => [
      row({
        kind: 'update',
        source_revision: 3,
        approved_payload: { name: 'Name', description: '', category: 'Tests', extra: 'bad' },
      }),
    ],
  );
  const jobs = createJobRepository(
    db.pool,
    /** Suppress expected diagnostics in the test runner. */ () => {},
  );
  await assert.rejects(jobs.latest('JOB-TEST', Date.now() + 1000), /snapshot|payload|immutable/i);
});

for (const operation of ['claim', 'finalize'] as const) {
  test(`${operation} excludes retained audit jobs even when explicitly addressed`, /** The repository must keep unrelated audit rows outside every operational path. */ async () => {
    const db = database(
      /** Simulate a retained audit row that SQL kind predicates exclude. */ async (sql) => {
        if (!/^(SELECT|UPDATE)/u.test(sql) || /\bkind\s*=\s*'update'/u.test(sql)) return [];
        return [
          row({
            kind: 'audit',
            source_revision: null,
            approved_payload: null,
            status: 'processing',
          }),
        ];
      },
    );
    const jobs = createJobRepository(
      db.pool,
      /** Discard expected absence/lost-ownership events. */ () => {},
    );
    if (operation === 'claim')
      assert.deepEqual(await jobs.claim(Date.now() + 1000, id), { kind: 'none' });
    else assert.equal(await jobs.finalize(id, token, completed(), Date.now() + 1000), null);
  });
}
