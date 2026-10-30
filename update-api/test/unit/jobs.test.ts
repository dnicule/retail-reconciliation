/**
 * @file Tests admission, job projections, lease bookkeeping, and connection cleanup.
 * Scripted driver replies make lost acknowledgments and delayed acquisition deterministic.
 * These tests inspect query intent and ownership; the integration suite verifies SQL behavior.
 */
import assert from 'node:assert/strict';
import test from 'node:test';
import type { Pool, QueryResult } from 'pg';
import { createJobRepository } from '../../src/jobs.js';
import { query } from '../../src/postgres.js';
import type { ApprovedUpdate, AttemptResult } from '../../src/model.js';

const event: ApprovedUpdate = {
  eventId: '30000000-0000-4000-8000-000000000001',
  productId: 'UNIT-JOBS',
  revision: 1,
  product: { name: 'Item', description: '', category: 'Test' },
};

/**
 * Build a driver-shaped job row with a private lease token and PostgreSQL Date values.
 * @param overrides - Columns to replace for the admission, collision, or status scenario.
 * @returns A fresh mutable row; the base event itself is not changed.
 */
function jobRow(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    job_id: event.eventId,
    kind: 'update',
    product_id: event.productId,
    source_revision: 1,
    approved_payload: event.product,
    status: 'processing',
    attempt_count: 1,
    next_attempt_at: null,
    lease_token: 'secret-token',
    lease_expires_at: new Date('2026-10-24T10:00:30Z'),
    sql_outcome: 'unknown',
    mongo_outcome: 'unknown',
    last_error: null,
    conflict_details: null,
    created_at: new Date('2026-10-24T10:00:00Z'),
    updated_at: new Date('2026-10-24T10:00:00Z'),
    completed_at: null,
    observed_at: new Date('2026-10-24T10:00:00Z'),
    ...overrides,
  };
}

/**
 * Wrap fixture rows in the node-postgres result shape consumed by the repository.
 * @param values - Rows returned by the scripted statement, in their supplied order.
 * @returns A SELECT-shaped result whose row count equals the number of rows.
 */
function rows(...values: Record<string, unknown>[]): QueryResult<Record<string, unknown>> {
  return { rows: values, rowCount: values.length, command: 'SELECT', oid: 0, fields: [] };
}

/** One queued reply that may inspect a submitted statement, resolve its rows, or reject. */
type Step = (sql: string, values: unknown[]) => Promise<QueryResult<Record<string, unknown>>>;

/** Models a single leased driver connection; scripts emulate acknowledgment loss, not SQL semantics. */
class ScriptedClient {
  readonly calls: { sql: string; values: unknown[] }[] = [];
  released: boolean | Error | undefined;
  /**
   * Retain the mutable reply queue for one acquired connection.
   * @param steps - Replies consumed in order; a missing reply fails the test.
   */
  constructor(private readonly steps: Step[]) {}
  /**
   * Record a statement and consume the next reply without interpreting SQL.
   * @param sql - Statement text submitted by the repository or query helper.
   * @param values - Bound parameters, retained for predicate and payload assertions.
   * @returns The next scripted result, which may remain pending behind a test barrier.
   * @throws If no reply remains or the selected reply rejects.
   */
  async query(sql: string, values: unknown[] = []): Promise<QueryResult<Record<string, unknown>>> {
    this.calls.push({ sql, values });
    const step = this.steps.shift();
    if (!step) throw new Error(`Unexpected statement: ${sql}`);
    return step(sql, values);
  }
  /**
   * Record how production cleanup relinquishes this connection.
   * @param error - An error or true means discard; omission records a normal pool return.
   */
  release(error?: boolean | Error): void {
    this.released = error ?? false;
  }
}

/**
 * Expose the scripted connection through the pool acquisition boundary.
 * @param client - Test-owned connection returned by every acquisition.
 * @returns A minimal pool stub; no sockets or actual pool resources are created.
 */
function poolFor(client: ScriptedClient): Pool {
  // Casting is confined to this driver stub; production receives an actual pg.Pool.
  return {
    connect: /** Acquires the scripted connection without network access. */ async () => client,
  } as unknown as Pool;
}

/**
 * Create a reply for an acknowledged statement that needs no query-specific checks.
 * @param values - Fixture rows to include in the acknowledgment.
 * @returns An asynchronous scripted step that ignores its SQL and parameter inputs.
 */
function succeeds(...values: Record<string, unknown>[]): Step {
  return /** Resolves one successful query acknowledgment. */ async () => rows(...values);
}

test('accepted admission commits once and retains newest approved snapshot without changing submitted job', /** Checks immutable admission and retained approved history. */ async () => {
  const newer = jobRow({
    source_revision: 9,
    approved_payload: { ...event.product, name: 'Newer' },
  });
  const client = new ScriptedClient([succeeds(), succeeds(jobRow()), succeeds(newer), succeeds()]);
  const admission = await createJobRepository(poolFor(client)).admit(event, Date.now() + 10_000);
  assert.equal(admission.kind, 'accepted');
  if (admission.kind !== 'accepted') return;
  assert.equal(admission.job.revision, 1);
  assert.equal(admission.selected.revision, 9);
  assert.equal(admission.selected.product.name, 'Newer');
  assert.equal(client.calls.at(-1)?.sql, 'COMMIT');
  assert.equal(client.released, false);
  assert.equal('lease_token' in admission.job, false);
});

test('commit acknowledgment loss is uncertain and destroys the leased connection', /** Simulates a server commit whose acknowledgment never arrives. */ async () => {
  const client = new ScriptedClient([
    succeeds(),
    succeeds(jobRow()),
    succeeds(jobRow()),
    /** Fails at the commit boundary, where persistence is unknowable to the caller. */ async () => {
      throw new Error('lost commit acknowledgment with secret connection data');
    },
  ]);
  assert.deepEqual(await createJobRepository(poolFor(client)).admit(event, Date.now() + 10_000), {
    kind: 'unavailable',
    acceptance: 'unknown',
  });
  assert.equal(client.released, true);
});

test('pre-commit failure is definitely unaccepted and cannot retain an open transaction', /** Confirms a broken uncommitted connection is discarded. */ async () => {
  const client = new ScriptedClient([
    succeeds(),
    /** Simulates failure before any COMMIT is sent. */ async () => {
      throw new Error('insert failed');
    },
  ]);
  assert.deepEqual(await createJobRepository(poolFor(client)).admit(event, Date.now() + 10_000), {
    kind: 'unavailable',
    acceptance: 'not_accepted',
  });
  assert.equal(client.released, true);
});

test('an identical event returns its existing job and does not select delivery or commit a new job', /** Checks replays do not initiate fresh work. */ async () => {
  const client = new ScriptedClient([succeeds(), succeeds(), succeeds(jobRow()), succeeds()]);
  const admission = await createJobRepository(poolFor(client)).admit(event, Date.now() + 10_000);
  assert.equal(admission.kind, 'duplicate');
  assert.equal(client.calls.at(-1)?.sql, 'ROLLBACK');
  assert.equal(client.calls.length, 4);
});

test('duplicate remains confirmed if read-only transaction cleanup loses acknowledgment', /** Prevents a verified durable event from being reported as definitely unaccepted. */ async () => {
  const client = new ScriptedClient([
    succeeds(),
    succeeds(),
    succeeds(jobRow()),
    /** Simulates connection loss after the existing immutable event was confirmed. */ async () => {
      throw new Error('rollback acknowledgment unavailable');
    },
  ]);
  const admission = await createJobRepository(poolFor(client)).admit(event, Date.now() + 1000);
  assert.equal(admission.kind, 'duplicate');
  assert.equal(client.released, true);
});

test('event identity collision takes precedence and is never treated as duplicate', /** Checks an existing audit cannot be mistaken for an update replay. */ async () => {
  const client = new ScriptedClient([
    succeeds(),
    succeeds(),
    succeeds(jobRow({ kind: 'audit', source_revision: null, approved_payload: null })),
    succeeds(),
  ]);
  assert.deepEqual(await createJobRepository(poolFor(client)).admit(event, Date.now() + 10_000), {
    kind: 'collision',
    code: 'EVENT_ID_COLLISION',
    canonicalJobId: event.eventId,
  });
});

test('product-revision collision identifies the existing canonical event', /** Checks immutable revision ownership across different event IDs. */ async () => {
  const canonical = '30000000-0000-4000-8000-000000000009';
  const client = new ScriptedClient([
    succeeds(),
    succeeds(),
    succeeds(),
    succeeds(jobRow({ job_id: canonical })),
    succeeds(),
  ]);
  assert.deepEqual(await createJobRepository(poolFor(client)).admit(event, Date.now() + 10_000), {
    kind: 'collision',
    code: 'REVISION_ALREADY_REGISTERED',
    canonicalJobId: canonical,
  });
});

test('read-only job lookup proves absence and projects timestamps without lease secrets', /** Checks the public job projection and successful missing lookup. */ async () => {
  const client = new ScriptedClient([succeeds(jobRow())]);
  const observed = await createJobRepository(poolFor(client)).get(event.eventId, Date.now() + 1000);
  assert.equal(observed?.job.createdAt, '2026-10-24T10:00:00.000Z');
  assert.equal(observed?.observedAt, '2026-10-24T10:00:00.000Z');
  assert.equal(JSON.stringify(observed).includes('secret-token'), false);
  const missing = new ScriptedClient([succeeds()]);
  assert.equal(
    await createJobRepository(poolFor(missing)).get(event.eventId, Date.now() + 1000),
    null,
  );
});

test('expired or replaced ownership returns no confirmation and retains every lease guard', /** Checks zero-row bookkeeping is reported as lost ownership. */ async () => {
  const result: AttemptResult = {
    status: 'completed',
    sql: { outcome: 'satisfied', action: 'unchanged' },
    mongo: { outcome: 'satisfied', action: 'unchanged' },
    retryDelayMs: 0,
  };
  const client = new ScriptedClient([succeeds()]);
  assert.equal(
    await createJobRepository(poolFor(client)).finalize(
      event.eventId,
      'token',
      result,
      Date.now() + 1000,
    ),
    null,
  );
  assert.match(client.calls[0]?.sql ?? '', /status = 'processing'/u);
  assert.match(client.calls[0]?.sql ?? '', /lease_token = \$2/u);
  assert.match(client.calls[0]?.sql ?? '', /lease_expires_at > clock_timestamp\(\)/u);
});

test('query timeout destroys active connection and does not report failed work as confirmed', /** Simulates an operation that never acknowledges its result. */ async () => {
  const client = new ScriptedClient([
    /** Keeps a submitted query unresolved until the local deadline. */ async () =>
      new Promise(/** Intentionally never resolves the query. */ () => {}),
  ]);
  await assert.rejects(
    query(poolFor(client), 'SELECT pg_sleep(10)', [], Date.now() + 15),
    /deadline/iu,
  );
  assert.equal(client.released, true);
});

test('late pool acquisition is released without submitting a statement', /** Simulates pool saturation followed by a connection after the caller timed out. */ async () => {
  const client = new ScriptedClient([]);
  let acquired: ((value: ScriptedClient) => void) | undefined;
  const pool = {
    connect: /** Defers acquisition until the test explicitly resolves it. */ () =>
      new Promise<ScriptedClient>(
        /** Exposes the deterministic acquisition gate. */ (resolve) => {
          acquired = resolve;
        },
      ),
  } as unknown as Pool;
  await assert.rejects(query(pool, 'SELECT 1', [], Date.now() + 15), /deadline/iu);
  assert.ok(acquired);
  acquired(client);
  await new Promise(
    /** Lets the late acquisition cleanup finish. */ (resolve) => setImmediate(resolve),
  );
  assert.equal(client.released, false);
  assert.equal(client.calls.length, 0);
});

test('admission does not return accepted while COMMIT acknowledgment is pending', /** Uses an explicit gate to make the durability boundary observable. */ async () => {
  let acknowledge: (() => void) | undefined;
  const client = new ScriptedClient([
    succeeds(),
    succeeds(jobRow()),
    succeeds(jobRow()),
    /** Holds the final response after the server has received COMMIT. */ async () =>
      new Promise(
        /** Exposes the acknowledgment gate to the test. */ (resolve) => {
          acknowledge = /** Supplies the previously withheld COMMIT acknowledgment. */ () =>
            resolve(rows());
        },
      ),
  ]);
  let settled = false;
  const admission = createJobRepository(poolFor(client))
    .admit(event, Date.now() + 1000)
    .then(
      /** Observes when the caller would be allowed to dispatch destinations. */ (result) => {
        settled = true;
        return result;
      },
    );
  await new Promise(
    /** Allows the preceding transactional queries to settle. */ (resolve) => setImmediate(resolve),
  );
  assert.equal(settled, false);
  assert.ok(acknowledge);
  acknowledge();
  assert.equal((await admission).kind, 'accepted');
  assert.equal(client.released, false);
});

test('a failed lookup rejects instead of proving the job missing', /** Distinguishes unavailable data from absence. */ async () => {
  const client = new ScriptedClient([
    /** Simulates a transport failure while reading. */ async () => {
      throw new Error('connection unavailable');
    },
  ]);
  await assert.rejects(createJobRepository(poolFor(client)).get(event.eventId, Date.now() + 1000));
  assert.equal(client.released, true);
});

test('finalization records both targets and database-scheduled retry while clearing ownership', /** Confirms the saved view and per-target diagnostics survive partial success. */ async () => {
  const result: AttemptResult = {
    status: 'pending',
    sql: { outcome: 'satisfied', action: 'updated' },
    mongo: {
      outcome: 'retry',
      action: 'unavailable',
      diagnostic: { code: 'UNAVAILABLE', message: 'Destination unavailable.' },
    },
    retryDelayMs: 750,
  };
  const client = new ScriptedClient([
    succeeds(
      jobRow({
        status: 'pending',
        lease_expires_at: null,
        next_attempt_at: new Date('2026-10-24T10:00:01Z'),
        sql_outcome: 'satisfied',
        mongo_outcome: 'retry',
      }),
    ),
  ]);
  const saved = await createJobRepository(poolFor(client)).finalize(
    event.eventId,
    'token',
    result,
    Date.now() + 1000,
  );
  assert.equal(saved?.job.status, 'pending');
  assert.deepEqual(saved?.job.outcomes, { sql: 'satisfied', mongo: 'retry' });
  assert.equal(saved?.job.leaseExpiresAt, null);
  assert.deepEqual(client.calls[0]?.values.slice(2, 6), ['pending', 'satisfied', 'retry', 750]);
  assert.match(
    client.calls[0]?.sql ?? '',
    /clock_timestamp\(\) \+ \(\$6 \* interval '1 millisecond'\)/u,
  );
});

test('readiness exposes sanitized failure and respects the schema/privilege check', /** Ensures readiness does not leak transport error contents or equate connectivity with compatibility. */ async () => {
  const ready = new ScriptedClient([succeeds({ ready: true })]);
  assert.deepEqual(await createJobRepository(poolFor(ready)).ready(Date.now() + 1000), {
    available: true,
  });
  const incompatible = new ScriptedClient([succeeds({ ready: false })]);
  assert.equal(
    (await createJobRepository(poolFor(incompatible)).ready(Date.now() + 1000)).available,
    false,
  );
  const failed = new ScriptedClient([
    /** Supplies deliberately sensitive raw driver content. */ async () => {
      throw new Error('postgres://secret:password@host/db');
    },
  ]);
  const unavailable = await createJobRepository(poolFor(failed)).ready(Date.now() + 1000);
  assert.equal(unavailable.available, false);
  assert.equal(JSON.stringify(unavailable).includes('password'), false);
});

test('history is a read-only bounded projection and close leaves the caller-owned pool open', /** Checks adapter lifecycle and the history limit without starting retries. */ async () => {
  const client = new ScriptedClient([succeeds(jobRow())]);
  const repository = createJobRepository(poolFor(client));
  const history = await repository.recent(event.productId, Date.now() + 1000);
  assert.equal(history.length, 1);
  assert.equal(history[0]?.jobId, event.eventId);
  assert.match(client.calls[0]?.sql ?? '', /LIMIT 10/u);
  await repository.close();
});
