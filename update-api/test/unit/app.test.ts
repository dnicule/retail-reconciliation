/**
 * @file Tests the HTTP contract against injectable in-memory job and product adapters.
 * Fastify injection avoids a network listener; explicit promise barriers expose
 * commit-before-dispatch ordering, independent fan-out, and uncertain bookkeeping.
 */
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import { buildApp } from '../../src/app.js';
import type {
  Admission,
  ApprovedUpdate,
  JobRepository,
  JobView,
  ProductDestination,
  TargetResult,
} from '../../src/model.js';

const update: ApprovedUpdate = {
  eventId: randomUUID(),
  productId: 'API-UNIT',
  revision: 1,
  product: { name: 'Bottle', description: '', category: 'Home' },
};
const stamp = '2026-01-01T12:00:00.000Z';
/**
 * Build a public job view with fixed observation dates for route assertions.
 * @param status - Durable state reported by the fake repository; defaults to processing.
 * @returns A fresh view of the shared event fixture, without private lease ownership.
 */
function job(status: JobView['status'] = 'processing'): JobView {
  return {
    jobId: update.eventId,
    kind: 'update',
    productId: update.productId,
    revision: 1,
    approvedPayload: update.product,
    status,
    attemptCount: 1,
    outcomes: { sql: 'unknown', mongo: 'unknown' },
    nextAttemptAt: null,
    leaseExpiresAt: stamp,
    lastError: null,
    conflictDetails: null,
    createdAt: stamp,
    updatedAt: stamp,
    completedAt: null,
  };
}
/**
 * Create independent adapters for one route case without database or network resources.
 * Tests replace individual methods to inject failures and synchronization barriers.
 * @returns Fake job and product adapters plus counters for admission, fan-out, and finalization.
 */
function setup() {
  const calls = { admit: 0, sql: 0, mongo: 0, finish: 0 };
  const jobs: JobRepository = {
    /** Simulates acknowledged durable admission. */
    async admit(): Promise<Admission> {
      calls.admit++;
      return {
        kind: 'accepted',
        job: job(),
        selected: update,
        leaseToken: randomUUID(),
        observedAt: stamp,
      };
    },
    /** Returns a fresh persisted observation. */
    async get() {
      return { job: job('completed'), observedAt: stamp };
    },
    /** Returns an empty successful history observation. */
    async recent() {
      return [];
    },
    /**
     * Project the supplied attempt into a confirmed public observation.
     * @param _id - Job identity is intentionally unchecked by this route-only stub.
     * @param _token - Lease ownership is exercised by repository tests, not this stub.
     * @param result - Semantic destination outcomes selected by the route.
     * @returns A fresh job view and fixed observation time; increments the finalization counter.
     */
    async finalize(_id, _token, result) {
      calls.finish++;
      return {
        job: {
          ...job(result.status),
          outcomes: { sql: result.sql.outcome, mongo: result.mongo.outcome },
        },
        observedAt: stamp,
      };
    },
    /** Reports available admission storage. */
    async ready() {
      return { available: true };
    },
    /** No resources are owned by the test adapter. */
    async close() {},
  };
  /**
   * Create a successful destination while tracking which branch receives delivery.
   * @param target - Counter key distinguishing SQL from MongoDB fan-out.
   * @returns An adapter whose reads prove absence and whose writes return acknowledged success.
   */
  function destination(target: 'sql' | 'mongo'): ProductDestination {
    return {
      /** Returns confirmed absence without mutation. */
      async read() {
        return { state: 'missing', observedAt: stamp };
      },
      /** Records independent fan-out invocation. */
      async deliver() {
        calls[target]++;
        return { outcome: 'satisfied', action: 'inserted' };
      },
      /** Reports dependency availability. */
      async ready() {
        return { available: true };
      },
      /** No resources are owned by the test adapter. */
      async close() {},
    };
  }
  return { jobs, sql: destination('sql'), mongo: destination('mongo'), calls };
}

test('rejects invalid input before any admission or product calls', /** Validation must reject an envelope before it can create durable or destination work. */ async (t) => {
  const adapters = setup();
  const app = buildApp({ ...adapters, logger: false });
  t.after(/** Closes the test-owned HTTP application after assertions finish. */ () => app.close());
  const invalid = [
    { ...update, revision: 0 },
    { ...update, revision: '1' },
    { ...update, revision: 2147483648 },
    { ...update, eventId: 'invalid' },
    { ...update, productId: '-bad' },
    { ...update, extra: true },
    { ...update, product: { ...update.product, extra: true } },
    { ...update, product: { name: 'Bottle' } },
  ];
  for (const payload of invalid)
    assert.equal(
      (await app.inject({ method: 'POST', url: '/approved-updates', payload })).statusCode,
      400,
    );
  assert.deepEqual(adapters.calls, { admit: 0, sql: 0, mongo: 0, finish: 0 });
});

test('rejects oversized bodies and unsupported media types', /** Transport limits protect admission even when the body never reaches envelope validation. */ async (t) => {
  const adapters = setup();
  const app = buildApp({ ...adapters, logger: false });
  t.after(/** Closes the test-owned HTTP application after assertions finish. */ () => app.close());
  assert.equal(
    (
      await app.inject({
        method: 'POST',
        url: '/approved-updates',
        payload: { ...update, product: { ...update.product, description: 'x'.repeat(33000) } },
      })
    ).statusCode,
    413,
  );
  assert.equal(
    (
      await app.inject({
        method: 'POST',
        url: '/approved-updates',
        headers: { 'content-type': 'text/plain' },
        payload: 'hello',
      })
    ).statusCode,
    415,
  );
  assert.equal(adapters.calls.admit, 0);
});

test('starts neither destination before commit and starts both independently afterward', /** Hold SQL completion after commit so MongoDB dispatch must be observable independently. */ async (t) => {
  const adapters = setup();
  let releaseAdmission!: (value: Admission) => void;
  let releaseSql!: (value: TargetResult) => void;
  const admission = new Promise<Admission>(
    /** Exposes the admission acknowledgement gate to the test. */ (resolve) => {
      releaseAdmission = resolve;
    },
  );
  const sqlResult = new Promise<TargetResult>(
    /** Exposes the SQL completion gate to control independent delivery. */ (resolve) => {
      releaseSql = resolve;
    },
  );
  let entered!: () => void;
  const enteredAdmission = new Promise<void>(
    /** Signals when admission has started before either product operation. */ (resolve) => {
      entered = resolve;
    },
  );
  adapters.jobs.admit =
    /** Holds durable admission until the test acknowledges its commit. */ async () => {
      entered();
      return admission;
    };
  adapters.sql.deliver =
    /** Counts SQL dispatch and delays its acknowledgement behind a barrier. */ async () => {
      adapters.calls.sql++;
      return sqlResult;
    };
  let mongoStarted!: () => void;
  const started = new Promise<void>(
    /** Signals when MongoDB delivery starts independently. */ (resolve) => {
      mongoStarted = resolve;
    },
  );
  adapters.mongo.deliver =
    /** Records MongoDB dispatch and simulates a sanitized dependency failure. */ async () => {
      adapters.calls.mongo++;
      mongoStarted();
      throw Error('credentials-secret');
    };
  const app = buildApp({ ...adapters, logger: false });
  t.after(/** Closes the test-owned HTTP application after assertions finish. */ () => app.close());
  const response = app.inject({ method: 'POST', url: '/approved-updates', payload: update });
  await enteredAdmission;
  assert.equal(adapters.calls.sql + adapters.calls.mongo, 0);
  releaseAdmission({
    kind: 'accepted',
    job: job(),
    observedAt: stamp,
    selected: update,
    leaseToken: randomUUID(),
  });
  await started;
  assert.equal(adapters.calls.sql, 1);
  releaseSql({ outcome: 'satisfied', action: 'inserted' });
  const result = await response;
  assert.equal(result.statusCode, 202);
  assert.equal(result.json().status, 'pending');
  assert.ok(!result.body.includes('credentials-secret'));
  assert.equal(result.headers.location, `/jobs/${update.eventId}`);
});

test('normalizes UUID and returns completed confirmed result', /** The normalized event identity reaches admission and only persisted completion produces HTTP 200. */ async (t) => {
  const adapters = setup();
  const admit = adapters.jobs.admit;
  adapters.jobs.admit =
    /** Checks UUID normalization before delegating to confirmed admission. */ async (
      input,
      deadline,
    ) => {
      assert.equal(input.eventId, update.eventId);
      return admit(input, deadline);
    };
  const app = buildApp({ ...adapters, logger: false });
  t.after(/** Closes the test-owned HTTP application after assertions finish. */ () => app.close());
  const result = await app.inject({
    method: 'POST',
    url: '/approved-updates',
    payload: { ...update, eventId: update.eventId.toUpperCase() },
  });
  assert.equal(result.statusCode, 200);
  assert.equal(result.json().statusFresh, true);
  assert.equal(result.json().revision, 1);
});

test('duplicate processing job does not dispatch or finalize', /** A replay observes existing work without taking ownership of the original attempt. */ async (t) => {
  const adapters = setup();
  adapters.jobs.admit =
    /** Returns an existing processing job without admitting fresh work. */ async () => ({
      kind: 'duplicate',
      job: job(),
      observedAt: stamp,
    });
  const app = buildApp({ ...adapters, logger: false });
  t.after(/** Closes the test-owned HTTP application after assertions finish. */ () => app.close());
  const result = await app.inject({ method: 'POST', url: '/approved-updates', payload: update });
  assert.equal(result.statusCode, 202);
  assert.equal(result.json().duplicate, true);
  assert.equal(adapters.calls.sql + adapters.calls.mongo + adapters.calls.finish, 0);
});

test('admission uncertainty starts no product writes and preserves retry identity', /** An ambiguous commit keeps the original event identity while preventing initial fan-out. */ async (t) => {
  const adapters = setup();
  adapters.jobs.admit =
    /** Simulates admission whose commit acknowledgement was lost. */ async () => ({
      kind: 'unavailable',
      acceptance: 'unknown',
    });
  const app = buildApp({ ...adapters, logger: false });
  t.after(/** Closes the test-owned HTTP application after assertions finish. */ () => app.close());
  const result = await app.inject({ method: 'POST', url: '/approved-updates', payload: update });
  assert.equal(result.statusCode, 503);
  assert.equal(result.json().error.acceptance, 'unknown');
  assert.equal(result.json().eventId, update.eventId);
  assert.equal(adapters.calls.sql + adapters.calls.mongo, 0);
});

test('unconfirmed finalization plus unavailable status returns last confirmed stale processing', /** When bookkeeping and its follow-up read fail, retain the dated admission observation. */ async (t) => {
  const adapters = setup();
  adapters.jobs.finalize =
    /** Simulates bookkeeping acknowledgement loss after product delivery. */ async () => {
      throw Error('lost ack');
    };
  adapters.jobs.get =
    /** Simulates unavailable storage without claiming that records are absent. */ async () => {
      throw Error('offline');
    };
  const app = buildApp({ ...adapters, logger: false });
  t.after(/** Closes the test-owned HTTP application after assertions finish. */ () => app.close());
  const result = await app.inject({ method: 'POST', url: '/approved-updates', payload: update });
  assert.equal(result.statusCode, 202);
  assert.equal(result.json().statusFresh, false);
  assert.equal(result.json().status, 'processing');
  assert.equal(result.json().statusAsOf, stamp);
});

test('lost ownership returns current owner result through one status read', /** A rejected lease update is followed by durable state lookup instead of reporting local delivery state. */ async (t) => {
  const adapters = setup();
  adapters.jobs.finalize =
    /** Reports lost lease ownership without changing the durable job. */ async () => null;
  const app = buildApp({ ...adapters, logger: false });
  t.after(/** Closes the test-owned HTTP application after assertions finish. */ () => app.close());
  const result = await app.inject({ method: 'POST', url: '/approved-updates', payload: update });
  assert.equal(result.statusCode, 200);
  assert.equal(result.json().status, 'completed');
});

test('conflict takes precedence over retry and permanent failure', /** Conflicting approved content determines job status while both target outcomes are retained. */ async (t) => {
  const adapters = setup();
  adapters.sql.deliver =
    /** Reports a destination disagreement at the selected revision. */ async () => ({
      outcome: 'conflict',
      action: 'conflict',
    });
  adapters.mongo.deliver =
    /** Reports permanent invalid destination evidence requiring attention. */ async () => ({
      outcome: 'failed',
      action: 'invalid',
    });
  const app = buildApp({ ...adapters, logger: false });
  t.after(/** Closes the test-owned HTTP application after assertions finish. */ () => app.close());
  const result = await app.inject({ method: 'POST', url: '/approved-updates', payload: update });
  assert.equal(result.statusCode, 202);
  assert.equal(result.json().status, 'conflict');
});

test('late acknowledged admission schedules pending without unsafe dispatch', /** Exhausted delivery time leaves accepted work pending for the worker without starting writes. */ async (t) => {
  const adapters = setup();
  let clock = 10000;
  const admit = adapters.jobs.admit;
  adapters.jobs.admit =
    /** Makes confirmed admission consume the initial delivery allowance. */ async (
      input,
      deadline,
    ) => {
      const value = await admit(input, deadline);
      clock += 12500;
      return value;
    };
  const app = buildApp({
    ...adapters,
    logger: false,
    now: /** Supplies a controlled epoch clock for deterministic deadline assertions. */ () =>
      clock,
  });
  t.after(/** Closes the test-owned HTTP application after assertions finish. */ () => app.close());
  const result = await app.inject({ method: 'POST', url: '/approved-updates', payload: update });
  assert.equal(result.json().status, 'pending');
  assert.equal(adapters.calls.sql + adapters.calls.mongo, 0);
});

test('failed job read is unavailable rather than a false 404', /** Unavailable storage cannot establish absence and raw driver failures must stay private. */ async (t) => {
  const adapters = setup();
  adapters.jobs.get =
    /** Returns a raw private failure to verify response sanitization. */ async () => {
      throw Error('secret');
    };
  const app = buildApp({ ...adapters, logger: false });
  t.after(/** Closes the test-owned HTTP application after assertions finish. */ () => app.close());
  const result = await app.inject(`/jobs/${update.eventId}`);
  assert.equal(result.statusCode, 503);
  assert.ok(!result.body.includes('secret'));
});

test('product diagnostic reads are independent and history failure stays explicit', /** One unavailable store and unavailable history must not erase the other store's valid observation. */ async (t) => {
  const adapters = setup();
  adapters.jobs.recent =
    /** Simulates unavailable storage without claiming that records are absent. */ async () => {
      throw Error('offline');
    };
  adapters.sql.read =
    /** Simulates unavailable storage without claiming that records are absent. */ async () => {
      throw Error('offline');
    };
  const app = buildApp({ ...adapters, logger: false });
  t.after(/** Closes the test-owned HTTP application after assertions finish. */ () => app.close());
  const result = await app.inject('/products/API-UNIT/state');
  assert.equal(result.statusCode, 200);
  assert.equal(result.json().sql.state, 'unavailable');
  assert.equal(result.json().mongo.state, 'missing');
  assert.equal(result.json().comparison, 'unknown');
  assert.equal(result.json().jobs.state, 'unavailable');
  assert.equal(result.json().atomic, false);
  assert.equal(adapters.calls.admit + adapters.calls.finish, 0);
});

test('Mongo outage degrades readiness without blocking admission readiness', /** Health distinguishes a live process and durable admission from an unavailable catalog. */ async (t) => {
  const adapters = setup();
  adapters.mongo.ready =
    /** Reports this dependency unavailable during readiness checks. */ async () => ({
      available: false,
    });
  const app = buildApp({ ...adapters, logger: false });
  t.after(/** Closes the test-owned HTTP application after assertions finish. */ () => app.close());
  assert.equal((await app.inject('/health/live')).statusCode, 200);
  const ready = await app.inject('/health/ready');
  assert.equal(ready.statusCode, 200);
  assert.equal(ready.json().dependencies.mongodb.available, false);
  adapters.jobs.ready =
    /** Reports this dependency unavailable during readiness checks. */ async () => ({
      available: false,
    });
  assert.equal((await app.inject('/health/ready')).statusCode, 503);
});

test('rejects trailing newline identifiers in envelopes and diagnostic paths', /** Checks absolute identifier boundaries before any database operation. */ async (t) => {
  const adapters = setup();
  const app = buildApp({ ...adapters, logger: false });
  t.after(/** Closes the route instance. */ () => app.close());
  for (const payload of [
    { ...update, eventId: `${update.eventId}\n` },
    { ...update, productId: `${update.productId}\n` },
  ]) {
    assert.equal(
      (await app.inject({ method: 'POST', url: '/approved-updates', payload })).statusCode,
      400,
    );
  }
  assert.equal((await app.inject(`/jobs/${update.eventId}%0A`)).statusCode, 400);
  assert.equal((await app.inject(`/products/${update.productId}%0A/state`)).statusCode, 400);
  assert.equal(adapters.calls.admit, 0);
});

test('permanent destination failures are logged at error with safe correlation fields', /** Distinguishes permanent data failures from retry warnings in operation logs. */ async (t) => {
  const adapters = setup();
  const lines: string[] = [];
  adapters.mongo.deliver = /** Simulates a permanent stored-data failure. */ async () => ({
    outcome: 'failed',
    action: 'invalid',
  });
  const app = buildApp({
    ...adapters,
    logger: {
      level: 'info',
      stream: {
        /** Captures structured logs without writing process output. */ write(line: string) {
          lines.push(line);
        },
      },
    },
  });
  t.after(/** Closes the route instance. */ () => app.close());
  const result = await app.inject({ method: 'POST', url: '/approved-updates', payload: update });
  assert.equal(result.json().status, 'needs_attention');
  const logs = lines.map(
    /** Parses one logger event. */ (line): Record<string, unknown> => JSON.parse(line),
  );
  const failure = logs.find(
    /** Selects only this target delivery event. */ (log) =>
      log.event === 'product_delivery' && log.target === 'mongo',
  );
  assert.equal(failure?.level, 50);
  assert.equal(failure?.jobId, update.eventId);
  assert.ok(!JSON.stringify(logs).includes('leaseToken'));
});
