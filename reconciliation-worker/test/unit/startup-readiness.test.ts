/**
 * @file Verifies process-lifetime startup validation, concurrent probes, and safe failure reporting.
 * Controlled probe callbacks exercise partial availability without accessing either database.
 */
import assert from 'node:assert/strict';
import test from 'node:test';
import type { Logger } from '../../src/model/contracts.js';
import { SystemicError } from '../../src/model/diagnostic.js';
import { createStartupReadiness } from '../../src/readiness.js';

/** Capture the coordinator's fields before production logging can sanitize or filter them. */
function captureLogs(): {
  events: Array<{ event: string; fields: Record<string, unknown> }>;
  log: Logger;
} {
  const events: Array<{ event: string; fields: Record<string, unknown> }> = [];
  return {
    events,
    /** Retain only the supplied fields so tests can detect raw diagnostic leakage. */ log(
      event,
      fields,
    ) {
      events.push({ event, fields });
    },
  };
}

/** Create a manually fulfilled probe result without timers or database connections. */
function probeGate(): { promise: Promise<void>; release(): void } {
  let fulfill: (() => void) | undefined;
  const promise = new Promise<void>(
    /** Hold successful validation until the test confirms both probes have started. */ (
      resolve,
    ) => {
      fulfill = resolve;
    },
  );
  return {
    promise,
    /** Confirm the pending probe's success. */ release() {
      assert.ok(fulfill);
      fulfill();
    },
  };
}

test('startup probes run concurrently with bounded deadlines and cache confirmed success', /** Both independent checks begin before either result, then are never repeated. */ async () => {
  const postgres = probeGate();
  const mongo = probeGate();
  const started: Array<{ target: string; deadline: number }> = [];
  const { events, log } = captureLogs();
  const ready = createStartupReadiness(
    /** Record PostgreSQL startup without resolving it yet. */ (deadline) => {
      started.push({ target: 'postgresql', deadline });
      return postgres.promise;
    },
    /** Record MongoDB startup independently of the pending PostgreSQL result. */ (deadline) => {
      started.push({ target: 'mongodb', deadline });
      return mongo.promise;
    },
    log,
  );
  const before = Date.now();
  const attempt = ready();
  await Promise.resolve();
  const after = Date.now();
  assert.deepEqual(
    started.map(/** Extract each started target in launch order. */ (entry) => entry.target),
    ['postgresql', 'mongodb'],
  );
  for (const entry of started) {
    assert.ok(entry.deadline >= before + 3000);
    assert.ok(entry.deadline <= after + 3000);
  }
  assert.equal(events.length, 0);
  postgres.release();
  mongo.release();
  await attempt;
  await ready();
  await ready();
  assert.equal(started.length, 2);
  assert.equal(events.length, 2);
  for (const entry of events) {
    assert.equal(entry.event, 'database_validated');
    assert.equal(entry.fields.outcome, 'ready');
    assert.equal(typeof entry.fields.durationMs, 'number');
    assert.ok(Number(entry.fields.durationMs) >= 0);
  }
  assert.deepEqual(
    events.map(
      /** Inspect each independently confirmed dependency. */ (entry) => entry.fields.target,
    ),
    ['postgresql', 'mongodb'],
  );
});

test('MongoDB startup retries preserve a successful PostgreSQL check', /** A transient MongoDB failure prevents startup and only its probe runs again. */ async () => {
  let postgresCalls = 0;
  let mongoCalls = 0;
  const unavailable = new Error('temporary MongoDB outage');
  const { events, log } = captureLogs();
  const ready = createStartupReadiness(
    /** Confirm PostgreSQL once. */ async () => {
      postgresCalls += 1;
    },
    /** Fail MongoDB once, then confirm compatibility on its later attempt. */ async () => {
      mongoCalls += 1;
      if (mongoCalls === 1) throw unavailable;
    },
    log,
  );
  await assert.rejects(ready(), unavailable);
  await ready();
  await ready();
  assert.equal(postgresCalls, 1);
  assert.equal(mongoCalls, 2);
  assert.deepEqual(
    events.map(
      /** Retain target and outcome to verify one success per database. */ (entry) => [
        entry.fields.target,
        entry.fields.outcome,
      ],
    ),
    [
      ['postgresql', 'ready'],
      ['mongodb', 'unknown'],
      ['mongodb', 'ready'],
    ],
  );
});

test('PostgreSQL startup retries preserve a successful MongoDB check', /** Successful catalog evidence survives a transient queue-database failure. */ async () => {
  let postgresCalls = 0;
  let mongoCalls = 0;
  const unavailable = new Error('temporary PostgreSQL outage');
  const { log } = captureLogs();
  const ready = createStartupReadiness(
    /** Fail PostgreSQL once before the schema can be confirmed. */ async () => {
      postgresCalls += 1;
      if (postgresCalls === 1) throw unavailable;
    },
    /** Confirm MongoDB even while PostgreSQL is unavailable. */ async () => {
      mongoCalls += 1;
    },
    log,
  );
  await assert.rejects(ready(), unavailable);
  await ready();
  assert.equal(postgresCalls, 2);
  assert.equal(mongoCalls, 1);
});

test('failed startup probes both retry and log only sanitized transient details', /** Driver messages, credentials and arbitrary rejection objects never become event fields. */ async () => {
  const privateMessage = 'postgresql://private-user:private-secret@private-host/db';
  const postgresFailure = new Error(privateMessage);
  const mongoFailure = { password: 'private-secret', connection: 'mongodb://private-host/db' };
  let postgresCalls = 0;
  let mongoCalls = 0;
  const { events, log } = captureLogs();
  const ready = createStartupReadiness(
    /** Keep PostgreSQL unconfirmed until the next caller-driven attempt. */ async () => {
      postgresCalls += 1;
      if (postgresCalls === 1) throw postgresFailure;
    },
    /** Keep MongoDB independently unconfirmed until its next attempt. */ async () => {
      mongoCalls += 1;
      if (mongoCalls === 1) throw mongoFailure;
    },
    log,
  );
  await assert.rejects(ready(), postgresFailure);
  assert.equal(events.length, 2);
  for (const entry of events) {
    assert.equal(entry.event, 'startup_check_unavailable');
    assert.equal(entry.fields.outcome, 'unknown');
    assert.equal(typeof entry.fields.durationMs, 'number');
  }
  assert.doesNotMatch(JSON.stringify(events), /private-|postgresql:\/\/|mongodb:\/\//);
  await ready();
  assert.equal(postgresCalls, 2);
  assert.equal(mongoCalls, 2);
});

test('systemic startup failure takes precedence over an earlier transient rejection', /** Schema incompatibility stops startup even when PostgreSQL also fails temporarily. */ async () => {
  const systemic = new SystemicError(
    'MONGODB_SCHEMA_INCOMPATIBLE',
    'Unsupported collection schema.',
  );
  const { log } = captureLogs();
  const ready = createStartupReadiness(
    /** Fail the first dependency transiently. */ async () => {
      throw new Error('temporary PostgreSQL outage');
    },
    /** Identify an incompatible second dependency. */ async () => {
      throw systemic;
    },
    log,
  );
  await assert.rejects(ready(), systemic);
});

test('a synchronous probe failure does not prevent the other database from validating', /** Independent startup work still runs and its successful result remains cached. */ async () => {
  let postgresCalls = 0;
  let mongoCalls = 0;
  const unavailable = new Error('synchronous dependency failure');
  const { log } = captureLogs();
  const ready = createStartupReadiness(
    /** Exercise a synchronous rejection before a driver promise exists. */ () => {
      postgresCalls += 1;
      if (postgresCalls === 1) throw unavailable;
      return Promise.resolve();
    },
    /** Confirm the other target despite the first callback throwing. */ async () => {
      mongoCalls += 1;
    },
    log,
  );
  await assert.rejects(ready(), unavailable);
  await ready();
  assert.equal(postgresCalls, 2);
  assert.equal(mongoCalls, 1);
});
