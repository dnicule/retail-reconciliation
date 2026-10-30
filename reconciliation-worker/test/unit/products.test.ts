/**
 * @file Tests product evidence validation and conditional repair boundaries.
 * Driver handles use controlled replies; real database behavior is covered separately.
 */
import assert from 'node:assert/strict';
import test, { afterEach, mock } from 'node:test';
import { Double, Int32, MongoClient } from 'mongodb';
import type {
  Document,
  Filter,
  FindOptions,
  InsertOneOptions,
  UpdateFilter,
  UpdateOptions,
} from 'mongodb';
import { Client, Pool } from 'pg';
import { createMongoStore, createSqlStore } from '../../src/products.js';
import { SystemicError } from '../../src/model/diagnostic.js';
import type { Observation } from '../../src/model/reconciliation.js';
import type { ProductStore } from '../../src/model/contracts.js';
import type { Snapshot } from '../../src/model/product.js';

const snapshot: Snapshot = {
  productId: 'UNIT-WORKER',
  revision: 4,
  product: { name: 'Flask', description: '', category: 'Travel' },
};
const timestamp = new Date('2026-01-01T00:00:00.000Z');
const clients: MongoClient[] = [];
afterEach(
  /** Closes test-owned lazy clients and resets controlled driver entry points. */ async () => {
    for (const client of clients.splice(0)) await client.close();
    mock.restoreAll();
  },
);

/** Builds BSON evidence with independent catalog-only fields. */
function document(revision = 3): Document {
  return {
    _id: snapshot.productId,
    ...snapshot.product,
    sourceRevision: new Int32(revision),
    appliedAt: timestamp,
    seoTitle: 'Local',
    localFlag: true,
  };
}

type State = {
  value: Document | null;
  failRead?: Error;
  failWrite?: Error;
  race?: boolean;
  reads: number;
  writes: number;
  filters: Filter<Document>[];
  options: { timeoutMS?: number; signal?: AbortSignal; promoteValues?: boolean }[];
  events: string[];
};
/** Constructs isolated collection evidence and counters. */
function state(value: Document | null): State {
  return { value, reads: 0, writes: 0, filters: [], options: [], events: [] };
}

/** Replaces native collection I/O while retaining the production store's decisions and validation. */
function store(value: State): ProductStore {
  const client = new MongoClient('mongodb://127.0.0.1:27017', { retryWrites: false });
  clients.push(client);
  const database = client.db('unit_worker');
  const collection = database.collection('products');
  mock.method(client, 'db', /** Selects the controlled existing handle. */ () => database);
  mock.method(
    database,
    'collection',
    /** Selects the controlled product handle. */ () => collection,
  );
  mock.method(
    collection,
    'findOne',
    /** Supplies evidence or injects an unavailable read. */ async (
      _filter: Filter<Document>,
      options: FindOptions,
    ) => {
      value.reads += 1;
      value.options.push(options);
      if (value.failRead) throw value.failRead;
      return value.value;
    },
  );
  mock.method(
    collection,
    'insertOne',
    /** Simulates acknowledged insertion or an uncertain/raced write. */ async (
      row: Document,
      options: InsertOneOptions,
    ) => {
      value.writes += 1;
      value.options.push(options);
      if (value.failWrite) throw value.failWrite;
      if (value.race) throw Object.assign(new Error('duplicate'), { code: 11000 });
      value.value = row;
      return { acknowledged: true, insertedId: snapshot.productId };
    },
  );
  mock.method(
    collection,
    'updateOne',
    /** Captures expected-version filters and modifies only managed fields. */ async (
      filter: Filter<Document>,
      update: UpdateFilter<Document>,
      options: UpdateOptions,
    ) => {
      value.writes += 1;
      value.filters.push(filter);
      value.options.push(options);
      if (value.failWrite) throw value.failWrite;
      if (!value.race) value.value = { ...value.value, ...update.$set };
      return {
        acknowledged: true,
        matchedCount: value.race ? 0 : 1,
        modifiedCount: value.race ? 0 : 1,
        upsertedCount: 0,
        upsertedId: null,
      };
    },
  );
  return createMongoStore(
    client,
    'unit_worker',
    /** Captures meaningful operation events. */ (event) => value.events.push(event),
  );
}

test('Mongo repair preserves local fields and predicates the observed BSON revision', /** Stale observations must not permit a blind overwrite. */ async () => {
  const value = state(document());
  const adapter = store(value);
  const observation = await adapter.read(snapshot.productId, Date.now() + 1000);
  assert.equal((await adapter.repair(snapshot, observation, Date.now() + 1000)).outcome, 'applied');
  assert.deepEqual(value.filters[0]?.sourceRevision, new Int32(3));
  assert.equal(value.value?.seoTitle, 'Local');
  assert.equal(value.value?.localFlag, true);
  assert.equal(value.value?.sourceRevision.valueOf(), 4);
  assert.ok(value.events.includes('product_write_succeeded'));
  for (const options of value.options) {
    assert.ok(options.signal instanceof AbortSignal);
    assert.ok((options.timeoutMS ?? 0) <= 1000);
  }
  assert.equal(value.options[0]?.promoteValues, false);
});

test('equal and newer observations never change timestamps', /** Reconciliation can safely pass already satisfied targets through the adapter. */ async () => {
  for (const revision of [4, 5]) {
    const value = state(document(revision));
    const adapter = store(value);
    assert.equal(
      (
        await adapter.repair(
          snapshot,
          await adapter.read(snapshot.productId, Date.now() + 1000),
          Date.now() + 1000,
        )
      ).outcome,
      'noop',
    );
    assert.equal(value.writes, 0);
    assert.equal(value.value?.appliedAt, timestamp);
    assert.ok(value.events.includes('product_noop'));
  }
});

test('missing copies insert explicit Int32 with null known local field', /** Reconstruction restores approved fields without inventing catalog-only information. */ async () => {
  const value = state(null);
  const adapter = store(value);
  assert.equal(
    (
      await adapter.repair(
        snapshot,
        await adapter.read(snapshot.productId, Date.now() + 1000),
        Date.now() + 1000,
      )
    ).outcome,
    'applied',
  );
  assert.ok(value.value?.sourceRevision instanceof Int32);
  assert.equal(value.value?.seoTitle, null);
});

test('duplicate inserts and zero-match updates report races for both-store refresh', /** An adapter cannot call a race success from stale evidence. */ async () => {
  for (const record of [null, document()]) {
    const value = state(record);
    value.race = true;
    const adapter = store(value);
    assert.equal(
      (
        await adapter.repair(
          snapshot,
          await adapter.read(snapshot.productId, Date.now() + 1000),
          Date.now() + 1000,
        )
      ).outcome,
      'race',
    );
    assert.equal(value.reads, 1);
  }
});

test('unavailable and malformed observations never authorize writes', /** Failure and corruption must remain distinct from confirmed absence. */ async () => {
  const failed = state(null);
  failed.failRead = new Error('secret connection');
  const unavailable = store(failed);
  const observation = await unavailable.read(snapshot.productId, Date.now() + 1000);
  assert.equal(observation.state, 'unavailable');
  assert.equal(JSON.stringify(observation).includes('secret'), false);
  assert.equal(
    (await unavailable.repair(snapshot, observation, Date.now() + 1000)).outcome,
    'retry',
  );
  assert.equal(failed.writes, 0);
  for (const record of [
    { ...document(), sourceRevision: new Double(3) },
    { ...document(), sourceRevision: 3 },
    { ...document(), appliedAt: new Date('invalid') },
    { ...document(), seoTitle: true },
  ]) {
    const value = state(record);
    const adapter = store(value);
    const invalid = await adapter.read(snapshot.productId, Date.now() + 1000);
    assert.equal(invalid.state, 'invalid');
    assert.equal((await adapter.repair(snapshot, invalid, Date.now() + 1000)).outcome, 'failed');
    assert.equal(value.writes, 0);
  }
});

test('uncertain writes retry while authentication and schema failures stop processing', /** A lost acknowledgement may hide a committed write; systemic errors must escape. */ async () => {
  const value = state(null);
  const adapter = store(value);
  const observation = await adapter.read(snapshot.productId, Date.now() + 1000);
  value.failWrite = new Error('acknowledgement lost');
  assert.equal((await adapter.repair(snapshot, observation, Date.now() + 1000)).outcome, 'retry');
  value.failRead = Object.assign(new Error('private message'), { code: 18 });
  await assert.rejects(adapter.read(snapshot.productId, Date.now() + 1000), SystemicError);
  value.failWrite = Object.assign(new Error('private validation'), { code: 121 });
  assert.equal((await adapter.repair(snapshot, observation, Date.now() + 1000)).outcome, 'failed');
});

test('expired budgets start no Mongo operation', /** Deadline exhaustion is uncertainty, not absence. */ async () => {
  const value = state(null);
  const adapter = store(value);
  assert.equal((await adapter.read(snapshot.productId, Date.now() - 1)).state, 'unavailable');
  assert.equal(
    (
      await adapter.repair(
        snapshot,
        { state: 'missing', observedAt: timestamp.toISOString() },
        Date.now() - 1,
      )
    ).outcome,
    'retry',
  );
  assert.equal(value.reads, 0);
  assert.equal(value.writes, 0);
});

test('SQL writes use exact revision guards, preserve local fields, and avoid filtered upserts', /** Native result counts define applied versus raced SQL outcomes. */ async () => {
  const pool = new Pool();
  const statements: { text: string; values: unknown[] }[] = [];
  const client = Object.assign(new Client(), {
    /** Releases the controlled client without networking. */ release(): void {},
  });
  mock.method(pool, 'connect', /** Acquires the controlled connection. */ async () => client);
  mock.method(
    client,
    'query',
    /** Records writes and acknowledges one affected product. */ async (
      text: string,
      values: unknown[],
    ) => {
      statements.push({ text, values });
      return { rows: [], rowCount: 1, command: 'UPDATE', oid: 0, fields: [] };
    },
  );
  const adapter = createSqlStore(
    pool,
    /** Omits log output from this SQL predicate test. */ () => {},
  );
  const older: Observation = {
    state: 'present',
    observedAt: timestamp.toISOString(),
    snapshot: { ...snapshot, revision: 3 },
  };
  assert.equal((await adapter.repair(snapshot, older, Date.now() + 1000)).outcome, 'applied');
  assert.match(
    statements[0]?.text ?? '',
    /source_revision\s*=\s*\$6\s+AND\s+source_revision\s*<\s*\$5/i,
  );
  assert.equal(statements[0]?.text.includes('internal_note'), false);
  assert.equal(statements[0]?.values[5], 3);
  await adapter.repair(
    snapshot,
    { state: 'missing', observedAt: timestamp.toISOString() },
    Date.now() + 1000,
  );
  assert.match(statements[1]?.text ?? '', /ON CONFLICT\s*\(product_id\)\s*DO NOTHING/i);
  assert.match(statements[1]?.text ?? '', /internal_note/);
  await pool.end();
});
