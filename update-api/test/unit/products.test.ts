/**
 * @file Tests destination validation, revision guards, races, and failure classification.
 * Native driver handles use controlled in-memory replies; no database server is contacted.
 * Assertions inspect writes and options while real-database tests verify storage semantics.
 */
import assert from 'node:assert/strict';
import test, { afterEach, mock } from 'node:test';
import { Int32, MongoClient } from 'mongodb';
import { Client, Pool } from 'pg';
import type { QueryConfig } from 'pg';
import type {
  Document,
  Filter,
  FindOptions,
  InsertOneOptions,
  UpdateFilter,
  UpdateOptions,
} from 'mongodb';
import { createMongoDestination, createSqlDestination, sameProduct } from '../../src/products.js';
import type { ProductDestination, Snapshot } from '../../src/model.js';

const snapshot: Snapshot = {
  productId: 'UNIT-PRODUCT',
  revision: 4,
  product: { name: 'Flask', description: 'A complete snapshot.', category: 'Outdoors' },
};
const before = new Date('2026-01-01T00:00:00.000Z');
const clients: MongoClient[] = [];

afterEach(
  /** Close every test-owned client and restore native methods after each isolated case. */ async () => {
    for (const client of clients.splice(0)) await client.close();
    mock.restoreAll();
  },
);

/**
 * Build a valid MongoDB document with stable timestamps and destination-only fields.
 * @param revision - Source-assigned revision encoded as BSON Int32; defaults to four.
 * @returns A fresh document sharing the fixture's immutable managed product content.
 */
function document(revision = 4): Document {
  return {
    _id: snapshot.productId,
    ...snapshot.product,
    sourceRevision: new Int32(revision),
    appliedAt: before,
    seoTitle: 'Local SEO',
    localFlag: true,
  };
}

/**
 * Mutable evidence owned by one adapter test. Failure fields inject driver errors;
 * race replaces the stored value during a write to model a competing winner.
 * Counters, filters, and options let assertions inspect attempted operations.
 */
type MemoryCollection = {
  value: Document | null;
  reads: number;
  writes: number;
  failRead?: Error;
  failWrite?: Error;
  race?: Document;
  filters: Filter<Document>[];
  options: { timeoutMS?: number; promoteValues?: boolean; writeConcern?: unknown }[];
};

/**
 * Replace collection I/O with controlled state transitions while retaining adapter logic.
 * The client is registered for afterEach cleanup, which also restores every driver mock.
 * @param state - Test-owned record, failure injections, and operation evidence to mutate.
 * @returns A MongoDB destination whose reads and writes never contact a server.
 */
function destination(state: MemoryCollection): ProductDestination {
  const client = new MongoClient('mongodb://127.0.0.1:27017', { retryWrites: false });
  clients.push(client);
  const database = client.db('unit_products');
  const collection = database.collection('products');
  mock.method(client, 'db', /** Return the isolated database handle. */ () => database);
  mock.method(
    database,
    'collection',
    /** Return the controlled product collection. */ () => collection,
  );
  mock.method(
    collection,
    'findOne',
    /** Record read evidence or reproduce a dependency failure. */ async (
      _filter: Filter<Document>,
      options: FindOptions,
    ) => {
      state.reads += 1;
      state.options.push(options);
      if (state.failRead) throw state.failRead;
      return state.value;
    },
  );
  mock.method(
    collection,
    'insertOne',
    /** Insert into memory or reproduce a competing insert or lost acknowledgement. */ async (
      value: Document,
      options: InsertOneOptions,
    ) => {
      state.writes += 1;
      state.options.push(options);
      if (state.failWrite) throw state.failWrite;
      if (state.race) {
        state.value = state.race;
        throw Object.assign(new Error('duplicate'), { code: 11000 });
      }
      state.value = value;
      return { acknowledged: true, insertedId: value._id };
    },
  );
  mock.method(
    collection,
    'updateOne',
    /** Apply only managed fields or simulate a competing writer winning first. */ async (
      filter: Filter<Document>,
      update: UpdateFilter<Document>,
      options: UpdateOptions,
    ) => {
      state.writes += 1;
      state.filters.push(filter);
      state.options.push(options);
      if (state.failWrite) throw state.failWrite;
      if (state.race) {
        state.value = state.race;
        return {
          acknowledged: true,
          matchedCount: 0,
          modifiedCount: 0,
          upsertedCount: 0,
          upsertedId: null,
        };
      }
      state.value = { ...state.value, ...update.$set };
      return {
        acknowledged: true,
        matchedCount: 1,
        modifiedCount: 1,
        upsertedCount: 0,
        upsertedId: null,
      };
    },
  );
  return createMongoDestination(client, 'unit_products');
}

/**
 * Start a clean operation history around a chosen stored document or confirmed absence.
 * @param value - Initial document reference, or null to model a successful missing read.
 * @returns Fresh counters, captured-option arrays, and the supplied record reference.
 */
function memory(value: Document | null): MemoryCollection {
  return { value, reads: 0, writes: 0, filters: [], options: [] };
}

test('shared comparison uses exact managed fields', /** Whitespace remains approved content; comparison must not silently normalize it. */ () => {
  assert.equal(sameProduct(snapshot.product, { ...snapshot.product }), true);
  assert.equal(sameProduct(snapshot.product, { ...snapshot.product, name: ' Flask' }), false);
});

test('equal revisions are no-ops that preserve timestamps and local fields', /** Satisfied delivery must leave the stored timestamp and catalog-only content untouched. */ async () => {
  const state = memory(document());
  const result = await destination(state).deliver(snapshot, Date.now() + 10_000);
  assert.deepEqual(result, { outcome: 'satisfied', action: 'unchanged' });
  assert.equal(state.writes, 0);
  assert.equal(state.value?.appliedAt, before);
  assert.equal(state.value?.seoTitle, 'Local SEO');
});

test('missing copies insert an explicit BSON Int32 and null local field', /** New copies must satisfy storage types while giving local-only content an explicit initial value. */ async () => {
  const state = memory(null);
  assert.equal(
    (await destination(state).deliver(snapshot, Date.now() + 10_000)).action,
    'inserted',
  );
  assert.ok(state.value?.sourceRevision instanceof Int32);
  assert.equal(state.value?.sourceRevision.valueOf(), 4);
  assert.equal(state.value?.seoTitle, null);
  assert.ok(state.value?.appliedAt instanceof Date);
  for (const options of state.options)
    assert.ok((options.timeoutMS ?? 0) > 0 && (options.timeoutMS ?? 0) <= 3000);
  assert.deepEqual(state.options[1]?.writeConcern, { w: 1 });
});

test('stale copies advance with an expected revision filter and preserve local fields', /** Only the observed stale version may advance, and its unrelated catalog fields must survive. */ async () => {
  const state = memory(document(3));
  assert.equal((await destination(state).deliver(snapshot, Date.now() + 10_000)).action, 'updated');
  assert.equal(state.filters[0]?._id, snapshot.productId);
  assert.deepEqual(state.filters[0]?.sourceRevision, new Int32(3));
  assert.equal(state.value?.seoTitle, 'Local SEO');
  assert.equal(state.value?.localFlag, true);
});

test('newer valid copies supersede the delivery without writes', /** A newer destination fulfills the older delivery obligation without being downgraded. */ async () => {
  const state = memory(document(5));
  assert.equal(
    (await destination(state).deliver(snapshot, Date.now() + 10_000)).action,
    'superseded',
  );
  assert.equal(state.writes, 0);
});

test('equal revision disagreements become conflicts without overwriting evidence', /** An equal revision cannot authorize overwriting different approved content. */ async () => {
  const state = memory({ ...document(), name: 'Conflicting name' });
  assert.equal(
    (await destination(state).deliver(snapshot, Date.now() + 10_000)).outcome,
    'conflict',
  );
  assert.equal(state.writes, 0);
});

test('duplicate insert races are reread and newer winners are preserved', /** A duplicate-key reply is evidence of a race; the next read decides whether delivery is satisfied. */ async () => {
  const state = memory(null);
  state.race = document(5);
  assert.equal(
    (await destination(state).deliver(snapshot, Date.now() + 10_000)).action,
    'superseded',
  );
  assert.equal(state.reads, 2);
  assert.equal(state.writes, 1);
});

test('lost expected revision updates reread the winning record', /** A zero-match guarded update must observe the competing winner before choosing its outcome. */ async () => {
  const state = memory(document(2));
  state.race = document(5);
  assert.equal(
    (await destination(state).deliver(snapshot, Date.now() + 10_000)).action,
    'superseded',
  );
  assert.equal(state.reads, 2);
});

test('continued contention stops after three rounds', /** Repeated zero-match updates consume the bounded retry rounds without claiming success. */ async () => {
  const state = memory(document(2));
  state.race = document(2);
  assert.equal(
    (await destination(state).deliver(snapshot, Date.now() + 10_000)).action,
    'contention',
  );
  assert.equal(state.reads, 3);
  assert.equal(state.writes, 3);
});

test('failed reads stay unavailable and never cause inserts', /** A transport failure provides no evidence of absence and must not trigger reconstruction. */ async () => {
  const state = memory(null);
  state.failRead = new Error('mongodb://secret:password@host');
  const result = await destination(state).deliver(snapshot, Date.now() + 10_000);
  assert.equal(result.outcome, 'retry');
  assert.equal(result.action, 'unavailable');
  assert.equal(state.writes, 0);
  assert.equal(JSON.stringify(result).includes('password'), false);
});

test('write acknowledgement loss is retryable uncertainty', /** An unconfirmed write cannot establish failure or success at the destination. */ async () => {
  const state = memory(null);
  state.failWrite = new Error('acknowledgement lost');
  assert.equal((await destination(state).deliver(snapshot, Date.now() + 10_000)).outcome, 'retry');
});

test('stored numeric revision and invalid dates are rejected before delivery', /** Malformed stored evidence requires attention before any repair can overwrite it. */ async () => {
  for (const invalid of [
    { ...document(), sourceRevision: 4 },
    { ...document(), appliedAt: new Date('invalid') },
    { ...document(), name: '' },
    { ...document(), description: 'x'.repeat(4001) },
    { ...document(), category: null },
    { ...document(), seoTitle: true },
    { ...document(), _id: `${snapshot.productId}\n` },
  ]) {
    const state = memory(invalid);
    assert.equal(
      (await destination(state).deliver(snapshot, Date.now() + 10_000)).outcome,
      'failed',
    );
    assert.equal(state.writes, 0);
  }
});

test('stored strings use Unicode character limits, matching database validators', /** Character limits count Unicode code points, and BSON types remain available for validation. */ async () => {
  const state = memory({ ...document(), name: '😀'.repeat(200) });
  const result = await destination(state).read(snapshot.productId, Date.now() + 10_000);
  assert.equal(result.state, 'present');
  assert.equal(state.options[0]?.promoteValues, false);
});

test('known MongoDB validation failures require attention with sanitized diagnostics', /** A server validation rejection is permanent evidence; raw document details stay out of diagnostics. */ async () => {
  const state = memory(null);
  state.failWrite = Object.assign(new Error('private document validation details'), { code: 121 });
  const result = await destination(state).deliver(snapshot, Date.now() + 10_000);
  assert.equal(result.outcome, 'failed');
  assert.equal(JSON.stringify(result).includes('private document'), false);
});

test('exhausted request budget does not begin a database operation', /** Expired work must stop at the adapter boundary before either observing or mutating storage. */ async () => {
  const state = memory(null);
  assert.equal((await destination(state).deliver(snapshot, Date.now() - 1)).outcome, 'retry');
  assert.equal(state.reads, 0);
  assert.equal(state.writes, 0);
});

test('MongoDB selection and operation timeouts use the remaining request budget', /** A short request budget must cap driver work below the usual per-operation allowance. */ async () => {
  const state = memory(document());
  assert.equal(
    (await destination(state).read(snapshot.productId, Date.now() + 200)).state,
    'present',
  );
  assert.ok((state.options[0]?.timeoutMS ?? 0) > 0);
  assert.ok((state.options[0]?.timeoutMS ?? 0) <= 200);
});

/**
 * Stub SQL acquisition and statements while retaining production adapter decisions.
 * Reads always return the supplied observation; writes are captured and acknowledged,
 * so this helper does not model SQL execution or concurrent changes to stored rows.
 * @param value - Stored row fixture, or null for a successful missing read.
 * @returns The adapter, captured writes, and caller-owned pool to close after assertions.
 */
function sqlDestination(value: Record<string, unknown> | null): {
  adapter: ProductDestination;
  writes: QueryConfig[];
  pool: Pool;
} {
  const pool = new Pool();
  const client = Object.assign(new Client(), {
    /** Release this in-memory client without network work. */ release(): void {},
  });
  const writes: QueryConfig[] = [];
  mock.method(
    pool,
    'connect',
    /** Acquire the controlled client without a network connection. */ async () => client,
  );
  mock.method(
    client,
    'query',
    /** Return controlled observations and retain writes for concurrency-predicate assertions. */ async (
      input: string | QueryConfig,
      values?: unknown[],
    ) => {
      const statement = typeof input === 'string' ? { text: input, values: values ?? [] } : input;
      if (statement.text.trimStart().startsWith('SELECT')) {
        return {
          rows: value ? [value] : [],
          rowCount: value ? 1 : 0,
          command: 'SELECT',
          oid: 0,
          fields: [],
        };
      }
      writes.push(statement);
      return {
        rows: [{ source_revision: snapshot.revision }],
        rowCount: 1,
        command: 'UPDATE',
        oid: 0,
        fields: [],
      };
    },
  );
  return { adapter: createSqlDestination(pool), writes, pool };
}

/**
 * Build a valid PostgreSQL product row using driver-facing column names and values.
 * @param revision - Stored source revision; defaults to the submitted revision of four.
 * @returns A fresh row with the fixed timestamp and local-only internal note.
 */
function sqlRow(revision = 4): Record<string, unknown> {
  return {
    product_id: snapshot.productId,
    ...snapshot.product,
    source_revision: revision,
    applied_at: before,
    internal_note: 'Sales only',
  };
}

test('SQL unchanged products cause no writes', /** A satisfied SQL copy must bypass mutation statements entirely. */ async () => {
  const { adapter, writes, pool } = sqlDestination(sqlRow());
  assert.equal((await adapter.deliver(snapshot, Date.now() + 10_000)).action, 'unchanged');
  assert.equal(writes.length, 0);
  await pool.end();
});

test('SQL advancement uses observed revision and preserves the local column', /** The SQL predicate must fence stale observations while the assignment excludes local-only data. */ async () => {
  const { adapter, writes, pool } = sqlDestination(sqlRow(3));
  assert.equal((await adapter.deliver(snapshot, Date.now() + 10_000)).action, 'updated');
  assert.match(writes[0]?.text ?? '', /source_revision\s*=\s*\$\d+/i);
  assert.ok(writes[0]?.values?.includes(3));
  assert.equal(writes[0]?.text.includes('internal_note'), false);
  await pool.end();
});

test('SQL missing insertion uses conflict-aware insert rather than an unconditional overwrite', /** Insertion must let a concurrent owner keep its row instead of replacing that evidence. */ async () => {
  const { adapter, writes, pool } = sqlDestination(null);
  assert.equal((await adapter.deliver(snapshot, Date.now() + 10_000)).action, 'inserted');
  assert.match(writes[0]?.text ?? '', /ON CONFLICT\s*\(product_id\)\s*DO NOTHING/i);
  await pool.end();
});
