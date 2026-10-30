/**
 * @file Tests MongoDB deadlines across lazy connection setup and command execution.
 * Real driver handles use mocked I/O, allowing abort behavior to be checked without sockets.
 * Each case closes its clients and restores driver entry points through afterEach.
 */
import assert from 'node:assert/strict';
import test, { afterEach, mock } from 'node:test';
import { Int32, MongoClient } from 'mongodb';
import type {
  Document,
  Filter,
  FindOptions,
  InsertOneOptions,
  RunCommandOptions,
  UpdateFilter,
  UpdateOptions,
} from 'mongodb';
import { createMongoDestination } from '../../src/products.js';
import type { Snapshot } from '../../src/model.js';

const clients: MongoClient[] = [];
const snapshot: Snapshot = {
  productId: 'DEADLINE-UNIT',
  revision: 2,
  product: { name: 'Bottle', description: '', category: 'Home' },
};

afterEach(
  /** Closes every test-owned client and restores mocked driver entry points. */ async () => {
    for (const client of clients.splice(0)) await client.close();
    mock.restoreAll();
  },
);

/**
 * Create a lazy driver client and register it for afterEach cleanup.
 * Tests replace native I/O before using adapters, so construction does not contact MongoDB.
 * @returns A test-owned client with standalone-compatible write retry settings.
 */
function client(): MongoClient {
  const value = new MongoClient('mongodb://127.0.0.1:27017', { retryWrites: false });
  clients.push(value);
  return value;
}

test('every MongoDB read, insert, update, and readiness command receives an abort deadline', /** Checks deadline propagation through all product operations. */ async () => {
  const mongo = client();
  const db = mongo.db('unit_deadlines');
  const products = db.collection('products');
  const options: {
    method: string;
    signal?: AbortSignal | undefined;
    timeoutMS?: number | undefined;
  }[] = [];
  let row: Document | null = null;
  mock.method(
    mongo,
    'db',
    /** Returns the controlled database without connection work. */ () => db,
  );
  mock.method(db, 'collection', /** Returns the isolated collection handle. */ () => products);
  mock.method(
    products,
    'findOne',
    /** Captures read budgets and returns the current test record. */ async (
      _filter: Filter<Document>,
      config: FindOptions,
    ) => {
      options.push({ method: 'findOne', ...config });
      return row;
    },
  );
  mock.method(
    products,
    'insertOne',
    /** Captures insertion budgets without changing a database. */ async (
      _row: Document,
      config: InsertOneOptions,
    ) => {
      options.push({ method: 'insertOne', ...config });
      return { acknowledged: true, insertedId: snapshot.productId };
    },
  );
  mock.method(
    products,
    'updateOne',
    /** Captures guarded update budgets without changing a database. */ async (
      _filter: Filter<Document>,
      _update: UpdateFilter<Document>,
      config: UpdateOptions,
    ) => {
      options.push({ method: 'updateOne', ...config });
      return {
        acknowledged: true,
        matchedCount: 1,
        modifiedCount: 1,
        upsertedId: null,
        upsertedCount: 0,
      };
    },
  );
  mock.method(
    db,
    'command',
    /** Captures readiness budgets without opening a connection. */ async (
      _command: Document,
      config: RunCommandOptions,
    ) => {
      options.push({ method: 'command', ...config });
      return { ok: 1 };
    },
  );
  const destination = createMongoDestination(mongo, 'unit_deadlines');
  await destination.read(snapshot.productId, Date.now() + 10_000);
  await destination.deliver(snapshot, Date.now() + 10_000);
  row = {
    _id: snapshot.productId,
    ...snapshot.product,
    sourceRevision: new Int32(1),
    appliedAt: new Date(),
    seoTitle: null,
  };
  await destination.deliver(snapshot, Date.now() + 10_000);
  await destination.ready(Date.now() + 10_000);
  assert.deepEqual(
    new Set(
      options.map(
        /** Extracts which native operation received a deadline. */ (option) => option.method,
      ),
    ),
    new Set(['findOne', 'insertOne', 'updateOne', 'command']),
  );
  for (const option of options) {
    assert.ok(
      option.signal instanceof AbortSignal,
      `${option.method} requires an abort signal before lazy connection`,
    );
    assert.ok((option.timeoutMS ?? 0) > 0 && (option.timeoutMS ?? 0) <= 3000);
  }
  assert.equal(
    new Set(options.map(/** Extracts per-operation abort identities. */ (option) => option.signal))
      .size,
    options.length,
  );
});

test('a short deadline aborts lazy connection before the command timeout context exists', /** Exercises the actual driver autoConnect boundary using a connection that never completes. */ async () => {
  const mongo = client();
  let connects = 0;
  mock.method(
    mongo,
    'connect',
    /** Holds lazy connection setup without creating sockets or a topology. */ () => {
      connects += 1;
      return new Promise<MongoClient>(
        /** Intentionally leaves initial connection incomplete until abortion. */ () => {},
      );
    },
  );
  const destination = createMongoDestination(mongo, 'unit_deadlines');
  let guard: NodeJS.Timeout | undefined;
  const timedOut = new Promise<'deadline missed'>(
    /** Bounds this regression test if the adapter omits its abort signal. */ (resolve) => {
      guard = setTimeout(
        /** Reports that lazy connection escaped the operation budget. */ () =>
          resolve('deadline missed'),
        300,
      );
    },
  );
  try {
    const result = await Promise.race([destination.ready(Date.now() + 25), timedOut]);
    assert.notEqual(result, 'deadline missed');
    assert.deepEqual(typeof result === 'object' ? result.available : null, false);
    assert.equal(connects, 1);
  } finally {
    clearTimeout(guard);
  }
});

test('expired deadlines prevent reads, delivery, and probes from attempting lazy connection', /** Confirms exhausted work does not reach the native connection boundary. */ async () => {
  const mongo = client();
  let connects = 0;
  mock.method(
    mongo,
    'connect',
    /** Records any forbidden connection start after deadline exhaustion. */ async () => {
      connects += 1;
      throw new Error('Connection should not have started');
    },
  );
  const destination = createMongoDestination(mongo, 'unit_deadlines');
  assert.equal((await destination.read(snapshot.productId, Date.now() - 1)).state, 'unavailable');
  assert.equal((await destination.deliver(snapshot, Date.now() - 1)).outcome, 'retry');
  assert.equal((await destination.ready(Date.now() - 1)).available, false);
  assert.equal(connects, 0);
});
