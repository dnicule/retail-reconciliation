/**
 * @file Exercises bounded PostgreSQL acquisition, cleanup, and systemic classification.
 * Native driver methods are replaced with controlled promises; no server is contacted.
 */
import assert from 'node:assert/strict';
import test, { afterEach, mock } from 'node:test';
import { Client, Pool } from 'pg';
import { query, withClient } from '../../src/postgres.js';
import { SystemicError } from '../../src/model/diagnostic.js';

afterEach(/** Restores driver methods between isolated deadline cases. */ () => mock.restoreAll());

/** Creates a controlled borrowed client and records how it is released. */
function fixture(): {
  pool: Pool;
  client: Client & { release(destroy?: boolean): void };
  released: boolean[];
} {
  const released: boolean[] = [];
  const client = Object.assign(new Client(), {
    /** Records normal release versus disposal. */ release(destroy = false): void {
      released.push(destroy);
    },
  });
  const pool = new Pool();
  mock.method(
    pool,
    'connect',
    /** Hands out the owned test client without networking. */ async () => client,
  );
  mock.method(
    client,
    'query',
    /** Acknowledges harmless statements without a server. */ async () => ({
      rows: [],
      rowCount: 0,
      command: 'SELECT',
      oid: 0,
      fields: [],
    }),
  );
  return { pool, client, released };
}

test('one transaction uses one connection and releases after success', /** All statements must share the exclusively borrowed connection. */ async () => {
  const { pool, client, released } = fixture();
  const statements: string[] = [];
  mock.method(
    client,
    'query',
    /** Records transaction statement order. */ async (text: string) => {
      statements.push(text);
      return { rows: [], rowCount: 0, command: text, oid: 0, fields: [] };
    },
  );
  await withClient(
    pool,
    Date.now() + 1000,
    /** Completes the transaction before returning its client. */ async (connection) => {
      await connection.query('BEGIN');
      await connection.query('COMMIT');
    },
  );
  assert.deepEqual(statements, ['BEGIN', 'COMMIT']);
  assert.deepEqual(released, [false]);
  await pool.end();
});

test('expired query budgets never acquire a connection', /** Local deadline exhaustion must stop before borrowing scarce pool capacity. */ async () => {
  const { pool, released } = fixture();
  const acquire = mock.method(
    pool,
    'connect',
    /** Fails if a forbidden acquisition reaches the pool. */ async () => {
      throw new Error('unexpected acquisition');
    },
  );
  await assert.rejects(query(pool, 'SELECT 1', [], Date.now() - 1), /deadline/i);
  assert.equal(acquire.mock.callCount(), 0);
  assert.deepEqual(released, []);
  await pool.end();
});

test('late acquisition releases the abandoned connection', /** An expired pool wait must not leak a client that arrives after rejection. */ async () => {
  const { pool, client, released } = fixture();
  let resolveConnection: ((value: typeof client) => void) | undefined;
  mock.method(
    pool,
    'connect',
    /** Holds acquisition until the local waiter expires. */ () =>
      new Promise<typeof client>(
        /** Exposes the controlled connection barrier. */ (resolve) => {
          resolveConnection = resolve;
        },
      ),
  );
  await assert.rejects(query(pool, 'SELECT 1', [], Date.now() + 15), /deadline/i);
  assert.ok(resolveConnection);
  resolveConnection(client);
  await new Promise<void>(
    /** Allows the late-client cleanup microtask to run. */ (resolve) => setImmediate(resolve),
  );
  assert.deepEqual(released, [false]);
  await pool.end();
});

test('an uncertain statement discards its connection', /** A timeout cannot safely return a possibly active protocol exchange to the pool. */ async () => {
  const { pool, client, released } = fixture();
  mock.method(
    client,
    'query',
    /** Leaves an acknowledgement unresolved to model transport loss. */ () =>
      new Promise<never>(/** Keeps the server outcome unknown. */ () => {}),
  );
  await assert.rejects(
    query(pool, 'UPDATE products SET name=$1', ['x'], Date.now() + 15),
    /deadline/i,
  );
  assert.deepEqual(released, [true]);
  await pool.end();
});

test('authentication and incompatible schema errors are systemic and sanitized', /** These failures must stop claims instead of consuming every job retry. */ async () => {
  for (const code of ['28P01', '42501', '42P01', '42703', '42804']) {
    const { pool, client, released } = fixture();
    mock.method(
      client,
      'query',
      /** Supplies a recognized systemic server code with a private message. */ async () => {
        throw Object.assign(new Error('postgres://private-secret'), { code });
      },
    );
    await assert.rejects(
      query(pool, 'SELECT 1', [], Date.now() + 1000),
      /** Confirms classification without leaking the driver message. */ (error: unknown) =>
        error instanceof SystemicError && !error.message.includes('private-secret'),
    );
    assert.deepEqual(released, [true]);
    await pool.end();
  }
});
