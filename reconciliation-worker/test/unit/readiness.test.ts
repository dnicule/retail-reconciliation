/**
 * @file Verifies startup probes reject incompatible schema using read-only driver calls.
 * Controlled catalog replies exercise refusal paths without provisioning any database.
 */
import assert from 'node:assert/strict';
import test, { afterEach, mock } from 'node:test';
import { MongoClient } from 'mongodb';
import { Client, Pool } from 'pg';
import { assertMongoReady, assertPostgresReady } from '../../src/readiness.js';
import { mongoValidator } from '../../src/resources/mongodb/readiness.js';
import { SystemicError } from '../../src/model/diagnostic.js';

afterEach(/** Isolates native driver substitutions across probe cases. */ () => mock.restoreAll());

test('PostgreSQL readiness only reads catalogs and rejects incomplete structures', /** Failed structural evidence is systemic rather than a retry of every product. */ async () => {
  const pool = new Pool();
  const statements: string[] = [];
  const client = Object.assign(new Client(), {
    /** Releases the local controlled handle. */ release(): void {},
  });
  mock.method(pool, 'connect', /** Acquires the controlled catalog session. */ async () => client);
  mock.method(
    client,
    'query',
    /** Records the query and rejects its structural predicate. */ async (sql: string) => {
      statements.push(sql);
      return { rows: [{ ready: false }], rowCount: 1, fields: [], oid: 0, command: 'SELECT' };
    },
  );
  await assert.rejects(assertPostgresReady(pool, Date.now() + 1000), SystemicError);
  assert.equal(statements.length, 1);
  assert.match(statements[0] ?? '', /^WITH/);
  assert.doesNotMatch(
    statements[0] ?? '',
    /\b(?:CREATE|DROP|INSERT|UPDATE|DELETE)\s+(?:TABLE|INTO|public\.)/i,
  );
  await pool.end();
});

test('PostgreSQL readiness accepts complete read-only catalog evidence', /** Successful structural verification needs no temporary object or sample write. */ async () => {
  const pool = new Pool();
  const client = Object.assign(new Client(), {
    /** Releases the local controlled handle. */ release(): void {},
  });
  mock.method(pool, 'connect', /** Acquires the controlled catalog session. */ async () => client);
  mock.method(
    client,
    'query',
    /** Supplies confirmed compatible metadata. */ async () => ({
      rows: [{ ready: true }],
      rowCount: 1,
      fields: [],
      oid: 0,
      command: 'SELECT',
    }),
  );
  await assertPostgresReady(pool, Date.now() + 1000);
  await pool.end();
});

test('Mongo readiness checks the validator and built-in index without writes', /** Collection drift must stop claiming work even when ping succeeds. */ async () => {
  const client = new MongoClient('mongodb://127.0.0.1:27017');
  const database = client.db('unit_worker');
  mock.method(
    client,
    'db',
    /** Keeps inspection on the controlled database handle. */ () => database,
  );
  const commands: Record<string, unknown>[] = [];
  let compatible = true;
  mock.method(
    database,
    'command',
    /** Returns read-only collection/index metadata for the probe. */ async (
      command: Record<string, unknown>,
    ) => {
      commands.push(command);
      if ('listCollections' in command)
        return {
          cursor: {
            id: 0,
            firstBatch: [
              {
                name: 'products',
                type: 'collection',
                options: {
                  validator: compatible ? mongoValidator : {},
                  validationLevel: 'strict',
                  validationAction: 'error',
                },
              },
            ],
          },
        };
      if ('listIndexes' in command)
        return { cursor: { id: 0, firstBatch: [{ name: '_id_', key: { _id: 1 }, v: 2 }] } };
      if ('connectionStatus' in command)
        return { authInfo: { authenticatedUsers: [], authenticatedUserPrivileges: [] } };
      return { ok: 1 };
    },
  );
  await assertMongoReady(client, 'unit_worker', Date.now() + 1000);
  assert.ok(
    commands.some(
      /** Confirms validator metadata is requested. */ (command) => 'listCollections' in command,
    ),
  );
  assert.ok(
    commands.some(
      /** Confirms index metadata is requested. */ (command) => 'listIndexes' in command,
    ),
  );
  assert.ok(
    commands.every(
      /** Rejects any mutation command in startup inspection. */ (command) =>
        ['ping', 'listCollections', 'listIndexes', 'connectionStatus'].some(
          /** Matches the documented read-only command set. */ (name) => name in command,
        ),
    ),
  );
  compatible = false;
  await assert.rejects(assertMongoReady(client, 'unit_worker', Date.now() + 1000), SystemicError);
  await client.close();
});
